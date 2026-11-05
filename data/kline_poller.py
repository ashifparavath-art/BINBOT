"""Closed-candle feed for BINBOT2.

The existing `BinanceUniverseFeed` listens to the all-market MARK PRICE
stream, which carries price and funding but no traded volume — so the bars
built from it always have volume 0. The ATR breakout strategy needs real
OHLCV with USDT volume (for its liquidity floor and volume-spike filter),
so this feed pulls proper futures klines over REST instead.

Once per candle (a few seconds after each `interval` boundary) it fetches
the latest klines for every symbol, appends any newly CLOSED candles to an
in-memory rolling history, and publishes each one as a `BarEvent` (with
`timeframe_s` = the candle interval) on the shared event bus. Strategies
read the full history back via `candles(symbol)`.

Cost: one request per symbol per candle, weight 1 each. For ~780 symbols
on 5-minute candles that's ~156 weight/min against Binance's 2400/min.
Requests are paced (`max_requests_per_s`), and if Binance ever answers
with a rate-limit / IP-ban response (HTTP 429/418, code -1003) the poller
stops sending anything until the ban window has passed, instead of
hammering the API and extending the ban.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from config.logging_config import get_logger
from core.event_bus import EventBus
from core.events import BarEvent

if TYPE_CHECKING:
    from binance import AsyncClient

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Candle:
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float  # base asset
    quote_volume: float  # USDT


def parse_kline(row: list[Any]) -> Candle:
    return Candle(
        open_time=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
    )


def closed_rows(rows: list[list[Any]], now_ms: int) -> list[list[Any]]:
    """Drop the still-forming candle (its close time is in the future)."""
    return [r for r in rows if int(r[6]) < now_ms]


class KlinePoller:
    def __init__(
        self,
        client: "AsyncClient",
        event_bus: EventBus,
        symbols: list[str],
        *,
        interval: str = "5m",
        interval_s: int = 300,
        history_candles: int = 60,
        poll_delay_s: float = 3.0,
        concurrency: int = 10,
        max_requests_per_s: float = 40.0,
    ) -> None:
        self._client = client
        self._event_bus = event_bus
        self._symbols = list(symbols)
        self._interval = interval
        self._interval_s = interval_s
        self._history_candles = history_candles
        self._poll_delay_s = poll_delay_s
        self._sem = asyncio.Semaphore(concurrency)
        self._min_interval_s = 1.0 / max_requests_per_s if max_requests_per_s > 0 else 0.0
        self._pace_lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._banned_until = 0.0  # monotonic time; no requests before this

        self._history: dict[str, deque[Candle]] = defaultdict(lambda: deque(maxlen=history_candles))
        self._task: asyncio.Task[None] | None = None
        self._running = False

    # ---- public API -------------------------------------------------------

    def candles(self, symbol: str) -> list[Candle]:
        return list(self._history.get(symbol, ()))

    def set_symbols(self, symbols: list[str]) -> None:
        self._symbols = list(symbols)

    async def backfill(self) -> None:
        """Load `history_candles` closed candles for every symbol so the
        strategy can evaluate from the very first live candle."""
        logger.info("kline_poller.backfill_starting", symbols=len(self._symbols), candles=self._history_candles)
        started = time.monotonic()
        results = await asyncio.gather(
            *(self._fetch_and_store(s, limit=self._history_candles + 1, publish=False) for s in self._symbols)
        )
        logger.info(
            "kline_poller.backfill_complete",
            filled=sum(1 for ok in results if ok),
            failed=sum(1 for ok in results if not ok),
            seconds=round(time.monotonic() - started, 1),
        )

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="kline-poller")
        logger.info("kline_poller.started", interval=self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()

    # ---- internals --------------------------------------------------------

    def _seconds_until_next_poll(self) -> float:
        now = time.time()
        next_boundary = (now // self._interval_s + 1) * self._interval_s
        return next_boundary - now + self._poll_delay_s

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._seconds_until_next_poll())
            started = time.monotonic()
            try:
                # limit=3: the candle that just closed, the one before (in
                # case a previous cycle missed it), and the forming one.
                results = await asyncio.gather(
                    *(self._fetch_and_store(s, limit=3, publish=True) for s in self._symbols)
                )
                logger.info(
                    "kline_poller.cycle",
                    ok=sum(1 for r in results if r),
                    failed=sum(1 for r in results if not r),
                    seconds=round(time.monotonic() - started, 1),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("kline_poller.cycle_failed")

    async def _pace(self) -> None:
        async with self._pace_lock:
            now = time.monotonic()
            wait = self._next_request_at - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_request_at = max(now, self._next_request_at) + self._min_interval_s

    @staticmethod
    def is_rate_limit_error(exc: Exception) -> bool:
        status = getattr(exc, "status_code", None)
        code = getattr(exc, "code", None)
        return status in (418, 429) or code == -1003

    async def _fetch_and_store(self, symbol: str, *, limit: int, publish: bool) -> bool:
        async with self._sem:
            if time.monotonic() < self._banned_until:
                return False
            await self._pace()
            try:
                rows = await self._client.futures_klines(symbol=symbol, interval=self._interval, limit=limit)
            except Exception as exc:
                if self.is_rate_limit_error(exc):
                    # Back off hard: skip the rest of this cycle and the next
                    # couple of minutes entirely.
                    if time.monotonic() >= self._banned_until:
                        logger.error("kline_poller.rate_limited_backing_off", error=str(exc), backoff_s=120)
                    self._banned_until = time.monotonic() + 120.0
                else:
                    logger.debug("kline_poller.fetch_failed", symbol=symbol, error=str(exc))
                return False
        new_candles = self.ingest(symbol, rows, now_ms=int(time.time() * 1000))
        if publish:
            for c in new_candles:
                await self._event_bus.publish(
                    BarEvent(
                        ts=c.open_time,
                        symbol=symbol,
                        timeframe_s=self._interval_s,
                        open=c.open,
                        high=c.high,
                        low=c.low,
                        close=c.close,
                        volume=c.quote_volume,
                        vwap=(c.quote_volume / c.volume) if c.volume > 0 else c.close,
                    )
                )
        return True

    def ingest(self, symbol: str, rows: list[list[Any]], *, now_ms: int) -> list[Candle]:
        """Append closed candles newer than what we already hold. Returns the
        ones that were actually new (in time order). Pure apart from the
        history dict, so it's unit-testable without a network."""
        history = self._history[symbol]
        last_open = history[-1].open_time if history else None
        new: list[Candle] = []
        for row in closed_rows(rows, now_ms):
            candle = parse_kline(row)
            if last_open is None or candle.open_time > last_open:
                history.append(candle)
                new.append(candle)
                last_open = candle.open_time
        return new
