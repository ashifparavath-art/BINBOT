"""Exchange-wide market data feed for the funding-momentum scanner.

Rather than subscribing per-symbol for a small, fixed watchlist, this feed
subscribes ONCE to Binance's combined all-market mark-price stream — a
single websocket connection that pushes
mark price + funding rate for every USDT-M futures symbol in one message,
about once a second. That is what makes scanning ~300 symbols practical:
one connection instead of hundreds.

It does not implement `data.market_data_feed.MarketDataFeed` — that
interface's per-symbol subscribe/unsubscribe doesn't fit "always streaming
everything." Instead it exposes rolling-window queries
(`price_change_pct`, `funding_trend`) the scanner strategy polls directly,
and separately republishes each symbol's price as a `TickEvent` on the
shared bus so the existing `BarAggregator` -> `BarEvent` pipeline keeps
working unmodified for every symbol.

`backfill()` seeds both rolling windows from historical klines before the
live stream starts, so a restart doesn't need to sit idle re-accumulating
a window's worth of live ticks before it can evaluate anything again.
"""
from __future__ import annotations

import asyncio
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from config.logging_config import get_logger
from core.event_bus import EventBus
from core.events import TickEvent

if TYPE_CHECKING:
    from binance import AsyncClient
    from binance.ws.streams import BinanceSocketManager

logger = get_logger(__name__)


class BinanceUniverseFeed:
    def __init__(
        self,
        socket_manager: "BinanceSocketManager",
        event_bus: EventBus,
        *,
        price_window_min: float = 15.0,
        funding_window_min: float = 30.0,
        reconnect_backoff_s: float = 2.0,
        reconnect_backoff_max_s: float = 60.0,
        rest_poll_interval_s: float = 5.0,
    ) -> None:
        self._bsm = socket_manager
        self._event_bus = event_bus
        self._price_window = timedelta(minutes=price_window_min)
        self._funding_window = timedelta(minutes=funding_window_min)
        self._reconnect_backoff_s = reconnect_backoff_s
        self._reconnect_backoff_max_s = reconnect_backoff_max_s

        self._price_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._funding_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._latest_price: dict[str, float] = {}
        self._connected = False
        self._task: asyncio.Task[None] | None = None
        self._message_count = 0
        self._rest_poll_interval_s = rest_poll_interval_s
        self._client: "AsyncClient | None" = None
        self._rest_poll_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._connected = True
        self._task = asyncio.create_task(self._stream(), name="binance-universe-feed")
        if self._client is not None:
            self._rest_poll_task = asyncio.create_task(
                self._rest_poll_loop(), name="binance-universe-feed-rest-fallback"
            )
        logger.info("universe_feed.started")

    async def stop(self) -> None:
        self._connected = False
        if self._task is not None:
            self._task.cancel()
        if self._rest_poll_task is not None:
            self._rest_poll_task.cancel()

    async def backfill(self, client: "AsyncClient", symbols: list[str], *, concurrency: int = 15) -> None:
        """Seed price/funding history from historical klines, covering the
        same span as the live rolling windows, before `start()` begins
        streaming. Must be called before `start()` — it appends points in
        chronological order and the rolling-window trim logic assumes
        history stays ordered oldest-to-newest.

        Funding rate has no historical series of its own on Binance (only
        realized 8-hourly settlements, not the continuously-updating
        predicted rate `funding_trend()` actually tracks) — premium index
        is the basis that predicted rate is computed from, so its kline
        history is the closest available stand-in.
        """
        self._client = client
        price_limit = max(2, math.ceil(self._price_window.total_seconds() / 60) + 1)
        funding_limit = max(2, math.ceil(self._funding_window.total_seconds() / 60) + 1)
        sem = asyncio.Semaphore(concurrency)
        filled = 0
        failed = 0

        async def backfill_one(symbol: str) -> None:
            nonlocal filled, failed
            async with sem:
                try:
                    price_klines, funding_klines = await asyncio.gather(
                        client.futures_mark_price_klines(symbol=symbol, interval="1m", limit=price_limit),
                        client.futures_premium_index_klines(symbol=symbol, interval="1m", limit=funding_limit),
                    )
                except Exception as exc:
                    failed += 1
                    logger.debug("universe_feed.backfill_symbol_failed", symbol=symbol, error=str(exc))
                    return
                for row in price_klines:
                    ts = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc)
                    self._append_trimmed(self._price_history[symbol], ts, float(row[4]), self._price_window)
                if price_klines:
                    self._latest_price[symbol] = float(price_klines[-1][4])
                for row in funding_klines:
                    ts = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc)
                    self._append_trimmed(self._funding_history[symbol], ts, float(row[4]), self._funding_window)
                filled += 1

        logger.info("universe_feed.backfill_starting", symbol_count=len(symbols))
        await asyncio.gather(*(backfill_one(s) for s in symbols))
        logger.info("universe_feed.backfill_complete", filled=filled, failed=failed)

    def latest_price(self, symbol: str) -> float | None:
        return self._latest_price.get(symbol)

    def price_change_pct(self, symbol: str) -> float | None:
        """% price change from the start of the rolling window to now.
        Returns None until we've actually accumulated a full window of
        history for this symbol — never compute this off partial data."""
        history = self._price_history.get(symbol)
        if not history or len(history) < 2:
            return None
        oldest_ts, oldest_price = history[0]
        if (history[-1][0] - oldest_ts) < self._price_window * 0.9 or oldest_price == 0:
            return None
        latest_price = history[-1][1]
        return (latest_price - oldest_price) / oldest_price * 100.0

    def funding_trend(self, symbol: str) -> tuple[float, float] | None:
        """Returns (funding_rate_now, funding_rate_at_window_start), or
        None until a full window of history has accumulated."""
        history = self._funding_history.get(symbol)
        if not history or len(history) < 2:
            return None
        oldest_ts, oldest_rate = history[0]
        if (history[-1][0] - oldest_ts) < self._funding_window * 0.9:
            return None
        return history[-1][1], oldest_rate

    async def _stream(self) -> None:
        backoff = self._reconnect_backoff_s
        while self._connected:
            try:
                async with self._bsm.all_mark_price_socket(fast=True) as stream:
                    backoff = self._reconnect_backoff_s
                    while self._connected:
                        try:
                            msg = await asyncio.wait_for(stream.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            logger.warning(
                                "universe_feed.recv_timeout",
                                msg="no message in 30s, forcing reconnect",
                            )
                            break
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("universe_feed.stream_error", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_backoff_max_s)

    async def _handle_message(self, msg: Any) -> None:
        payload = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(payload, list):
            return

        now = datetime.now(timezone.utc)
        for item in payload:
            try:
                symbol = item["s"]
                price = float(item["p"])
                funding_rate = float(item["r"])
            except (KeyError, ValueError, TypeError):
                continue

            self._append_trimmed(self._price_history[symbol], now, price, self._price_window)
            self._append_trimmed(self._funding_history[symbol], now, funding_rate, self._funding_window)
            self._latest_price[symbol] = price

            await self._event_bus.publish(
                TickEvent(
                    ts=now,
                    symbol=symbol,
                    bid=price,
                    ask=price,
                    bid_size=0,
                    ask_size=0,
                    last=price,
                    last_size=0,
                )
            )

        self._message_count += 1
        if self._message_count % 300 == 0:  # roughly every 5 minutes at 1 msg/s
            logger.info("universe_feed.heartbeat", symbols_tracked=len(self._latest_price))

    async def _rest_poll_loop(self) -> None:
        """Fallback path for when the websocket stream isn't delivering
        data (seen on testnet: handshake succeeds but no messages ever
        arrive). Polls the mark-price REST endpoint with no `symbol` arg,
        which returns every symbol in a single request — cheap enough to
        run continuously alongside the websocket as a safety net, since
        duplicate updates are harmless to the rolling windows and
        downstream strategy.
        """
        backoff = self._reconnect_backoff_s
        while self._connected:
            try:
                rows = await self._client.futures_mark_price()
                await self._handle_rest_snapshot(rows)
                backoff = self._reconnect_backoff_s
                await asyncio.sleep(self._rest_poll_interval_s)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("universe_feed.rest_poll_error", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_backoff_max_s)

    async def _handle_rest_snapshot(self, rows: list[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc)
        for row in rows:
            try:
                symbol = row["symbol"]
                price = float(row["markPrice"])
                funding_rate = float(row["lastFundingRate"])
            except (KeyError, ValueError, TypeError):
                continue

            self._append_trimmed(self._price_history[symbol], now, price, self._price_window)
            self._append_trimmed(self._funding_history[symbol], now, funding_rate, self._funding_window)
            self._latest_price[symbol] = price

            await self._event_bus.publish(
                TickEvent(
                    ts=now,
                    symbol=symbol,
                    bid=price,
                    ask=price,
                    bid_size=0,
                    ask_size=0,
                    last=price,
                    last_size=0,
                )
            )

    @staticmethod
    def _append_trimmed(
        history: deque[tuple[datetime, float]], ts: datetime, value: float, window: timedelta
    ) -> None:
        history.append((ts, value))
        cutoff = ts - window
        while history and history[0][0] < cutoff:
            history.popleft()
