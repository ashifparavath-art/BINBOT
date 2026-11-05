"""Funding-momentum scanner strategy.

Scans every symbol in its universe (typically ALL USDT-M Binance Futures
perpetuals) for:

- **LONG**: price up >= `price_jump_pct` within `price_jump_window_min`,
  AND the funding rate has trended more negative over
  `funding_trend_window_min` (a general/lenient trend check — compares the
  window's start value against its latest value, tolerating noise in
  between, rather than requiring every intermediate sample to move the
  same direction).
- **SHORT**: price down >= `price_jump_pct` within the same window, AND
  the funding rate was negative at the start of the window and has been
  rising (becoming less negative) since.

Reads price/funding history from a `BinanceUniverseFeed` rather than
maintaining its own — that feed already keeps the rolling windows this
needs. Position-count gating (`max_open_positions`) and "don't double-enter
a symbol already held" are checked against a shared `PositionMonitor`, the
single source of truth for what's currently open, so entries and the
live exit-watcher can never disagree about state.
"""
from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from config.logging_config import get_logger
from core.enums import OrderSide, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import BarEvent, SignalEvent
from data.binance_universe_feed import BinanceUniverseFeed
from execution.position_monitor import PositionMonitor
from strategies.base_strategy import BaseStrategy

logger = get_logger(__name__)

_PENDING_ENTRY_TTL_S = 10.0


@dataclass(frozen=True, slots=True)
class ScannerParams:
    max_open_positions: int
    order_notional_usdt: float
    price_jump_pct: float


def compute_lots(*, notional_usdt: float, price: float, step_size: float) -> int:
    """Whole number of lots whose combined notional doesn't exceed
    `notional_usdt`. Floors (never rounds up) so a position never exceeds
    the intended order size."""
    if price <= 0 or step_size <= 0:
        return 0
    return math.floor(notional_usdt / (price * step_size))


def evaluate_entry(
    *,
    price_change_pct: float | None,
    funding_now: float | None,
    funding_window_start: float | None,
    price_jump_pct: float,
) -> OrderSide | None:
    """Pure decision function — no I/O — so the entry logic can be unit
    tested directly without a running event loop or live data feed."""
    if price_change_pct is None or funding_now is None or funding_window_start is None:
        return None

    if price_change_pct >= price_jump_pct:
        if funding_now < funding_window_start:  # trending MORE negative
            return OrderSide.SELL
    elif price_change_pct <= -price_jump_pct:
        if funding_window_start < 0 and funding_now > funding_window_start:  # started negative, rising since
            return OrderSide.BUY
    return None


class FundingMomentumScannerStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        event_bus: EventBus,
        symbols: list[str],
        universe_feed: BinanceUniverseFeed,
        position_monitor: PositionMonitor,
        params: ScannerParams,
        step_size_lookup: Callable[[str], float],
    ) -> None:
        super().__init__(strategy_id, symbols, event_bus)
        self._feed = universe_feed
        self._position_monitor = position_monitor
        self._params = params
        self._step_size_lookup = step_size_lookup
        # Guards against briefly overshooting max_open_positions: many
        # symbols' bars can close in the same instant (they're all bucketed
        # by wall-clock minute), but the position monitor only confirms a
        # new position on its own ~2s poll cycle. This closes that gap.
        self._pending_entries: dict[str, float] = {}

    async def on_bar(self, bar: BarEvent) -> None:
        symbol = bar.symbol
        self._prune_pending_entries()

        open_symbols = self._position_monitor.open_symbols() | self._pending_entries.keys()
        if symbol in open_symbols:
            return
        if len(open_symbols) >= self._params.max_open_positions:
            return

        price_change_pct = self._feed.price_change_pct(symbol)
        funding = self._feed.funding_trend(symbol)
        funding_now, funding_window_start = funding if funding else (None, None)

        side = evaluate_entry(
            price_change_pct=price_change_pct,
            funding_now=funding_now,
            funding_window_start=funding_window_start,
            price_jump_pct=self._params.price_jump_pct,
        )
        if side is None:
            logger.debug(
                "scanner.no_setup",
                symbol=symbol,
                price_change_pct=price_change_pct,
                funding_now=funding_now,
                funding_window_start=funding_window_start,
            )
            return

        step_size = self._step_size_lookup(symbol)
        price = self._feed.latest_price(symbol) or bar.close
        lots = compute_lots(notional_usdt=self._params.order_notional_usdt, price=price, step_size=step_size)
        if lots <= 0:
            return

        logger.info(
            "scanner.setup_found",
            symbol=symbol,
            side=side.value,
            price_change_pct=price_change_pct,
            funding_now=funding_now,
            funding_window_start=funding_window_start,
            lots=lots,
        )
        self._pending_entries[symbol] = time.monotonic()

        await self.emit_signal(
            SignalEvent(
                ts=datetime.now(timezone.utc),
                symbol=symbol,
                strategy_id=self.strategy_id,
                side=side,
                target_quantity=lots,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
                metadata={
                    "price_change_pct": round(price_change_pct, 3),
                    "funding_now": round(funding_now, 6),
                    "funding_window_start": round(funding_window_start, 6),
                },
            )
        )

    def _prune_pending_entries(self) -> None:
        now = time.monotonic()
        confirmed_or_stale = {
            sym
            for sym, ts in self._pending_entries.items()
            if sym in self._position_monitor.open_symbols() or (now - ts) > _PENDING_ENTRY_TTL_S
        }
        for sym in confirmed_or_stale:
            del self._pending_entries[sym]
