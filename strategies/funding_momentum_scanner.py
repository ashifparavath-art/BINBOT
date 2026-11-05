"""Funding-momentum scanner strategy.

Scans every symbol in its universe (typically ALL USDT-M Binance Futures
perpetuals) for:

- **PUMP**: price up >= `price_jump_pct` within `price_jump_window_min`,
  AND the funding rate has trended more negative over
  `funding_trend_window_min` (a general/lenient trend check — compares the
  window's start value against its latest value, tolerating noise in
  between, rather than requiring every intermediate sample to move the
  same direction).
- **DUMP**: price down >= `price_jump_pct` within the same window, AND
  the funding rate was negative at the start of the window and has been
  rising (becoming less negative) since.

`trade_direction="follow"` (default) trades WITH the move: pump -> LONG,
dump -> SHORT. `"fade"` trades against it (pump -> SHORT, dump -> LONG).

Entry gating (all checked before a signal is emitted):
- never a second position on a symbol that's already open or pending;
- hard cap of `max_open_positions` open at once;
- if `max_young_positions` > 0: at most that many positions younger than
  `young_age_s` (older positions don't use up these slots);
- if `reentry_pause_s` > 0: no new entry on a symbol until that long after
  its previous position closed.

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
    trade_direction: str = "follow"  # "follow" (pump->LONG) or "fade" (pump->SHORT)
    max_young_positions: int = 0  # 0 = no separate limit for young positions
    young_age_s: float = 24 * 3600.0
    reentry_pause_s: float = 0.0  # 0 = may re-enter a symbol right after it closes


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
    trade_direction: str = "follow",
) -> OrderSide | None:
    """Pure decision function — no I/O — so the entry logic can be unit
    tested directly without a running event loop or live data feed."""
    if price_change_pct is None or funding_now is None or funding_window_start is None:
        return None

    fade = trade_direction == "fade"
    if price_change_pct >= price_jump_pct:
        if funding_now < funding_window_start:  # trending MORE negative
            return OrderSide.SELL if fade else OrderSide.BUY
    elif price_change_pct <= -price_jump_pct:
        if funding_window_start < 0 and funding_now > funding_window_start:  # started negative, rising since
            return OrderSide.BUY if fade else OrderSide.SELL
    return None


def entry_slots_available(
    *,
    open_ages_s: list[float],
    pending_count: int,
    max_open_positions: int,
    max_young_positions: int,
    young_age_s: float,
) -> bool:
    """Pure gating check. `open_ages_s` = age in seconds of every open
    position; pending (just-submitted, not yet confirmed) entries count as
    open AND young."""
    if len(open_ages_s) + pending_count >= max_open_positions:
        return False
    if max_young_positions > 0:
        young = sum(1 for a in open_ages_s if a < young_age_s) + pending_count
        if young >= max_young_positions:
            return False
    return True


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

        confirmed = self._position_monitor.open_symbols()
        pending = set(self._pending_entries.keys()) - confirmed
        if symbol in confirmed or symbol in pending:
            return
        now_wall = time.time()
        opened_at = getattr(self._position_monitor, "opened_at", None)
        opened = opened_at() if callable(opened_at) else {}
        ages = [now_wall - opened.get(sym, now_wall) for sym in confirmed]
        if not entry_slots_available(
            open_ages_s=ages,
            pending_count=len(pending),
            max_open_positions=self._params.max_open_positions,
            max_young_positions=self._params.max_young_positions,
            young_age_s=self._params.young_age_s,
        ):
            return
        if self._params.reentry_pause_s > 0:
            last_closed = getattr(self._position_monitor, "last_closed_at", None)
            closed_ts = last_closed(symbol) if callable(last_closed) else None
            if closed_ts is not None and now_wall - closed_ts < self._params.reentry_pause_s:
                return

        price_change_pct = self._feed.price_change_pct(symbol)
        funding = self._feed.funding_trend(symbol)
        funding_now, funding_window_start = funding if funding else (None, None)

        side = evaluate_entry(
            price_change_pct=price_change_pct,
            funding_now=funding_now,
            funding_window_start=funding_window_start,
            price_jump_pct=self._params.price_jump_pct,
            trade_direction=self._params.trade_direction,
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
