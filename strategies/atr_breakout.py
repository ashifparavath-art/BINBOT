"""BINBOT2 — ATR volatility-breakout strategy.

On every newly closed candle (5m by default) for every USDT-M perpetual:

- Baseline, computed from the candles BEFORE the trigger candle so the
  breakout can't inflate its own yardstick:
    * mean  = average close of the previous `mean_period` candles
    * ATR   = average true range of the previous `atr_period` candles
- **LONG**  when close >= mean + k * ATR
- **SHORT** when close <= mean - k * ATR

…and only if ALL of these filters pass:

1. Fresh breakout — the previous candle was NOT already beyond the band
   (so we don't keep chasing a move that's already extended).
2. Liquidity floor — the trigger candle traded at least
   `min_candle_quote_volume_usdt` USDT (absolute, not relative).
3. Volume spike — trigger candle volume >= `min_volume_multiple` x the
   average volume of the previous `mean_period` candles.
4. Wick filter — a long must close in the top part of the candle's range,
   a short in the bottom part (`min_close_position`).
5. Symbol cooldown — no new signal on a symbol within
   `symbol_cooldown_min` of its last one.

Because the threshold is in ATRs, it scales to each symbol's own
volatility — a 5% move on a sleepy large-cap can qualify while the same
5% on a memecoin that does that every hour won't.

Exits are handled by the shared PositionMonitor (stop-loss / take-profit in
USDT), exactly like the funding-momentum scanner.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from config.logging_config import get_logger
from core.enums import OrderSide, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import BarEvent, SignalEvent
from data.kline_poller import Candle, KlinePoller
from execution.position_monitor import PositionMonitor
from strategies.base_strategy import BaseStrategy
from strategies.funding_momentum_scanner import compute_lots

logger = get_logger(__name__)

_PENDING_ENTRY_TTL_S = 10.0


@dataclass(frozen=True, slots=True)
class ATRParams:
    atr_period: int = 14
    mean_period: int = 20
    atr_multiplier: float = 2.0
    min_candle_quote_volume_usdt: float = 100_000.0
    min_volume_multiple: float = 2.0
    min_close_position: float = 0.7
    allow_long: bool = True
    allow_short: bool = True


@dataclass(frozen=True, slots=True)
class ATRSignal:
    side: OrderSide
    close: float
    mean: float
    atr: float
    deviation_atr: float  # how many ATRs from the mean the close is
    quote_volume: float
    volume_multiple: float
    close_position: float  # 0 = closed at the low, 1 = closed at the high


def average_true_range(candles: Sequence[Candle]) -> float:
    """Simple average of true range over `candles[1:]` (each TR needs the
    previous candle's close). Pass atr_period + 1 candles."""
    trs = []
    for prev, cur in zip(candles, candles[1:]):
        trs.append(max(cur.high - cur.low, abs(cur.high - prev.close), abs(cur.low - prev.close)))
    return sum(trs) / len(trs) if trs else 0.0


def required_candles(params: ATRParams) -> int:
    # trigger candle + previous candle (fresh-breakout check needs its own
    # baseline one step earlier) + the longer of the two lookbacks + 1 for TR.
    return max(params.atr_period + 1, params.mean_period) + 2


def _baseline(candles: Sequence[Candle], params: ATRParams) -> tuple[float, float, float] | None:
    """(mean_close, atr, avg_quote_volume) from candles strictly before the
    last element of `candles`."""
    prior = candles[:-1]
    if len(prior) < max(params.atr_period + 1, params.mean_period):
        return None
    mean_window = prior[-params.mean_period :]
    mean = sum(c.close for c in mean_window) / len(mean_window)
    avg_qv = sum(c.quote_volume for c in mean_window) / len(mean_window)
    atr = average_true_range(prior[-(params.atr_period + 1) :])
    return mean, atr, avg_qv


def evaluate_atr_breakout(candles: Sequence[Candle], params: ATRParams) -> ATRSignal | None:
    """Pure decision function — no I/O — so it can be unit-tested and
    reused unchanged by a backtester. `candles` must be closed candles in
    time order; the LAST one is the trigger candle."""
    if len(candles) < required_candles(params):
        return None

    trigger = candles[-1]
    base = _baseline(candles, params)
    if base is None:
        return None
    mean, atr, avg_qv = base
    if atr <= 0 or mean <= 0:
        return None

    deviation_atr = (trigger.close - mean) / atr
    if deviation_atr >= params.atr_multiplier and params.allow_long:
        side = OrderSide.BUY
    elif deviation_atr <= -params.atr_multiplier and params.allow_short:
        side = OrderSide.SELL
    else:
        return None

    # 1. Fresh breakout: previous candle must not already have been beyond
    #    the band, measured against ITS own baseline.
    prev_base = _baseline(candles[:-1], params)
    if prev_base is not None:
        p_mean, p_atr, _ = prev_base
        if p_atr > 0:
            prev_dev = (candles[-2].close - p_mean) / p_atr
            if side is OrderSide.BUY and prev_dev >= params.atr_multiplier:
                return None
            if side is OrderSide.SELL and prev_dev <= -params.atr_multiplier:
                return None

    # 2. Absolute liquidity floor.
    if trigger.quote_volume < params.min_candle_quote_volume_usdt:
        return None

    # 3. Volume spike vs. recent average.
    volume_multiple = trigger.quote_volume / avg_qv if avg_qv > 0 else float("inf")
    if volume_multiple < params.min_volume_multiple:
        return None

    # 4. Wick filter.
    rng = trigger.high - trigger.low
    close_position = (trigger.close - trigger.low) / rng if rng > 0 else 0.5
    if side is OrderSide.BUY and close_position < params.min_close_position:
        return None
    if side is OrderSide.SELL and close_position > 1.0 - params.min_close_position:
        return None

    return ATRSignal(
        side=side,
        close=trigger.close,
        mean=mean,
        atr=atr,
        deviation_atr=deviation_atr,
        quote_volume=trigger.quote_volume,
        volume_multiple=volume_multiple,
        close_position=close_position,
    )


class ATRBreakoutStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        event_bus: EventBus,
        symbols: list[str],
        kline_poller: KlinePoller,
        position_monitor: PositionMonitor,
        params: ATRParams,
        *,
        candle_interval_s: int,
        max_open_positions: int,
        order_notional_usdt: float,
        symbol_cooldown_s: float,
        step_size_lookup: Callable[[str], float],
        price_lookup: Callable[[str], float | None] | None = None,
    ) -> None:
        super().__init__(strategy_id, symbols, event_bus)
        self._poller = kline_poller
        self._position_monitor = position_monitor
        self._params = params
        self._candle_interval_s = candle_interval_s
        self._max_open_positions = max_open_positions
        self._order_notional_usdt = order_notional_usdt
        self._symbol_cooldown_s = symbol_cooldown_s
        self._step_size_lookup = step_size_lookup
        self._price_lookup = price_lookup
        self._pending_entries: dict[str, float] = {}
        self._last_signal_at: dict[str, float] = {}

    async def on_bar(self, bar: BarEvent) -> None:
        # The bus also carries the 1-minute bars built from mark-price ticks;
        # this strategy only acts on its own candle interval.
        if bar.timeframe_s != self._candle_interval_s:
            return
        symbol = bar.symbol
        self._prune_pending_entries()

        open_symbols = self._position_monitor.open_symbols() | self._pending_entries.keys()
        if symbol in open_symbols or len(open_symbols) >= self._max_open_positions:
            return
        now = time.monotonic()
        last = self._last_signal_at.get(symbol)
        if last is not None and (now - last) < self._symbol_cooldown_s:
            return

        result = evaluate_atr_breakout(self._poller.candles(symbol), self._params)
        if result is None:
            return

        price = (self._price_lookup(symbol) if self._price_lookup else None) or result.close
        lots = compute_lots(
            notional_usdt=self._order_notional_usdt, price=price, step_size=self._step_size_lookup(symbol)
        )
        if lots <= 0:
            return

        logger.info(
            "binbot2.setup_found",
            symbol=symbol,
            side=result.side.value,
            deviation_atr=round(result.deviation_atr, 2),
            atr=result.atr,
            mean=result.mean,
            close=result.close,
            quote_volume=round(result.quote_volume),
            volume_multiple=round(result.volume_multiple, 2),
            close_position=round(result.close_position, 2),
            lots=lots,
        )
        self._pending_entries[symbol] = now
        self._last_signal_at[symbol] = now

        await self.emit_signal(
            SignalEvent(
                ts=datetime.now(timezone.utc),
                symbol=symbol,
                strategy_id=self.strategy_id,
                side=result.side,
                target_quantity=lots,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
                metadata={
                    "deviation_atr": round(result.deviation_atr, 3),
                    "atr": result.atr,
                    "mean": result.mean,
                    "quote_volume": round(result.quote_volume, 2),
                    "volume_multiple": round(result.volume_multiple, 3),
                    "close_position": round(result.close_position, 3),
                },
            )
        )

    def _prune_pending_entries(self) -> None:
        now = time.monotonic()
        open_now = self._position_monitor.open_symbols()
        for sym in [s for s, ts in self._pending_entries.items() if s in open_now or (now - ts) > _PENDING_ENTRY_TTL_S]:
            del self._pending_entries[sym]
