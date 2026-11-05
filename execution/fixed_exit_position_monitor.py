"""Position monitor used by BINBOT2.

Same as `PositionMonitor` (same broker snapshot, same native exchange-side
stop-loss backstop armed/disarmed per position, same reduce-only market
close, same close callbacks) but with binbot2's own exit rules. All
percentages are of the position's own notional (entry price x size), so
they scale with trade size — including manually opened positions.

Exit modes
----------
1. **Profit ladder** (when `ladder` is given) — no fixed take-profit:
     peak profit <  4%  -> stop at -2%          (initial stop-loss)
     peak profit >= 4%  -> stop at  0%          (breakeven)
     peak profit >= 8%  -> stop at +4%          (trailing starts)
     every further +2% of peak -> stop +1%      (10% -> 5%, 12% -> 6%, ...)
   The stop only ever moves up. Closes when current profit <= stop.
2. **Fixed 1:2** (no ladder): stop-loss and take-profit at fixed levels
   (% of notional if the *_pct value is > 0, else fixed USDT). BOTH are
   placed on Binance as soon as the position is seen (STOP_MARKET +
   TAKE_PROFIT_MARKET, closePosition), and the bot enforces them too.

The native Binance stop order FOLLOWS the ladder: it is placed at the
initial stop and moved up (breakeven, +4%, +5%, ...) as the ladder level
rises, so the exchange enforces the locked level even if the bot is down
(see execution/native_stop_manager.py). The bot also enforces the same
levels itself every few seconds.

Peak profit per position is persisted to disk, so a restart doesn't lose
an already-reached breakeven / trailing level.

Optional (used by BINBOT1 Phase 1 Option C, off by default for BINBOT2):
- `max_hold_s` > 0: close any position at market once it has been open
  that long. Open time = when this monitor first saw the position,
  persisted to `opened_path` so a restart doesn't reset the clock.
- `opened_at()` / `last_closed_at(symbol)` expose open and close times so
  the scanner can apply its young-position limit and re-entry pause.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config.logging_config import get_logger
from core.enums import OrderSide
from execution.binance_broker import PositionDetail
from execution.native_stop_manager import NativeStopManager
from execution.position_monitor import PositionMonitor

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ProfitLadder:
    """All values are % of notional."""

    breakeven_trigger_pct: float = 4.0
    breakeven_lock_pct: float = 0.0
    trail_start_pct: float = 8.0
    trail_start_lock_pct: float = 4.0
    trail_step_pct: float = 2.0
    trail_step_lock_pct: float = 1.0

    def stop_level_pct(self, peak_pct: float, initial_stop_pct: float) -> float:
        """Stop level (as signed % of notional) for a given peak profit %.
        `initial_stop_pct` is the positive stop-loss size, e.g. 2.0 -> -2%."""
        if peak_pct >= self.trail_start_pct:
            steps = math.floor((peak_pct - self.trail_start_pct) / self.trail_step_pct + 1e-9)
            return self.trail_start_lock_pct + steps * self.trail_step_lock_pct
        if peak_pct >= self.breakeven_trigger_pct:
            return self.breakeven_lock_pct
        return -initial_stop_pct

    def describe(self) -> str:
        return (
            f"BE at +{self.breakeven_trigger_pct:g}% (lock {self.breakeven_lock_pct:g}%), "
            f"trail from +{self.trail_start_pct:g}% (lock {self.trail_start_lock_pct:g}%), "
            f"+{self.trail_step_lock_pct:g}% per +{self.trail_step_pct:g}%"
        )


class FixedExitPositionMonitor(PositionMonitor):
    def __init__(
        self,
        *args: Any,
        take_profit_usdt: float,
        stop_loss_pct: float = 0.0,
        take_profit_pct: float = 0.0,
        ladder: ProfitLadder | None = None,
        peaks_path: str | None = None,
        max_hold_s: float = 0.0,
        opened_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        # The parent requires trailing-profit parameters; they're unused here
        # because _check_once is replaced below.
        kwargs.setdefault("margin_usdt", 1.0)
        kwargs.setdefault("trailing_profit_arm_roi_pct", 0.0)
        kwargs.setdefault("trailing_profit_base_roi_pct", 0.0)
        kwargs.setdefault("trailing_profit_step_roi_pct", 0.0)
        kwargs.setdefault("trailing_profit_step_increment_roi_pct", 1.0)
        super().__init__(*args, **kwargs)
        self._take_profit_usdt = take_profit_usdt
        self._stop_loss_pct = stop_loss_pct
        self._take_profit_pct = take_profit_pct
        self._ladder = ladder
        self._peaks_path = Path(peaks_path) if peaks_path else None
        # key "SYMBOL|SIDE|entry" -> peak profit % of notional
        self._peaks: dict[str, float] = self._load_peaks()
        self._stops = NativeStopManager(self._broker)
        self._max_hold_s = max_hold_s
        self._opened_path = Path(opened_path) if opened_path else None
        # key "SYMBOL|SIDE|entry" -> unix time the position was first seen
        self._opened: dict[str, float] = self._load_json(self._opened_path)
        self._closed_at: dict[str, float] = {}  # symbol -> unix time it was last seen closed

    # ---- open / close times -------------------------------------------------

    def opened_at(self) -> dict[str, float]:
        """symbol -> unix time each currently open position was first seen."""
        out: dict[str, float] = {}
        for key, ts in self._opened.items():
            sym = key.split("|", 1)[0]
            if sym in self._positions:
                out[sym] = ts
        return out

    def last_closed_at(self, symbol: str) -> float | None:
        return self._closed_at.get(symbol)

    @staticmethod
    def _load_json(path: Path | None) -> dict[str, float]:
        if path is None or not path.exists():
            return {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return {str(k): float(v) for k, v in raw.items()}
        except Exception as exc:
            logger.error("fixed_exit_monitor.json_load_failed", path=str(path), error=str(exc))
            return {}

    @staticmethod
    def _save_json(path: Path | None, data: dict[str, float]) -> None:
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(path)
        except Exception as exc:
            logger.error("fixed_exit_monitor.json_save_failed", path=str(path), error=str(exc))

    def start(self) -> None:
        super().start()
        if self._ladder is not None:
            logger.info(
                "fixed_exit_monitor.config",
                stop_loss=f"{self._stop_loss_pct:g}% of notional"
                if self._stop_loss_pct > 0
                else f"{self._stop_loss_usdt:g} USDT",
                take_profit="profit ladder (no fixed TP)",
                ladder=self._ladder.describe(),
                peaks_restored=len(self._peaks),
                max_hold_hours=round(self._max_hold_s / 3600.0, 2) if self._max_hold_s > 0 else "off",
            )
        else:
            logger.info(
                "fixed_exit_monitor.config",
                stop_loss=f"{self._stop_loss_pct:g}% of notional"
                if self._stop_loss_pct > 0
                else f"{self._stop_loss_usdt:g} USDT",
                take_profit=f"{self._take_profit_pct:g}% of notional"
                if self._take_profit_pct > 0
                else f"{self._take_profit_usdt:g} USDT",
                trailing_profit="disabled",
            )

    # ---- amounts ----------------------------------------------------------

    def _notional(self, detail: PositionDetail) -> float:
        try:
            qty = self._broker.lots_to_quantity(detail.symbol, detail.quantity)
            return abs(float(qty) * detail.entry_price)
        except Exception:
            return 0.0

    def stop_loss_amount(self, detail: PositionDetail) -> float:
        if self._stop_loss_pct > 0:
            notional = self._notional(detail)
            if notional > 0:
                return notional * self._stop_loss_pct / 100.0
        return self._stop_loss_usdt

    def take_profit_amount(self, detail: PositionDetail) -> float:
        if self._take_profit_pct > 0:
            notional = self._notional(detail)
            if notional > 0:
                return notional * self._take_profit_pct / 100.0
        return self._take_profit_usdt

    # ---- peak persistence -------------------------------------------------

    @staticmethod
    def _key(detail: PositionDetail) -> str:
        return f"{detail.symbol}|{detail.side.value}|{detail.entry_price:.12g}"

    def _load_peaks(self) -> dict[str, float]:
        if self._peaks_path is None or not self._peaks_path.exists():
            return {}
        try:
            raw = json.loads(self._peaks_path.read_text(encoding="utf-8"))
            return {str(k): float(v) for k, v in raw.items()}
        except Exception as exc:
            logger.error("fixed_exit_monitor.peaks_load_failed", error=str(exc))
            return {}

    def _save_peaks(self) -> None:
        if self._peaks_path is None:
            return
        try:
            self._peaks_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._peaks_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._peaks), encoding="utf-8")
            tmp.replace(self._peaks_path)
        except Exception as exc:
            logger.error("fixed_exit_monitor.peaks_save_failed", error=str(exc))

    # ---- monitor ----------------------------------------------------------

    async def _arm_native_stop(self, detail: PositionDetail) -> None:
        # Superseded by NativeStopManager.sync (called from _check_once).
        return None

    async def _disarm_native_stop(self, symbol: str) -> None:
        # Called by the parent right before a software close, and below when
        # a position disappears: remove this symbol's exchange stop.
        self._native_stop_order_id.pop(symbol, None)
        await self._stops.on_position_closed(symbol)

    def _initial_stop_pct(self, notional: float) -> float:
        if self._stop_loss_pct > 0:
            return self._stop_loss_pct
        return self._stop_loss_usdt / notional * 100.0 if notional > 0 else 0.0

    async def _check_once(self) -> None:
        details = await self._broker.get_position_details()
        current_symbols = {d.symbol for d in details}
        self._closing &= current_symbols

        previous_symbols = set(self._positions.keys())
        self._positions = {d.symbol: (d.quantity if d.side is OrderSide.BUY else -d.quantity) for d in details}

        peaks_changed = False
        opened_changed = False
        live_keys = set()
        now_wall = time.time()
        for detail in details:
            key = self._key(detail)
            live_keys.add(key)
            if key not in self._opened:
                self._opened[key] = now_wall
                opened_changed = True
            if detail.symbol in self._closing:
                continue
            pnl = detail.unrealized_pnl
            if self._max_hold_s > 0 and now_wall - self._opened[key] >= self._max_hold_s:
                hours = (now_wall - self._opened[key]) / 3600.0
                await self._close_position(
                    detail.symbol,
                    self._positions[detail.symbol],
                    reason=f"MAX-HOLD: open {hours:.1f} h >= {self._max_hold_s / 3600.0:g} h ({pnl:+.2f} USDT)",
                )
                continue
            if self._ladder is not None:
                notional = self._notional(detail)
                if notional <= 0:
                    continue
                pnl_pct = pnl / notional * 100.0
                prev_peak = self._peaks.get(key)
                peak = pnl_pct if prev_peak is None else max(prev_peak, pnl_pct)
                if prev_peak is None or peak > prev_peak:
                    self._peaks[key] = peak
                    peaks_changed = True
                initial_sl_pct = self._initial_stop_pct(notional)
                level = self._ladder.stop_level_pct(peak, initial_sl_pct)
                if prev_peak is not None:
                    old_level = self._ladder.stop_level_pct(prev_peak, initial_sl_pct)
                    if level > old_level:
                        logger.info(
                            "fixed_exit_monitor.stop_raised",
                            symbol=detail.symbol,
                            peak_pct=round(peak, 2),
                            stop_pct=level,
                        )
                if pnl_pct <= level:
                    kind = ("STOP-LOSS" if level <= -initial_sl_pct + 1e-9 else
                            "BREAKEVEN-STOP" if level == 0 else "TRAILING-STOP")
                    await self._close_position(
                        detail.symbol,
                        self._positions[detail.symbol],
                        reason=(
                            f"{kind}: {pnl_pct:+.2f}% ({pnl:+.2f} USDT) <= stop {level:+g}% "
                            f"(peak {peak:+.2f}%)"
                        ),
                    )
                else:
                    await self._stops.sync(detail, level)
            else:
                sl = self.stop_loss_amount(detail)
                tp = self.take_profit_amount(detail)
                if pnl <= -sl:
                    await self._close_position(
                        detail.symbol,
                        self._positions[detail.symbol],
                        reason=f"STOP-LOSS: {pnl:.2f} USDT <= -{sl:.2f} USDT",
                    )
                elif pnl >= tp:
                    await self._close_position(
                        detail.symbol,
                        self._positions[detail.symbol],
                        reason=f"TAKE-PROFIT: {pnl:.2f} USDT >= {tp:.2f} USDT",
                    )
                else:
                    notional = self._notional(detail)
                    if notional > 0:
                        await self._stops.sync(detail, -self._initial_stop_pct(notional))
                        tp_pct = self._take_profit_pct if self._take_profit_pct > 0 else tp / notional * 100.0
                        await self._stops.sync_take_profit(detail, tp_pct)

        # forget peaks of positions that no longer exist
        stale = [k for k in self._peaks if k not in live_keys]
        for k in stale:
            del self._peaks[k]
        if stale or peaks_changed:
            self._save_peaks()
        stale_opened = [k for k in self._opened if k not in live_keys]
        for k in stale_opened:
            del self._opened[k]
        if stale_opened or opened_changed:
            self._save_json(self._opened_path, self._opened)

        for symbol in previous_symbols - current_symbols:
            self._closed_at[symbol] = now_wall
            await self._disarm_native_stop(symbol)

        # Leftover stops on symbols with no position (e.g. from before a restart).
        await self._stops.sweep_orphans(current_symbols)
