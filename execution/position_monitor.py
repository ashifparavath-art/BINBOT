"""Continuously watches every open position's real unrealized P&L (read
directly from Binance, not recomputed locally — see
`BinanceFuturesBroker.get_position_details`) and closes a position the
instant it crosses the stop-loss or take-profit threshold.

Also arms a native, exchange-side stop-loss order the moment a position is
first seen, and disarms it once the position is closed by any other means
(the software check below, a manual flatten, a kill switch). The software
check is the primary, faster mechanism while the bot is running; the native
order is strictly a backstop for the gap where it isn't — a crash, a
restart, the server rebooting — see `BinanceFuturesBroker.place_stop_loss`.

This is also the single shared source of truth for "how many positions are
open right now" and "which symbols" — the scanner strategy queries it
before allowing a new entry, so entry-gating and exit-watching can never
disagree about current state (both read the same broker snapshot, taken
here, rather than each polling the exchange independently).
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone

from config.logging_config import get_logger
from core.enums import OrderSide, OrderType, TimeInForce
from core.events import SignalEvent
from execution.binance_broker import BinanceFuturesBroker, PositionDetail
from execution.order_manager import OrderLifecycleManager

logger = get_logger(__name__)


class PositionMonitor:
    def __init__(
        self,
        broker: BinanceFuturesBroker,
        order_manager: OrderLifecycleManager,
        *,
        stop_loss_usdt: float,
        take_profit_usdt: float,
        poll_interval_s: float = 2.0,
        on_close_event: Callable[[str, str], "asyncio.Future[None] | None"] | None = None,
        price_lookup: Callable[[str], float | None] | None = None,
    ) -> None:
        self._broker = broker
        self._order_manager = order_manager
        self._stop_loss_usdt = stop_loss_usdt
        self._take_profit_usdt = take_profit_usdt
        self._poll_interval_s = poll_interval_s
        self._on_close_event = on_close_event
        self._price_lookup = price_lookup

        self._positions: dict[str, int] = {}
        self._closing: set[str] = set()
        self._native_stop_order_id: dict[str, str] = {}
        self._task: asyncio.Task[None] | None = None
        self._running = False

    def open_symbols(self) -> set[str]:
        return set(self._positions.keys())

    def open_count(self) -> int:
        return len(self._positions)

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="position-monitor")
        logger.info(
            "position_monitor.started",
            stop_loss_usdt=self._stop_loss_usdt,
            take_profit_usdt=self._take_profit_usdt,
            poll_interval_s=self._poll_interval_s,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._check_once()
            except Exception:
                logger.exception("position_monitor.check_failed")
            await asyncio.sleep(self._poll_interval_s)

    async def _check_once(self) -> None:
        details = await self._broker.get_position_details()
        current_symbols = {d.symbol for d in details}
        self._closing &= current_symbols  # a symbol no longer open can be re-armed for future monitoring

        previous_symbols = set(self._positions.keys())
        self._positions = {d.symbol: (d.quantity if d.side is OrderSide.BUY else -d.quantity) for d in details}

        for detail in details:
            if detail.symbol in self._closing:
                continue
            if detail.symbol not in previous_symbols:
                # Newly seen — either a fresh entry, or a position that was
                # already open when this process (re)started. Either way,
                # Binance's own recorded entry price makes this correct
                # regardless of which case it is.
                await self._arm_native_stop(detail)
            if detail.unrealized_pnl <= -self._stop_loss_usdt:
                await self._close_position(
                    detail.symbol,
                    self._positions[detail.symbol],
                    reason=f"STOP-LOSS: {detail.unrealized_pnl:.2f} USDT <= -{self._stop_loss_usdt:.0f} USDT",
                )
            elif detail.unrealized_pnl >= self._take_profit_usdt:
                await self._close_position(
                    detail.symbol,
                    self._positions[detail.symbol],
                    reason=f"TAKE-PROFIT: {detail.unrealized_pnl:.2f} USDT >= {self._take_profit_usdt:.0f} USDT",
                )

        for symbol in previous_symbols - current_symbols:
            await self._disarm_native_stop(symbol)

    async def _arm_native_stop(self, detail: PositionDetail) -> None:
        order_id = await self._broker.place_stop_loss(
            detail.symbol,
            is_long=detail.side is OrderSide.BUY,
            entry_price=detail.entry_price,
            quantity_lots=detail.quantity,
            max_loss_usdt=self._stop_loss_usdt,
        )
        if order_id is not None:
            self._native_stop_order_id[detail.symbol] = order_id

    async def _disarm_native_stop(self, symbol: str) -> None:
        order_id = self._native_stop_order_id.pop(symbol, None)
        if order_id is not None:
            await self._broker.cancel_stop_loss(symbol, order_id)

    async def _close_position(self, symbol: str, qty: int, *, reason: str) -> None:
        self._closing.add(symbol)
        side = OrderSide.SELL if qty > 0 else OrderSide.BUY
        signal = SignalEvent(
            ts=datetime.now(timezone.utc),
            symbol=symbol,
            strategy_id="position-monitor",
            side=side,
            target_quantity=abs(qty),
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )
        logger.warning("position_monitor.closing", symbol=symbol, reason=reason)
        if self._on_close_event is not None:
            maybe_awaitable = self._on_close_event(symbol, reason)
            if maybe_awaitable is not None:
                await maybe_awaitable
        expected_price = self._price_lookup(symbol) if self._price_lookup else None
        # Deliberately does not go through RiskEngine.check_order(): this
        # order only ever REDUCES exposure (it's a full close of an
        # existing position), and a stop-loss/take-profit exit must not be
        # blockable by limits designed to prevent taking on MORE risk.
        await self._order_manager.submit(signal, expected_price=expected_price)
