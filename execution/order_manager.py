"""Asynchronous order lifecycle manager.

Owns the full Signal -> Submission -> Ack -> Fill state machine for every
order in flight, republishes broker-originated events onto the shared
EventBus (so strategies/risk/logging all observe the same truth), tracks
slippage, and implements OCA/OCO grouping by assigning a shared
`oca_group` id to a batch of signals — IBKR (and our simulated broker)
cancels the siblings automatically the instant one leg fills.

This class assumes every order it receives has ALREADY passed
`RiskEngine.check_order()` — it performs no risk logic itself, keeping a
strict separation of concerns between "should we trade" (risk) and "how do
we execute the trade we decided on" (this module).
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from config.logging_config import get_logger
from core.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import ExecutionEvent, OrderAckEvent, OrderRejectEvent, SignalEvent
from execution.broker_interface import BrokerInterface, BrokerOrderRequest
from execution.slippage import SlippageTracker

logger = get_logger(__name__)


@dataclass
class ManagedOrder:
    order_id: str
    correlation_id: str
    symbol: str
    side: OrderSide
    quantity: int
    order_type: OrderType
    limit_price: float | None
    time_in_force: TimeInForce
    expected_price: float | None
    oca_group: str | None = None
    status: OrderStatus = OrderStatus.PENDING_NEW
    filled_quantity: int = 0
    remaining_quantity: int = 0
    avg_fill_price: float = 0.0
    reject_reason: str = ""
    created_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        self.remaining_quantity = self.quantity


class OrderLifecycleManager:
    def __init__(
        self,
        broker: BrokerInterface,
        event_bus: EventBus,
        slippage_tracker: SlippageTracker | None = None,
    ) -> None:
        self._broker = broker
        self._event_bus = event_bus
        self._slippage_tracker = slippage_tracker or SlippageTracker()
        self._orders: dict[str, ManagedOrder] = {}
        self._pending_order_id_by_correlation: dict[str, str] = {}
        self._consume_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._consume_task = asyncio.create_task(self._consume_broker_events(), name="order-mgr-consume")

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass

    async def submit(self, signal: SignalEvent, *, expected_price: float | None = None) -> ManagedOrder:
        order_id = uuid.uuid4().hex
        managed = ManagedOrder(
            order_id=order_id,
            correlation_id=signal.correlation_id,
            symbol=signal.symbol,
            side=signal.side,
            quantity=abs(signal.target_quantity),
            order_type=signal.order_type,
            limit_price=signal.limit_price,
            time_in_force=signal.time_in_force,
            expected_price=expected_price,
            oca_group=signal.oca_group,
        )
        self._orders[order_id] = managed

        request = BrokerOrderRequest(
            order_id=order_id,
            correlation_id=signal.correlation_id,
            symbol=signal.symbol,
            side=signal.side,
            quantity=managed.quantity,
            order_type=signal.order_type,
            limit_price=signal.limit_price,
            time_in_force=signal.time_in_force,
            oca_group=signal.oca_group,
            reduce_only=signal.reduce_only,
        )
        logger.info(
            "order_manager.submitting",
            order_id=order_id,
            symbol=signal.symbol,
            side=signal.side.value,
            quantity=managed.quantity,
            oca_group=signal.oca_group,
            correlation_id=signal.correlation_id,
        )
        managed.status = OrderStatus.SUBMITTED
        await self._broker.place_order(request)
        return managed

    async def submit_oco(self, signals: list[SignalEvent]) -> list[ManagedOrder]:
        """Submit a batch of signals as a One-Cancels-Other group: the fill
        of any leg cancels all remaining legs at the broker."""
        if len(signals) < 2:
            raise ValueError("OCO group requires at least 2 signals")
        oca_group = f"oca-{uuid.uuid4().hex}"
        grouped = [
            SignalEvent(
                ts=s.ts,
                symbol=s.symbol,
                strategy_id=s.strategy_id,
                side=s.side,
                strength=s.strength,
                target_quantity=s.target_quantity,
                order_type=s.order_type,
                limit_price=s.limit_price,
                time_in_force=s.time_in_force,
                correlation_id=s.correlation_id,
                oca_group=oca_group,
                metadata=s.metadata,
            )
            for s in signals
        ]
        return [await self.submit(s) for s in grouped]

    async def cancel(self, order_id: str) -> None:
        order = self._orders.get(order_id)
        if order is None:
            logger.warning("order_manager.cancel_unknown_order", order_id=order_id)
            return
        order.status = OrderStatus.CANCEL_PENDING
        await self._broker.cancel_order(order_id)

    async def cancel_all(self) -> None:
        logger.warning("order_manager.cancel_all_triggered", open_orders=len(self.open_orders))
        await self._broker.cancel_all()

    def get_order(self, order_id: str) -> ManagedOrder | None:
        return self._orders.get(order_id)

    @property
    def open_orders(self) -> list[ManagedOrder]:
        return [o for o in self._orders.values() if not o.status.is_terminal]

    async def _consume_broker_events(self) -> None:
        async for event in self._broker.stream_events():
            try:
                await self._handle_broker_event(event)
            except Exception:
                logger.exception("order_manager.event_handling_error", event_type=type(event).__name__)

    async def _handle_broker_event(
        self, event: OrderAckEvent | OrderRejectEvent | ExecutionEvent
    ) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            logger.warning(
                "order_manager.event_for_unknown_order", order_id=event.order_id
            )
            return

        order.updated_ts = datetime.now(timezone.utc)

        if isinstance(event, OrderAckEvent):
            order.status = event.status
            logger.info(
                "order_manager.ack",
                order_id=order.order_id,
                status=order.status.value,
                correlation_id=order.correlation_id,
            )
        elif isinstance(event, OrderRejectEvent):
            order.status = OrderStatus.REJECTED
            order.reject_reason = event.reason
            logger.error(
                "order_manager.rejected",
                order_id=order.order_id,
                reason=event.reason,
                correlation_id=order.correlation_id,
            )
        elif isinstance(event, ExecutionEvent):
            prev_notional = order.avg_fill_price * order.filled_quantity
            order.filled_quantity = event.cumulative_quantity
            order.remaining_quantity = event.remaining_quantity
            new_notional = prev_notional + event.fill_price * event.fill_quantity
            order.avg_fill_price = (
                new_notional / order.filled_quantity if order.filled_quantity else 0.0
            )
            order.status = (
                OrderStatus.FILLED if event.remaining_quantity == 0 else OrderStatus.PARTIALLY_FILLED
            )
            enriched_event = ExecutionEvent(
                ts=event.ts,
                order_id=event.order_id,
                correlation_id=event.correlation_id,
                symbol=event.symbol,
                side=event.side,
                fill_quantity=event.fill_quantity,
                fill_price=event.fill_price,
                cumulative_quantity=event.cumulative_quantity,
                remaining_quantity=event.remaining_quantity,
                commission=event.commission,
                liquidity=event.liquidity,
                expected_price=order.expected_price,
            )
            self._slippage_tracker.record(enriched_event)
            logger.info(
                "order_manager.fill",
                order_id=order.order_id,
                symbol=order.symbol,
                fill_qty=event.fill_quantity,
                fill_price=event.fill_price,
                cumulative=event.cumulative_quantity,
                slippage=enriched_event.slippage,
                correlation_id=order.correlation_id,
            )
            event = enriched_event

        await self._event_bus.publish(event)
