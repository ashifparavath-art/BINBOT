"""Abstract broker interface. Every concrete broker adapter (IBKR live, a FIX
adapter, or the backtester's simulated broker) implements this contract so
the OrderLifecycleManager and strategies never depend on a specific vendor
SDK.
"""
from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass

from core.enums import OrderSide, OrderType, TimeInForce
from core.events import ExecutionEvent, OrderAckEvent, OrderRejectEvent


@dataclass(frozen=True, slots=True)
class BrokerOrderRequest:
    order_id: str
    correlation_id: str
    symbol: str
    side: OrderSide
    quantity: int
    order_type: OrderType
    limit_price: float | None
    time_in_force: TimeInForce
    oca_group: str | None = None
    oca_type: int = 1  # IBKR OCA type 1 = cancel all remaining on any fill
    reduce_only: bool = False


class BrokerInterface(abc.ABC):
    """Contract every broker/exchange adapter must satisfy."""

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def disconnect(self) -> None: ...

    @property
    @abc.abstractmethod
    def is_connected(self) -> bool: ...

    @abc.abstractmethod
    async def place_order(self, request: BrokerOrderRequest) -> None:
        """Fire-and-forget submission; acks/fills/rejects arrive via
        `stream_events()`."""

    @abc.abstractmethod
    async def cancel_order(self, order_id: str) -> None: ...

    @abc.abstractmethod
    async def cancel_all(self) -> None: ...

    @abc.abstractmethod
    def stream_events(self) -> AsyncIterator[OrderAckEvent | OrderRejectEvent | ExecutionEvent]:
        """Async-iterate broker-originated order lifecycle events."""

    @abc.abstractmethod
    async def get_account_equity(self) -> float: ...

    @abc.abstractmethod
    async def get_positions(self) -> dict[str, int]: ...

    @abc.abstractmethod
    async def get_margin_usage(self) -> tuple[float, float]:
        """Returns (initial_margin_used, maintenance_margin_used)."""

    @abc.abstractmethod
    async def reconcile_state(self) -> dict[str, int]:
        """Idempotently rebuild position inventory from the broker's own
        trade/execution log (used on startup / reconnect for crash recovery).
        Returns the reconciled {symbol: quantity} book."""
