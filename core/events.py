"""Immutable event definitions that flow through the EventBus.

Every event carries a `ts` (exchange/generation time) and the bus stamps a
`recv_ts` on ingestion so latency between generation and processing is always
measurable. `correlation_id` threads a signal through submission, ack, and
fill so the full order lifecycle can be reconstructed from logs alone.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto

from core.enums import OrderSide, OrderStatus, OrderType, TimeInForce


class EventType(Enum):
    TICK = auto()
    BAR = auto()
    ORDER_BOOK = auto()
    SIGNAL = auto()
    ORDER_ACK = auto()
    ORDER_REJECT = auto()
    EXECUTION = auto()
    RISK_HALT = auto()
    HEARTBEAT = auto()


def _new_id() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class Event:
    """Base class. Subclasses set `event_type` via a class attribute."""

    event_type: EventType = field(init=False)
    ts: datetime
    event_id: str = field(default_factory=_new_id)


@dataclass(frozen=True, slots=True)
class TickEvent(Event):
    symbol: str = ""
    bid: float = 0.0
    ask: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    last: float = 0.0
    last_size: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.TICK)

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        return self.last


@dataclass(frozen=True, slots=True)
class BarEvent(Event):
    symbol: str = ""
    timeframe_s: int = 60
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    vwap: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.BAR)


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: float
    size: int


@dataclass(frozen=True, slots=True)
class OrderBookEvent(Event):
    symbol: str = ""
    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.ORDER_BOOK)


@dataclass(frozen=True, slots=True)
class SignalEvent(Event):
    """A strategy's intent to trade. Not yet risk-checked or routed."""

    symbol: str = ""
    strategy_id: str = ""
    side: OrderSide = OrderSide.BUY
    strength: float = 1.0  # -1..1 confidence/target-weight, strategy-defined
    target_quantity: int = 0
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    correlation_id: str = field(default_factory=_new_id)
    oca_group: str | None = None
    reduce_only: bool = False
    metadata: dict[str, float | str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.SIGNAL)


@dataclass(frozen=True, slots=True)
class OrderAckEvent(Event):
    order_id: str = ""
    correlation_id: str = ""
    symbol: str = ""
    status: OrderStatus = OrderStatus.ACKNOWLEDGED
    broker_order_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.ORDER_ACK)


@dataclass(frozen=True, slots=True)
class OrderRejectEvent(Event):
    order_id: str = ""
    correlation_id: str = ""
    symbol: str = ""
    reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.ORDER_REJECT)


@dataclass(frozen=True, slots=True)
class ExecutionEvent(Event):
    order_id: str = ""
    correlation_id: str = ""
    symbol: str = ""
    side: OrderSide = OrderSide.BUY
    fill_quantity: int = 0
    fill_price: float = 0.0
    cumulative_quantity: int = 0
    remaining_quantity: int = 0
    commission: float = 0.0
    liquidity: str = "UNKNOWN"  # MAKER | TAKER | UNKNOWN
    expected_price: float | None = None  # decision-time price, for slippage calc

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.EXECUTION)

    @property
    def slippage(self) -> float | None:
        if self.expected_price is None:
            return None
        signed = 1 if self.side is OrderSide.BUY else -1
        return (self.fill_price - self.expected_price) * signed


@dataclass(frozen=True, slots=True)
class RiskHaltEvent(Event):
    reason: str = ""
    triggered_by: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.RISK_HALT)


@dataclass(frozen=True, slots=True)
class HeartbeatEvent(Event):
    source: str = ""
    healthy: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_type", EventType.HEARTBEAT)
