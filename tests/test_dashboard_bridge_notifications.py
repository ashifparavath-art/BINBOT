"""Tests for DashboardBridge's Telegram notification triggers.

Covers the three things that would be easy to get subtly wrong here: (1)
"opened" fires exactly once per new position, not once per fill while
scaling in, (2) a close notification is correctly labeled STOP-LOSS /
TAKE-PROFIT / generic based on whatever PositionMonitor recorded via
note_close_reason() — and that recorded reason doesn't leak into a later,
unrelated close on the same symbol, (3) nothing here ever raises when no
notifier is configured at all.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from core.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import ExecutionEvent, RiskHaltEvent
from execution.order_manager import ManagedOrder
from execution.trade_tracker import TradeTracker
from server.dashboard_bridge import DashboardBridge


class _FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)


async def _noop() -> None:
    return None


def _make_bridge(*, notifier=None, trade_tracker=None):
    order_manager = MagicMock()
    bridge = DashboardBridge(
        event_bus=EventBus(),
        broker=MagicMock(),
        order_manager=order_manager,
        circuit_breaker=MagicMock(),
        max_daily_loss_usd=1000.0,
        max_position_contracts=100,
        chart_symbol="BTCUSDT",
        strategy_id="test-strategy",
        control_token="tok",
        on_pause=_noop,
        on_resume=_noop,
        on_flatten=_noop,
        on_kill_switch=_noop,
        on_reset_kill_switch=_noop,
        is_strategy_paused=lambda: False,
        trade_tracker=trade_tracker,
        notifier=notifier,
    )
    return bridge, order_manager


def _managed_order(order_id: str, symbol: str, side: OrderSide) -> ManagedOrder:
    return ManagedOrder(
        order_id=order_id,
        correlation_id="corr-1",
        symbol=symbol,
        side=side,
        quantity=10,
        order_type=OrderType.MARKET,
        limit_price=None,
        time_in_force=TimeInForce.DAY,
        expected_price=None,
        status=OrderStatus.FILLED,
    )


def _execution(symbol: str, side: OrderSide, qty: int, price: float, order_id: str) -> ExecutionEvent:
    return ExecutionEvent(
        ts=datetime.now(timezone.utc), order_id=order_id, symbol=symbol, side=side, fill_quantity=qty, fill_price=price
    )


class TestOpenNotification:
    async def test_new_position_sends_opened_alert(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)

        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))

        assert len(notifier.sent) == 1
        assert "Opened" in notifier.sent[0] and "BTCUSDT" in notifier.sent[0]

    async def test_scaling_into_existing_position_does_not_resend_opened_alert(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)

        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 5, 65100.0, "o1"))

        opened_msgs = [m for m in notifier.sent if "Opened" in m]
        assert len(opened_msgs) == 1


class TestCloseNotification:
    async def test_close_without_recorded_reason_uses_generic_label(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))

        order_manager.get_order.return_value = _managed_order("o2", "BTCUSDT", OrderSide.SELL)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.SELL, 10, 65100.0, "o2"))

        close_msgs = [m for m in notifier.sent if "Position closed" in m]
        assert len(close_msgs) == 1
        assert "65100" in close_msgs[0]

    async def test_close_with_recorded_stop_loss_reason_labels_correctly(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))

        bridge.note_close_reason("BTCUSDT", "STOP-LOSS: -201.00 USDT <= -200 USDT")
        order_manager.get_order.return_value = _managed_order("o2", "BTCUSDT", OrderSide.SELL)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.SELL, 10, 64900.0, "o2"))

        assert any("STOP-LOSS HIT" in m for m in notifier.sent)

    async def test_close_with_recorded_take_profit_reason_labels_correctly(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "ETHUSDT", OrderSide.SELL)
        await bridge._handle_order_event(_execution("ETHUSDT", OrderSide.SELL, 100, 1900.0, "o1"))

        bridge.note_close_reason("ETHUSDT", "TAKE-PROFIT: 16.00 USDT >= 15 USDT")
        order_manager.get_order.return_value = _managed_order("o2", "ETHUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("ETHUSDT", OrderSide.BUY, 100, 1884.0, "o2"))

        assert any("TAKE-PROFIT HIT" in m for m in notifier.sent)

    async def test_recorded_reason_does_not_leak_into_a_later_unrelated_close(self):
        notifier = _FakeNotifier()
        bridge, order_manager = _make_bridge(notifier=notifier, trade_tracker=TradeTracker(lambda s: 1.0))

        # round trip 1: labeled stop-loss close
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))
        bridge.note_close_reason("BTCUSDT", "STOP-LOSS: -201 <= -200")
        order_manager.get_order.return_value = _managed_order("o2", "BTCUSDT", OrderSide.SELL)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.SELL, 10, 64900.0, "o2"))

        # round trip 2 on the same symbol: no reason recorded this time (e.g. manual flatten)
        order_manager.get_order.return_value = _managed_order("o3", "BTCUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o3"))
        order_manager.get_order.return_value = _managed_order("o4", "BTCUSDT", OrderSide.SELL)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.SELL, 10, 65050.0, "o4"))

        assert sum("STOP-LOSS HIT" in m for m in notifier.sent) == 1
        assert sum("Position closed" in m for m in notifier.sent) == 1


class TestNoNotifierConfigured:
    async def test_fills_do_not_raise_without_a_notifier(self):
        bridge, order_manager = _make_bridge(notifier=None, trade_tracker=TradeTracker(lambda s: 1.0))
        order_manager.get_order.return_value = _managed_order("o1", "BTCUSDT", OrderSide.BUY)
        await bridge._handle_order_event(_execution("BTCUSDT", OrderSide.BUY, 10, 65000.0, "o1"))


class TestHaltNotification:
    async def test_risk_halt_event_sends_alert(self):
        notifier = _FakeNotifier()
        bridge, _ = _make_bridge(notifier=notifier)

        event = RiskHaltEvent(
            ts=datetime.now(timezone.utc), reason="daily drawdown exceeded", triggered_by="circuit_breaker"
        )
        await bridge._on_event(event)

        assert any("Circuit breaker tripped" in m for m in notifier.sent)
