"""Tests for PositionMonitor's exit logic and its native-stop lifecycle.

The native stop is the resilience backstop from the AWS crash-loop
incident (2026-08-10): if the bot process itself is down, nothing but an
exchange-side order protects an open position. These tests exist to catch
exactly the class of bug that would silently defeat that backstop — an
arm that never fires, or a disarm that leaves a stale order behind.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from core.enums import OrderSide
from execution.binance_broker import PositionDetail
from execution.position_monitor import PositionMonitor


def detail(symbol: str, *, side: OrderSide, qty: int, entry: float, unrealized: float) -> PositionDetail:
    return PositionDetail(
        symbol=symbol, side=side, quantity=qty, entry_price=entry, mark_price=entry, unrealized_pnl=unrealized
    )


@pytest.fixture
def broker() -> AsyncMock:
    b = AsyncMock()
    b.place_stop_loss.return_value = "native-order-1"
    return b


@pytest.fixture
def order_manager() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def monitor(broker: AsyncMock, order_manager: AsyncMock) -> PositionMonitor:
    return PositionMonitor(broker, order_manager, stop_loss_usdt=200.0, take_profit_usdt=15.0)


class TestNativeStopArmingAndDisarming:
    async def test_new_position_arms_a_native_stop(self, monitor: PositionMonitor, broker: AsyncMock):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=0.0)
        ]

        await monitor._check_once()

        broker.place_stop_loss.assert_awaited_once_with(
            "BTCUSDT", is_long=True, entry_price=65000.0, quantity_lots=15, max_loss_usdt=200.0
        )
        assert monitor._native_stop_order_id["BTCUSDT"] == "native-order-1"

    async def test_same_position_across_polls_arms_only_once(self, monitor: PositionMonitor, broker: AsyncMock):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=5.0)
        ]

        await monitor._check_once()
        await monitor._check_once()
        await monitor._check_once()

        assert broker.place_stop_loss.await_count == 1

    async def test_position_closing_disarms_the_native_stop(self, monitor: PositionMonitor, broker: AsyncMock):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=5.0)
        ]
        await monitor._check_once()

        broker.get_position_details.return_value = []  # position now gone (closed elsewhere)
        await monitor._check_once()

        broker.cancel_stop_loss.assert_awaited_once_with("BTCUSDT", "native-order-1")
        assert "BTCUSDT" not in monitor._native_stop_order_id

    async def test_failed_native_placement_does_not_track_a_stop(self, monitor: PositionMonitor, broker: AsyncMock):
        broker.place_stop_loss.return_value = None  # e.g. Binance rejected it
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=0.0)
        ]

        await monitor._check_once()

        assert "BTCUSDT" not in monitor._native_stop_order_id

    async def test_short_position_arms_with_is_long_false(self, monitor: PositionMonitor, broker: AsyncMock):
        broker.get_position_details.return_value = [
            detail("ETHUSDT", side=OrderSide.SELL, qty=100, entry=1900.0, unrealized=0.0)
        ]

        await monitor._check_once()

        broker.place_stop_loss.assert_awaited_once_with(
            "ETHUSDT", is_long=False, entry_price=1900.0, quantity_lots=100, max_loss_usdt=200.0
        )


class TestSoftwareExitLogic:
    async def test_stop_loss_triggers_a_closing_sell_for_a_long(
        self, monitor: PositionMonitor, broker: AsyncMock, order_manager: AsyncMock
    ):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=-201.0)
        ]

        await monitor._check_once()

        order_manager.submit.assert_awaited_once()
        signal = order_manager.submit.call_args.args[0]
        assert signal.symbol == "BTCUSDT"
        assert signal.side is OrderSide.SELL
        assert signal.target_quantity == 15

    async def test_take_profit_triggers_a_closing_buy_for_a_short(
        self, monitor: PositionMonitor, broker: AsyncMock, order_manager: AsyncMock
    ):
        broker.get_position_details.return_value = [
            detail("ETHUSDT", side=OrderSide.SELL, qty=100, entry=1900.0, unrealized=16.0)
        ]

        await monitor._check_once()

        signal = order_manager.submit.call_args.args[0]
        assert signal.side is OrderSide.BUY
        assert signal.target_quantity == 100

    async def test_a_closing_symbol_is_not_re_triggered_next_poll(
        self, monitor: PositionMonitor, broker: AsyncMock, order_manager: AsyncMock
    ):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=-250.0)
        ]

        await monitor._check_once()
        await monitor._check_once()  # still shows the same losing position (fill hasn't landed yet)

        assert order_manager.submit.await_count == 1

    async def test_healthy_position_neither_closes_nor_reports_open_incorrectly(
        self, monitor: PositionMonitor, broker: AsyncMock, order_manager: AsyncMock
    ):
        broker.get_position_details.return_value = [
            detail("BTCUSDT", side=OrderSide.BUY, qty=15, entry=65000.0, unrealized=5.0)
        ]

        await monitor._check_once()

        order_manager.submit.assert_not_awaited()
        assert monitor.open_symbols() == {"BTCUSDT"}
        assert monitor.open_count() == 1
