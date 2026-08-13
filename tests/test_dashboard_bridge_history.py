"""Tests for DashboardBridge's day-history backfill and the realized vs.
unrealized P&L fix.

Backfill matters because a browser tab reconnecting (or opening for the
first time after the bot process restarted) previously saw nothing until
a new live event happened — these tests confirm a fresh connection is
immediately caught up. The P&L tests exist because of a real production
bug: realizedPnl was hardcoded to 0.0 while unrealizedPnl silently carried
a mix of both realized and unrealized change — these pin the fix so it
can't regress back to that.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.enums import OrderSide
from core.event_bus import EventBus
from execution.binance_broker import PositionDetail
from server.dashboard_bridge import DashboardBridge


class _FakeConnection:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)


async def _noop() -> None:
    return None


def _make_bridge(*, broker=None, position_details_provider=None):
    return DashboardBridge(
        event_bus=EventBus(),
        broker=broker or MagicMock(),
        order_manager=MagicMock(),
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
        position_details_provider=position_details_provider,
    )


class TestBackfillOnConnect:
    async def test_sends_all_three_backfill_types_when_history_exists(self):
        bridge = _make_bridge()
        bridge._closed_trades_recent = [{"symbol": "BTCUSDT", "pnl": 12.5}]
        bridge._audit_recent = [{"level": "info", "message": "hi"}]
        bridge._candles_by_symbol = {
            "BTCUSDT": [{"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}]
        }
        conn = _FakeConnection()

        await bridge._send_backfill(conn)

        types = [json.loads(m)["type"] for m in conn.sent]
        assert "closed_trades_backfill" in types
        assert "audit_backfill" in types
        assert "candles_backfill" in types

    async def test_sends_nothing_when_there_is_no_history_yet(self):
        bridge = _make_bridge()
        conn = _FakeConnection()

        await bridge._send_backfill(conn)

        assert conn.sent == []

    async def test_candles_backfill_payload_carries_the_right_symbol(self):
        bridge = _make_bridge()
        bridge._candles_by_symbol = {
            "ETHUSDT": [{"time": 1, "open": 2000.0, "high": 2001.0, "low": 1999.0, "close": 2000.5, "volume": 0}]
        }
        conn = _FakeConnection()

        await bridge._send_backfill(conn)

        msg = json.loads(conn.sent[0])
        assert msg["type"] == "candles_backfill"
        assert msg["payload"]["symbol"] == "ETHUSDT"
        assert len(msg["payload"]["candles"]) == 1

    async def test_a_connection_send_failure_does_not_raise(self):
        bridge = _make_bridge()
        bridge._audit_recent = [{"level": "info", "message": "hi"}]
        conn = _FakeConnection()
        conn.send = AsyncMock(side_effect=ConnectionError("closed"))

        await bridge._send_backfill(conn)  # must not raise


class TestPnlComputation:
    def _broker(self, *, equity=5100.0, initial_margin=100.0, maintenance_margin=50.0):
        broker = MagicMock()
        broker.is_connected = True
        broker.get_account_equity = AsyncMock(return_value=equity)
        broker.get_margin_usage = AsyncMock(return_value=(initial_margin, maintenance_margin))
        return broker

    async def test_realized_and_unrealized_are_reported_separately(self):
        details = [
            PositionDetail(symbol="BTCUSDT", side=OrderSide.BUY, quantity=10, entry_price=100.0, mark_price=105.0, unrealized_pnl=25.0),
            PositionDetail(symbol="ETHUSDT", side=OrderSide.SELL, quantity=5, entry_price=50.0, mark_price=49.0, unrealized_pnl=5.0),
        ]
        provider = AsyncMock(return_value=details)
        bridge = _make_bridge(broker=self._broker(), position_details_provider=provider)
        bridge._closed_trades_recent = [{"pnl": 10.0}, {"pnl": -3.0}]
        bridge._broadcast = AsyncMock()

        await bridge._push_status_and_kpis()

        kpis_call = next(c for c in bridge._broadcast.call_args_list if c.args[0]["type"] == "kpis")
        payload = kpis_call.args[0]["payload"]
        assert payload["realizedPnl"] == pytest.approx(7.0)  # 10.0 + -3.0
        assert payload["unrealizedPnl"] == pytest.approx(30.0)  # 25.0 + 5.0

    async def test_realized_pnl_is_zero_not_hardcoded_when_no_closed_trades(self):
        provider = AsyncMock(return_value=[])
        bridge = _make_bridge(broker=self._broker(), position_details_provider=provider)
        bridge._broadcast = AsyncMock()

        await bridge._push_status_and_kpis()

        kpis_call = next(c for c in bridge._broadcast.call_args_list if c.args[0]["type"] == "kpis")
        assert kpis_call.args[0]["payload"]["realizedPnl"] == 0.0
        assert kpis_call.args[0]["payload"]["unrealizedPnl"] == 0.0

    async def test_unrealized_pnl_does_not_leak_into_realized(self):
        # Regression guard for the original bug: a position with large
        # unrealized P&L and zero closed trades must show realizedPnl == 0,
        # not some equity-delta figure that happens to match unrealizedPnl.
        details = [
            PositionDetail(symbol="BTCUSDT", side=OrderSide.BUY, quantity=10, entry_price=100.0, mark_price=150.0, unrealized_pnl=500.0),
        ]
        provider = AsyncMock(return_value=details)
        bridge = _make_bridge(broker=self._broker(equity=5500.0), position_details_provider=provider)
        bridge._broadcast = AsyncMock()

        await bridge._push_status_and_kpis()

        kpis_call = next(c for c in bridge._broadcast.call_args_list if c.args[0]["type"] == "kpis")
        payload = kpis_call.args[0]["payload"]
        assert payload["unrealizedPnl"] == pytest.approx(500.0)
        assert payload["realizedPnl"] == 0.0

    async def test_falls_back_gracefully_without_a_position_details_provider(self):
        broker = self._broker()
        broker.get_positions = AsyncMock(return_value={"BTCUSDT": 10})
        bridge = _make_bridge(broker=broker, position_details_provider=None)
        bridge._broadcast = AsyncMock()

        await bridge._push_status_and_kpis()  # must not raise

        kpis_call = next(c for c in bridge._broadcast.call_args_list if c.args[0]["type"] == "kpis")
        assert kpis_call.args[0]["payload"]["unrealizedPnl"] == 0.0
