"""BINBOT2 exits: fixed USDT stop-loss / take-profit, no trailing stop."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from config.binbot2_settings import ATRBreakoutSettings
from core.enums import OrderSide
from execution.binance_broker import PositionDetail
from execution.fixed_exit_position_monitor import FixedExitPositionMonitor


def detail(symbol: str, *, side: OrderSide = OrderSide.BUY, qty: int = 1000, unrealized: float) -> PositionDetail:
    return PositionDetail(symbol=symbol, side=side, quantity=qty, entry_price=0.1, mark_price=0.1,
                          unrealized_pnl=unrealized)


@pytest.fixture
def broker() -> AsyncMock:
    b = AsyncMock()
    b._filters = {}  # exchange-stop behaviour is covered in test_native_stop_manager.py
    from unittest.mock import MagicMock
    b.lots_to_quantity = MagicMock(side_effect=lambda sym, lots: lots * 0.1)
    return b


@pytest.fixture
def order_manager() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def monitor(broker, order_manager) -> FixedExitPositionMonitor:
    return FixedExitPositionMonitor(broker, order_manager, stop_loss_usdt=5.0, take_profit_usdt=10.0)


def submitted(order_manager: AsyncMock):
    return [c.args[0] for c in order_manager.submit.await_args_list]


async def test_stop_loss_at_minus_5(monitor, broker, order_manager):
    broker.get_position_details.return_value = [detail("XUSDT", unrealized=-4.99)]
    await monitor._check_once()
    assert submitted(order_manager) == []
    broker.get_position_details.return_value = [detail("XUSDT", unrealized=-5.0)]
    await monitor._check_once()
    (sig,) = submitted(order_manager)
    assert sig.side is OrderSide.SELL and sig.reduce_only and sig.target_quantity == 1000


async def test_take_profit_at_plus_10(monitor, broker, order_manager):
    broker.get_position_details.return_value = [detail("XUSDT", side=OrderSide.SELL, unrealized=9.99)]
    await monitor._check_once()
    assert submitted(order_manager) == []
    broker.get_position_details.return_value = [detail("XUSDT", side=OrderSide.SELL, unrealized=10.2)]
    await monitor._check_once()
    (sig,) = submitted(order_manager)
    assert sig.side is OrderSide.BUY and sig.reduce_only


async def test_no_trailing_exit_on_pullback(monitor, broker, order_manager):
    # Rises to +9, pulls back to +1: a trailing stop would have closed; fixed exits don't.
    for pnl in (3.0, 9.0, 6.0, 1.0):
        broker.get_position_details.return_value = [detail("XUSDT", unrealized=pnl)]
        await monitor._check_once()
    assert submitted(order_manager) == []


async def test_closing_is_not_retriggered(monitor, broker, order_manager):
    broker.get_position_details.return_value = [detail("XUSDT", unrealized=12.0)]
    await monitor._check_once()
    await monitor._check_once()  # still reported open while the close fills
    assert len(submitted(order_manager)) == 1


def test_default_sizing_and_exits():
    s = ATRBreakoutSettings(_env_file=None)
    assert s.order_notional_usdt == 250.0 and not s.ladder_enabled and s.max_daily_loss_usdt == 100.0
    assert (s.stop_loss_pct, s.take_profit_pct) == (2.0, 4.0)


class TestPercentStopLoss:
    @pytest.fixture
    def pct_broker(self) -> AsyncMock:
        from unittest.mock import MagicMock
        b = AsyncMock()
        b._filters = {}  # exchange-stop behaviour is covered in test_native_stop_manager.py
        # 1000 lots x step 0.1 = 100 units; entry 0.1 below -> overridden per test
        b.lots_to_quantity = MagicMock(side_effect=lambda sym, lots: lots * 0.1)
        return b

    def pos(self, *, entry: float, lots: int, pnl: float, side=OrderSide.BUY) -> PositionDetail:
        return PositionDetail(symbol="XUSDT", side=side, quantity=lots, entry_price=entry, mark_price=entry,
                              unrealized_pnl=pnl)

    async def test_scales_with_size(self, pct_broker, order_manager):
        m = FixedExitPositionMonitor(pct_broker, order_manager, stop_loss_usdt=999, take_profit_usdt=10.0,
                                     stop_loss_pct=5.0)
        # 300 USDT notional -> 15 USDT stop: -14 holds, -15 closes
        pct_broker.get_position_details.return_value = [self.pos(entry=3.0, lots=1000, pnl=-14.0)]
        await m._check_once()
        assert submitted(order_manager) == []
        pct_broker.get_position_details.return_value = [self.pos(entry=3.0, lots=1000, pnl=-15.0)]
        await m._check_once()
        (sig,) = submitted(order_manager)
        assert sig.reduce_only

    async def test_take_profit_stays_fixed_10_usdt(self, pct_broker, order_manager):
        m = FixedExitPositionMonitor(pct_broker, order_manager, stop_loss_usdt=999, take_profit_usdt=10.0,
                                     stop_loss_pct=5.0)
        pct_broker.get_position_details.return_value = [self.pos(entry=3.0, lots=1000, pnl=10.0)]
        await m._check_once()
        assert len(submitted(order_manager)) == 1

    def test_5pct_of_100_usdt_notional_is_5_usdt(self, pct_broker, order_manager):
        m = FixedExitPositionMonitor(pct_broker, order_manager, stop_loss_usdt=999, take_profit_usdt=10.0,
                                     stop_loss_pct=5.0)
        assert m.stop_loss_amount(self.pos(entry=1.0, lots=1000, pnl=0.0)) == pytest.approx(5.0)

    def test_pct_zero_falls_back_to_fixed(self, pct_broker, order_manager):
        m = FixedExitPositionMonitor(pct_broker, order_manager, stop_loss_usdt=5.0, take_profit_usdt=10.0)
        assert m.stop_loss_amount(self.pos(entry=3.0, lots=1000, pnl=0)) == 5.0


def test_default_stop_is_2pct():
    s = ATRBreakoutSettings(_env_file=None)
    assert s.stop_loss_pct == 2.0


# ---------------------------------------------------------------------------
# Profit ladder: -2% stop; BE at +4%; +4% lock at +8%; +1% per further +2%
# ---------------------------------------------------------------------------
from execution.fixed_exit_position_monitor import ProfitLadder  # noqa: E402


class TestLadderLevels:
    L = ProfitLadder()

    @pytest.mark.parametrize(
        "peak,level",
        [(-1.0, -2.0), (0.0, -2.0), (3.99, -2.0), (4.0, 0.0), (7.9, 0.0), (8.0, 4.0), (9.9, 4.0),
         (10.0, 5.0), (11.5, 5.0), (12.0, 6.0), (20.0, 10.0)],
    )
    def test_levels(self, peak, level):
        assert self.L.stop_level_pct(peak, 2.0) == pytest.approx(level)


class TestLadderMonitor:
    @pytest.fixture
    def lb(self) -> AsyncMock:
        from unittest.mock import MagicMock
        b = AsyncMock()
        b._filters = {}  # exchange-stop behaviour is covered in test_native_stop_manager.py
        b.lots_to_quantity = MagicMock(side_effect=lambda sym, lots: lots * 1.0)
        return b

    def mon(self, lb, order_manager, peaks_path=None):
        return FixedExitPositionMonitor(lb, order_manager, stop_loss_usdt=999, take_profit_usdt=1.0,
                                        stop_loss_pct=2.0, ladder=ProfitLadder(), peaks_path=peaks_path)

    def pos(self, pnl, side=OrderSide.BUY):
        # 250 units x entry 1.0 = 250 USDT notional -> 1% = 2.5 USDT
        return PositionDetail(symbol="XUSDT", side=side, quantity=250, entry_price=1.0, mark_price=1.0,
                              unrealized_pnl=pnl)

    async def run(self, m, lb, pnls, side=OrderSide.BUY):
        for pnl in pnls:
            lb.get_position_details.return_value = [self.pos(pnl, side)]
            await m._check_once()

    async def test_initial_stop_minus_2pct(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [-4.9])  # -1.96%
        assert submitted(order_manager) == []
        await self.run(m, lb, [-5.0])  # -2%
        assert len(submitted(order_manager)) == 1

    async def test_no_fixed_take_profit(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [10.0, 15.0, 18.0])  # +4%, +6%, +7.2%: still rising, no close
        assert submitted(order_manager) == []

    async def test_breakeven_after_4pct(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [10.0, 2.0])  # hit +4%, back to +0.8%: hold
        assert submitted(order_manager) == []
        await self.run(m, lb, [0.0])  # back to entry -> close
        assert len(submitted(order_manager)) == 1

    async def test_trailing_lock_4pct_after_8pct(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [20.0, 11.0])  # peak +8%, now +4.4%: hold
        assert submitted(order_manager) == []
        await self.run(m, lb, [10.0])  # +4% -> close
        assert len(submitted(order_manager)) == 1

    async def test_trailing_steps(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [30.0, 15.5])  # peak +12% -> stop +6%; now +6.2%: hold
        assert submitted(order_manager) == []
        await self.run(m, lb, [15.0])  # +6% -> close
        assert len(submitted(order_manager)) == 1

    async def test_short_side_same_rules(self, lb, order_manager):
        m = self.mon(lb, order_manager)
        await self.run(m, lb, [10.0, 0.0], side=OrderSide.SELL)
        (sig,) = submitted(order_manager)
        assert sig.side is OrderSide.BUY and sig.reduce_only

    async def test_peak_survives_restart(self, lb, order_manager, tmp_path):
        path = str(tmp_path / "peaks.json")
        m1 = self.mon(lb, order_manager, peaks_path=path)
        await self.run(m1, lb, [20.0])  # reached +8%
        m2 = self.mon(lb, AsyncMock(), peaks_path=path)  # "restart"
        om2 = m2._order_manager
        await self.run(m2, lb, [10.0])  # +4% after restart -> must close at locked +4%
        assert len(submitted(om2)) == 1

    async def test_peak_forgotten_after_close(self, lb, order_manager, tmp_path):
        import json as _json
        path = tmp_path / "peaks.json"
        m = self.mon(lb, order_manager, peaks_path=str(path))
        await self.run(m, lb, [20.0])
        lb.get_position_details.return_value = []
        await m._check_once()
        assert _json.loads(path.read_text()) == {}


# ---------------------------------------------------------------------------
# Daily loss limit in USDT
# ---------------------------------------------------------------------------
from datetime import datetime, timezone  # noqa: E402

from risk.daily_loss_circuit_breaker import DailyLossCircuitBreaker  # noqa: E402

NOW = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)


def cb() -> DailyLossCircuitBreaker:
    b = DailyLossCircuitBreaker(max_daily_drawdown_pct=100.0, starting_equity=1.0, max_daily_loss_usdt=100.0)
    b.reset_session(1400.0, session_date=NOW.date())
    return b


def test_halts_at_100_usdt_loss_from_start():
    b = cb()
    assert b.update(1300.01, now=NOW) is False
    assert b.update(1300.0, now=NOW) is True
    assert b.is_halted and "100.00 USDT" in b.halt_reason


def test_halts_on_100_usdt_giveback_from_peak():
    b = cb()
    b.update(1550.0, now=NOW)  # up 150 on the day
    assert b.update(1460.0, now=NOW) is False  # gave back 90
    assert b.update(1450.0, now=NOW) is True  # gave back 100


def test_new_day_resets():
    b = cb()
    b.update(1290.0, now=NOW)
    assert b.is_halted
    b.update(1290.0, now=NOW.replace(day=25))
    assert not b.is_halted


def test_snapshot_restore_roundtrip():
    b = cb()
    b.update(1250.0, now=NOW)
    snap = b.snapshot()
    b2 = DailyLossCircuitBreaker(max_daily_drawdown_pct=100.0, starting_equity=1.0, max_daily_loss_usdt=100.0)
    from datetime import date
    b2.restore(session_date=date.fromisoformat(snap["session_date"]), starting_equity=snap["starting_equity"],
               peak_equity=snap["peak_equity"], halted=snap["halted"], halt_reason=snap["halt_reason"])
    assert b2.is_halted
