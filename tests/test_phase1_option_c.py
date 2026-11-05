"""BINBOT1 Phase 1 Option C (final): follow direction, young/total slot
limits, 2-min re-entry pause, ladder exit (-10% stop, 20 points behind the
peak from +15%), 3-day max hold, USDT daily halt from the day's start."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.settings import ScannerStrategySettings
from core.enums import OrderSide
from core.event_bus import EventBus
from core.events import BarEvent, Event, SignalEvent
from execution.binance_broker import PositionDetail
from execution.fixed_exit_position_monitor import FixedExitPositionMonitor, ProfitLadder
from risk.daily_loss_circuit_breaker import DailyLossCircuitBreaker
from strategies.funding_momentum_scanner import (
    FundingMomentumScannerStrategy,
    ScannerParams,
    entry_slots_available,
    evaluate_entry,
)

PUMP = dict(price_change_pct=12.0, funding_now=-0.0002, funding_window_start=-0.0001, price_jump_pct=10.0)
DUMP = dict(price_change_pct=-12.0, funding_now=-0.0001, funding_window_start=-0.0003, price_jump_pct=10.0)


class TestDirection:
    def test_follow_pump_goes_long(self):
        assert evaluate_entry(**PUMP) is OrderSide.BUY

    def test_follow_dump_goes_short(self):
        assert evaluate_entry(**DUMP) is OrderSide.SELL

    def test_fade_reverses_both(self):
        assert evaluate_entry(**PUMP, trade_direction="fade") is OrderSide.SELL
        assert evaluate_entry(**DUMP, trade_direction="fade") is OrderSide.BUY

    def test_funding_filter_still_required(self):
        assert evaluate_entry(**{**PUMP, "funding_now": 0.0}) is None
        assert evaluate_entry(**{**DUMP, "funding_window_start": 0.0001}) is None


H = 3600.0


class TestSlots:
    def kw(self, ages, pending=0):
        return dict(open_ages_s=ages, pending_count=pending, max_open_positions=6, max_young_positions=4,
                    young_age_s=24 * H)

    def test_fourth_young_trade_allowed(self):
        assert entry_slots_available(**self.kw([1 * H, 2 * H, 3 * H]))

    def test_fifth_young_trade_blocked(self):
        assert not entry_slots_available(**self.kw([1 * H, 2 * H, 3 * H, 4 * H]))

    def test_old_trades_free_young_slots(self):
        assert entry_slots_available(**self.kw([30 * H, 40 * H, 1 * H, 2 * H, 3 * H]))

    def test_hard_cap_six(self):
        assert not entry_slots_available(**self.kw([30 * H, 40 * H, 50 * H, 60 * H, 1 * H, 2 * H]))

    def test_pending_counts_as_young(self):
        assert not entry_slots_available(**self.kw([1 * H, 2 * H, 3 * H], pending=1))

    def test_young_limit_off(self):
        kw = self.kw([1 * H] * 5)
        kw["max_young_positions"] = 0
        assert entry_slots_available(**kw)


class _Feed:
    def price_change_pct(self, s):
        return 12.0

    def funding_trend(self, s):
        return (-0.0002, -0.0001)

    def latest_price(self, s):
        return 1.0


class _Monitor:
    def __init__(self, open_=None, opened=None, closed=None):
        self._open = open_ or set()
        self._opened = opened or {}
        self._closed = closed or {}

    def open_symbols(self):
        return set(self._open)

    def opened_at(self):
        return dict(self._opened)

    def last_closed_at(self, s):
        return self._closed.get(s)


def _strategy(monitor, **params):
    p = dict(max_open_positions=6, order_notional_usdt=100.0, price_jump_pct=10.0, max_young_positions=4,
             young_age_s=24 * H, reentry_pause_s=120.0)
    p.update(params)
    strat = FundingMomentumScannerStrategy(
        strategy_id="funding-momentum-scanner", event_bus=EventBus(), symbols=["XUSDT"], universe_feed=_Feed(),
        position_monitor=monitor, params=ScannerParams(**p), step_size_lookup=lambda s: 0.1,
    )
    signals: list[SignalEvent] = []

    async def capture(e: Event) -> None:
        signals.append(e)  # type: ignore[arg-type]

    strat._event_bus.publish = capture  # type: ignore[method-assign]
    return strat, signals


BAR = BarEvent(ts=datetime(2026, 11, 5, tzinfo=timezone.utc), symbol="XUSDT", timeframe_s=60, close=1.0)


class TestScannerGating:
    def test_enters_long_on_pump(self):
        strat, sig = _strategy(_Monitor())
        asyncio.run(strat.on_bar(BAR))
        (s,) = sig
        assert s.side is OrderSide.BUY and s.target_quantity == 1000  # 100 USDT / (1.0 x 0.1)

    def test_reentry_pause_blocks_within_2_min(self):
        strat, sig = _strategy(_Monitor(closed={"XUSDT": time.time() - 60}))
        asyncio.run(strat.on_bar(BAR))
        assert sig == []

    def test_reentry_allowed_after_2_min(self):
        strat, sig = _strategy(_Monitor(closed={"XUSDT": time.time() - 125}))
        asyncio.run(strat.on_bar(BAR))
        assert len(sig) == 1

    def test_four_young_positions_block(self):
        now = time.time()
        opened = {f"S{i}USDT": now - i * H for i in range(1, 5)}
        strat, sig = _strategy(_Monitor(open_=set(opened), opened=opened))
        asyncio.run(strat.on_bar(BAR))
        assert sig == []

    def test_old_positions_dont_block(self):
        now = time.time()
        opened = {"AUSDT": now - 30 * H, "BUSDT": now - 2 * H, "CUSDT": now - 3 * H, "DUSDT": now - 4 * H}
        strat, sig = _strategy(_Monitor(open_=set(opened), opened=opened))
        asyncio.run(strat.on_bar(BAR))
        assert len(sig) == 1

    def test_same_symbol_never_doubled(self):
        strat, sig = _strategy(_Monitor(open_={"XUSDT"}, opened={"XUSDT": time.time()}))
        asyncio.run(strat.on_bar(BAR))
        assert sig == []


LADDER = ProfitLadder(breakeven_trigger_pct=15, breakeven_lock_pct=-5, trail_start_pct=15, trail_start_lock_pct=-5,
                      trail_step_pct=1, trail_step_lock_pct=1)


class TestLadderLevels:
    @pytest.mark.parametrize(
        "peak,level",
        [(0, -10), (9.9, -10), (14.99, -10), (15, -5), (15.9, -5), (16, -4), (20, 0), (25, 5), (30, 10),
         (50, 30), (100, 80)],
    )
    def test_twenty_points_behind_peak_from_15(self, peak, level):
        assert LADDER.stop_level_pct(peak, 10.0) == pytest.approx(level)


def _detail(symbol="XUSDT", side=OrderSide.BUY, pnl=0.0):
    # 1000 lots x 0.1 = 100 units @ 1.0 = 100 USDT notional
    return PositionDetail(symbol=symbol, side=side, quantity=1000, entry_price=1.0, mark_price=1.0,
                          unrealized_pnl=pnl)


@pytest.fixture
def broker():
    b = AsyncMock()
    b._filters = {}
    b.lots_to_quantity = MagicMock(side_effect=lambda sym, lots: lots * 0.1)
    return b


@pytest.fixture
def om():
    return AsyncMock()


def _sent(om):
    return [c.args[0] for c in om.submit.await_args_list]


def _monitor(broker, om, tmp_path, **kw):
    return FixedExitPositionMonitor(broker, om, stop_loss_usdt=10.0, take_profit_usdt=1e12, stop_loss_pct=10.0,
                                    ladder=LADDER, peaks_path=str(tmp_path / "peaks.json"),
                                    opened_path=str(tmp_path / "opened.json"), **kw)


class TestLadderMonitor:
    async def test_stop_at_minus_10_usdt(self, broker, om, tmp_path):
        m = _monitor(broker, om, tmp_path)
        broker.get_position_details.return_value = [_detail(pnl=-9.9)]
        await m._check_once()
        assert _sent(om) == []
        broker.get_position_details.return_value = [_detail(pnl=-10.0)]
        await m._check_once()
        (s,) = _sent(om)
        assert s.side is OrderSide.SELL and s.reduce_only

    async def test_no_take_profit_lets_winner_run(self, broker, om, tmp_path):
        m = _monitor(broker, om, tmp_path)
        for pnl in (5, 12, 14, 9, 3, -2):  # never reached +15 -> stop stays at -10
            broker.get_position_details.return_value = [_detail(pnl=pnl)]
            await m._check_once()
        assert _sent(om) == []

    async def test_trail_after_peak_30_closes_at_plus_10(self, broker, om, tmp_path):
        m = _monitor(broker, om, tmp_path)
        for pnl in (10, 22, 30, 18, 11):
            broker.get_position_details.return_value = [_detail(pnl=pnl)]
            await m._check_once()
        assert _sent(om) == []
        broker.get_position_details.return_value = [_detail(pnl=10.0)]
        await m._check_once()
        (s,) = _sent(om)
        assert s.side is OrderSide.SELL

    async def test_peak_15_moves_stop_to_minus_5(self, broker, om, tmp_path):
        m = _monitor(broker, om, tmp_path)
        for pnl in (15.2, 0, -4.9):
            broker.get_position_details.return_value = [_detail(pnl=pnl)]
            await m._check_once()
        assert _sent(om) == []
        broker.get_position_details.return_value = [_detail(pnl=-5.0)]
        await m._check_once()
        assert len(_sent(om)) == 1

    async def test_short_same_rules(self, broker, om, tmp_path):
        m = _monitor(broker, om, tmp_path)
        broker.get_position_details.return_value = [_detail(side=OrderSide.SELL, pnl=-10.0)]
        await m._check_once()
        (s,) = _sent(om)
        assert s.side is OrderSide.BUY and s.reduce_only

    async def test_max_hold_3_days(self, broker, om, tmp_path, monkeypatch):
        m = _monitor(broker, om, tmp_path, max_hold_s=72 * H)
        t0 = 1_800_000_000.0
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: t0)
        broker.get_position_details.return_value = [_detail(pnl=3.0)]
        await m._check_once()
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: t0 + 72 * H - 60)
        await m._check_once()
        assert _sent(om) == []
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: t0 + 72 * H)
        await m._check_once()
        (s,) = _sent(om)
        assert s.side is OrderSide.SELL

    async def test_open_time_survives_restart(self, broker, om, tmp_path, monkeypatch):
        t0 = 1_800_000_000.0
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: t0)
        m = _monitor(broker, om, tmp_path, max_hold_s=72 * H)
        broker.get_position_details.return_value = [_detail(pnl=1.0)]
        await m._check_once()
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: t0 + 72 * H + 1)
        m2 = _monitor(broker, om, tmp_path, max_hold_s=72 * H)  # restarted process
        await m2._check_once()
        assert len(_sent(om)) == 1

    async def test_open_and_close_times_exposed(self, broker, om, tmp_path, monkeypatch):
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: 1000.0)
        m = _monitor(broker, om, tmp_path)
        broker.get_position_details.return_value = [_detail(pnl=1.0)]
        await m._check_once()
        assert m.opened_at() == {"XUSDT": 1000.0} and m.last_closed_at("XUSDT") is None
        monkeypatch.setattr("execution.fixed_exit_position_monitor.time.time", lambda: 1500.0)
        broker.get_position_details.return_value = []
        await m._check_once()
        assert m.opened_at() == {} and m.last_closed_at("XUSDT") == 1500.0


class TestDailyHalt:
    def _cb(self):
        cb = DailyLossCircuitBreaker(max_daily_drawdown_pct=100.0, starting_equity=1.0, max_daily_loss_usdt=200.0,
                                     include_peak_giveback=False)
        cb.reset_session(2500.0, session_date=datetime(2026, 11, 5, tzinfo=timezone.utc).date())
        return cb

    def test_halts_at_200_below_day_start(self):
        cb = self._cb()
        now = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
        assert not cb.update(2301.0, now=now)
        assert cb.update(2300.0, now=now) and cb.is_halted

    def test_giving_back_open_profit_does_not_halt(self):
        cb = self._cb()
        now = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
        cb.update(2800.0, now=now)  # +300 on the day
        assert not cb.update(2550.0, now=now)  # gave back 250 from the high, still +50 on the day
        assert not cb.is_halted


class TestSettings:
    def test_defaults_keep_old_behaviour(self):
        s = ScannerStrategySettings(_env_file=None)
        assert not s.ladder_enabled and s.max_hold_hours == 0 and s.max_young_positions == 0
        assert s.reentry_pause_s == 0 and s.max_daily_loss_usdt == 0
        assert s.order_notional_usdt == s.margin_usdt * s.leverage

    def test_final_setup_values(self, monkeypatch):
        env = dict(SCANNER_TRADE_DIRECTION="follow", SCANNER_PRICE_JUMP_PCT="10", SCANNER_PRICE_JUMP_WINDOW_MIN="20",
                   SCANNER_FUNDING_TREND_WINDOW_MIN="20", SCANNER_LEVERAGE="3", SCANNER_NOTIONAL_USDT="100",
                   SCANNER_LADDER_ENABLED="true", SCANNER_STOP_LOSS_PCT="10", SCANNER_MAX_HOLD_HOURS="72",
                   SCANNER_MAX_OPEN_POSITIONS="6", SCANNER_MAX_YOUNG_POSITIONS="4", SCANNER_REENTRY_PAUSE_S="120",
                   SCANNER_MAX_DAILY_LOSS_USDT="200")
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        s = ScannerStrategySettings(_env_file=None)
        assert s.order_notional_usdt == 100.0 and s.leverage == 3 and s.ladder_enabled
        assert (s.ladder_trail_start_pct, s.ladder_trail_start_lock_pct) == (15.0, -5.0)
        assert s.max_hold_hours == 72 and s.max_young_positions == 4 and s.max_open_positions == 6

    def test_bad_direction_rejected(self):
        with pytest.raises(ValueError):
            ScannerStrategySettings(_env_file=None, trade_direction="sideways")
