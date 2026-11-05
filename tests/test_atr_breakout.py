from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from config.binbot2_settings import ATRBreakoutSettings
from core.enums import OrderSide
from core.event_bus import EventBus
from core.events import BarEvent, Event, EventType, SignalEvent
from data.kline_poller import Candle, KlinePoller, closed_rows
from strategies.atr_breakout import (
    ATRBreakoutStrategy,
    ATRParams,
    average_true_range,
    evaluate_atr_breakout,
    required_candles,
)

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
PARAMS = ATRParams()  # defaults: ATR14, mean20, 2.0x, 100k USDT floor, 2x volume, 0.7 close position


def candle(i: int, close: float, *, high: float | None = None, low: float | None = None,
           open_: float | None = None, qv: float = 200_000.0) -> Candle:
    o = close if open_ is None else open_
    return Candle(
        open_time=T0 + timedelta(minutes=5 * i),
        open=o,
        high=high if high is not None else max(o, close) + 0.5,
        low=low if low is not None else min(o, close) - 0.5,
        close=close,
        volume=qv / close,
        quote_volume=qv,
    )


def quiet_history(n: int = 30, price: float = 100.0) -> list[Candle]:
    """Flat-ish market: closes alternate 99.8/100.2, range ~1.4 -> ATR ~1.4."""
    return [candle(i, price + (0.2 if i % 2 else -0.2)) for i in range(n)]


def with_trigger(history: list[Candle], close: float, *, qv: float = 1_000_000.0,
                 high: float | None = None, low: float | None = None, open_: float = 100.0) -> list[Candle]:
    i = len(history)
    return history + [candle(i, close, high=high, low=low, open_=open_, qv=qv)]


class TestATRMath:
    def test_true_range_uses_gaps(self):
        a = Candle(T0, 10, 11, 9, 10, 1, 10)
        b = Candle(T0 + timedelta(minutes=5), 14, 15, 14, 15, 1, 15)  # gap up: TR = 15 - 10
        assert average_true_range([a, b]) == pytest.approx(5.0)

    def test_needs_enough_candles(self):
        assert evaluate_atr_breakout(quiet_history(required_candles(PARAMS) - 1), PARAMS) is None


class TestSignals:
    def test_long_breakout(self):
        sig = evaluate_atr_breakout(with_trigger(quiet_history(), 104.0, high=104.2, low=99.9), PARAMS)
        assert sig is not None and sig.side is OrderSide.BUY
        assert sig.deviation_atr >= 2.0

    def test_short_breakout(self):
        sig = evaluate_atr_breakout(with_trigger(quiet_history(), 96.0, high=100.1, low=95.8), PARAMS)
        assert sig is not None and sig.side is OrderSide.SELL

    def test_normal_move_no_signal(self):
        assert evaluate_atr_breakout(with_trigger(quiet_history(), 101.0, high=101.2, low=99.9), PARAMS) is None

    def test_threshold_scales_with_volatility(self):
        # Same +4 move, but on a symbol whose normal range is ~6 -> not a breakout.
        wild = [candle(i, 100 + (3 if i % 2 else -3), high=104, low=96) for i in range(30)]
        assert evaluate_atr_breakout(with_trigger(wild, 104.0, high=104.2, low=99.9), PARAMS) is None

    def test_liquidity_floor_blocks_thin_symbol(self):
        thin = [candle(i, c.close, qv=1_000) for i, c in enumerate(quiet_history())]
        # 20x volume spike, but only 20k USDT traded — below the 100k floor.
        assert evaluate_atr_breakout(with_trigger(thin, 104.0, qv=20_000, high=104.2, low=99.9), PARAMS) is None

    def test_volume_spike_required(self):
        # Breakout on ordinary (1x) volume -> rejected.
        assert evaluate_atr_breakout(with_trigger(quiet_history(), 104.0, qv=200_000, high=104.2, low=99.9), PARAMS) is None

    def test_wick_filter_rejects_spike_and_reject(self):
        # Closed at 104 but the high was 110 -> close in bottom part of range.
        assert evaluate_atr_breakout(with_trigger(quiet_history(), 104.0, high=110.0, low=99.9), PARAMS) is None

    def test_not_fresh_breakout_rejected(self):
        hist = with_trigger(quiet_history(), 104.0, high=104.2, low=99.9)  # first breakout candle
        hist = with_trigger(hist, 107.0, high=107.2, low=103.9, open_=104.0)  # continuation
        assert evaluate_atr_breakout(hist, PARAMS) is None

    def test_direction_toggles(self):
        hist = with_trigger(quiet_history(), 104.0, high=104.2, low=99.9)
        assert evaluate_atr_breakout(hist, ATRParams(allow_long=False)) is None


class TestKlinePoller:
    def _row(self, i: int, close: float, closed: bool = True) -> list:
        open_ms = int((T0 + timedelta(minutes=5 * i)).timestamp() * 1000)
        close_ms = open_ms + 300_000 - 1
        return [open_ms, "100", "101", "99", str(close), "10", close_ms, "1000", 5, "5", "500", "0"]

    def test_closed_rows_drops_forming_candle(self):
        rows = [self._row(0, 100), self._row(1, 101)]
        now_ms = rows[1][0] + 1000  # inside candle 1
        assert len(closed_rows(rows, now_ms)) == 1

    def test_ingest_dedupes(self):
        poller = KlinePoller(client=None, event_bus=EventBus(), symbols=["XUSDT"], history_candles=5)  # type: ignore[arg-type]
        now_ms = self._row(3, 0)[0]
        new = poller.ingest("XUSDT", [self._row(0, 100), self._row(1, 101), self._row(2, 102)], now_ms=now_ms)
        assert len(new) == 3
        again = poller.ingest("XUSDT", [self._row(1, 101), self._row(2, 102), self._row(3, 103)], now_ms=now_ms)
        assert again == []  # candle 3 still forming, 1-2 already held
        assert [c.close for c in poller.candles("XUSDT")] == [100, 101, 102]
        assert poller.candles("XUSDT")[0].quote_volume == 1000.0

    def test_history_is_capped(self):
        poller = KlinePoller(client=None, event_bus=EventBus(), symbols=["XUSDT"], history_candles=3)  # type: ignore[arg-type]
        poller.ingest("XUSDT", [self._row(i, 100 + i) for i in range(6)], now_ms=self._row(7, 0)[0])
        assert [c.close for c in poller.candles("XUSDT")] == [103, 104, 105]


class _FakePoller:
    def __init__(self, candles: list[Candle]) -> None:
        self._c = candles

    def candles(self, symbol: str) -> list[Candle]:
        return self._c


class _FakeMonitor:
    def __init__(self, open_syms: set[str] | None = None) -> None:
        self._open = open_syms or set()

    def open_symbols(self) -> set[str]:
        return set(self._open)


def _make_strategy(candles, monitor=None, **kw):
    bus = EventBus()
    signals: list[SignalEvent] = []

    async def capture(e: Event) -> None:
        signals.append(e)  # type: ignore[arg-type]

    strat = ATRBreakoutStrategy(
        strategy_id="binbot2-atr",
        event_bus=bus,
        symbols=["XUSDT", "YUSDT"],
        kline_poller=_FakePoller(candles),  # type: ignore[arg-type]
        position_monitor=monitor or _FakeMonitor(),  # type: ignore[arg-type]
        params=PARAMS,
        candle_interval_s=300,
        max_open_positions=kw.get("max_open", 4),
        order_notional_usdt=750.0,
        symbol_cooldown_s=3600.0,
        step_size_lookup=lambda s: 0.1,
    )
    # Bypass the async bus: capture published signals directly.
    strat._event_bus.publish = capture  # type: ignore[method-assign]
    return strat, signals


def _bar(symbol: str, tf: int = 300) -> BarEvent:
    return BarEvent(ts=T0, symbol=symbol, timeframe_s=tf, close=104.0)


class TestStrategy:
    breakout = with_trigger(quiet_history(), 104.0, high=104.2, low=99.9)

    def test_emits_signal_with_lots(self):
        strat, sigs = _make_strategy(self.breakout)
        asyncio.run(strat.on_bar(_bar("XUSDT")))
        assert len(sigs) == 1
        assert sigs[0].side is OrderSide.BUY
        assert sigs[0].target_quantity == 72  # floor(750 / (104 * 0.1))
        assert sigs[0].strategy_id == "binbot2-atr"

    def test_ignores_one_minute_bars(self):
        strat, sigs = _make_strategy(self.breakout)
        asyncio.run(strat.on_bar(_bar("XUSDT", tf=60)))
        assert sigs == []

    def test_cooldown_blocks_repeat(self):
        strat, sigs = _make_strategy(self.breakout)
        asyncio.run(strat.on_bar(_bar("XUSDT")))
        strat._pending_entries.clear()
        asyncio.run(strat.on_bar(_bar("XUSDT")))
        assert len(sigs) == 1

    def test_max_open_positions(self):
        strat, sigs = _make_strategy(self.breakout, monitor=_FakeMonitor({"A", "B"}), max_open=2)
        asyncio.run(strat.on_bar(_bar("XUSDT")))
        assert sigs == []

    def test_skips_symbol_already_held(self):
        strat, sigs = _make_strategy(self.breakout, monitor=_FakeMonitor({"XUSDT"}))
        asyncio.run(strat.on_bar(_bar("XUSDT")))
        assert sigs == []


def test_settings_interval_seconds():
    assert ATRBreakoutSettings(candle_interval="5m").interval_seconds == 300
    assert ATRBreakoutSettings(candle_interval="15m").interval_seconds == 900
    assert ATRBreakoutSettings(candle_interval="1h").interval_seconds == 3600


class TestPollerRateLimit:
    def test_rate_limit_detection(self):
        class E(Exception):
            def __init__(self, status_code=None, code=None):
                self.status_code, self.code = status_code, code
        assert KlinePoller.is_rate_limit_error(E(status_code=429))
        assert KlinePoller.is_rate_limit_error(E(status_code=418))
        assert KlinePoller.is_rate_limit_error(E(code=-1003))
        assert not KlinePoller.is_rate_limit_error(E(status_code=400, code=-1121))

    def test_backs_off_after_ban(self):
        calls = []

        class BanClient:
            async def futures_klines(self, **kw):
                calls.append(kw["symbol"])
                e = Exception("banned")
                e.status_code = 418  # type: ignore[attr-defined]
                raise e

        poller = KlinePoller(BanClient(), EventBus(), [f"S{i}USDT" for i in range(20)],  # type: ignore[arg-type]
                             concurrency=1, max_requests_per_s=0)

        async def run():
            return await asyncio.gather(*(poller._fetch_and_store(s, limit=3, publish=False) for s in poller._symbols))

        results = asyncio.run(run())
        assert not any(results)
        assert len(calls) == 1  # first ban response stops every later request
