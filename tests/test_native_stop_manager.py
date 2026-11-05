"""Exchange-side stop that follows the profit ladder, against a fake Binance
algo-order API that enforces the real -4130 rule (one closePosition stop
per direction)."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.enums import OrderSide
from execution.binance_broker import PositionDetail
from execution.fixed_exit_position_monitor import FixedExitPositionMonitor, ProfitLadder
from execution.native_stop_manager import NativeStopManager, is_bot_stop, stop_price_for_level


@dataclass
class Filt:
    tick_size: float = 0.0001
    price_precision: int = 4


class FakeAlgoClient:
    def __init__(self):
        self.orders: list[dict] = []
        self.next_id = 1000
        self.calls: list[str] = []

    async def futures_create_order(self, **p):
        self.calls.append("create")
        for o in self.orders:
            if (o["symbol"] == p["symbol"] and o["side"] == p["side"] and o["closePosition"] == "true"
                    and o["orderType"] == p["type"]):
                raise Exception("APIError(code=-4130): An open stop or take profit order with GTE and "
                                "closePosition in the direction is existing.")
        self.next_id += 1
        o = {"algoId": self.next_id, "clientAlgoId": p.get("clientAlgoId"), "symbol": p["symbol"],
             "side": p["side"], "orderType": p["type"], "triggerPrice": str(p["stopPrice"]),
             "closePosition": "true" if p.get("closePosition") else "false", "algoStatus": "NEW"}
        self.orders.append(o)
        return {"algoId": o["algoId"], "clientAlgoId": o["clientAlgoId"]}

    async def futures_get_open_algo_orders(self, **p):
        self.calls.append("list")
        sym = p.get("symbol")
        return [dict(o) for o in self.orders if sym is None or o["symbol"] == sym]

    async def futures_cancel_algo_order(self, **p):
        self.calls.append("cancel")
        self.orders = [o for o in self.orders if o["algoId"] != p["algoId"]]
        return {"code": "200"}

    def stops(self, symbol):
        return [float(o["triggerPrice"]) for o in self.orders
                if o["symbol"] == symbol and o["orderType"] == "STOP_MARKET"]

    def tps(self, symbol):
        return [float(o["triggerPrice"]) for o in self.orders
                if o["symbol"] == symbol and o["orderType"] == "TAKE_PROFIT_MARKET"]


def fake_broker() -> MagicMock:
    b = MagicMock()
    b.client = FakeAlgoClient()
    b._filters = {"XUSDT": Filt(), "YUSDT": Filt()}
    b.lots_to_quantity = MagicMock(side_effect=lambda sym, lots: lots * 1.0)
    b.get_position_details = AsyncMock()
    return b


def pos(pnl, *, side=OrderSide.BUY, symbol="XUSDT", entry=1.0, lots=250):
    return PositionDetail(symbol=symbol, side=side, quantity=lots, entry_price=entry, mark_price=entry,
                          unrealized_pnl=pnl)


def monitor(b, peaks_path=None):
    return FixedExitPositionMonitor(b, AsyncMock(), stop_loss_usdt=999, take_profit_usdt=1.0, stop_loss_pct=2.0,
                                    ladder=ProfitLadder(), peaks_path=peaks_path)


async def run(m, b, pnls, **kw):
    for pnl in pnls:
        b.get_position_details.return_value = [pos(pnl, **kw)]
        await m._check_once()


def test_price_math_and_rounding():
    assert stop_price_for_level(1.0, -2, is_long=True, tick=0.0001, precision=4) == pytest.approx(0.98)
    assert stop_price_for_level(1.0, 0, is_long=True, tick=0.0001, precision=4) == pytest.approx(1.0)
    assert stop_price_for_level(1.0, 4, is_long=True, tick=0.0001, precision=4) == pytest.approx(1.04)
    assert stop_price_for_level(1.0, -2, is_long=False, tick=0.0001, precision=4) == pytest.approx(1.02)
    assert stop_price_for_level(1.0, 4, is_long=False, tick=0.0001, precision=4) == pytest.approx(0.96)
    # long rounds UP, short rounds DOWN (never a worse exit than the level)
    assert stop_price_for_level(1.00003, 0, is_long=True, tick=0.0001, precision=4) == pytest.approx(1.0001)
    assert stop_price_for_level(1.00007, 0, is_long=False, tick=0.0001, precision=4) == pytest.approx(1.0)


def test_is_bot_stop_ignores_manual_take_profits():
    assert is_bot_stop({"orderType": "STOP_MARKET", "closePosition": "true"})
    assert is_bot_stop({"type": "STOP_MARKET", "closePosition": True})
    assert not is_bot_stop({"orderType": "TAKE_PROFIT_MARKET", "closePosition": "true"})
    assert not is_bot_stop({"orderType": "STOP_MARKET", "closePosition": "false"})


async def test_initial_stop_placed_at_minus_2pct():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0])
    assert b.client.stops("XUSDT") == [pytest.approx(0.98)]


async def test_stop_moves_to_breakeven_then_trails_on_exchange():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0])                      # -2%
    await run(m, b, [10.0])                     # peak +4% -> breakeven
    assert b.client.stops("XUSDT") == [pytest.approx(1.0)]
    await run(m, b, [20.0])                     # peak +8% -> +4%
    assert b.client.stops("XUSDT") == [pytest.approx(1.04)]
    await run(m, b, [25.0])                     # peak +10% -> +5%
    assert b.client.stops("XUSDT") == [pytest.approx(1.05)]
    await run(m, b, [21.0])                     # pullback: stop must NOT move down
    assert b.client.stops("XUSDT") == [pytest.approx(1.05)]


async def test_short_stop_moves_down_in_price():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0, 10.0, 20.0], side=OrderSide.SELL)
    assert b.client.stops("XUSDT") == [pytest.approx(0.96)]


async def test_no_extra_calls_when_level_unchanged():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0])
    creates = b.client.calls.count("create")
    await run(m, b, [1.0, 2.0, 3.0])
    assert b.client.calls.count("create") == creates


async def test_replaces_leftover_stop_after_restart():
    b = fake_broker()
    # stop left over from a previous run / the old bot at -2%
    await b.client.futures_create_order(symbol="XUSDT", side="SELL", type="STOP_MARKET", stopPrice=0.98,
                                        closePosition=True, workingType="MARK_PRICE")
    m = monitor(b)
    await run(m, b, [10.0])  # already at +4% -> breakeven, must replace (no -4130)
    assert b.client.stops("XUSDT") == [pytest.approx(1.0)]


async def test_peak_restored_after_restart_puts_stop_at_locked_level(tmp_path):
    b = fake_broker()
    path = str(tmp_path / "peaks.json")
    await run(monitor(b, path), b, [20.0])   # reached +8% -> +4% lock
    m2 = monitor(b, path)                    # restart
    await run(m2, b, [15.0])                 # now +6%
    assert b.client.stops("XUSDT") == [pytest.approx(1.04)]


async def test_stop_cancelled_when_bot_closes_position():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0, 10.0])
    await run(m, b, [0.0])                   # back to entry -> software close
    assert b.client.stops("XUSDT") == []
    assert len([c for c in m._order_manager.submit.await_args_list]) == 1


async def test_stop_cancelled_when_position_closed_manually():
    b = fake_broker()
    m = monitor(b)
    await run(m, b, [0.0])
    b.get_position_details.return_value = []
    await m._check_once()
    assert b.client.stops("XUSDT") == []


async def test_orphan_stops_swept():
    b = fake_broker()
    await b.client.futures_create_order(symbol="YUSDT", side="BUY", type="STOP_MARKET", stopPrice=1.1,
                                        closePosition=True, workingType="MARK_PRICE")
    await b.client.futures_create_order(symbol="YUSDT", side="SELL", type="TAKE_PROFIT_MARKET", stopPrice=0.9,
                                        closePosition=True, workingType="MARK_PRICE")
    m = monitor(b)
    await run(m, b, [0.0])  # only XUSDT open
    remaining = [(o["symbol"], o["orderType"]) for o in b.client.orders]
    assert ("YUSDT", "STOP_MARKET") not in remaining
    assert ("YUSDT", "TAKE_PROFIT_MARKET") in remaining  # manual TP left alone


async def test_place_failure_backs_off_and_software_still_protects():
    b = fake_broker()

    async def boom(**p):
        raise Exception("network")

    b.client.futures_create_order = boom
    m = monitor(b)
    await run(m, b, [0.0, -5.0])  # -2% hit in software
    assert len(m._order_manager.submit.await_args_list) == 1


async def test_manager_only_moves_up():
    b = fake_broker()
    mgr = NativeStopManager(b)
    assert await mgr.sync(pos(0), 0.0) is True
    assert await mgr.sync(pos(0), -2.0) is False
    assert mgr.current_level("XUSDT") == 0.0



# ---------------------------------------------------------------------------
# Straight 1:2 mode: SL -2% and TP +4% both placed on Binance at entry
# ---------------------------------------------------------------------------

def fixed_monitor(b):
    return FixedExitPositionMonitor(b, AsyncMock(), stop_loss_usdt=999, take_profit_usdt=999,
                                    stop_loss_pct=2.0, take_profit_pct=4.0, ladder=None)


async def test_fixed_places_sl_and_tp_on_binance_long():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [0.0])
    assert b.client.stops("XUSDT") == [pytest.approx(0.98)]
    assert b.client.tps("XUSDT") == [pytest.approx(1.04)]


async def test_fixed_places_sl_and_tp_on_binance_short():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [0.0], side=OrderSide.SELL)
    assert b.client.stops("XUSDT") == [pytest.approx(1.02)]
    assert b.client.tps("XUSDT") == [pytest.approx(0.96)]


async def test_fixed_usdt_amounts_are_5_and_10_on_250():
    b = fake_broker()
    m = fixed_monitor(b)
    p = pos(0.0)  # 250 USDT notional
    assert m.stop_loss_amount(p) == pytest.approx(5.0)
    assert m.take_profit_amount(p) == pytest.approx(10.0)


async def test_fixed_orders_placed_once_not_every_poll():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [0.0])
    creates = b.client.calls.count("create")
    await run(m, b, [3.0, 6.0, 9.0, -3.0])
    assert b.client.calls.count("create") == creates
    assert len(b.client.orders) == 2


async def test_fixed_no_trailing_stop_stays_at_minus_2():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [0.0, 9.5, 2.0])  # +3.8% then back to +0.8%: stop must not move
    assert b.client.stops("XUSDT") == [pytest.approx(0.98)]
    assert m._order_manager.submit.await_count == 0


async def test_fixed_software_tp_at_plus_4pct():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [9.9])
    assert m._order_manager.submit.await_count == 0
    await run(m, b, [10.0])  # +4% = 10 USDT on 250
    assert m._order_manager.submit.await_count == 1
    assert b.client.orders == []  # both exchange orders removed on close


async def test_fixed_software_sl_at_minus_2pct():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [-5.0])
    assert m._order_manager.submit.await_count == 1


async def test_fixed_replaces_own_tp_after_restart_keeps_manual_tp():
    b = fake_broker()
    # binbot2's own TP left over from a previous run
    await b.client.futures_create_order(symbol="XUSDT", side="SELL", type="TAKE_PROFIT_MARKET", stopPrice=1.04,
                                        closePosition=True, workingType="MARK_PRICE", clientAlgoId="b2tp-old")
    m = fixed_monitor(b)
    await run(m, b, [0.0])
    assert b.client.tps("XUSDT") == [pytest.approx(1.04)]
    assert len(b.client.tps("XUSDT")) == 1


async def test_fixed_manual_tp_is_not_cancelled():
    b = fake_broker()
    await b.client.futures_create_order(symbol="XUSDT", side="SELL", type="TAKE_PROFIT_MARKET", stopPrice=1.10,
                                        closePosition=True, workingType="MARK_PRICE", clientAlgoId="manual-app")
    m = fixed_monitor(b)
    await run(m, b, [0.0])
    assert b.client.tps("XUSDT") == [pytest.approx(1.10)]  # yours kept; bot still enforces +4% itself
    await run(m, b, [10.0])
    assert m._order_manager.submit.await_count == 1


async def test_fixed_manual_close_cancels_both_orders():
    b = fake_broker()
    m = fixed_monitor(b)
    await run(m, b, [0.0])
    b.get_position_details.return_value = []
    await m._check_once()
    assert b.client.orders == []


async def test_orphan_bot_tp_swept():
    b = fake_broker()
    await b.client.futures_create_order(symbol="YUSDT", side="SELL", type="TAKE_PROFIT_MARKET", stopPrice=1.04,
                                        closePosition=True, workingType="MARK_PRICE", clientAlgoId="b2tp-YUSDT-1")
    m = fixed_monitor(b)
    await run(m, b, [0.0])  # only XUSDT open
    assert b.client.tps("YUSDT") == []
