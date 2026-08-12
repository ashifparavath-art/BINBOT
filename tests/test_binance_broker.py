"""Pure, offline tests for the Binance adapter's quantity/price rounding.

These are the functions where a bug would most directly translate into
"the bot sent the wrong size to a real exchange," so they get dedicated
coverage independent of any network access.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from execution.binance_broker import BinanceFuturesBroker, SymbolFilters, round_step


class TestRoundStep:
    def test_rounds_down_to_nearest_step(self):
        assert round_step(0.7345, 0.001) == pytest.approx(0.734)

    def test_exact_multiple_is_unchanged(self):
        assert round_step(1.5, 0.5) == pytest.approx(1.5)

    def test_never_rounds_up(self):
        # a naive round() here would give 0.005, overshooting the input
        assert round_step(0.0049999, 0.001) == pytest.approx(0.004)

    def test_zero_step_is_a_no_op(self):
        assert round_step(1.23456, 0.0) == 1.23456


@pytest.fixture
def broker() -> BinanceFuturesBroker:
    b = BinanceFuturesBroker(
        api_key="test", api_secret="test", testnet=True, symbols=["BTCUSDT", "ETHUSDT"]
    )
    b._filters["BTCUSDT"] = SymbolFilters(
        symbol="BTCUSDT",
        tick_size=0.10,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
        quantity_precision=3,
        price_precision=1,
    )
    b._filters["ETHUSDT"] = SymbolFilters(
        symbol="ETHUSDT",
        tick_size=0.01,
        step_size=0.01,
        min_qty=0.01,
        min_notional=5.0,
        quantity_precision=2,
        price_precision=2,
    )
    return b


class TestLotConversion:
    def test_lots_to_quantity_btc(self, broker: BinanceFuturesBroker):
        # 734 lots * 0.001 step = 0.734 BTC
        assert broker.lots_to_quantity("BTCUSDT", 734) == pytest.approx(0.734)

    def test_quantity_to_lots_btc(self, broker: BinanceFuturesBroker):
        assert broker.quantity_to_lots("BTCUSDT", 0.734) == 734

    def test_round_trip_is_stable(self, broker: BinanceFuturesBroker):
        for lots in (1, 5, 100, 1234, 99999):
            qty = broker.lots_to_quantity("BTCUSDT", lots)
            assert broker.quantity_to_lots("BTCUSDT", qty) == lots

    def test_different_symbols_have_independent_step_sizes(self, broker: BinanceFuturesBroker):
        assert broker.get_step_size("BTCUSDT") == pytest.approx(0.001)
        assert broker.get_step_size("ETHUSDT") == pytest.approx(0.01)
        # the same lot count means a different real quantity per symbol
        assert broker.lots_to_quantity("BTCUSDT", 100) != broker.lots_to_quantity("ETHUSDT", 100)

    def test_zero_lots_is_zero_quantity(self, broker: BinanceFuturesBroker):
        assert broker.lots_to_quantity("BTCUSDT", 0) == 0.0


class TestLeverageCapping:
    """Some symbols cap out below the requested leverage (Binance error
    -4028). The broker should fall back to that symbol's actual max rather
    than refusing to trade it, and never repeat the failed call or the
    bracket lookup for the same symbol again."""

    def _mock_client(self, broker: BinanceFuturesBroker) -> AsyncMock:
        client = AsyncMock()
        broker._client = client
        return client

    async def test_falls_back_to_symbol_max_leverage(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_change_leverage.side_effect = [Exception("APIError(code=-4028): Leverage 15 is not valid"), None]
        client.futures_leverage_bracket.return_value = [{"brackets": [{"initialLeverage": 10}]}]

        await broker._configure_leverage_and_margin("BICOUSDT", leverage=15, margin_type="CROSSED")

        assert broker._configured_leverage_margin["BICOUSDT"] == (10, "CROSSED")
        assert client.futures_change_leverage.call_count == 2
        client.futures_change_leverage.assert_any_call(symbol="BICOUSDT", leverage=10)

    async def test_reraises_when_bracket_lookup_also_fails(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        original_error = Exception("APIError(code=-4028): Leverage 15 is not valid")
        client.futures_change_leverage.side_effect = original_error
        client.futures_leverage_bracket.side_effect = Exception("network error")

        with pytest.raises(Exception, match="Leverage 15 is not valid"):
            await broker._configure_leverage_and_margin("BICOUSDT", leverage=15, margin_type="CROSSED")
        assert "BICOUSDT" not in broker._configured_leverage_margin

    async def test_second_call_does_not_repeat_api_calls(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_change_leverage.side_effect = [Exception("APIError(code=-4028): Leverage 15 is not valid"), None]
        client.futures_leverage_bracket.return_value = [{"brackets": [{"initialLeverage": 10}]}]

        await broker._configure_leverage_and_margin("BICOUSDT", leverage=15, margin_type="CROSSED")
        await broker._configure_leverage_and_margin("BICOUSDT", leverage=15, margin_type="CROSSED")

        assert client.futures_change_leverage.call_count == 2  # not 3 or 4
        assert client.futures_leverage_bracket.call_count == 1

    async def test_no_fallback_needed_when_leverage_is_valid(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)

        await broker._configure_leverage_and_margin("BTCUSDT", leverage=2, margin_type="ISOLATED")

        assert broker._configured_leverage_margin["BTCUSDT"] == (2, "ISOLATED")
        client.futures_leverage_bracket.assert_not_called()


class TestNativeStopLoss:
    """A native stop must never let the triggered loss exceed the
    configured cap — the whole point is a hard ceiling, so the rounding
    direction (up for a long's stop, down for a short's) is exactly what's
    under test here, not just that a price was produced."""

    def _mock_client(self, broker: BinanceFuturesBroker) -> AsyncMock:
        client = AsyncMock()
        broker._client = client
        return client

    async def test_long_stop_price_rounds_up_never_exceeding_max_loss(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_create_order.return_value = {"orderId": 555}

        order_id = await broker.place_stop_loss(
            "BTCUSDT", is_long=True, entry_price=65000.0, quantity_lots=15, max_loss_usdt=200.0
        )

        assert order_id == "555"
        call = client.futures_create_order.call_args.kwargs
        assert call["symbol"] == "BTCUSDT"
        assert call["side"] == "SELL"  # closes a long
        assert call["type"] == "STOP_MARKET"
        assert call["closePosition"] is True
        real_qty = 15 * 0.001  # BTCUSDT step_size
        triggered_loss = (65000.0 - call["stopPrice"]) * real_qty
        assert triggered_loss <= 200.0 + 1e-6
        assert triggered_loss == pytest.approx(200.0, abs=0.01)  # rounded, not padded far under

    async def test_short_stop_price_rounds_down_never_exceeding_max_loss(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_create_order.return_value = {"orderId": 556}

        order_id = await broker.place_stop_loss(
            "BTCUSDT", is_long=False, entry_price=65000.0, quantity_lots=15, max_loss_usdt=200.0
        )

        assert order_id == "556"
        call = client.futures_create_order.call_args.kwargs
        assert call["side"] == "BUY"  # closes a short
        real_qty = 15 * 0.001
        triggered_loss = (call["stopPrice"] - 65000.0) * real_qty
        assert triggered_loss <= 200.0 + 1e-6
        assert triggered_loss == pytest.approx(200.0, abs=0.01)

    async def test_returns_none_and_does_not_raise_when_order_rejected(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_create_order.side_effect = Exception("APIError(code=-2021): Order would immediately trigger")

        order_id = await broker.place_stop_loss(
            "BTCUSDT", is_long=True, entry_price=65000.0, quantity_lots=15, max_loss_usdt=200.0
        )

        assert order_id is None

    async def test_returns_none_for_unfiltered_symbol(self, broker: BinanceFuturesBroker):
        self._mock_client(broker)
        order_id = await broker.place_stop_loss(
            "NOSUCHUSDT", is_long=True, entry_price=1.0, quantity_lots=10, max_loss_usdt=50.0
        )
        assert order_id is None

    async def test_cancel_does_not_raise_when_already_gone(self, broker: BinanceFuturesBroker):
        client = self._mock_client(broker)
        client.futures_cancel_order.side_effect = Exception("APIError(code=-2011): Unknown order sent")
        await broker.cancel_stop_loss("BTCUSDT", "555")  # must not raise
        client.futures_cancel_order.assert_awaited_once()
