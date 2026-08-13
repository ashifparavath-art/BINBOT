"""Binance USDT-M Futures broker adapter.

Implements the same `BrokerInterface` contract as `IBKRBroker`, so the risk
engine, order manager, and strategies do not know or care which exchange
they are trading on — this is the entire point of the abstraction.

Quantity model
--------------
Binance quantities are fractional (e.g. 0.734 BTC), but the rest of this
codebase (risk engine, order manager, strategies) was built assuming
whole-number contract counts, matching IBKR futures. Rather than widen
those to floats — which would reintroduce floating-point drift into
position/PnL arithmetic everywhere — this adapter treats
`BrokerOrderRequest.quantity` as an integer count of *lots*, where one lot
equals the exchange's minimum step size for that symbol (`stepSize` from
`futures_exchange_info`). E.g. if BTCUSDT's step size is 0.001 BTC, a lot
quantity of 734 means 0.734 BTC. The float<->lot conversion happens only
at this adapter's boundary (`lots_to_quantity` / `quantity_to_lots`).

Safety notes
------------
- `testnet=True` (the default in `config.settings.BinanceSettings`) routes
  every request to Binance's practice exchange. Flipping it to False must
  be a deliberate, explicit choice by the operator, never a default.
- OCA/OCO groups: Binance USDT-M Futures has no native "cancel siblings on
  fill" order-group primitive the way IBKR does, so it is emulated here —
  the instant a grouped order fills, this adapter cancels the rest of the
  group itself.
"""
from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from config.logging_config import get_logger
from core.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from core.events import ExecutionEvent, OrderAckEvent, OrderRejectEvent
from execution.broker_interface import BrokerInterface, BrokerOrderRequest

if TYPE_CHECKING:
    from binance import AsyncClient
    from binance.ws.streams import BinanceSocketManager

logger = get_logger(__name__)

_ORDER_TYPE_MAP = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP: "STOP_MARKET",
    OrderType.STOP_LIMIT: "STOP",
    OrderType.MARKET_ON_CLOSE: "MARKET",  # no native MOC on Binance futures
}

_TIF_MAP = {
    TimeInForce.DAY: "GTC",  # Binance futures has no DAY TIF; GTC is the closest analogue
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
}

_ORDER_STATUS_MAP = {
    "NEW": OrderStatus.ACKNOWLEDGED,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELLED,
    "EXPIRED": OrderStatus.EXPIRED,
    "REJECTED": OrderStatus.REJECTED,
}


def round_step(value: float, step: float) -> float:
    """Round DOWN to the nearest multiple of `step`. Flooring (not
    round-to-nearest) guarantees the result never exceeds the caller's
    intended size/price — the one direction that must never happen for an
    order quantity."""
    if step <= 0:
        return value
    steps = math.floor(value / step + 1e-9)
    return round(steps * step, 12)


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Exchange-enforced precision for a symbol, loaded once at connect
    time from `futures_exchange_info()`. Every quantity/price sent to
    Binance must respect these or the order is rejected outright."""

    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float
    quantity_precision: int
    price_precision: int


@dataclass(frozen=True, slots=True)
class PositionDetail:
    symbol: str
    side: OrderSide
    quantity: int  # lots, always positive
    entry_price: float
    mark_price: float
    unrealized_pnl: float


@dataclass
class _OcaGroup:
    order_ids: set[str] = field(default_factory=set)


class BinanceFuturesBroker(BrokerInterface):
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        testnet: bool,
        symbols: list[str],
        leverage: int = 2,
        margin_type: str = "ISOLATED",
        recv_window_ms: int = 5000,
        reconnect_backoff_s: float = 2.0,
        reconnect_backoff_max_s: float = 60.0,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._testnet = testnet
        self._symbols = symbols
        self._leverage = leverage
        self._margin_type = margin_type
        self._recv_window_ms = recv_window_ms
        self._reconnect_backoff_s = reconnect_backoff_s
        self._reconnect_backoff_max_s = reconnect_backoff_max_s

        self._client: "AsyncClient | None" = None
        self._bsm: "BinanceSocketManager | None" = None
        self._filters: dict[str, SymbolFilters] = {}
        self._exchange_info_by_symbol: dict[str, dict[str, Any]] = {}
        self._configured_leverage_margin: dict[str, tuple[int, str]] = {}
        self._event_queue: asyncio.Queue[OrderAckEvent | OrderRejectEvent | ExecutionEvent] = (
            asyncio.Queue(maxsize=10_000)
        )
        self._client_order_to_correlation: dict[str, str] = {}
        self._client_order_to_internal_id: dict[str, str] = {}
        self._order_symbol: dict[str, str] = {}
        self._oca_groups: dict[str, _OcaGroup] = {}
        self._order_to_oca_group: dict[str, str] = {}
        self._connected = False
        self._user_stream_task: asyncio.Task[None] | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def client(self) -> "AsyncClient":
        """Expose the underlying `binance.AsyncClient` for adapters that need
        it directly (e.g. `BinanceUniverseFeed`), once connected."""
        if self._client is None:
            raise RuntimeError("BinanceFuturesBroker.connect() has not been called yet")
        return self._client

    @property
    def socket_manager(self) -> "BinanceSocketManager":
        if self._bsm is None:
            raise RuntimeError("BinanceFuturesBroker.connect() has not been called yet")
        return self._bsm

    async def connect(self) -> None:
        from binance import AsyncClient, BinanceSocketManager

        backoff = self._reconnect_backoff_s
        while True:
            try:
                self._client = await AsyncClient.create(
                    api_key=self._api_key, api_secret=self._api_secret, testnet=self._testnet
                )
                break
            except Exception as exc:
                logger.error("binance.connect_failed", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_backoff_max_s)

        self._bsm = BinanceSocketManager(self._client)
        await self._load_symbol_filters()
        await self._configure_symbols()

        self._connected = True
        self._user_stream_task = asyncio.create_task(
            self._consume_user_stream(), name="binance-user-stream"
        )
        logger.info(
            "binance.connected",
            testnet=self._testnet,
            symbols=self._symbols,
            leverage=self._leverage,
            margin_type=self._margin_type,
        )

    async def _refresh_exchange_info(self) -> None:
        """Fetch the full exchange-info payload ONCE and index it by symbol,
        so both the fixed-symbol-list path (`_load_symbol_filters`) and the
        scan-the-whole-exchange path (`ensure_symbol_ready`,
        `get_usdt_perpetual_universe`) can pull filters for any symbol
        without a separate API call per symbol."""
        assert self._client is not None
        info = await self._client.futures_exchange_info()
        self._exchange_info_by_symbol = {s["symbol"]: s for s in info["symbols"]}
        logger.info("binance.exchange_info_refreshed", symbol_count=len(self._exchange_info_by_symbol))

    @staticmethod
    def _parse_filters(symbol: str, data: dict[str, Any]) -> SymbolFilters:
        tick_size = step_size = min_qty = min_notional = 0.0
        for f in data["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                tick_size = float(f["tickSize"])
            elif f["filterType"] == "LOT_SIZE":
                step_size = float(f["stepSize"])
                min_qty = float(f["minQty"])
            elif f["filterType"] == "MIN_NOTIONAL":
                min_notional = float(f.get("notional", 0.0))
        return SymbolFilters(
            symbol=symbol,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
            quantity_precision=int(data["quantityPrecision"]),
            price_precision=int(data["pricePrecision"]),
        )

    async def _load_symbol_filters(self) -> None:
        if not self._exchange_info_by_symbol:
            await self._refresh_exchange_info()
        for symbol in self._symbols:
            data = self._exchange_info_by_symbol.get(symbol)
            if data is None:
                raise ValueError(f"symbol {symbol} not found on Binance Futures")
            self._filters[symbol] = self._parse_filters(symbol, data)
        logger.info(
            "binance.symbol_filters_loaded",
            filters={s: f.step_size for s, f in self._filters.items()},
        )

    async def _configure_symbols(self) -> None:
        for symbol in self._symbols:
            await self._configure_leverage_and_margin(symbol, leverage=self._leverage, margin_type=self._margin_type)

    async def _configure_leverage_and_margin(self, symbol: str, *, leverage: int, margin_type: str) -> None:
        assert self._client is not None
        if symbol in self._configured_leverage_margin:
            return  # already resolved for this symbol — avoid redundant API calls
        try:
            await self._client.futures_change_margin_type(symbol=symbol, marginType=margin_type)
        except Exception as exc:
            # Binance raises an error if the margin type is already set to
            # this value — that is not a real failure, just log it quietly.
            logger.debug("binance.margin_type_unchanged", symbol=symbol, detail=str(exc))

        effective_leverage = leverage
        try:
            await self._client.futures_change_leverage(symbol=symbol, leverage=leverage)
        except Exception as exc:
            # Some symbols cap out below the requested leverage (Binance
            # error -4028) — fall back to that symbol's actual max instead
            # of refusing to trade it at all. Resolved once and cached
            # below, so this symbol never repeats the lookup or the error.
            max_leverage = await self._max_leverage_for(symbol)
            if max_leverage is not None and max_leverage < leverage:
                await self._client.futures_change_leverage(symbol=symbol, leverage=max_leverage)
                effective_leverage = max_leverage
                logger.warning(
                    "binance.leverage_capped", symbol=symbol, requested=leverage, using=max_leverage
                )
            else:
                logger.error("binance.set_leverage_failed", symbol=symbol, error=str(exc))
                raise
        self._configured_leverage_margin[symbol] = (effective_leverage, margin_type)

    async def _max_leverage_for(self, symbol: str) -> int | None:
        """The highest leverage Binance allows for this symbol at its
        lowest notional tier (bracket 1). None if the lookup itself fails
        — the caller then surfaces the original error instead of masking
        it with a second, unrelated one."""
        assert self._client is not None
        try:
            brackets = await self._client.futures_leverage_bracket(symbol=symbol)
            return int(brackets[0]["brackets"][0]["initialLeverage"])
        except Exception as exc:
            logger.debug("binance.leverage_bracket_lookup_failed", symbol=symbol, error=str(exc))
            return None

    async def get_usdt_perpetual_universe(self, *, force_refresh: bool = False) -> list[str]:
        """Every currently-tradeable USDT-margined perpetual future — the
        scan universe for a strategy that isn't tied to one fixed pair."""
        if force_refresh or not self._exchange_info_by_symbol:
            await self._refresh_exchange_info()
        return sorted(
            symbol
            for symbol, data in self._exchange_info_by_symbol.items()
            if data.get("status") == "TRADING"
            and data.get("contractType") == "PERPETUAL"
            and data.get("quoteAsset") == "USDT"
        )

    async def load_all_filters(self) -> None:
        """Parse `SymbolFilters` for every symbol in the cached exchange-info
        map at once. Pure CPU — no extra API calls beyond whatever already
        populated the cache (`get_usdt_perpetual_universe` or
        `_refresh_exchange_info`). Use this for a scanner that needs
        correct notional math for the whole universe immediately, while
        still deferring the actual leverage/margin-type API calls
        (`ensure_symbol_ready`) until a symbol is really about to trade."""
        if not self._exchange_info_by_symbol:
            await self._refresh_exchange_info()
        for symbol, data in self._exchange_info_by_symbol.items():
            if symbol not in self._filters:
                self._filters[symbol] = self._parse_filters(symbol, data)
        logger.info("binance.all_filters_loaded", count=len(self._filters))

    async def ensure_symbol_ready(self, symbol: str, *, leverage: int, margin_type: str) -> SymbolFilters:
        """Lazily qualify a symbol the first time we're about to trade it:
        load its precision filters and set leverage/margin type. Safe to
        call repeatedly — both steps are cheap no-ops once already done for
        this symbol at this leverage/margin combination."""
        if symbol not in self._filters:
            if symbol not in self._exchange_info_by_symbol:
                await self._refresh_exchange_info()
            data = self._exchange_info_by_symbol.get(symbol)
            if data is None:
                raise ValueError(f"symbol {symbol} not found on Binance Futures")
            self._filters[symbol] = self._parse_filters(symbol, data)
        await self._configure_leverage_and_margin(symbol, leverage=leverage, margin_type=margin_type)
        return self._filters[symbol]

    def get_step_size(self, symbol: str) -> float:
        """The real-asset-units represented by one 'lot' — feed this
        straight into `RiskEngine.set_contract_multiplier()` so notional
        math (lots * price * multiplier) comes out in correct USD terms."""
        return self._filters[symbol].step_size

    def lots_to_quantity(self, symbol: str, lots: int) -> float:
        filt = self._filters[symbol]
        return round(lots * filt.step_size, filt.quantity_precision)

    def quantity_to_lots(self, symbol: str, quantity: float) -> int:
        filt = self._filters[symbol]
        if filt.step_size <= 0:
            return int(round(quantity))
        return int(round(quantity / filt.step_size))

    async def disconnect(self) -> None:
        self._connected = False
        if self._user_stream_task:
            self._user_stream_task.cancel()
        if self._client is not None:
            await self._client.close_connection()
        logger.info("binance.disconnected")

    async def place_order(self, request: BrokerOrderRequest) -> None:
        assert self._client is not None
        filt = self._filters.get(request.symbol)
        if filt is None:
            raise ValueError(f"no exchange filters loaded for symbol={request.symbol}; was connect() called?")

        raw_quantity = self.lots_to_quantity(request.symbol, request.quantity)
        if raw_quantity < filt.min_qty:
            reason = (
                f"quantity {raw_quantity} below exchange minimum {filt.min_qty} for {request.symbol}"
            )
            logger.error("binance.order_below_min_qty", symbol=request.symbol, reason=reason)
            await self._event_queue.put(
                OrderRejectEvent(
                    ts=datetime.now(timezone.utc),
                    order_id=request.order_id,
                    correlation_id=request.correlation_id,
                    symbol=request.symbol,
                    reason=reason,
                )
            )
            return

        client_order_id = request.order_id[:36]
        self._client_order_to_correlation[client_order_id] = request.correlation_id
        self._client_order_to_internal_id[client_order_id] = request.order_id
        self._order_symbol[request.order_id] = request.symbol
        if request.oca_group:
            self._order_to_oca_group[request.order_id] = request.oca_group
            self._oca_groups.setdefault(request.oca_group, _OcaGroup()).order_ids.add(request.order_id)

        params: dict[str, Any] = {
            "symbol": request.symbol,
            "side": request.side.value,
            "type": _ORDER_TYPE_MAP[request.order_type],
            "quantity": raw_quantity,
            "newClientOrderId": client_order_id,
            "recvWindow": self._recv_window_ms,
        }
        if request.order_type is OrderType.LIMIT:
            if request.limit_price is None:
                raise ValueError("LIMIT order requires limit_price")
            params["price"] = round_step(request.limit_price, filt.tick_size)
            params["timeInForce"] = _TIF_MAP[request.time_in_force]

        try:
            await self._client.futures_create_order(**params)
        except Exception as exc:
            logger.error("binance.order_rejected", symbol=request.symbol, error=str(exc))
            await self._event_queue.put(
                OrderRejectEvent(
                    ts=datetime.now(timezone.utc),
                    order_id=request.order_id,
                    correlation_id=request.correlation_id,
                    symbol=request.symbol,
                    reason=str(exc),
                )
            )
            return

        logger.info(
            "binance.order_submitted",
            order_id=request.order_id,
            symbol=request.symbol,
            side=request.side.value,
            quantity=raw_quantity,
            correlation_id=request.correlation_id,
        )
        # acks and fills arrive asynchronously via the user data websocket
        # (_consume_user_stream), not from this REST response.

    async def cancel_order(self, order_id: str) -> None:
        assert self._client is not None
        symbol = self._order_symbol.get(order_id)
        if symbol is None:
            logger.warning("binance.cancel_unknown_order", order_id=order_id)
            return
        try:
            await self._client.futures_cancel_order(
                symbol=symbol, origClientOrderId=order_id[:36], recvWindow=self._recv_window_ms
            )
        except Exception as exc:
            logger.warning("binance.cancel_failed", order_id=order_id, error=str(exc))

    async def place_stop_loss(
        self,
        symbol: str,
        *,
        is_long: bool,
        entry_price: float,
        quantity_lots: int,
        max_loss_usdt: float,
    ) -> str | None:
        """Place a native, exchange-side STOP_MARKET order that closes the
        entire current position for `symbol` if the mark price crosses the
        computed stop price — enforced by Binance itself, independent of
        whether this bot process is even running. This is a backstop, not
        the primary mechanism: PositionMonitor's own polling (faster, and
        based on Binance's own live unrealized-P&L figure rather than a
        static price level) is what normally closes a losing position;
        this exists for the gap where the bot itself is down (crashed,
        restarting, the server rebooting) and nothing else is watching.

        The stop price is rounded in whichever direction keeps the
        triggered loss at or under `max_loss_usdt` — never over it — since
        the entire point is a hard ceiling on loss, not an approximation.
        Returns the order id, or None if placement failed (logged, not
        raised: a failure here must not block the entry itself).
        """
        assert self._client is not None
        filt = self._filters.get(symbol)
        if filt is None or quantity_lots <= 0:
            return None
        real_qty = self.lots_to_quantity(symbol, quantity_lots)
        if real_qty <= 0:
            return None

        loss_per_unit = max_loss_usdt / real_qty
        raw_price = entry_price - loss_per_unit if is_long else entry_price + loss_per_unit
        tick = filt.tick_size
        if tick > 0:
            steps = math.ceil(raw_price / tick) if is_long else math.floor(raw_price / tick)
            stop_price = round(steps * tick, filt.price_precision)
        else:
            stop_price = round(raw_price, filt.price_precision)
        if stop_price <= 0:
            logger.warning("binance.native_stop_invalid_price", symbol=symbol, raw_price=raw_price)
            return None

        close_side = OrderSide.SELL if is_long else OrderSide.BUY
        client_order_id = f"sl-{symbol}-{int(datetime.now(timezone.utc).timestamp() * 1000)}"[:36]
        try:
            result = await self._client.futures_create_order(
                symbol=symbol,
                side=close_side.value,
                type="STOP_MARKET",
                stopPrice=stop_price,
                closePosition=True,
                newClientOrderId=client_order_id,
                recvWindow=self._recv_window_ms,
            )
        except Exception as exc:
            logger.error("binance.native_stop_failed", symbol=symbol, error=str(exc))
            return None

        order_id = str(result.get("orderId", client_order_id))
        logger.info(
            "binance.native_stop_placed",
            symbol=symbol,
            stop_price=stop_price,
            max_loss_usdt=max_loss_usdt,
            order_id=order_id,
        )
        return order_id

    async def cancel_stop_loss(self, symbol: str, order_id: str) -> None:
        """Best-effort cancel of a native stop placed by `place_stop_loss`
        — called once the position it was protecting is closed by some
        other means (the software take-profit/stop-loss, a manual
        flatten, a kill switch), so it doesn't sit on the account as a
        dead order. Safe to call even if Binance already auto-resolved it
        (e.g. the position closed and the order simply has nothing left
        to act on) — that failure is expected and only logged quietly."""
        assert self._client is not None
        try:
            await self._client.futures_cancel_order(
                symbol=symbol, orderId=int(order_id), recvWindow=self._recv_window_ms
            )
            logger.info("binance.native_stop_cancelled", symbol=symbol, order_id=order_id)
        except Exception as exc:
            logger.debug("binance.native_stop_cancel_failed", symbol=symbol, order_id=order_id, error=str(exc))

    async def cancel_all(self) -> None:
        assert self._client is not None
        for symbol in self._symbols:
            try:
                await self._client.futures_cancel_all_open_orders(
                    symbol=symbol, recvWindow=self._recv_window_ms
                )
            except Exception as exc:
                logger.warning("binance.cancel_all_failed", symbol=symbol, error=str(exc))
        logger.warning("binance.cancel_all_issued", symbols=self._symbols)

    async def stream_events(self) -> AsyncIterator[OrderAckEvent | OrderRejectEvent | ExecutionEvent]:
        while True:
            yield await self._event_queue.get()

    async def get_account_equity(self) -> float:
        assert self._client is not None
        account = await self._client.futures_account(recvWindow=self._recv_window_ms)
        return float(account["totalMarginBalance"])

    async def get_positions(self) -> dict[str, int]:
        assert self._client is not None
        positions = await self._client.futures_position_information(recvWindow=self._recv_window_ms)
        result: dict[str, int] = {}
        for p in positions:
            symbol = p["symbol"]
            if symbol not in self._filters:
                continue
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            lots = self.quantity_to_lots(symbol, abs(amt))
            result[symbol] = lots if amt > 0 else -lots
        return result

    async def get_position_pnl(self) -> dict[str, float]:
        """Unrealized P&L per open position, in USDT — read directly from
        Binance's own mark-price-based calculation rather than
        recomputed locally, so a stop-loss/take-profit check always
        agrees with what the exchange itself would show you."""
        assert self._client is not None
        positions = await self._client.futures_position_information(recvWindow=self._recv_window_ms)
        return {
            p["symbol"]: float(p["unRealizedProfit"])
            for p in positions
            if float(p["positionAmt"]) != 0
        }

    async def get_position_details(self) -> list[PositionDetail]:
        """Full per-position detail (side, size, entry/mark price,
        unrealized P&L) for the dashboard's open-positions view — all read
        directly from Binance rather than reconstructed locally, so it
        always agrees with what the exchange itself would show."""
        assert self._client is not None
        positions = await self._client.futures_position_information(recvWindow=self._recv_window_ms)
        result: list[PositionDetail] = []
        for p in positions:
            symbol = p["symbol"]
            if symbol not in self._filters:
                continue
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            result.append(
                PositionDetail(
                    symbol=symbol,
                    side=OrderSide.BUY if amt > 0 else OrderSide.SELL,
                    quantity=self.quantity_to_lots(symbol, abs(amt)),
                    entry_price=float(p["entryPrice"]),
                    mark_price=float(p["markPrice"]),
                    unrealized_pnl=float(p["unRealizedProfit"]),
                )
            )
        return result

    async def get_mark_price_klines(self, symbol: str, limit: int = 1440) -> list[dict]:
        """Historical 1-minute mark-price candles for the dashboard's chart
        backfill. Deliberately mark-price klines (not trade klines): the
        live candle stream the dashboard already gets is itself built from
        mark-price ticks (see on_tick/BarAggregator in
        scripts/run_live_binance_scanner.py), so using the same basis here
        keeps historical and live bars visually continuous instead of
        showing a seam where one data source hands off to the other.
        `limit` defaults to 1440 (24h at 1-minute resolution) — Binance
        allows up to 1500 per call.
        """
        assert self._client is not None
        rows = await self._client.futures_mark_price_klines(
            symbol=symbol, interval="1m", limit=min(limit, 1500)
        )
        return [
            {
                "time": int(row[0] / 1000),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": 0.0,  # mark-price klines carry no real trade volume
            }
            for row in rows
        ]

    async def get_margin_usage(self) -> tuple[float, float]:
        assert self._client is not None
        account = await self._client.futures_account(recvWindow=self._recv_window_ms)
        return float(account["totalInitialMargin"]), float(account["totalMaintMargin"])

    async def reconcile_state(self) -> dict[str, int]:
        positions = await self.get_positions()
        logger.info("binance.state_reconciled", positions=positions)
        return positions

    async def _consume_user_stream(self) -> None:
        assert self._bsm is not None
        backoff = self._reconnect_backoff_s
        while self._connected:
            try:
                async with self._bsm.futures_user_socket() as stream:
                    backoff = self._reconnect_backoff_s
                    while self._connected:
                        msg = await stream.recv()
                        await self._handle_user_stream_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("binance.user_stream_error", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._reconnect_backoff_max_s)

    async def _handle_user_stream_message(self, msg: dict[str, Any]) -> None:
        if msg.get("e") != "ORDER_TRADE_UPDATE":
            return
        o = msg["o"]
        client_order_id = o["c"]
        correlation_id = self._client_order_to_correlation.get(client_order_id, "")
        internal_order_id = self._client_order_to_internal_id.get(client_order_id, client_order_id)
        symbol = o["s"]
        side = OrderSide.BUY if o["S"] == "BUY" else OrderSide.SELL
        exec_type = o["x"]  # NEW | CANCELED | CALCULATED | EXPIRED | TRADE
        order_status = o["X"]

        if exec_type == "TRADE":
            fill_qty_lots = self.quantity_to_lots(symbol, float(o["l"]))
            cumulative_lots = self.quantity_to_lots(symbol, float(o["z"]))
            original_qty_lots = self.quantity_to_lots(symbol, float(o["q"]))
            remaining_lots = max(original_qty_lots - cumulative_lots, 0)
            await self._event_queue.put(
                ExecutionEvent(
                    ts=datetime.fromtimestamp(int(o["T"]) / 1000, tz=timezone.utc),
                    order_id=internal_order_id,
                    correlation_id=correlation_id,
                    symbol=symbol,
                    side=side,
                    fill_quantity=fill_qty_lots,
                    fill_price=float(o["L"]),
                    cumulative_quantity=cumulative_lots,
                    remaining_quantity=remaining_lots,
                    commission=float(o.get("n") or 0.0),
                    liquidity="MAKER" if o.get("m") else "TAKER",
                )
            )
            if order_status == "FILLED":
                await self._handle_oca_fill(internal_order_id)
            return  # the TRADE branch fully represents this update; no separate ack needed

        status = _ORDER_STATUS_MAP.get(order_status)
        if status is None:
            return
        if status is OrderStatus.REJECTED:
            await self._event_queue.put(
                OrderRejectEvent(
                    ts=datetime.now(timezone.utc),
                    order_id=internal_order_id,
                    correlation_id=correlation_id,
                    symbol=symbol,
                    reason=f"Binance order status={order_status}",
                )
            )
        else:
            await self._event_queue.put(
                OrderAckEvent(
                    ts=datetime.now(timezone.utc),
                    order_id=internal_order_id,
                    correlation_id=correlation_id,
                    symbol=symbol,
                    status=status,
                    broker_order_id=str(o.get("i")),
                )
            )

    async def _handle_oca_fill(self, order_id: str) -> None:
        group_name = self._order_to_oca_group.get(order_id)
        if group_name is None:
            return
        group = self._oca_groups.get(group_name)
        if group is None:
            return
        siblings = [oid for oid in group.order_ids if oid != order_id]
        for sibling_id in siblings:
            logger.info("binance.oca_cancel_sibling", filled_order=order_id, cancelling=sibling_id)
            await self.cancel_order(sibling_id)
        group.order_ids.clear()
