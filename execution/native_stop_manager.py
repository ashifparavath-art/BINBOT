"""Exchange-side (native) stop orders for BINBOT2 that FOLLOW the profit ladder.

Why this exists
---------------
Binance now routes conditional orders (STOP_MARKET etc.) through its
separate *algo order* service: they come back with an `algoId`, not an
`orderId`, and must be listed / cancelled through the algo endpoints. The
original broker helpers cancel with `orderId=int(...)`, which silently
fails for these — so stops were never moved or cleaned up.

This manager:
- places ONE closePosition STOP_MARKET per open position, at the price
  matching the current ladder level (-2% -> breakeven -> +4% -> ...);
- moves it up (cancel, then place) whenever the ladder level rises;
- cancels it when the position closes;
- periodically cancels leftover bot-style stops on symbols with no
  position (e.g. after a restart).

Only closePosition STOP_MARKET orders are touched; manual TAKE_PROFIT /
limit / other orders are left alone.
"""
from __future__ import annotations

import math
import time
from typing import Any

from config.logging_config import get_logger
from core.enums import OrderSide
from execution.binance_broker import PositionDetail

logger = get_logger(__name__)

CLIENT_ID_PREFIX = "b2sl"
TP_CLIENT_ID_PREFIX = "b2tp"


def stop_price_for_level(entry_price: float, level_pct: float, *, is_long: bool, tick: float,
                         precision: int) -> float:
    """Price at which the position's P&L equals `level_pct` % of notional.
    Rounded to the tick in the direction that never makes the exit worse
    than the level (long: up, short: down)."""
    raw = entry_price * (1 + level_pct / 100.0) if is_long else entry_price * (1 - level_pct / 100.0)
    if tick > 0:
        steps = math.ceil(raw / tick - 1e-9) if is_long else math.floor(raw / tick + 1e-9)
        return round(steps * tick, precision)
    return round(raw, precision)


def _truthy(v: Any) -> bool:
    return v is True or str(v).lower() == "true"


def is_bot_stop(order: dict, *, close_side: str | None = None) -> bool:
    """A closePosition STOP_MARKET (what this bot and the old scanner place)."""
    otype = str(order.get("orderType") or order.get("type") or "").upper()
    if otype != "STOP_MARKET" or not _truthy(order.get("closePosition")):
        return False
    return close_side is None or str(order.get("side", "")).upper() == close_side


def is_bot_take_profit(order: dict) -> bool:
    """A closePosition TAKE_PROFIT_MARKET placed by binbot2 (identified by its
    client id, so take-profits you set manually in the app are never touched)."""
    otype = str(order.get("orderType") or order.get("type") or "").upper()
    cid = str(order.get("clientAlgoId") or order.get("clientOrderId") or "")
    return otype == "TAKE_PROFIT_MARKET" and _truthy(order.get("closePosition")) and cid.startswith(TP_CLIENT_ID_PREFIX)


class NativeStopManager:
    def __init__(self, broker: Any, *, orphan_sweep_interval_s: float = 60.0) -> None:
        self._broker = broker
        self._level: dict[str, float] = {}  # symbol -> ladder level currently on the exchange
        self._failed_at: dict[str, float] = {}
        self._tp_level: dict[str, float] = {}  # symbol -> take-profit level on the exchange
        self._tp_failed_at: dict[str, float] = {}
        self._orphan_sweep_interval_s = orphan_sweep_interval_s
        self._last_sweep = 0.0

    # ---- low-level API ----------------------------------------------------

    @property
    def _client(self) -> Any:
        return self._broker.client

    async def _open_algo_orders(self, symbol: str | None = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else {}
        c = self._client
        if hasattr(c, "futures_get_open_algo_orders"):
            res = await c.futures_get_open_algo_orders(**params)
        else:
            res = await c._request_futures_api("get", "openAlgoOrders", True, data=params)
        if isinstance(res, dict):  # some versions wrap the list
            res = res.get("orders") or res.get("data") or []
        return list(res or [])

    async def _cancel_algo(self, algo_id: Any) -> None:
        c = self._client
        if hasattr(c, "futures_cancel_algo_order"):
            await c.futures_cancel_algo_order(algoId=algo_id)
        else:
            await c._request_futures_api("delete", "algoOrder", True, data={"algoId": algo_id})

    def _filters(self, symbol: str) -> Any:
        return self._broker._filters.get(symbol)

    # ---- public -----------------------------------------------------------

    def current_level(self, symbol: str) -> float | None:
        return self._level.get(symbol)

    async def cancel_stops(self, symbol: str, *, close_side: str | None = None) -> int:
        """Cancel this bot's style of stop on `symbol`. Returns how many."""
        try:
            orders = await self._open_algo_orders(symbol)
        except Exception as exc:
            logger.warning("native_stop.list_failed", symbol=symbol, error=str(exc))
            return 0
        n = 0
        for o in orders:
            if o.get("symbol", symbol) != symbol or not is_bot_stop(o, close_side=close_side):
                continue
            try:
                await self._cancel_algo(o.get("algoId"))
                n += 1
            except Exception as exc:
                logger.warning("native_stop.cancel_failed", symbol=symbol, algo_id=o.get("algoId"), error=str(exc))
        return n

    async def sync(self, detail: PositionDetail, level_pct: float) -> bool:
        """Make sure the exchange stop for this position sits at `level_pct`.
        Only ever moves the stop UP the ladder. Returns True if it changed."""
        symbol = detail.symbol
        current = self._level.get(symbol)
        if current is not None and level_pct <= current + 1e-9:
            return False
        failed = self._failed_at.get(symbol)
        if failed is not None and time.monotonic() - failed < 30.0:
            return False  # back off after a failure; software stop still protects

        filt = self._filters(symbol)
        if filt is None or detail.entry_price <= 0:
            return False
        is_long = detail.side is OrderSide.BUY
        close_side = "SELL" if is_long else "BUY"
        price = stop_price_for_level(detail.entry_price, level_pct, is_long=is_long, tick=filt.tick_size,
                                     precision=filt.price_precision)
        if price <= 0:
            return False

        # Binance allows only one closePosition stop per direction, so the
        # old one must go before the new one can be placed.
        cancelled = await self.cancel_stops(symbol, close_side=close_side)
        client_id = f"{CLIENT_ID_PREFIX}-{symbol}-{int(time.time() * 1000)}"[:36]
        try:
            res = await self._client.futures_create_order(
                symbol=symbol,
                side=close_side,
                type="STOP_MARKET",
                stopPrice=price,
                closePosition=True,
                workingType="MARK_PRICE",
                clientAlgoId=client_id,
            )
        except Exception as exc:
            self._failed_at[symbol] = time.monotonic()
            self._level.pop(symbol, None)
            logger.error("native_stop.place_failed", symbol=symbol, level_pct=level_pct, stop_price=price,
                         error=str(exc))
            if "-4130" in str(exc):
                # A stop we didn't recognise is still there: log what Binance
                # returned so the field matching can be checked.
                try:
                    sample = (await self._open_algo_orders(symbol))[:3]
                    logger.error("native_stop.unrecognised_existing_orders", symbol=symbol, orders=sample)
                except Exception:
                    pass
            return False

        self._failed_at.pop(symbol, None)
        self._level[symbol] = level_pct
        logger.info(
            "native_stop.placed" if current is None else "native_stop.moved",
            symbol=symbol,
            level_pct=level_pct,
            stop_price=price,
            previous_level_pct=current,
            replaced=cancelled,
            algo_id=(res or {}).get("algoId") if isinstance(res, dict) else None,
        )
        return True

    async def cancel_take_profits(self, symbol: str) -> int:
        try:
            orders = await self._open_algo_orders(symbol)
        except Exception as exc:
            logger.warning("native_tp.list_failed", symbol=symbol, error=str(exc))
            return 0
        n = 0
        for o in orders:
            if o.get("symbol", symbol) != symbol or not is_bot_take_profit(o):
                continue
            try:
                await self._cancel_algo(o.get("algoId"))
                n += 1
            except Exception as exc:
                logger.warning("native_tp.cancel_failed", symbol=symbol, algo_id=o.get("algoId"), error=str(exc))
        return n

    async def sync_take_profit(self, detail: PositionDetail, tp_pct: float) -> bool:
        """Make sure a closePosition TAKE_PROFIT_MARKET sits at +tp_pct % of
        notional for this position. Placed once; replaced only if the level
        changes (e.g. after a settings change)."""
        symbol = detail.symbol
        current = self._tp_level.get(symbol)
        if current is not None and abs(current - tp_pct) < 1e-9:
            return False
        failed = self._tp_failed_at.get(symbol)
        if failed is not None and time.monotonic() - failed < 300.0:
            return False
        filt = self._filters(symbol)
        if filt is None or detail.entry_price <= 0 or tp_pct <= 0:
            return False
        is_long = detail.side is OrderSide.BUY
        close_side = "SELL" if is_long else "BUY"
        price = stop_price_for_level(detail.entry_price, tp_pct, is_long=is_long, tick=filt.tick_size,
                                     precision=filt.price_precision)
        if price <= 0:
            return False
        replaced = await self.cancel_take_profits(symbol)
        client_id = f"{TP_CLIENT_ID_PREFIX}-{symbol}-{int(time.time() * 1000)}"[:36]
        try:
            res = await self._client.futures_create_order(
                symbol=symbol,
                side=close_side,
                type="TAKE_PROFIT_MARKET",
                stopPrice=price,
                closePosition=True,
                workingType="MARK_PRICE",
                clientAlgoId=client_id,
            )
        except Exception as exc:
            self._tp_failed_at[symbol] = time.monotonic()
            if "-4130" in str(exc):
                # Another closePosition take-profit already exists on this
                # position (e.g. one you set in the app). Yours is left in
                # place; the bot still closes at +tp% itself. Retries in 5 min.
                logger.warning("native_tp.blocked_by_existing_order", symbol=symbol, tp_pct=tp_pct, error=str(exc))
            else:
                logger.error("native_tp.place_failed", symbol=symbol, tp_pct=tp_pct, tp_price=price, error=str(exc))
            return False
        self._tp_failed_at.pop(symbol, None)
        self._tp_level[symbol] = tp_pct
        logger.info(
            "native_tp.placed",
            symbol=symbol,
            tp_pct=tp_pct,
            tp_price=price,
            replaced=replaced,
            algo_id=(res or {}).get("algoId") if isinstance(res, dict) else None,
        )
        return True

    async def on_position_closed(self, symbol: str) -> None:
        self._level.pop(symbol, None)
        self._failed_at.pop(symbol, None)
        self._tp_level.pop(symbol, None)
        self._tp_failed_at.pop(symbol, None)
        n = await self.cancel_stops(symbol)
        n += await self.cancel_take_profits(symbol)
        if n:
            logger.info("native_stop.cancelled_after_close", symbol=symbol, cancelled=n)

    async def sweep_orphans(self, open_symbols: set[str], *, force: bool = False) -> None:
        """Cancel leftover bot-style stops on symbols with no open position."""
        now = time.monotonic()
        if not force and now - self._last_sweep < self._orphan_sweep_interval_s:
            return
        self._last_sweep = now
        try:
            orders = await self._open_algo_orders()
        except Exception as exc:
            logger.warning("native_stop.sweep_list_failed", error=str(exc))
            return
        for o in orders:
            sym = o.get("symbol")
            if sym and sym not in open_symbols and (is_bot_stop(o) or is_bot_take_profit(o)):
                try:
                    await self._cancel_algo(o.get("algoId"))
                    logger.info("native_stop.orphan_cancelled", symbol=sym, algo_id=o.get("algoId"))
                except Exception as exc:
                    logger.warning("native_stop.orphan_cancel_failed", symbol=sym, error=str(exc))
