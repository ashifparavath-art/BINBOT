"""WebSocket bridge between the live trading loop and the Next.js dashboard
(dashboard/hooks/useWebSocket.ts).

**Telemetry (bot -> dashboard):** every event that already flows through
the shared `EventBus` (bars, fills, order acks/rejects, risk halts) is
mirrored here as a small JSON message and broadcast to every connected
browser tab. Portfolio KPIs, margin usage, and risk-limit usage are polled
from the broker on a timer (`_kpi_loop`) since those aren't discrete
events — they're a running snapshot of account state.

**Control (dashboard -> bot):** the browser can send back exactly five
commands — pause, resume, flatten, kill_switch, reset_kill_switch — each
requiring a shared-secret token (see `run_live_binance_scanner.py` for how
it's generated/loaded) so a stray device on the network can't issue commands to
a live account. This bridge does not implement the actions itself; it
dispatches to callables injected by the caller, keeping this module
broker/strategy-agnostic. Defaults to binding `127.0.0.1` only (not
`0.0.0.0`) now that a write path exists — do not widen this without adding
real authentication first.
"""
from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from config.logging_config import get_logger
from core.enums import OrderStatus, OrderType
from core.event_bus import EventBus
from core.events import BarEvent, Event, ExecutionEvent, OrderAckEvent, OrderRejectEvent, RiskHaltEvent, SignalEvent
from execution.broker_interface import BrokerInterface
from execution.order_manager import OrderLifecycleManager
from risk.circuit_breaker import DrawdownCircuitBreaker
from utils.daily_log import DailyJsonlLog

if TYPE_CHECKING:
    from websockets.asyncio.server import Server, ServerConnection

    from execution.binance_broker import PositionDetail
    from execution.trade_tracker import TradeTracker
    from notifications.telegram_notifier import TelegramNotifier

AsyncCommandHandler = Callable[[], Awaitable[None]]

# Generous in-memory ceilings for the recent-history lists backing new-client
# backfill — bounded mainly so a very-long-uptime process (no restart across
# many days) can't grow these forever; disk rotation in DailyJsonlLog is the
# real bound on how much history exists at all.
_MAX_RECENT_CLOSED_TRADES = 2000
_MAX_RECENT_AUDIT_EVENTS = 5000
_MAX_CANDLES_PER_SYMBOL_CACHE = 1600


def load_or_create_control_token(path: str = "data_store/dashboard_control_token.txt") -> str:
    """A shared secret the dashboard must present with every control
    command (pause/flatten/kill-switch/etc). Persisted locally so it's
    stable across restarts instead of invalidating the dashboard's config
    every run; lives under data_store/, which is already gitignored.
    Shared by every live entry-point script so they all authenticate the
    same way."""
    token_path = Path(path)
    if token_path.exists():
        token = token_path.read_text(encoding="utf-8").strip()
        if token:
            return token
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(24)
    token_path.write_text(token, encoding="utf-8")
    return token

logger = get_logger(__name__)

_ORDER_STATUS_TO_DASHBOARD = {
    OrderStatus.PENDING_NEW: "WORKING",
    OrderStatus.SUBMITTED: "WORKING",
    OrderStatus.ACKNOWLEDGED: "WORKING",
    OrderStatus.PARTIALLY_FILLED: "PARTIAL",
    OrderStatus.FILLED: "FILLED",
    OrderStatus.CANCEL_PENDING: "WORKING",
    OrderStatus.CANCELLED: "CANCELED",
    OrderStatus.REJECTED: "REJECTED",
    OrderStatus.EXPIRED: "REJECTED",
}

_ORDER_TYPE_TO_DASHBOARD = {
    OrderType.MARKET: "MKT",
    OrderType.LIMIT: "LMT",
    OrderType.STOP: "STP",
    OrderType.STOP_LIMIT: "STP",
    OrderType.MARKET_ON_CLOSE: "MKT",
}


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class DashboardBridge:
    def __init__(
        self,
        *,
        event_bus: EventBus,
        broker: BrokerInterface,
        order_manager: OrderLifecycleManager,
        circuit_breaker: DrawdownCircuitBreaker,
        max_daily_loss_usd: float,
        max_position_contracts: int,
        chart_symbol: str,
        strategy_id: str,
        control_token: str,
        on_pause: AsyncCommandHandler,
        on_resume: AsyncCommandHandler,
        on_flatten: AsyncCommandHandler,
        on_kill_switch: AsyncCommandHandler,
        on_reset_kill_switch: AsyncCommandHandler,
        is_strategy_paused: Callable[[], bool],
        trade_tracker: "TradeTracker | None" = None,
        position_details_provider: Callable[[], Awaitable[list["PositionDetail"]]] | None = None,
        notifier: "TelegramNotifier | None" = None,
        closed_trades_log: "DailyJsonlLog | None" = None,
        audit_log: "DailyJsonlLog | None" = None,
        historical_klines_provider: Callable[[str, int], Awaitable[list[dict]]] | None = None,
        host: str = "127.0.0.1",
        port: int = 8765,
        kpi_push_interval_s: float = 5.0,
    ) -> None:
        self._event_bus = event_bus
        self._broker = broker
        self._order_manager = order_manager
        self._circuit_breaker = circuit_breaker
        self._max_daily_loss_usd = max_daily_loss_usd
        self._max_position_contracts = max_position_contracts
        self._chart_symbol = chart_symbol
        self._strategy_id = strategy_id
        self._control_token = control_token
        self._is_strategy_paused = is_strategy_paused
        self._trade_tracker = trade_tracker
        self._position_details_provider = position_details_provider
        self._notifier = notifier
        self._closed_trades_log = closed_trades_log
        self._audit_log = audit_log
        self._historical_klines_provider = historical_klines_provider
        self._host = host
        self._port = port
        self._kpi_push_interval_s = kpi_push_interval_s
        self._close_reasons: dict[str, str] = {}

        # Backing store for new-client backfill — "today" as seen by a
        # dashboard viewer is computed client-side in their own local
        # timezone (see dashboard/lib/day.ts); these just need to hold a
        # window generous enough to cover that regardless of server
        # timezone, which DailyJsonlLog.read_recent()'s default already is.
        self._closed_trades_recent: list[dict[str, Any]] = []
        self._audit_recent: list[dict[str, Any]] = []
        self._candles_by_symbol: dict[str, list[dict[str, Any]]] = {}
        self._backfilled_symbols: set[str] = set()

        self._commands: dict[str, AsyncCommandHandler] = {
            "pause": on_pause,
            "resume": on_resume,
            "flatten": on_flatten,
            "kill_switch": on_kill_switch,
            "reset_kill_switch": on_reset_kill_switch,
        }

        self._clients: set["ServerConnection"] = set()
        self._server: "Server | None" = None
        self._kpi_task: asyncio.Task[None] | None = None
        self._starting_equity: float | None = None

    async def start(self) -> None:
        import websockets

        if self._closed_trades_log is not None:
            self._closed_trades_recent = self._closed_trades_log.read_recent()
        if self._audit_log is not None:
            self._audit_recent = self._audit_log.read_recent()

        self._server = await websockets.serve(self._handle_client, self._host, self._port)
        self._event_bus.subscribe_all(self._on_event)
        self._kpi_task = asyncio.create_task(self._kpi_loop(), name="dashboard-bridge-kpi")
        logger.info(
            "dashboard_bridge.started",
            host=self._host,
            port=self._port,
            recovered_closed_trades=len(self._closed_trades_recent),
            recovered_audit_events=len(self._audit_recent),
        )

    async def stop(self) -> None:
        if self._kpi_task:
            self._kpi_task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        logger.info("dashboard_bridge.stopped")

    async def push_audit(
        self, *, level: str, source: str, message: str, meta: dict[str, Any] | None = None
    ) -> None:
        """Ad-hoc audit line for callers outside the event bus (e.g. the
        live script's own risk-check logging), so the dashboard sees the
        same approve/reject narrative an operator sees in the terminal.
        Every audit line — from any source — funnels through here, which is
        what makes this the single place to persist the day's full stream."""
        payload = {"ts": _now_ms(), "level": level, "source": source, "message": message, "meta": meta or {}}
        self._audit_recent.append(payload)
        if len(self._audit_recent) > _MAX_RECENT_AUDIT_EVENTS:
            del self._audit_recent[: len(self._audit_recent) - _MAX_RECENT_AUDIT_EVENTS]
        if self._audit_log is not None:
            self._audit_log.append(payload)
        await self._broadcast({"type": "audit", "payload": payload})

    def note_close_reason(self, symbol: str, reason: str) -> None:
        """Called by the live script's PositionMonitor.on_close_event
        callback right before it submits a stop-loss/take-profit closing
        order, so the Telegram notification for the eventual fill can say
        *why* the position closed (not just that it did). A close that
        never goes through PositionMonitor — manual flatten, kill switch —
        has no entry here and falls back to generic wording."""
        self._close_reasons[symbol] = reason

    async def _handle_client(self, connection: "ServerConnection") -> None:
        self._clients.add(connection)
        logger.info("dashboard_bridge.client_connected", total_clients=len(self._clients))
        await self._send_backfill(connection)
        try:
            async for raw in connection:
                await self._handle_incoming(raw)
        except Exception:
            pass
        finally:
            self._clients.discard(connection)
            logger.info("dashboard_bridge.client_disconnected", total_clients=len(self._clients))

    async def _send_backfill(self, connection: "ServerConnection") -> None:
        """Catches a freshly-connected tab up on history it would otherwise
        never see — closed trades and audit events survive a bot restart
        via disk (DailyJsonlLog); candles are held in-memory only per
        symbol, refreshed from Binance the first time each symbol needs
        charting (see the BarEvent branch of _on_event). Sent once, before
        this connection's message loop starts, so it can never race with a
        live broadcast for the same connection."""
        try:
            if self._closed_trades_recent:
                await connection.send(
                    json.dumps(
                        {"type": "closed_trades_backfill", "payload": self._closed_trades_recent}, default=str
                    )
                )
            if self._audit_recent:
                await connection.send(
                    json.dumps({"type": "audit_backfill", "payload": self._audit_recent}, default=str)
                )
            for symbol, candles in self._candles_by_symbol.items():
                if not candles:
                    continue
                await connection.send(
                    json.dumps(
                        {"type": "candles_backfill", "payload": {"symbol": symbol, "candles": candles}},
                        default=str,
                    )
                )
        except Exception:
            logger.debug("dashboard_bridge.backfill_send_failed")

    async def _handle_incoming(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("dashboard_bridge.malformed_command")
            return

        token = msg.get("token")
        if not self._control_token or token != self._control_token:
            logger.error("dashboard_bridge.command_rejected_bad_token", command=msg.get("command"))
            await self.push_audit(
                level="error",
                source="dashboard_bridge",
                message="Rejected a dashboard command: missing or invalid control token",
            )
            return

        command = msg.get("command")
        handler = self._commands.get(command)
        if handler is None:
            logger.warning("dashboard_bridge.unknown_command", command=command)
            return

        logger.warning("dashboard_bridge.command_received", command=command)
        try:
            await handler()
        except Exception:
            logger.exception("dashboard_bridge.command_failed", command=command)
            await self.push_audit(
                level="error", source="dashboard_bridge", message=f"Command '{command}' failed — see server logs"
            )

    async def _broadcast(self, message: dict[str, Any]) -> None:
        if not self._clients:
            return
        payload = json.dumps(message, default=str)
        dead: set["ServerConnection"] = set()
        for client in self._clients:
            try:
                await client.send(payload)
            except Exception:
                dead.add(client)
        self._clients -= dead

    def _wants_chart_for(self, symbol: str) -> bool:
        """Which symbols the dashboard needs candle data for: always the
        designated primary chart symbol (single-chart strategies like the
        pairs bot), plus — when a trade tracker is wired up — whatever
        symbols currently have an open position, so a multi-position
        strategy's chart grid gets data for exactly what's actually shown."""
        if symbol == self._chart_symbol:
            return True
        if self._trade_tracker is not None and symbol in self._trade_tracker.open_trades():
            return True
        return False

    async def _on_event(self, event: Event) -> None:
        if isinstance(event, BarEvent):
            if not self._wants_chart_for(event.symbol):
                return
            await self._backfill_candles_once(event.symbol)

            candle_payload = {
                "symbol": event.symbol,
                "time": int(event.ts.timestamp()),
                "open": event.open,
                "high": event.high,
                "low": event.low,
                "close": event.close,
                "volume": event.volume,
            }
            cache = self._candles_by_symbol.setdefault(event.symbol, [])
            if cache and cache[-1]["time"] == candle_payload["time"]:
                cache[-1] = candle_payload
            else:
                cache.append(candle_payload)
                if len(cache) > _MAX_CANDLES_PER_SYMBOL_CACHE:
                    del cache[0]
            await self._broadcast({"type": "candle", "payload": candle_payload})
        elif isinstance(event, SignalEvent):
            await self.push_audit(
                level="signal",
                source=event.strategy_id,
                message=f"{event.side.value} {event.target_quantity} {event.symbol} signal generated",
                meta={k: v for k, v in event.metadata.items()},
            )
        elif isinstance(event, (OrderAckEvent, OrderRejectEvent, ExecutionEvent)):
            await self._handle_order_event(event)
        elif isinstance(event, RiskHaltEvent):
            await self._broadcast(
                {"type": "risk", "payload": {"circuitBreakerHalted": True, "haltReason": event.reason}}
            )
            await self.push_audit(level="error", source=event.triggered_by, message=event.reason)
            if self._notifier is not None:
                await self._notifier.send(f"🚨 *Circuit breaker tripped*\n{event.reason}")

    async def _backfill_candles_once(self, symbol: str) -> None:
        """The first time a symbol needs charting (it's the fixed
        chart_symbol, or a position just opened on it), fetch its recent
        history from Binance and broadcast it once so charts open already
        populated instead of starting from a single live bar. A no-op on
        every subsequent bar for that symbol — cheap to call unconditionally
        from the BarEvent hot path."""
        if symbol in self._backfilled_symbols or self._historical_klines_provider is None:
            return
        self._backfilled_symbols.add(symbol)
        try:
            candles = await self._historical_klines_provider(symbol, _MAX_CANDLES_PER_SYMBOL_CACHE)
        except Exception:
            logger.exception("dashboard_bridge.klines_backfill_failed", symbol=symbol)
            return
        if not candles:
            return
        self._candles_by_symbol[symbol] = candles[-_MAX_CANDLES_PER_SYMBOL_CACHE:]
        await self._broadcast({"type": "candles_backfill", "payload": {"symbol": symbol, "candles": candles}})

    async def _handle_order_event(self, event: OrderAckEvent | OrderRejectEvent | ExecutionEvent) -> None:
        managed = self._order_manager.get_order(event.order_id)
        if managed is None:
            return
        await self._broadcast(
            {
                "type": "order",
                "payload": {
                    "id": managed.order_id,
                    "symbol": managed.symbol,
                    "side": managed.side.value,
                    "orderType": _ORDER_TYPE_TO_DASHBOARD.get(managed.order_type, "MKT"),
                    "quantity": managed.quantity,
                    "filledQuantity": managed.filled_quantity,
                    "limitPrice": managed.limit_price,
                    "avgFillPrice": managed.avg_fill_price or None,
                    "status": _ORDER_STATUS_TO_DASHBOARD.get(managed.status, "WORKING"),
                    "strategyId": self._strategy_id,
                    "slippageTicks": None,
                    "submittedAt": int(managed.created_ts.timestamp() * 1000),
                    "updatedAt": int(managed.updated_ts.timestamp() * 1000),
                    "rejectReason": managed.reject_reason or None,
                },
            }
        )
        if isinstance(event, ExecutionEvent):
            await self._broadcast(
                {
                    "type": "execution_marker",
                    "payload": {
                        "symbol": event.symbol,
                        "time": int(event.ts.timestamp()),
                        "price": event.fill_price,
                        "side": event.side.value,
                        "quantity": event.fill_quantity,
                    },
                }
            )
            await self.push_audit(
                level="fill",
                source="order_manager",
                message=f"FILLED {event.side.value} {event.fill_quantity} {event.symbol} @ {event.fill_price}",
            )
            if self._trade_tracker is not None:
                was_open = event.symbol in self._trade_tracker.open_trades()
                closed = self._trade_tracker.on_execution(event)
                now_open = event.symbol in self._trade_tracker.open_trades()

                if not was_open and now_open and self._notifier is not None:
                    await self._notifier.send(
                        f"🟢 *Opened* {event.side.value} `{event.symbol}`\n"
                        f"Qty: {event.fill_quantity}  Entry: {event.fill_price}"
                    )

                if closed is not None:
                    closed_payload = {
                        "symbol": closed.symbol,
                        "side": closed.side.value,
                        "quantity": closed.quantity,
                        "entryPrice": closed.entry_price,
                        "exitPrice": closed.exit_price,
                        "pnl": closed.pnl,
                        "openedAt": int(closed.entry_ts.timestamp() * 1000),
                        "closedAt": int(closed.exit_ts.timestamp() * 1000),
                        "durationSec": closed.duration_seconds,
                    }
                    self._closed_trades_recent.append(closed_payload)
                    if len(self._closed_trades_recent) > _MAX_RECENT_CLOSED_TRADES:
                        del self._closed_trades_recent[: len(self._closed_trades_recent) - _MAX_RECENT_CLOSED_TRADES]
                    if self._closed_trades_log is not None:
                        self._closed_trades_log.append(closed_payload)
                    await self._broadcast({"type": "closed_trade", "payload": closed_payload})
                    await self.push_audit(
                        level="fill" if closed.pnl >= 0 else "risk",
                        source="trade_tracker",
                        message=f"CLOSED {closed.symbol} {closed.side.value} — P&L {closed.pnl:+.2f} USDT",
                    )
                    if self._notifier is not None:
                        reason = self._close_reasons.pop(closed.symbol, "")
                        if reason.startswith("STOP-LOSS"):
                            label = "🔴 *STOP-LOSS HIT*"
                        elif reason.startswith("TAKE-PROFIT"):
                            label = "🟢 *TAKE-PROFIT HIT*"
                        else:
                            label = "⚪ *Position closed*"
                        await self._notifier.send(
                            f"{label}: `{closed.symbol}`\n"
                            f"P&L: *{closed.pnl:+.2f} USDT*\n"
                            f"Entry {closed.entry_price} → Exit {closed.exit_price}\n"
                            f"Duration: {int(closed.duration_seconds // 60)}m"
                        )

    async def _push_open_trades(self) -> None:
        if self._position_details_provider is None:
            return
        details = await self._position_details_provider()
        open_starts = self._trade_tracker.open_trades() if self._trade_tracker is not None else {}
        payload = [
            {
                "symbol": d.symbol,
                "side": d.side.value,
                "quantity": d.quantity,
                "entryPrice": d.entry_price,
                "markPrice": d.mark_price,
                "unrealizedPnl": d.unrealized_pnl,
                "openedAt": int(open_starts[d.symbol].entry_ts.timestamp() * 1000) if d.symbol in open_starts else None,
            }
            for d in details
        ]
        await self._broadcast({"type": "open_trades", "payload": payload})

    async def _kpi_loop(self) -> None:
        while True:
            try:
                await self._push_status_and_kpis()
                await self._push_open_trades()
            except Exception:
                logger.exception("dashboard_bridge.kpi_push_failed")
            await asyncio.sleep(self._kpi_push_interval_s)

    async def _push_status_and_kpis(self) -> None:
        equity = await self._broker.get_account_equity()
        if self._starting_equity is None:
            self._starting_equity = equity
        initial_margin, _ = await self._broker.get_margin_usage()

        # Prefer position_details_provider (one call gives both the
        # per-position unrealized P&L we need here AND the size used for
        # positionContractsUsed below) over the plain get_positions() call,
        # so this loop doesn't make two separate broker round-trips for
        # overlapping data. Falls back to 0 unrealized P&L only if no
        # provider was wired up (e.g. an older/minimal caller).
        if self._position_details_provider is not None:
            details = await self._position_details_provider()
            unrealized_pnl = sum(d.unrealized_pnl for d in details)
            position_contracts_used = sum(abs(d.quantity) for d in details)
        else:
            positions = await self._broker.get_positions()
            unrealized_pnl = 0.0
            position_contracts_used = sum(abs(q) for q in positions.values())

        # Realized P&L = actual sum of closed-trade P&L (previously
        # hardcoded to 0.0 while the equity-delta figure — which mixes
        # BOTH realized and unrealized — was mislabeled "unrealized").
        # This is the bridge's own all-time-this-process fallback; the
        # dashboard's headline tile instead sums today's (locally-scoped)
        # closed trades client-side from the same backfilled list, see
        # dashboard/components/dashboard/telemetry-ribbon.tsx.
        realized_pnl = sum(t["pnl"] for t in self._closed_trades_recent)
        total_pnl = realized_pnl + unrealized_pnl

        await self._broadcast(
            {
                "type": "status",
                "payload": {
                    "broker": "connected" if self._broker.is_connected else "disconnected",
                    "redis": "disconnected",  # this bridge talks directly over WebSocket, not Redis
                    "engine": "connected",
                    "latencyMs": 0,
                },
            }
        )
        await self._broadcast(
            {
                "type": "kpis",
                "payload": {
                    "portfolioValue": equity,
                    "realizedPnl": realized_pnl,
                    "unrealizedPnl": unrealized_pnl,
                    "sharpeRatio": 0.0,
                    "marginUsedUsd": initial_margin,
                    "marginLimitUsd": equity,
                },
            }
        )
        peak_equity = max(equity, self._starting_equity)
        await self._broadcast(
            {
                "type": "equity_point",
                "payload": {
                    "time": int(datetime.now(timezone.utc).timestamp()),
                    "equity": equity,
                    "drawdownPct": ((equity - peak_equity) / peak_equity) * 100 if peak_equity else 0.0,
                },
            }
        )
        await self._broadcast(
            {
                "type": "risk",
                "payload": {
                    "maxDailyLossUsd": self._max_daily_loss_usd,
                    "dailyLossUsedUsd": max(0.0, -total_pnl),
                    "maxPositionContracts": self._max_position_contracts,
                    "positionContractsUsed": position_contracts_used,
                    "strategyPaused": self._is_strategy_paused(),
                    "circuitBreakerHalted": self._circuit_breaker.is_halted,
                    "haltReason": self._circuit_breaker.halt_reason or None,
                },
            }
        )
