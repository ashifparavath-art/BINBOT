"""Entry point: run the funding-momentum scanner strategy across every
USDT-margined Binance Futures perpetual.

    python scripts/run_live_binance_scanner.py

This is a SEPARATE script from run_live_binance.py (the ES/NQ-style
Kalman pairs strategy) — that script and its fixed 2-symbol design are
untouched. This one trades a fundamentally different strategy: it scans
the whole exchange for a large price move confirmed by a funding-rate
trend, opens up to `SCANNER_MAX_OPEN_POSITIONS` positions at once, and
watches each one continuously for its own stop-loss/take-profit — see
config/settings.py's ScannerStrategySettings for every tunable.

Reads BINANCE_API_KEY / BINANCE_API_SECRET from the environment ONLY.
`BINANCE_TESTNET` defaults to true; never flip it without deliberately
meaning to. This strategy uses CROSS margin (SCANNER_MARGIN_TYPE) rather
than the isolated default used elsewhere — that is what makes the
200 USDT stop-loss enforceable in software; see the settings docstring
for why.
"""
from __future__ import annotations

import asyncio
import dataclasses
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import configure_logging, get_logger
from config.settings import get_settings
from core.enums import OrderSide, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import Event, EventType, SignalEvent, TickEvent
from data.bar_aggregator import BarAggregator
from data.binance_universe_feed import BinanceUniverseFeed
from data.storage import MarketDataStore
from execution.binance_broker import BinanceFuturesBroker
from execution.order_manager import OrderLifecycleManager
from execution.position_monitor import PositionMonitor
from execution.trade_tracker import TradeTracker
from notifications.telegram_notifier import TelegramNotifier
from risk.circuit_breaker import DrawdownCircuitBreaker
from utils.daily_log import DailyJsonlLog
from risk.margin import MarginCalculator, MarginRequirement
from risk.risk_engine import RiskEngine
from server.dashboard_bridge import DashboardBridge, load_or_create_control_token
from strategies.funding_momentum_scanner import FundingMomentumScannerStrategy, ScannerParams
from utils.state_recovery import StateRecoveryService

logger = get_logger(__name__)

DASHBOARD_CHART_SYMBOL = "BTCUSDT"  # the dashboard's price panel shows one
# symbol at a time; with positions spread across up to 4 of ~300 possible
# symbols, no single choice is fully representative — this just picks a
# liquid, always-present one so the chart panel has something to show.


async def main() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)

    bset = settings.binance
    sset = settings.scanner

    if bset.testnet:
        logger.warning("startup.TESTNET_MODE", msg="Trading on Binance TESTNET (practice funds only)")
    else:
        logger.warning(
            "startup.LIVE_MODE_REAL_MONEY",
            msg="BINANCE_TESTNET=false — this will place REAL orders with REAL funds",
        )

    api_key = bset.api_key.get_secret_value()
    api_secret = bset.api_secret.get_secret_value()
    if not api_key or not api_secret:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET are not set. Put them in your .env file "
            "(never in code, never committed to git) before running this script."
        )

    # symbols=[] deliberately: the scanner doesn't pre-load a fixed
    # watchlist the way the pairs script does — the universe (get_
    # usdt_perpetual_universe + load_all_filters below) is discovered and
    # bulk-loaded once instead, and leverage/margin-type is only actually
    # set on Binance for a symbol the instant we're really about to trade
    # it (ensure_symbol_ready, called from on_signal).
    broker = BinanceFuturesBroker(
        api_key=api_key,
        api_secret=api_secret,
        testnet=bset.testnet,
        symbols=[],
        leverage=sset.leverage,
        margin_type=sset.margin_type,
        recv_window_ms=bset.recv_window_ms,
        reconnect_backoff_s=bset.reconnect_backoff_s,
        reconnect_backoff_max_s=bset.reconnect_backoff_max_s,
    )
    await broker.connect()

    universe = await broker.get_usdt_perpetual_universe()
    universe_set = set(universe)
    await broker.load_all_filters()
    logger.info("startup.universe_loaded", symbol_count=len(universe))

    event_bus = EventBus()
    # Separate DB file from the pairs-trading script's default path — DuckDB
    # doesn't allow two processes to hold the same file open concurrently,
    # and both live scripts now run at once (one per dashboard tab).
    scanner_db_path = str(Path(settings.data.duckdb_path).with_name("market_data_scanner.duckdb"))
    store = MarketDataStore(scanner_db_path)

    circuit_breaker = DrawdownCircuitBreaker(
        max_daily_drawdown_pct=settings.risk.max_daily_drawdown_pct, starting_equity=1.0
    )
    margin_calc = MarginCalculator(
        initial_margin_buffer_pct=settings.risk.initial_margin_buffer_pct,
        maintenance_margin_buffer_pct=settings.risk.maintenance_margin_buffer_pct,
    )

    risk_engine = RiskEngine(
        max_contracts_per_order=settings.risk.max_contracts_per_order,
        max_contracts_per_symbol=settings.risk.max_contracts_per_symbol,
        max_position_notional_usd=settings.risk.max_position_notional_usd,
        max_orders_per_second=settings.risk.max_orders_per_second,
        max_orders_per_minute=settings.risk.max_orders_per_minute,
        max_gross_leverage=settings.risk.max_gross_leverage,
        circuit_breaker=circuit_breaker,
        margin_calculator=margin_calc,
    )
    for symbol in universe:
        # multiplier = "real asset units per lot", so notional math (lots *
        # price * multiplier) comes out correct in USD terms, for every
        # symbol, immediately — cheap, no extra API calls (filters were
        # already bulk-loaded above).
        risk_engine.set_contract_multiplier(symbol, broker.get_step_size(symbol))

    recovery = StateRecoveryService(broker=broker, risk_engine=risk_engine, circuit_breaker=circuit_breaker)
    recovery_result = await recovery.recover()
    logger.info("state_recovery.result", **dataclasses.asdict(recovery_result))

    order_manager = OrderLifecycleManager(broker, event_bus)
    order_manager.start()

    control_token = load_or_create_control_token()
    logger.warning(
        "dashboard_bridge.control_token",
        msg="add this exact line to dashboard/.env.local to enable dashboard controls, then restart the dashboard",
        env_line=f"NEXT_PUBLIC_DASHBOARD_CONTROL_TOKEN={control_token}",
    )

    # Forward reference, assigned below. Actually bound to None (not just a
    # bare annotation) because is_strategy_paused is polled unconditionally
    # by the dashboard's KPI loop as soon as dashboard_bridge.start() runs
    # below — well before `scanner` is constructed — unlike the other
    # closures here, which only ever fire in response to a dashboard
    # command sent after the whole bot is up.
    scanner: FundingMomentumScannerStrategy | None = None

    async def do_pause() -> None:
        scanner.pause()

    async def do_resume() -> None:
        scanner.resume()

    async def flatten_all_positions() -> None:
        positions = await broker.get_positions()
        open_positions = {sym: qty for sym, qty in positions.items() if qty != 0}
        if not open_positions:
            await dashboard_bridge.push_audit(
                level="risk", source="operator", message="Flatten requested: no open positions to close"
            )
            return
        await dashboard_bridge.push_audit(
            level="risk", source="operator", message=f"FLATTEN: closing {len(open_positions)} position(s) at market"
        )
        for symbol, qty in open_positions.items():
            side = OrderSide.SELL if qty > 0 else OrderSide.BUY
            close_signal = SignalEvent(
                ts=datetime.now(timezone.utc),
                symbol=symbol,
                strategy_id="operator-flatten",
                side=side,
                target_quantity=abs(qty),
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
            )
            await order_manager.submit(close_signal, expected_price=universe_feed.latest_price(symbol))

    async def engage_kill_switch() -> None:
        scanner.pause()
        await order_manager.cancel_all()
        circuit_breaker.force_halt("Manual kill switch via dashboard")
        recovery.persist()
        await dashboard_bridge.push_audit(
            level="error",
            source="operator",
            message="KILL SWITCH engaged via dashboard: cancelled all working orders and halted new entries "
            "(existing open positions are still watched for stop-loss/take-profit)",
        )

    async def reset_kill_switch() -> None:
        equity = await broker.get_account_equity()
        circuit_breaker.reset_session(equity)
        scanner.resume()
        recovery.persist()
        await dashboard_bridge.push_audit(
            level="info", source="operator", message="Kill switch reset via dashboard: trading loop re-armed"
        )

    trade_tracker = TradeTracker(broker.get_step_size)
    notifier = TelegramNotifier(
        settings.telegram.bot_token.get_secret_value(), settings.telegram.chat_id
    )
    if notifier.enabled:
        logger.info("telegram.enabled")
    else:
        logger.info("telegram.disabled", msg="set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env to enable")

    dashboard_bridge = DashboardBridge(
        event_bus=event_bus,
        broker=broker,
        order_manager=order_manager,
        circuit_breaker=circuit_breaker,
        max_daily_loss_usd=settings.risk.max_daily_drawdown_pct / 100.0 * (await broker.get_account_equity()),
        max_position_contracts=sset.max_open_positions,
        chart_symbol=DASHBOARD_CHART_SYMBOL,
        strategy_id="funding-momentum-scanner",
        control_token=control_token,
        on_pause=do_pause,
        on_resume=do_resume,
        on_flatten=flatten_all_positions,
        on_kill_switch=engage_kill_switch,
        on_reset_kill_switch=reset_kill_switch,
        is_strategy_paused=lambda: scanner.is_paused if scanner is not None else False,
        trade_tracker=trade_tracker,
        position_details_provider=broker.get_position_details,
        notifier=notifier,
        closed_trades_log=DailyJsonlLog("data_store/closed_trades"),
        audit_log=DailyJsonlLog("data_store/audit_log"),
        historical_klines_provider=broker.get_mark_price_klines,
        # Different port from the pairs-trading script (8765) so both bots
        # can run — and both be watched on the dashboard — at once.
        port=8766,
        # DashboardBridge defaults to 127.0.0.1 (loopback-only) since it has
        # a write path. Widened to all interfaces here specifically because
        # this deployment's actual access control is Tailscale (a private,
        # authenticated overlay network) plus the AWS security group, which
        # only allows inbound SSH — nothing routes to this port from the
        # public internet either way, and every command still requires the
        # control_token on top of that.
        host="0.0.0.0",
    )
    await dashboard_bridge.start()
    logger.info(
        "dashboard_bridge.ready",
        msg="dashboard can now connect at ws://localhost:8766 (set NEXT_PUBLIC_SCANNER_WS_URL)",
    )

    universe_feed = BinanceUniverseFeed(
        broker.socket_manager,
        event_bus,
        price_window_min=sset.price_jump_window_min,
        funding_window_min=sset.funding_trend_window_min,
    )
    # Backfill from historical klines before going live, so a restart
    # doesn't need to silently wait out a fresh rolling window — the
    # scanner can evaluate entries immediately using data covering the
    # window(s) leading up to this process starting.
    await universe_feed.backfill(broker.client, universe)
    await universe_feed.start()

    async def on_position_close(symbol: str, reason: str) -> None:
        # Recorded before the closing order is even submitted, so it's in
        # place by the time the fill lands and DashboardBridge sends the
        # Telegram notification — that's what lets that message say
        # STOP-LOSS/TAKE-PROFIT instead of generic "closed" wording.
        dashboard_bridge.note_close_reason(symbol, reason)
        await dashboard_bridge.push_audit(
            level="risk", source="position_monitor", message=f"{symbol}: {reason}"
        )

    position_monitor = PositionMonitor(
        broker,
        order_manager,
        stop_loss_usdt=sset.stop_loss_usdt,
        take_profit_usdt=sset.take_profit_usdt,
        poll_interval_s=sset.position_monitor_interval_s,
        on_close_event=on_position_close,
        price_lookup=universe_feed.latest_price,
    )
    position_monitor.start()

    scanner = FundingMomentumScannerStrategy(
        strategy_id="funding-momentum-scanner",
        event_bus=event_bus,
        symbols=universe,
        universe_feed=universe_feed,
        position_monitor=position_monitor,
        params=ScannerParams(
            max_open_positions=sset.max_open_positions,
            order_notional_usdt=sset.order_notional_usdt,
            price_jump_pct=sset.price_jump_pct,
        ),
        step_size_lookup=broker.get_step_size,
    )
    scanner.attach()

    bar_aggregator = BarAggregator(timeframe_s=60)
    last_price: dict[str, float] = {}

    async def refresh_margin_schedule(symbol: str, price: float) -> None:
        step_size = broker.get_step_size(symbol)
        per_lot_notional = price * step_size
        per_lot_margin = per_lot_notional / max(sset.leverage, 1)
        margin_calc.set_requirement(
            MarginRequirement(
                symbol=symbol,
                initial_margin_per_contract=per_lot_margin,
                maintenance_margin_per_contract=per_lot_margin * 0.8,
            )
        )

    async def on_tick(event: Event) -> None:
        assert isinstance(event, TickEvent)
        if event.symbol not in universe_set:
            # The combined mark-price stream carries every contract Binance
            # Futures offers, including COIN-margined and dated-delivery
            # contracts we never asked for and have no filters/multiplier
            # loaded for (get_usdt_perpetual_universe() already excluded
            # them). Skip rather than let get_step_size() KeyError below.
            return
        # Deliberately NOT calling store.write_tick() here: with ~300
        # symbols updating roughly once a second each, that's on the order
        # of 300 writes/sec sustained, mostly for symbols nothing ever
        # happens on. The rolling price/funding windows that actually
        # drive entries live in BinanceUniverseFeed's in-memory history,
        # not in DuckDB. Completed 1-min bars ARE still persisted below
        # (~5/sec at this scale) for a historical record.
        last_price[event.symbol] = event.mid
        risk_engine.update_market_state(event.symbol, event.mid)
        await refresh_margin_schedule(event.symbol, event.mid)
        completed_bar = bar_aggregator.on_tick(event)
        if completed_bar is not None:
            store.write_bar(completed_bar)
            await event_bus.publish(completed_bar)

    event_bus.subscribe(EventType.TICK, on_tick)

    async def on_signal(event: Event) -> None:
        assert isinstance(event, SignalEvent)
        account_equity = await broker.get_account_equity()
        positions = await broker.get_positions()
        gross_exposure_usd = sum(
            abs(qty) * last_price.get(sym, 0.0) * broker.get_step_size(sym)
            for sym, qty in positions.items()
        )
        initial_margin, maintenance_margin = await broker.get_margin_usage()
        result = risk_engine.check_order(
            event,
            account_equity=account_equity,
            gross_exposure_usd=gross_exposure_usd,
            current_initial_margin_used=initial_margin,
            current_maintenance_margin_used=maintenance_margin,
        )
        if not result.passed:
            await dashboard_bridge.push_audit(
                level="risk", source="risk_engine", message=f"REJECTED [{result.code}] {result.message}"
            )
            return
        await dashboard_bridge.push_audit(level="risk", source="risk_engine", message="pre-trade checks passed")
        # Only NOW — right before actually submitting — do we make the real
        # Binance API calls to set this symbol's leverage/margin type. Most
        # of the ~300-symbol universe will never reach this line.
        await broker.ensure_symbol_ready(event.symbol, leverage=sset.leverage, margin_type=sset.margin_type)
        await order_manager.submit(event, expected_price=last_price.get(event.symbol))

    event_bus.subscribe(EventType.SIGNAL, on_signal)

    await event_bus.start()

    shutdown_event = asyncio.Event()

    def _handle_shutdown_signal() -> None:
        logger.warning("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _handle_shutdown_signal)
            except NotImplementedError:
                pass  # not supported on Windows event loops

    async def monitor_loop() -> None:
        while not shutdown_event.is_set():
            await asyncio.sleep(5.0)
            try:
                equity = await broker.get_account_equity()
                for symbol, qty in (await broker.get_positions()).items():
                    risk_engine.update_position(symbol, qty)
                if circuit_breaker.update(equity):
                    logger.critical("circuit_breaker.tripped_cancelling_all_orders")
                    await order_manager.cancel_all()
                recovery.persist()
            except Exception:
                # A single failed Binance call here (rate limit, network
                # blip, timeout) used to kill this task, which tore down
                # the whole process via the asyncio.wait() below — and
                # systemd's Restart=always then immediately retried the
                # same still-failing call, crash-looping (175+ restarts
                # observed during one Binance rate-limit ban) and likely
                # extending the ban further with every retry. Log and
                # back off instead; next iteration tries again in 5s+.
                logger.exception("monitor_loop.check_failed")
                await asyncio.sleep(10.0)

    try:
        await asyncio.wait(
            [asyncio.create_task(monitor_loop()), asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        logger.info("shutdown.initiating")
        await order_manager.stop()
        await position_monitor.stop()
        await universe_feed.stop()
        await dashboard_bridge.stop()
        await event_bus.stop()
        await broker.disconnect()
        store.close()
        logger.info("shutdown.complete")


if __name__ == "__main__":
    asyncio.run(main())
