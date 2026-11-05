"""Historical backtest runner for the funding-momentum scanner strategy.

Fetches historical mark-price + premium-index klines from Binance's public
(no-auth-required) market data endpoints, replays them through the exact
same strategy/risk-engine/order-manager code path used live, and prints a
performance report.

USAGE (start small to validate, then scale up):
    python scripts/run_backtest.py --days 2 --symbols BTCUSDT,ETHUSDT
    python scripts/run_backtest.py --days 14                      # full universe

Uses MAINNET public data (no API key needed) for realistic historical
prices, regardless of whether live trading runs on testnet.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import re
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from config.settings import RiskSettings, ScannerStrategySettings
from backtest.engine import BacktestEngine
from backtest.market_simulator import MarketSimulatorBroker
from core.enums import OrderSide, OrderType, TimeInForce
from core.event_bus import EventBus
from core.events import BarEvent, SignalEvent
from risk.circuit_breaker import DrawdownCircuitBreaker
from risk.margin import MarginCalculator, MarginRequirement
from risk.risk_engine import RiskEngine
from strategies.funding_momentum_scanner import FundingMomentumScannerStrategy, ScannerParams

KLINE_LIMIT = 1500  # Binance's per-call cap
CACHE_DIR = Path(__file__).resolve().parent.parent / "data_store" / "backtest_cache"
_BAN_RE = re.compile(r"banned until (\d+)")


class GlobalRateLimiter:
    """One shared minimum delay between EVERY outbound Binance call in the
    whole script, regardless of how many symbols/endpoints are involved —
    this is what actually bounds the request rate; concurrency doesn't
    factor in at all since everything funnels through this single gate."""

    def __init__(self, min_interval_s: float) -> None:
        self._min_interval_s = min_interval_s
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._min_interval_s - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = asyncio.get_event_loop().time()


async def call_with_ban_recovery(limiter: GlobalRateLimiter, coro_fn, *, max_attempts: int = 8):
    """Runs one paced API call. On a -1003 ban, sleeps exactly until
    Binance's own 'banned until' timestamp (plus a small buffer) and
    retries — rather than giving up — since this is expected to run
    unattended for hours."""
    for attempt in range(max_attempts):
        await limiter.acquire()
        try:
            return await coro_fn()
        except BinanceAPIException as exc:
            msg = str(exc)
            match = _BAN_RE.search(msg)
            if match:
                ban_until_s = int(match.group(1)) / 1000
                now_s = datetime.now(timezone.utc).timestamp()
                sleep_s = max(ban_until_s - now_s, 0) + 5.0
                print(f"  [banned] sleeping {sleep_s:.0f}s until ban clears...")
                await asyncio.sleep(sleep_s)
                continue
            if "-1003" in msg or "Too many requests" in msg:
                backoff = 20.0 * (attempt + 1)
                print(f"  [throttled] backing off {backoff:.0f}s (attempt {attempt+1}/{max_attempts})...")
                await asyncio.sleep(backoff)
                continue
            raise
    raise RuntimeError(f"Exceeded {max_attempts} retry attempts")


class HistoricalUniverseFeed:
    """Offline replay stand-in for data.binance_universe_feed.BinanceUniverseFeed.
    Same public query interface (price_change_pct, funding_trend, latest_price)
    fed by record_tick() calls driven by the backtest replay loop, instead of
    a live websocket stream."""

    def __init__(self, *, price_window_min: float, funding_window_min: float) -> None:
        self._price_window = timedelta(minutes=price_window_min)
        self._funding_window = timedelta(minutes=funding_window_min)
        self._price_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._funding_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._latest_price: dict[str, float] = {}

    def record_tick(self, symbol: str, ts: datetime, price: float, funding_rate: float) -> None:
        self._append_trimmed(self._price_history[symbol], ts, price, self._price_window)
        self._append_trimmed(self._funding_history[symbol], ts, funding_rate, self._funding_window)
        self._latest_price[symbol] = price

    def latest_price(self, symbol: str) -> float | None:
        return self._latest_price.get(symbol)

    def price_change_pct(self, symbol: str) -> float | None:
        history = self._price_history.get(symbol)
        if not history or len(history) < 2:
            return None
        oldest_ts, oldest_price = history[0]
        if (history[-1][0] - oldest_ts) < self._price_window * 0.9 or oldest_price == 0:
            return None
        return (history[-1][1] - oldest_price) / oldest_price * 100.0

    def funding_trend(self, symbol: str) -> tuple[float, float] | None:
        history = self._funding_history.get(symbol)
        if not history or len(history) < 2:
            return None
        oldest_ts, oldest_rate = history[0]
        if (history[-1][0] - oldest_ts) < self._funding_window * 0.9:
            return None
        return history[-1][1], oldest_rate

    @staticmethod
    def _append_trimmed(history, ts, value, window) -> None:
        history.append((ts, value))
        cutoff = ts - window
        while history and history[0][0] < cutoff:
            history.popleft()


async def fetch_klines_paginated(
    limiter: GlobalRateLimiter, client: AsyncClient, *, method_name: str, symbol: str,
    interval: str, start_ms: int, end_ms: int,
) -> list[list]:
    method = getattr(client, method_name)
    rows: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        async def _call():
            return await method(symbol=symbol, interval=interval, startTime=cursor, endTime=end_ms, limit=KLINE_LIMIT)
        batch = await call_with_ban_recovery(limiter, _call)
        if not batch:
            break
        rows.extend(batch)
        last_open_time = batch[-1][0]
        if last_open_time <= cursor:
            break
        cursor = last_open_time + 1
        if len(batch) < KLINE_LIMIT:
            break
    return rows


def _cache_path(symbol: str, interval: str, days: int) -> Path:
    return CACHE_DIR / f"{symbol}_{interval}_{days}d.json"


def _load_cached(symbol: str, interval: str, days: int) -> list[dict] | None:
    path = _cache_path(symbol, interval, days)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            raw = json.load(f)
        return [
            {**row, "ts": datetime.fromisoformat(row["ts"])}
            for row in raw
        ]
    except Exception:
        return None  # corrupt/partial cache file — refetch


def _save_cache(symbol: str, interval: str, days: int, rows: list[dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(symbol, interval, days)
    serializable = [{**row, "ts": row["ts"].isoformat()} for row in rows]
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(serializable, f)
    tmp_path.replace(path)  # atomic — a crash mid-write can't leave a corrupt cache file


async def fetch_symbol_bars(
    limiter: GlobalRateLimiter, client: AsyncClient, symbol: str, interval: str,
    start_ms: int, end_ms: int, days: int,
) -> list[dict]:
    """Returns chronological list of {ts, open, high, low, close,
    funding_rate, volume} for one symbol, using a disk cache so a crash or
    restart part-way through a long run doesn't lose completed symbols."""
    cached = _load_cached(symbol, interval, days)
    if cached is not None:
        return cached

    try:
        price_rows = await fetch_klines_paginated(
            limiter, client, method_name="futures_mark_price_klines", symbol=symbol,
            interval=interval, start_ms=start_ms, end_ms=end_ms,
        )
        funding_rows = await fetch_klines_paginated(
            limiter, client, method_name="futures_premium_index_klines", symbol=symbol,
            interval=interval, start_ms=start_ms, end_ms=end_ms,
        )
        trade_rows = await fetch_klines_paginated(
            limiter, client, method_name="futures_klines", symbol=symbol,
            interval=interval, start_ms=start_ms, end_ms=end_ms,
        )
    except Exception as exc:
        print(f"  [skip] {symbol}: {exc}")
        return []

    funding_by_ts = {row[0]: float(row[4]) for row in funding_rows}
    volume_by_ts = {row[0]: float(row[5]) for row in trade_rows}
    out = []
    for row in price_rows:
        ts_ms = row[0]
        if ts_ms not in funding_by_ts:
            continue
        out.append({
            "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "funding_rate": funding_by_ts[ts_ms],
            "volume": volume_by_ts.get(ts_ms, 0.0),
        })
    if out:
        _save_cache(symbol, interval, days, out)
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--interval", type=str, default="5m")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated. Empty = full universe.")
    parser.add_argument("--starting-equity", type=float, default=5000.0)
    parser.add_argument(
        "--request-interval-s", type=float, default=3.0,
        help="Minimum seconds between ANY two outbound Binance calls, globally. "
             "Conservative by design — this is meant to run unattended for hours.",
    )
    parser.add_argument(
        "--max-daily-drawdown-pct", type=float, default=None,
        help=(
            "Overrides RISK_MAX_DAILY_DRAWDOWN_PCT for THIS BACKTEST ONLY — "
            "live trading's real circuit breaker is untouched. Use a large "
            "value (e.g. 1000) to let the backtest run its full date range "
            "uninterrupted, so you can see the strategy's raw signal quality "
            "separately from portfolio-level risk management. Defaults to "
            "the same value live trading uses if not set."
        ),
    )
    parser.add_argument(
        "--commission-per-contract", type=float, default=0.0,
        help=(
            "MarketSimulatorBroker charges this flat fee PER LOT filled. That "
            "model fits traditional futures (few large-notional contracts) but "
            "not crypto, where a single $750 order on a sub-cent coin can be "
            "1M+ lots — a nonzero value here can wipe out equity on one fill. "
            "Defaults to 0 (no commission modeled) until this is fixed properly."
        ),
    )
    args = parser.parse_args()

    sset = ScannerStrategySettings()
    rset = RiskSettings()
    limiter = GlobalRateLimiter(args.request_interval_s)

    client = await AsyncClient.create(testnet=False)  # public data only, no auth needed
    try:
        async def _exchange_info():
            return await client.futures_exchange_info()
        info = await call_with_ban_recovery(limiter, _exchange_info)

        if args.symbols:
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
            step_sizes: dict[str, float] = {}
            for s in info["symbols"]:
                if s["symbol"] in symbols:
                    for f in s["filters"]:
                        if f["filterType"] == "LOT_SIZE":
                            step_sizes[s["symbol"]] = float(f["stepSize"])
        else:
            symbols = []
            step_sizes = {}
            for s in info["symbols"]:
                if s.get("contractType") == "PERPETUAL" and s["symbol"].endswith("USDT") and s["status"] == "TRADING":
                    symbols.append(s["symbol"])
                    for f in s["filters"]:
                        if f["filterType"] == "LOT_SIZE":
                            step_sizes[s["symbol"]] = float(f["stepSize"])

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=args.days)
        start_ms, end_ms = int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)

        already_cached = sum(1 for s in symbols if _cache_path(s, args.interval, args.days).exists())
        print(
            f"Fetching {args.days}d of {args.interval} data for {len(symbols)} symbols "
            f"({already_cached} already cached from a previous run) — "
            f"~{args.request_interval_s}s between requests, so this will take a while by design."
        )

        per_symbol: dict[str, list[dict]] = {}
        for i, sym in enumerate(symbols, 1):
            rows = await fetch_symbol_bars(limiter, client, sym, args.interval, start_ms, end_ms, args.days)
            if rows:
                per_symbol[sym] = rows
            if i % 20 == 0 or i == len(symbols):
                print(f"  progress: {i}/{len(symbols)} symbols processed, {len(per_symbol)} with data so far")

        total_bars = sum(len(v) for v in per_symbol.values())
        print(f"Fetched {total_bars} bars across {len(per_symbol)} symbols (of {len(symbols)} requested).")

        if not per_symbol:
            print("No data fetched — aborting.")
            return
    finally:
        await client.close_connection()

    # --- wire up the exact live stack, replaying offline ---
    feed = HistoricalUniverseFeed(
        price_window_min=sset.price_jump_window_min, funding_window_min=sset.funding_trend_window_min
    )
    effective_drawdown_pct = (
        args.max_daily_drawdown_pct if args.max_daily_drawdown_pct is not None else rset.max_daily_drawdown_pct
    )
    circuit_breaker = DrawdownCircuitBreaker(max_daily_drawdown_pct=effective_drawdown_pct, starting_equity=1.0)
    margin_calc = MarginCalculator(
        initial_margin_buffer_pct=rset.initial_margin_buffer_pct,
        maintenance_margin_buffer_pct=rset.maintenance_margin_buffer_pct,
    )
    risk_engine = RiskEngine(
        max_contracts_per_order=rset.max_contracts_per_order,
        max_contracts_per_symbol=rset.max_contracts_per_symbol,
        max_position_notional_usd=rset.max_position_notional_usd,
        max_orders_per_second=rset.max_orders_per_second,
        max_orders_per_minute=rset.max_orders_per_minute,
        max_gross_leverage=rset.max_gross_leverage,
        circuit_breaker=circuit_breaker,
        margin_calculator=margin_calc,
    )
    contract_multipliers: dict[str, float] = {}
    for sym in per_symbol:
        step = step_sizes.get(sym, 1.0)
        risk_engine.set_contract_multiplier(sym, step)
        contract_multipliers[sym] = step

    broker = MarketSimulatorBroker(
        starting_equity=args.starting_equity, contract_multipliers=contract_multipliers,
        commission_per_contract=args.commission_per_contract,
    )
    engine = BacktestEngine(risk_engine=risk_engine, broker=broker, contract_multipliers=contract_multipliers)

    # Backtest has no live position-poller task — read the simulator
    # broker's own position book directly (sync) so the strategy's
    # re-entry/max-open-positions gating sees real, current state.
    open_symbols_tracker = _BrokerBackedOpenSymbols(broker)

    strategy = FundingMomentumScannerStrategy(
        "funding-momentum-scanner", engine.event_bus, list(per_symbol.keys()), feed,
        open_symbols_tracker, ScannerParams(
            max_open_positions=sset.max_open_positions,
            order_notional_usdt=sset.order_notional_usdt,
            price_jump_pct=sset.price_jump_pct,
        ), lambda sym: contract_multipliers.get(sym, 1.0),
    )
    strategy.attach()

    async def feed_updater(event) -> None:
        feed.record_tick(event.symbol, event.ts, event.close, _funding_lookup.get((event.symbol, event.ts), 0.0))
        # Mirrors live's refresh_margin_schedule(): margin requirement is
        # priced dynamically off the current bar, not a static table —
        # without this every order is rejected with "no margin schedule".
        step = contract_multipliers.get(event.symbol, 1.0)
        per_lot_margin = (event.close * step) / max(sset.leverage, 1)
        margin_calc.set_requirement(
            MarginRequirement(
                symbol=event.symbol,
                initial_margin_per_contract=per_lot_margin,
                maintenance_margin_per_contract=per_lot_margin * 0.8,
            )
        )

    _funding_lookup = {
        (sym, row["ts"]): row["funding_rate"] for sym, rows in per_symbol.items() for row in rows
    }
    from core.events import EventType
    engine.event_bus.subscribe(EventType.BAR, feed_updater)  # registered before strategy -> runs first each bar

    # Mirrors execution/position_monitor.py's exit watch — live has a
    # separate poller doing this every ~2s; the backtest checks once per
    # bar instead, using the same $-based stop-loss/take-profit thresholds
    # and, like the live version, bypassing the risk engine entirely for
    # exits (an exit only ever REDUCES exposure, so it must not be
    # blockable by limits meant to prevent taking on MORE risk).
    _closing: set[str] = set()

    async def exit_watcher(event) -> None:
        symbol = event.symbol
        qty = broker._positions.get(symbol, 0)
        if qty == 0:
            _closing.discard(symbol)  # position actually closed -> re-armed for future entries
            return
        if symbol in _closing:
            return
        entry_price = broker._avg_entry_price.get(symbol, 0.0)
        if entry_price == 0.0:
            return
        multiplier = contract_multipliers.get(symbol, 1.0)
        unrealized_pnl = (event.close - entry_price) * qty * multiplier
        reason = None
        if unrealized_pnl <= -sset.stop_loss_usdt:
            reason = f"STOP-LOSS: {unrealized_pnl:.2f} <= -{sset.stop_loss_usdt:.0f}"
        elif unrealized_pnl >= sset.take_profit_usdt:
            reason = f"TAKE-PROFIT: {unrealized_pnl:.2f} >= {sset.take_profit_usdt:.0f}"
        if reason is None:
            return
        _closing.add(symbol)
        signal = SignalEvent(
            ts=event.ts, symbol=symbol, strategy_id="position-monitor",
            side=OrderSide.SELL if qty > 0 else OrderSide.BUY,
            target_quantity=abs(qty), order_type=OrderType.MARKET, time_in_force=TimeInForce.DAY,
        )
        await engine.order_manager.submit(signal, expected_price=event.close)

    engine.event_bus.subscribe(EventType.BAR, exit_watcher)  # after strategy: same-bar entries aren't exit-checked

    bars = [
        BarEvent(
            ts=row["ts"], symbol=sym, timeframe_s=300, open=row["open"], high=row["high"],
            low=row["low"], close=row["close"], volume=row["volume"], vwap=row["close"],
        )
        for sym, rows in per_symbol.items() for row in rows
    ]

    print(f"Running backtest over {len(bars)} bars...")
    result = await engine.run(bars)

    perf = result.performance
    print("\n" + "=" * 50)
    print("BACKTEST RESULTS")
    print("=" * 50)
    for field_ in dataclasses.fields(perf):
        print(f"  {field_.name}: {getattr(perf, field_.name)}")
    print(f"  halted: {result.halted} ({result.halt_reason})")
    print("=" * 50)


class _BrokerBackedOpenSymbols:
    """Sync adapter over MarketSimulatorBroker's internal position book —
    the strategy calls open_symbols() synchronously (no live poller task
    exists in a backtest), so this reads the broker's real, current fills
    directly rather than a stale/empty stand-in."""

    def __init__(self, broker: MarketSimulatorBroker) -> None:
        self._broker = broker

    def open_symbols(self) -> set[str]:
        return {sym for sym, qty in self._broker._positions.items() if qty != 0}


if __name__ == "__main__":
    asyncio.run(main())
