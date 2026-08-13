"use client";

import { useEffect, useRef } from "react";
import type { StoreApi, UseBoundStore } from "zustand";

import type { TradingState } from "@/store/useTradingStore";
import type { Candle, ClosedTradeRecord, OpenPositionDetail, OrderRow, OrderSide } from "@/lib/types";

/**
 * Simulated real-time engine standing in for the live WebSocket bridge to
 * the Python ATS. Mirrors the causal shape of the real system (signal ->
 * risk check -> order -> fill) so swapping this for a real
 * `new WebSocket(url)` consumer is a localized change: everything
 * downstream reads from whichever store instance is passed in.
 */

const SYMBOL = "ES";
const CANDLE_BUCKET_S = 15; // sped up vs. real 1m bars, for a livelier demo
const CONTRACT_MULTIPLIER = 50;
const STARTING_PRICE = 5487.25;
const STARTING_EQUITY = 1_000_000;

type Store = UseBoundStore<StoreApi<TradingState>>;

let orderSeq = 0;
const nextOrderId = () => {
  orderSeq += 1;
  return `ORD-${String(orderSeq).padStart(5, "0")}`;
};

class MockTradingEngine {
  private price = STARTING_PRICE;
  private bucketStart = Math.floor(Date.now() / 1000 / CANDLE_BUCKET_S) * CANDLE_BUCKET_S;
  private candle: Candle = {
    symbol: SYMBOL,
    time: this.bucketStart,
    open: this.price,
    high: this.price,
    low: this.price,
    close: this.price,
    volume: 0,
  };
  private position = 0; // signed contracts
  private avgEntry = 0;
  private realizedPnl = 0;
  private zScore = 0;
  private equity = STARTING_EQUITY;
  private timers: number[] = [];
  private closingInFlight = false;

  constructor(private readonly store: Store) {}

  start() {
    const { setStatus, pushAuditEvent } = this.store.getState();

    setStatus({ broker: "degraded", redis: "degraded", engine: "degraded", latencyMs: 340 });
    pushAuditEvent({ ts: Date.now(), level: "info", source: "system", message: "Establishing broker + Redis connections..." });

    this.timers.push(
      window.setTimeout(() => {
        setStatus({ broker: "connected", redis: "connected", engine: "connected", latencyMs: 4 });
        pushAuditEvent({
          ts: Date.now(),
          level: "info",
          source: "system",
          message: "IBKR broker connected (paper) · Redis pub/sub online · engine heartbeat nominal",
        });
      }, 1100)
    );

    this.timers.push(window.setInterval(() => this.priceTick(), 350));
    this.timers.push(window.setInterval(() => this.maybeGenerateSignal(), 2600));
    this.timers.push(window.setInterval(() => this.heartbeat(), 1000));
    this.timers.push(window.setInterval(() => this.watchControls(), 500));
  }

  stop() {
    this.timers.forEach((t) => {
      window.clearInterval(t);
      window.clearTimeout(t);
    });
    this.timers = [];
  }

  private heartbeat() {
    const jitter = 2 + Math.random() * 6;
    this.store.getState().setStatus({ latencyMs: Math.round(jitter * 10) / 10 });
  }

  private priceTick() {
    const store = this.store.getState();
    if (store.killSwitchEngaged) return; // frozen: no new market data processed while halted

    const drift = (Math.random() - 0.5) * 1.1;
    this.price = Math.max(this.price + drift, 1);
    const now = Math.floor(Date.now() / 1000);
    const bucketStart = Math.floor(now / CANDLE_BUCKET_S) * CANDLE_BUCKET_S;

    if (bucketStart !== this.bucketStart) {
      this.bucketStart = bucketStart;
      this.candle = { symbol: SYMBOL, time: bucketStart, open: this.price, high: this.price, low: this.price, close: this.price, volume: 0 };
    }

    this.candle = {
      ...this.candle,
      high: Math.max(this.candle.high, this.price),
      low: Math.min(this.candle.low, this.price),
      close: this.price,
      volume: this.candle.volume + Math.round(Math.random() * 12),
    };
    store.pushCandle(this.candle);

    // mark-to-market unrealized P&L + equity curve
    const unrealizedPnl = this.position !== 0 ? (this.price - this.avgEntry) * this.position * CONTRACT_MULTIPLIER : 0;
    this.equity = STARTING_EQUITY + this.realizedPnl + unrealizedPnl;
    const peak = Math.max(this.equity, STARTING_EQUITY);
    store.pushEquityPoint({ time: now, equity: this.equity, drawdownPct: ((this.equity - peak) / peak) * 100 });

    const marginUsed = Math.abs(this.position) * 12_500;
    store.setKpis({
      portfolioValue: this.equity,
      realizedPnl: this.realizedPnl,
      unrealizedPnl,
      sharpeRatio: this.estimateSharpe(),
      marginUsedUsd: marginUsed,
    });
    store.setRisk({
      dailyLossUsedUsd: Math.max(0, -(this.realizedPnl + unrealizedPnl)),
      positionContractsUsed: Math.abs(this.position),
    });
  }

  private estimateSharpe(): number {
    const drawdownFactor = 1 - Math.abs(this.equity - STARTING_EQUITY) / STARTING_EQUITY;
    const base = ((this.equity - STARTING_EQUITY) / STARTING_EQUITY) * 40;
    return Math.max(-3, Math.min(3.5, base * drawdownFactor + 0.8));
  }

  private watchControls() {
    const store = this.store.getState();
    if (store.flattenInProgress && this.position !== 0) {
      this.closePosition("Flatten-all command");
    }
    if (store.killSwitchEngaged) {
      const cancellable = store.orders.filter((o) => o.status === "WORKING" || o.status === "PARTIAL");
      if (cancellable.length > 0) {
        cancellable.forEach((o) => store.upsertOrder({ ...o, status: "CANCELED", updatedAt: Date.now() }));
        store.pushAuditEvent({
          ts: Date.now(),
          level: "error",
          source: "risk_engine",
          message: `Kill switch: sent MKT cancel for ${cancellable.length} working order(s)`,
        });
      }
    }
  }

  private maybeGenerateSignal() {
    const store = this.store.getState();
    if (store.killSwitchEngaged || store.risk.strategyPaused) return;
    if (Math.random() > 0.55) return; // not every tick produces a signal, like the real pairs strategy

    this.zScore = Math.max(-3.4, Math.min(3.4, this.zScore * 0.6 + (Math.random() - 0.5) * 2.4));
    const entryThreshold = 2.0;
    const exitThreshold = 0.5;

    store.pushAuditEvent({
      ts: Date.now(),
      level: "signal",
      source: "kalman-pairs-1",
      message: `spread z-score=${this.zScore.toFixed(3)} beta=3.58 (ES vs NQ)`,
      meta: { z_score: Number(this.zScore.toFixed(3)) },
    });

    let side: OrderSide | null = null;
    if (this.position === 0 && this.zScore <= -entryThreshold) side = "BUY";
    else if (this.position === 0 && this.zScore >= entryThreshold) side = "SELL";
    else if (this.position !== 0 && Math.abs(this.zScore) <= exitThreshold) side = this.position > 0 ? "SELL" : "BUY";

    if (!side) return;

    store.pushAuditEvent({
      ts: Date.now(),
      level: "risk",
      source: "risk_engine",
      message: `pre-trade checks passed: fat-finger OK, notional OK, margin OK, rate-limit OK`,
    });

    this.submitOrder(side, 1);
  }

  private submitOrder(side: OrderSide, quantity: number, onFilled?: () => void) {
    const store = this.store.getState();
    const id = nextOrderId();
    const decisionPrice = this.price;
    const order: OrderRow = {
      id,
      symbol: SYMBOL,
      side,
      orderType: "MKT",
      quantity,
      filledQuantity: 0,
      limitPrice: null,
      avgFillPrice: null,
      status: "WORKING",
      strategyId: "kalman-pairs-1",
      slippageTicks: null,
      submittedAt: Date.now(),
      updatedAt: Date.now(),
    };
    store.upsertOrder(order);
    store.pushAuditEvent({
      ts: Date.now(),
      level: "info",
      source: "order_manager",
      message: `submitted ${side} ${quantity} ${SYMBOL} MKT (order ${id})`,
    });

    this.timers.push(
      window.setTimeout(() => {
        const slippage = (Math.random() - 0.3) * 0.4;
        const fillPrice = decisionPrice + slippage * (side === "BUY" ? 1 : -1);
        const filled: OrderRow = {
          ...order,
          filledQuantity: quantity,
          avgFillPrice: fillPrice,
          status: "FILLED",
          slippageTicks: slippage / 0.25,
          updatedAt: Date.now(),
        };
        store.upsertOrder(filled, side === "BUY" ? "profit" : "loss");
        store.pushExecutionMarker({ symbol: SYMBOL, time: Math.floor(Date.now() / 1000), price: fillPrice, side, quantity });
        store.pushAuditEvent({
          ts: Date.now(),
          level: "fill",
          source: "order_manager",
          message: `FILLED ${side} ${quantity} ${SYMBOL} @ ${fillPrice.toFixed(2)} (${(slippage / 0.25).toFixed(2)} ticks slippage)`,
        });

        this.applyFill(side, quantity, fillPrice);
        onFilled?.();
      }, 400 + Math.random() * 500)
    );
  }

  private applyFill(side: OrderSide, quantity: number, price: number) {
    const signedQty = side === "BUY" ? quantity : -quantity;
    const prevPosition = this.position;
    const newPosition = prevPosition + signedQty;

    if (prevPosition !== 0 && Math.sign(prevPosition) !== Math.sign(signedQty)) {
      const closingQty = Math.min(Math.abs(signedQty), Math.abs(prevPosition));
      const pnlSign = prevPosition > 0 ? 1 : -1;
      this.realizedPnl += pnlSign * (price - this.avgEntry) * closingQty * CONTRACT_MULTIPLIER;
    }

    if (newPosition === 0) {
      this.avgEntry = 0;
    } else if (Math.sign(newPosition) === Math.sign(signedQty) && prevPosition !== 0 && Math.sign(prevPosition) === Math.sign(signedQty)) {
      const totalQty = Math.abs(prevPosition) + Math.abs(signedQty);
      this.avgEntry = (this.avgEntry * Math.abs(prevPosition) + price * Math.abs(signedQty)) / totalQty;
    } else {
      this.avgEntry = price;
    }

    this.position = newPosition;
  }

  private closePosition(reason: string) {
    if (this.position === 0 || this.closingInFlight) return;
    this.closingInFlight = true;
    const side: OrderSide = this.position > 0 ? "SELL" : "BUY";
    const quantity = Math.abs(this.position);
    this.store.getState().pushAuditEvent({
      ts: Date.now(),
      level: "risk",
      source: "order_manager",
      message: `${reason}: closing ${quantity} ${SYMBOL} via ${side} MKT`,
    });
    this.submitOrder(side, quantity, () => {
      this.closingInFlight = false;
    });
  }
}

/**
 * Real client for server/dashboard_bridge.py. Connects over a plain
 * WebSocket and dispatches each incoming `{type, payload}` message to the
 * matching Zustand store action — the same actions the mock engine above
 * calls, so every panel works identically regardless of which data source
 * is active. Reconnects with backoff if the Python process restarts.
 */
class LiveWebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectTimer: number | null = null;
  private backoffMs = 1000;
  private readonly maxBackoffMs = 15_000;
  private readonly token = process.env.NEXT_PUBLIC_DASHBOARD_CONTROL_TOKEN || "";
  private stopped = false;

  constructor(
    private readonly store: Store,
    private readonly url: string
  ) {}

  start() {
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    this.store.getState().setLiveCommandSender(null);
    if (this.reconnectTimer !== null) window.clearTimeout(this.reconnectTimer);
    this.ws?.close();
    this.ws = null;
  }

  private sendCommand(command: string) {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    if (!this.token) {
      this.store.getState().pushAuditEvent({
        ts: Date.now(),
        level: "error",
        source: "dashboard",
        message: "Cannot send command: NEXT_PUBLIC_DASHBOARD_CONTROL_TOKEN is not set in dashboard/.env.local",
      });
      return;
    }
    this.ws.send(JSON.stringify({ command, token: this.token }));
  }

  private connect() {
    const store = this.store.getState();
    store.setStatus({ broker: "degraded", redis: "disconnected", engine: "degraded", latencyMs: 0 });

    const ws = new WebSocket(this.url);
    this.ws = ws;

    ws.onopen = () => {
      this.backoffMs = 1000;
      const s = this.store.getState();
      s.setStatus({ broker: "connected", engine: "connected" });
      s.setLiveCommandSender((command) => this.sendCommand(command));
      s.pushAuditEvent({
        ts: Date.now(),
        level: "info",
        source: "dashboard",
        message: `Connected to live bridge at ${this.url}`,
      });
    };

    ws.onmessage = (event) => {
      try {
        this.handleMessage(JSON.parse(event.data));
      } catch {
        // ignore malformed frames rather than crash the dashboard
      }
    };

    ws.onclose = () => {
      if (this.stopped) return;
      this.store.getState().setStatus({ broker: "disconnected", engine: "disconnected" });
      this.store.getState().setLiveCommandSender(null);
      this.scheduleReconnect();
    };

    ws.onerror = () => {
      ws.close();
    };
  }

  private scheduleReconnect() {
    if (this.stopped) return;
    this.reconnectTimer = window.setTimeout(() => this.connect(), this.backoffMs);
    this.backoffMs = Math.min(this.backoffMs * 2, this.maxBackoffMs);
  }

  private handleMessage(msg: { type: string; payload: unknown }) {
    const store = this.store.getState();
    switch (msg.type) {
      case "status":
        store.setStatus(msg.payload as Parameters<typeof store.setStatus>[0]);
        break;
      case "kpis":
        store.setKpis(msg.payload as Parameters<typeof store.setKpis>[0]);
        break;
      case "risk":
        store.setRisk(msg.payload as Parameters<typeof store.setRisk>[0]);
        break;
      case "candle":
        store.pushCandle(msg.payload as Candle);
        break;
      case "execution_marker":
        store.pushExecutionMarker(msg.payload as Parameters<typeof store.pushExecutionMarker>[0]);
        break;
      case "equity_point":
        store.pushEquityPoint(msg.payload as Parameters<typeof store.pushEquityPoint>[0]);
        break;
      case "order": {
        const order = msg.payload as OrderRow;
        const flash = order.status === "FILLED" ? (order.side === "BUY" ? "profit" : "loss") : undefined;
        store.upsertOrder(order, flash);
        break;
      }
      case "open_trades":
        store.setOpenPositions(msg.payload as OpenPositionDetail[]);
        break;
      case "closed_trade":
        store.pushClosedTrade(msg.payload as ClosedTradeRecord);
        break;
      case "audit":
        store.pushAuditEvent(msg.payload as Parameters<typeof store.pushAuditEvent>[0]);
        break;
      case "closed_trades_backfill":
        store.setClosedTradesBackfill(msg.payload as ClosedTradeRecord[]);
        break;
      case "audit_backfill":
        store.setAuditBackfill(msg.payload as Parameters<typeof store.pushAuditEvent>[0][]);
        break;
      case "candles_backfill": {
        const { symbol, candles } = msg.payload as { symbol: string; candles: Candle[] };
        store.setCandlesBackfill(symbol, candles);
        break;
      }
      default:
        break;
    }
  }
}

export function useWebSocket(store: Store, liveUrl?: string) {
  const engineRef = useRef<MockTradingEngine | LiveWebSocketClient | null>(null);

  useEffect(() => {
    const engine = liveUrl ? new LiveWebSocketClient(store, liveUrl) : new MockTradingEngine(store);
    engineRef.current = engine;
    engine.start();
    return () => engine.stop();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- store identity is stable (module-level singleton)
  }, [liveUrl]);
}
