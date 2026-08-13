import { create, type StoreApi, type UseBoundStore } from "zustand";

import type {
  AuditEvent,
  Candle,
  ClosedTradeRecord,
  EquityPoint,
  ExecutionMarker,
  OpenPositionDetail,
  OrderRow,
  PortfolioKpis,
  RiskLimits,
  SystemStatus,
} from "@/lib/types";

// Raised from the original session-only caps to comfortably hold the ~48h
// backfill window the backend now replays on connect (see
// server/dashboard_bridge.py's DailyJsonlLog usage) — these are no longer
// "how much of this session to remember" but "how much of the recent past
// to hold in memory," which is a bigger number.
const MAX_AUDIT_EVENTS = 3000;
const MAX_CANDLES_PER_SYMBOL = 1600; // ~a day of 1-minute bars
const MAX_EQUITY_POINTS = 240;
const MAX_MARKERS_PER_SYMBOL = 200;
const MAX_CLOSED_TRADES = 1500;

export interface TradingState {
  status: SystemStatus;
  kpis: PortfolioKpis;
  candlesBySymbol: Record<string, Candle[]>;
  executionMarkersBySymbol: Record<string, ExecutionMarker[]>;
  equityCurve: EquityPoint[];
  orders: OrderRow[];
  openPositions: OpenPositionDetail[];
  closedTrades: ClosedTradeRecord[];
  risk: RiskLimits;
  auditLog: AuditEvent[];
  flashingRows: Record<string, "profit" | "loss">;
  killSwitchEngaged: boolean;
  flattenInProgress: boolean;
  /** Set by the live WebSocket client while connected; null in mock mode
   * or while disconnected. When set, the four control actions below send
   * a real command to the live bot in addition to updating local state. */
  liveCommandSender: ((command: string) => void) | null;

  setLiveCommandSender: (fn: ((command: string) => void) | null) => void;
  setStatus: (status: Partial<SystemStatus>) => void;
  setKpis: (kpis: Partial<PortfolioKpis>) => void;
  pushCandle: (candle: Candle) => void;
  pushExecutionMarker: (marker: ExecutionMarker) => void;
  pushEquityPoint: (point: EquityPoint) => void;
  upsertOrder: (order: OrderRow, flash?: "profit" | "loss") => void;
  setOpenPositions: (positions: OpenPositionDetail[]) => void;
  pushClosedTrade: (trade: ClosedTradeRecord) => void;
  pushAuditEvent: (event: Omit<AuditEvent, "id">) => void;
  setRisk: (risk: Partial<RiskLimits>) => void;
  clearFlash: (orderId: string) => void;

  /** Wholesale-replace, not append — sent once by the backend right after
   * connecting, before any live push for that connection, so there's no
   * ordering race with pushClosedTrade/pushAuditEvent/pushCandle. */
  setClosedTradesBackfill: (trades: ClosedTradeRecord[]) => void;
  setAuditBackfill: (events: Omit<AuditEvent, "id">[]) => void;
  setCandlesBackfill: (symbol: string, candles: Candle[]) => void;

  togglePauseStrategy: () => void;
  triggerFlatten: () => void;
  triggerKillSwitch: () => void;
  resetKillSwitch: () => void;
}

let auditIdCounter = 0;

/**
 * Factory rather than a single module-level store: the dashboard runs two
 * independent live connections (one per strategy/bot), each needing its
 * own isolated copy of this exact shape — see store/pairsStore.ts and
 * store/scannerStore.ts.
 */
export function createTradingStore(): UseBoundStore<StoreApi<TradingState>> {
  return create<TradingState>((set, get) => ({
    status: { broker: "disconnected", redis: "disconnected", engine: "disconnected", latencyMs: 0 },
    kpis: {
      portfolioValue: 1_000_000,
      realizedPnl: 0,
      unrealizedPnl: 0,
      sharpeRatio: 0,
      marginUsedUsd: 0,
      marginLimitUsd: 500_000,
    },
    candlesBySymbol: {},
    executionMarkersBySymbol: {},
    equityCurve: [],
    orders: [],
    openPositions: [],
    closedTrades: [],
    risk: {
      maxDailyLossUsd: 10_000,
      dailyLossUsedUsd: 0,
      maxPositionContracts: 20,
      positionContractsUsed: 0,
      strategyPaused: false,
      circuitBreakerHalted: false,
      haltReason: null,
    },
    auditLog: [],
    flashingRows: {},
    killSwitchEngaged: false,
    flattenInProgress: false,
    liveCommandSender: null,

    setLiveCommandSender: (fn) => set({ liveCommandSender: fn }),
    setStatus: (status) => set((s) => ({ status: { ...s.status, ...status } })),
    setKpis: (kpis) => set((s) => ({ kpis: { ...s.kpis, ...kpis } })),

    pushCandle: (candle) =>
      set((s) => {
        const existing = s.candlesBySymbol[candle.symbol] ?? [];
        const last = existing[existing.length - 1];
        const updated =
          last && last.time === candle.time
            ? [...existing.slice(0, -1), candle]
            : [...existing, candle].slice(-MAX_CANDLES_PER_SYMBOL);
        return { candlesBySymbol: { ...s.candlesBySymbol, [candle.symbol]: updated } };
      }),

    pushExecutionMarker: (marker) =>
      set((s) => {
        const existing = s.executionMarkersBySymbol[marker.symbol] ?? [];
        const updated = [...existing, marker].slice(-MAX_MARKERS_PER_SYMBOL);
        return { executionMarkersBySymbol: { ...s.executionMarkersBySymbol, [marker.symbol]: updated } };
      }),

    pushEquityPoint: (point) =>
      set((s) => ({ equityCurve: [...s.equityCurve, point].slice(-MAX_EQUITY_POINTS) })),

    upsertOrder: (order, flash) =>
      set((s) => {
        const idx = s.orders.findIndex((o) => o.id === order.id);
        const orders = idx === -1 ? [order, ...s.orders] : s.orders.map((o) => (o.id === order.id ? order : o));
        const flashingRows = flash ? { ...s.flashingRows, [order.id]: flash } : s.flashingRows;
        return { orders: orders.slice(0, 200), flashingRows };
      }),

    setOpenPositions: (positions) => set({ openPositions: positions }),

    pushClosedTrade: (trade) =>
      set((s) => ({ closedTrades: [trade, ...s.closedTrades].slice(0, MAX_CLOSED_TRADES) })),

    setClosedTradesBackfill: (trades) =>
      set({ closedTrades: [...trades].reverse().slice(0, MAX_CLOSED_TRADES) }),

    setCandlesBackfill: (symbol, candles) =>
      set((s) => ({
        candlesBySymbol: { ...s.candlesBySymbol, [symbol]: candles.slice(-MAX_CANDLES_PER_SYMBOL) },
      })),

    clearFlash: (orderId) =>
      set((s) => {
        const rest = { ...s.flashingRows };
        delete rest[orderId];
        return { flashingRows: rest };
      }),

    pushAuditEvent: (event) =>
      set((s) => {
        auditIdCounter += 1;
        const entry: AuditEvent = { id: `audit-${auditIdCounter}`, ...event };
        return { auditLog: [...s.auditLog, entry].slice(-MAX_AUDIT_EVENTS) };
      }),

    setAuditBackfill: (events) =>
      set(() => ({
        // Backend sends these in the same chronological (oldest-first)
        // order pushAuditEvent already appends in, so no reordering needed.
        auditLog: events.slice(-MAX_AUDIT_EVENTS).map((event) => {
          auditIdCounter += 1;
          return { id: `audit-${auditIdCounter}`, ...event };
        }),
      })),

    setRisk: (risk) => set((s) => ({ risk: { ...s.risk, ...risk } })),

    togglePauseStrategy: () => {
      const next = !get().risk.strategyPaused;
      set((s) => ({ risk: { ...s.risk, strategyPaused: next } }));
      get().pushAuditEvent({
        ts: Date.now(),
        level: "risk",
        source: "operator",
        message: next ? "Strategy signal generation PAUSED by operator" : "Strategy signal generation RESUMED by operator",
      });
      get().liveCommandSender?.(next ? "pause" : "resume");
    },

    triggerFlatten: () => {
      set({ flattenInProgress: true });
      get().pushAuditEvent({
        ts: Date.now(),
        level: "risk",
        source: "operator",
        message: "FLATTEN POSITIONS triggered — liquidating all open exposure at market",
      });
      get().liveCommandSender?.("flatten");
      window.setTimeout(() => set({ flattenInProgress: false }), 1200);
    },

    triggerKillSwitch: () => {
      set((s) => ({
        killSwitchEngaged: true,
        risk: { ...s.risk, strategyPaused: true, circuitBreakerHalted: true, haltReason: "Manual kill switch" },
      }));
      get().pushAuditEvent({
        ts: Date.now(),
        level: "error",
        source: "operator",
        message: "SYSTEM KILL SWITCH engaged — cancelling all working orders and halting trading loop",
      });
      get().liveCommandSender?.("kill_switch");
    },

    resetKillSwitch: () => {
      set((s) => ({
        killSwitchEngaged: false,
        risk: { ...s.risk, strategyPaused: false, circuitBreakerHalted: false, haltReason: null },
      }));
      get().pushAuditEvent({
        ts: Date.now(),
        level: "info",
        source: "operator",
        message: "Kill switch reset — trading loop re-armed",
      });
      get().liveCommandSender?.("reset_kill_switch");
    },
  }));
}
