"use client";

import type { ElementType } from "react";
import type { StoreApi, UseBoundStore } from "zustand";
import { Activity, Radio, Server, Zap } from "lucide-react";

import { isLocalToday } from "@/lib/day";
import { cn, formatNumber, formatUsd } from "@/lib/utils";
import type { TradingState } from "@/store/useTradingStore";
import type { ConnectionState } from "@/lib/types";

const STATE_COLOR: Record<ConnectionState, string> = {
  connected: "bg-profit",
  degraded: "bg-pending",
  disconnected: "bg-loss",
};

const STATE_LABEL: Record<ConnectionState, string> = {
  connected: "LIVE",
  degraded: "SYNCING",
  disconnected: "DOWN",
};

function StatusBadge({
  icon: Icon,
  label,
  state,
}: {
  icon: ElementType;
  label: string;
  state: ConnectionState;
}) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-panel-border/60 bg-panel-2/60 px-3 py-1.5">
      <span className="relative flex h-2 w-2">
        {state !== "disconnected" && (
          <span
            className={cn(
              "absolute inline-flex h-full w-full animate-ping rounded-full opacity-60",
              STATE_COLOR[state]
            )}
          />
        )}
        <span className={cn("relative inline-flex h-2 w-2 rounded-full", STATE_COLOR[state])} />
      </span>
      <Icon className="size-3.5 text-slate-500" />
      <span className="text-xs font-medium text-slate-300 font-sans">{label}</span>
      <span
        className={cn(
          "text-[10px] font-semibold tracking-wider font-mono",
          state === "connected" ? "text-profit" : state === "degraded" ? "text-pending" : "text-loss"
        )}
      >
        {STATE_LABEL[state]}
      </span>
    </div>
  );
}

function Kpi({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value: string;
  tone?: "profit" | "loss" | "neutral" | "system";
}) {
  const toneClass =
    tone === "profit"
      ? "text-profit"
      : tone === "loss"
        ? "text-loss"
        : tone === "system"
          ? "text-system"
          : "text-slate-100";
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wider text-slate-500 font-sans">{label}</span>
      <span className={cn("text-lg font-semibold font-mono tabular-nums leading-none", toneClass)}>{value}</span>
    </div>
  );
}

export function TelemetryRibbon({
  store,
  strategyLabel = "Institutional OEMS",
}: {
  store: UseBoundStore<StoreApi<TradingState>>;
  strategyLabel?: string;
}) {
  const status = store((s) => s.status);
  const kpis = store((s) => s.kpis);
  const closedTrades = store((s) => s.closedTrades);

  const tradesToday = closedTrades.filter((t) => isLocalToday(t.closedAt));
  const realizedPnlToday = tradesToday.reduce((sum, t) => sum + t.pnl, 0);

  const marginPct = kpis.marginLimitUsd > 0 ? (kpis.marginUsedUsd / kpis.marginLimitUsd) * 100 : 0;

  return (
    <header className="glass-panel sticky top-0 z-40 flex flex-wrap items-center justify-between gap-4 rounded-none border-x-0 border-t-0 px-5 py-3">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 pr-3 mr-1 border-r border-panel-border/60">
          <div className="flex size-7 items-center justify-center rounded-md bg-gradient-to-br from-system/20 to-profit/10 border border-system/30">
            <Activity className="size-4 text-system" />
          </div>
          <div className="flex flex-col leading-none">
            <span className="text-sm font-bold tracking-tight text-slate-100 font-sans">BIN BOT</span>
            <span className="text-[10px] text-slate-500 font-sans">{strategyLabel}</span>
          </div>
        </div>

        <StatusBadge icon={Radio} label="BROKER" state={status.broker} />
        <StatusBadge icon={Server} label="REDIS" state={status.redis} />
        <StatusBadge icon={Activity} label="ENGINE" state={status.engine} />

        <div className="flex items-center gap-1.5 rounded-lg border border-panel-border/60 bg-panel-2/60 px-3 py-1.5">
          <Zap className="size-3.5 text-system" />
          <span className="text-xs font-mono tabular-nums text-system">{formatNumber(status.latencyMs, 1)}ms</span>
        </div>
      </div>

      <div className="flex items-center gap-6">
        <Kpi label="Portfolio Value" value={formatUsd(kpis.portfolioValue)} />
        <Kpi
          label="Realized P&L (Today)"
          value={formatUsd(realizedPnlToday, { signed: true })}
          tone={realizedPnlToday >= 0 ? "profit" : "loss"}
        />
        <Kpi
          label="Unrealized P&L"
          value={formatUsd(kpis.unrealizedPnl, { signed: true })}
          tone={kpis.unrealizedPnl >= 0 ? "profit" : "loss"}
        />
        <Kpi label="Trades Today" value={String(tradesToday.length)} />
        <Kpi label="Sharpe" value={formatNumber(kpis.sharpeRatio, 2)} tone="system" />
        <Kpi
          label="Margin Used"
          value={`${formatNumber(marginPct, 1)}%`}
          tone={marginPct > 70 ? "loss" : marginPct > 40 ? "system" : "neutral"}
        />
      </div>
    </header>
  );
}
