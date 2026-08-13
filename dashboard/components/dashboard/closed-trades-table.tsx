"use client";

import type { StoreApi, UseBoundStore } from "zustand";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { isLocalToday } from "@/lib/day";
import { cn, formatTime, formatUsd } from "@/lib/utils";
import type { TradingState } from "@/store/useTradingStore";

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

export function ClosedTradesTable({ store }: { store: UseBoundStore<StoreApi<TradingState>> }) {
  const allClosedTrades = store((s) => s.closedTrades);
  const closedTrades = allClosedTrades.filter((t) => isLocalToday(t.closedAt));

  return (
    <Card className="h-full">
      <CardHeader>
        <CardTitle>Closed Trades (Today)</CardTitle>
        <span className="text-[10px] text-slate-500 font-mono">{closedTrades.length} closed today</span>
      </CardHeader>
      <CardContent className="p-0">
        {closedTrades.length === 0 ? (
          <div className="flex items-center justify-center py-10 text-xs text-slate-600 font-sans">
            No closed trades yet today
          </div>
        ) : (
          <div className="max-h-[260px] overflow-y-auto scrollbar-thin">
            <table className="w-full text-xs font-mono tabular-nums">
              <thead className="sticky top-0 bg-panel-2/95 backdrop-blur text-slate-500 uppercase text-[10px] tracking-wider font-sans">
                <tr>
                  <th className="py-1.5 pl-3 pr-2 text-left font-medium">Closed</th>
                  <th className="py-1.5 px-2 text-left font-medium">Sym</th>
                  <th className="py-1.5 px-2 text-left font-medium">Side</th>
                  <th className="py-1.5 px-2 text-right font-medium">Qty</th>
                  <th className="py-1.5 px-2 text-right font-medium">Entry</th>
                  <th className="py-1.5 px-2 text-right font-medium">Exit</th>
                  <th className="py-1.5 px-2 text-right font-medium">Duration</th>
                  <th className="py-1.5 pl-2 pr-3 text-right font-medium">PnL</th>
                </tr>
              </thead>
              <tbody>
                {closedTrades.map((t, i) => (
                  <tr
                    key={`${t.symbol}-${t.closedAt}-${i}`}
                    className="border-b border-panel-border/40 last:border-0 hover:bg-slate-800/30 transition-colors"
                  >
                    <td className="py-1.5 pl-3 pr-2 text-slate-500">{formatTime(t.closedAt)}</td>
                    <td className="py-1.5 px-2 text-slate-100 font-medium">{t.symbol}</td>
                    <td className="py-1.5 px-2">
                      <Badge variant={t.side === "BUY" ? "profit" : "loss"}>{t.side}</Badge>
                    </td>
                    <td className="py-1.5 px-2 text-right text-slate-200">{t.quantity}</td>
                    <td className="py-1.5 px-2 text-right text-slate-200">{t.entryPrice.toFixed(4)}</td>
                    <td className="py-1.5 px-2 text-right text-slate-200">{t.exitPrice.toFixed(4)}</td>
                    <td className="py-1.5 px-2 text-right text-slate-400">{formatDuration(t.durationSec)}</td>
                    <td className={cn("py-1.5 pl-2 pr-3 text-right font-semibold", t.pnl >= 0 ? "text-profit" : "text-loss")}>
                      {formatUsd(t.pnl, { signed: true })}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
