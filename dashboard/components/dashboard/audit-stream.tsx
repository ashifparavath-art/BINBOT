"use client";

import { useEffect, useRef, useState } from "react";
import type { StoreApi, UseBoundStore } from "zustand";
import { TerminalSquare } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { isLocalToday } from "@/lib/day";
import { cn, formatTime } from "@/lib/utils";
import type { TradingState } from "@/store/useTradingStore";
import type { AuditLevel } from "@/lib/types";

const LEVEL_STYLE: Record<AuditLevel, string> = {
  info: "text-slate-400",
  signal: "text-system",
  risk: "text-pending",
  fill: "text-profit",
  warn: "text-pending",
  error: "text-loss",
};

const LEVEL_TAG: Record<AuditLevel, string> = {
  info: "INFO",
  signal: "SIGNAL",
  risk: "RISK",
  fill: "FILL",
  warn: "WARN",
  error: "ERROR",
};

export function AuditStream({ store }: { store: UseBoundStore<StoreApi<TradingState>> }) {
  const allAuditLog = store((s) => s.auditLog);
  const auditLog = allAuditLog.filter((e) => isLocalToday(e.ts));
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !autoScroll) return;
    el.scrollTop = el.scrollHeight;
  }, [auditLog, autoScroll]);

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    setAutoScroll(distanceFromBottom < 48);
  };

  return (
    <Card className="h-full">
      <CardHeader className="relative overflow-hidden">
        <div className="flex items-center gap-2">
          <TerminalSquare className="size-3.5 text-system" />
          <CardTitle>Audit &amp; Risk Event Stream (Today)</CardTitle>
        </div>
        <span className="text-[10px] text-slate-500 font-mono">{auditLog.length} events today</span>
        <div className="pointer-events-none absolute inset-x-0 bottom-0 h-px w-1/3 bg-gradient-to-r from-transparent via-system/70 to-transparent animate-scan" />
      </CardHeader>
      <CardContent className="p-0">
        <div
          ref={containerRef}
          onScroll={handleScroll}
          className="h-[220px] overflow-y-auto scrollbar-thin bg-black/20 px-3 py-2 font-mono text-[11px] leading-relaxed"
        >
          {auditLog.length === 0 && <div className="text-slate-600 py-8 text-center font-sans text-xs">Waiting for engine events...</div>}
          {auditLog.map((event) => (
            <div key={event.id} className="flex gap-2 whitespace-pre-wrap break-all py-0.5">
              <span className="text-slate-600 shrink-0">{formatTime(event.ts)}</span>
              <span className={cn("shrink-0 font-semibold", LEVEL_STYLE[event.level])}>
                [{LEVEL_TAG[event.level]}]
              </span>
              <span className="text-slate-500 shrink-0">{event.source}</span>
              <span className="text-slate-300">{event.message}</span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
