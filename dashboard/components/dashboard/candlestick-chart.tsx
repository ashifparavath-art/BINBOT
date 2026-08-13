"use client";

import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import { resampleCandles } from "@/lib/candles";
import type { Candle, ExecutionMarker } from "@/lib/types";

const CHART_COLORS = {
  profit: "#10b981",
  loss: "#ef4444",
  grid: "rgba(148, 163, 184, 0.06)",
  text: "#64748b",
  crosshair: "#06b6d4",
};

/**
 * Store-agnostic candlestick chart: takes candles/markers as plain props
 * rather than reading a specific Zustand store, so several instances can
 * render simultaneously for different symbols in the scanner's
 * multi-position grid.
 */
export function CandlestickChart({
  candles,
  markers,
  heightClass = "h-[280px]",
  compact = false,
  timeframeSeconds = 60,
}: {
  candles: Candle[];
  markers: ExecutionMarker[];
  heightClass?: string;
  compact?: boolean;
  /** Bucket size to resample the raw (1-minute) `candles` into before
   * rendering. Defaults to 60 (no resampling) for callers that don't care. */
  timeframeSeconds?: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const markersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const prevRenderedRef = useRef<Candle[]>([]);
  const prevTimeframeRef = useRef<number>(timeframeSeconds);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: CHART_COLORS.text,
        fontFamily: "var(--font-mono)",
        fontSize: compact ? 9 : 11,
      },
      grid: {
        vertLines: { color: CHART_COLORS.grid },
        horzLines: { color: CHART_COLORS.grid },
      },
      crosshair: {
        vertLine: { color: CHART_COLORS.crosshair, labelBackgroundColor: CHART_COLORS.crosshair },
        horzLine: { color: CHART_COLORS.crosshair, labelBackgroundColor: CHART_COLORS.crosshair },
      },
      rightPriceScale: { borderColor: "rgba(148, 163, 184, 0.12)" },
      timeScale: { borderColor: "rgba(148, 163, 184, 0.12)", timeVisible: true, secondsVisible: !compact },
      autoSize: true,
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: CHART_COLORS.profit,
      downColor: CHART_COLORS.loss,
      borderVisible: false,
      wickUpColor: CHART_COLORS.profit,
      wickDownColor: CHART_COLORS.loss,
    });

    const seriesMarkers = createSeriesMarkers(series, []);

    chartRef.current = chart;
    seriesRef.current = series;
    markersRef.current = seriesMarkers;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      markersRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- chart is created once per mount
  }, []);

  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    const resampled = resampleCandles(candles, timeframeSeconds);
    if (resampled.length === 0) return;

    const prev = prevRenderedRef.current;
    const timeframeChanged = prevTimeframeRef.current !== timeframeSeconds;
    const last = resampled[resampled.length - 1];
    const prevLast = prev[prev.length - 1];

    // Cheap incremental path: either the newest bucket was updated in
    // place (same bar, new tick) or exactly one new bucket was appended —
    // both are a single series.update() call. Anything else (first
    // render, a backfill arriving, a timeframe switch) needs a full
    // setData() since lightweight-charts' update() only ever touches the
    // single latest point and never seeds earlier history.
    const isSameBarUpdate = !timeframeChanged && resampled.length === prev.length && prevLast?.time === last.time;
    const isSingleNewBar =
      !timeframeChanged && resampled.length === prev.length + 1 && resampled[0]?.time === prev[0]?.time;

    if (isSameBarUpdate || isSingleNewBar) {
      series.update({
        time: last.time as UTCTimestamp,
        open: last.open,
        high: last.high,
        low: last.low,
        close: last.close,
      });
    } else {
      series.setData(
        resampled.map((c) => ({
          time: c.time as UTCTimestamp,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }))
      );
    }

    prevRenderedRef.current = resampled;
    prevTimeframeRef.current = timeframeSeconds;
  }, [candles, timeframeSeconds]);

  useEffect(() => {
    const plugin = markersRef.current;
    if (!plugin) return;
    plugin.setMarkers(
      markers.map((m) => ({
        time: m.time as UTCTimestamp,
        position: m.side === "BUY" ? "belowBar" : "aboveBar",
        color: m.side === "BUY" ? CHART_COLORS.profit : CHART_COLORS.loss,
        shape: m.side === "BUY" ? "arrowUp" : "arrowDown",
        text: `${m.side} ${m.quantity}`,
      }))
    );
  }, [markers]);

  return <div ref={containerRef} className={`${heightClass} w-full`} />;
}
