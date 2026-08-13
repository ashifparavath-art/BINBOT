import type { Candle } from "@/lib/types";

export interface TimeframeOption {
  label: string;
  seconds: number;
}

export const TIMEFRAME_OPTIONS: TimeframeOption[] = [
  { label: "1m", seconds: 60 },
  { label: "5m", seconds: 5 * 60 },
  { label: "15m", seconds: 15 * 60 },
  { label: "1h", seconds: 60 * 60 },
  { label: "4h", seconds: 4 * 60 * 60 },
];

export const DEFAULT_TIMEFRAME_SECONDS = 15 * 60;

/**
 * Downsamples a base 1-minute candle series into larger buckets entirely
 * client-side (open=first, high=max, low=min, close=last, volume=sum) —
 * the backend only ever streams/backfills 1-minute bars, so every other
 * timeframe is a resample of that one series rather than a separate
 * request, which is what makes switching timeframes instant.
 */
export function resampleCandles(candles: Candle[], bucketSeconds: number): Candle[] {
  if (bucketSeconds <= 60 || candles.length === 0) return candles;

  const buckets: Candle[] = [];
  for (const candle of candles) {
    const bucketTime = Math.floor(candle.time / bucketSeconds) * bucketSeconds;
    const last = buckets[buckets.length - 1];
    if (last && last.time === bucketTime) {
      last.high = Math.max(last.high, candle.high);
      last.low = Math.min(last.low, candle.low);
      last.close = candle.close;
      last.volume += candle.volume;
    } else {
      buckets.push({ ...candle, time: bucketTime });
    }
  }
  return buckets;
}
