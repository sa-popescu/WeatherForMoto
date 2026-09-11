import type { HourlyWeather } from '../../lib/types';
import { departureUtcMs, etaUtcMs, localIsoAt, pickSlot } from './timing';
import type { Departure, PointStatus, PointWeather, SamplePoint } from './types';

// The route timeline: when the rider reaches each sample point and which
// hourly forecast slot applies there. Scores are the backend's, untouched.

export interface TimelineRow {
  point: SamplePoint;
  etaUtcMs: number;
  /** ETA on the wall clock of that point. */
  etaLocal: string;
  status: PointStatus;
  slot: HourlyWeather | null;
}

export type WeatherById = Readonly<Record<string, PointWeather | undefined>>;

/** Offset of the origin; falls back to any loaded point, then to the given default. */
export function originOffsetSec(points: readonly SamplePoint[], weather: WeatherById, fallback: number): number {
  const origin = points.find((p) => p.stopIndex === 0);
  const own = origin ? weather[origin.id]?.forecast : null;
  if (own) return own.utcOffsetSeconds;
  for (const p of points) {
    const f = weather[p.id]?.forecast;
    if (f) return f.utcOffsetSeconds;
  }
  return fallback;
}

export function buildTimeline(
  points: readonly SamplePoint[],
  weather: WeatherById,
  dep: Departure,
  speedKmh: number,
  fallbackOffsetSec: number,
): TimelineRow[] {
  const originOffset = originOffsetSec(points, weather, fallbackOffsetSec);
  const start = departureUtcMs(dep, originOffset);
  return points.map((point) => {
    const state = weather[point.id];
    const forecast = state?.forecast ?? null;
    const eta = etaUtcMs(start, point.km, speedKmh);
    const etaLocal = localIsoAt(eta, forecast?.utcOffsetSeconds ?? originOffset);
    return {
      point,
      etaUtcMs: eta,
      etaLocal,
      status: state?.status ?? 'loading',
      slot: forecast ? pickSlot(forecast.hourly, etaLocal) : null,
    };
  });
}

export interface RouteStats {
  avgScore: number | null;
  worstScore: number | null;
  worstName: string | null;
  maxGustKmh: number | null;
  maxPrecipMm: number | null;
  /** Every point has a slot. */
  complete: boolean;
}

function maxOf(values: Array<number | null | undefined>): number | null {
  const finite = values.filter((v): v is number => v != null && Number.isFinite(v));
  return finite.length ? Math.max(...finite) : null;
}

export function routeStats(rows: readonly TimelineRow[]): RouteStats {
  const scored = rows.filter((r) => r.slot?.moto_score != null);
  const scores = scored.map((r) => r.slot?.moto_score as number);
  let worstRow: TimelineRow | null = null;
  for (const r of scored) {
    if (!worstRow || (r.slot?.moto_score as number) < (worstRow.slot?.moto_score as number)) worstRow = r;
  }
  return {
    avgScore: scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : null,
    worstScore: worstRow?.slot?.moto_score ?? null,
    worstName: worstRow?.point.name ?? null,
    maxGustKmh: maxOf(rows.map((r) => r.slot?.wind_gusts_kmh)),
    maxPrecipMm: maxOf(rows.map((r) => r.slot?.precipitation_mm)),
    complete: rows.length > 0 && scored.length === rows.length,
  };
}

/** Score of each stretch between consecutive points: the worse of its two ends. */
export function segmentScores(rows: readonly TimelineRow[]): Array<number | null> {
  const out: Array<number | null> = [];
  for (let i = 0; i < rows.length - 1; i += 1) {
    const a = rows[i].slot?.moto_score ?? null;
    const b = rows[i + 1].slot?.moto_score ?? null;
    out.push(a == null ? b : b == null ? a : Math.min(a, b));
  }
  return out;
}
