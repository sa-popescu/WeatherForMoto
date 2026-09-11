import { departureUtcMs, etaUtcMs, localIsoAt, minutesOf, pickSlot, timesBetween } from './timing';
import type { PointForecast } from './types';

// "Când pleci": for every 30-minute departure on the chosen day, the worst
// backend score met anywhere along the route. Uses the hourly data already
// loaded for the timeline, so it costs no extra requests.

export const SWEEP_FIRST = '05:00';
export const SWEEP_LAST = '20:00';

export interface SweepPointInput {
  km: number;
  name: string;
  forecast: PointForecast | null;
}

export interface SweepBar {
  time: string;
  worst: number | null;
  worstName: string | null;
  /** Every point had a score for this departure. */
  complete: boolean;
  /** The departure is already in the past. */
  past: boolean;
}

export interface SweepInput {
  points: readonly SweepPointInput[];
  date: string;
  speedKmh: number;
  originOffsetSec: number;
  nowMs: number;
  times?: readonly string[];
}

/** Worst score along the route for one departure instant. */
export function worstAlongRoute(points: readonly SweepPointInput[], depUtcMs: number, speedKmh: number): Omit<SweepBar, 'time' | 'past'> {
  let worst: number | null = null;
  let worstName: string | null = null;
  let complete = points.length > 0;
  for (const p of points) {
    if (!p.forecast) {
      complete = false;
      continue;
    }
    const at = localIsoAt(etaUtcMs(depUtcMs, p.km, speedKmh), p.forecast.utcOffsetSeconds);
    const score = pickSlot(p.forecast.hourly, at)?.moto_score ?? null;
    if (score == null) {
      complete = false;
      continue;
    }
    if (worst === null || score < worst) {
      worst = score;
      worstName = p.name;
    }
  }
  return { worst, worstName, complete };
}

export function departureSweep({ points, date, speedKmh, originOffsetSec, nowMs, times }: SweepInput): SweepBar[] {
  const list = times ?? timesBetween(SWEEP_FIRST, SWEEP_LAST);
  return list.map((time) => {
    const dep = departureUtcMs({ date, time }, originOffsetSec);
    return { time, past: dep < nowMs, ...worstAlongRoute(points, dep, speedKmh) };
  });
}

/**
 * Best departure: the highest worst-point score among future departures with
 * complete data. Ties go to the one closest to the chosen time, so the
 * current choice wins whenever it is already as good as anything else.
 */
export function bestBar(bars: readonly SweepBar[], chosenTime: string): SweepBar | null {
  const chosen = minutesOf(chosenTime);
  let best: SweepBar | null = null;
  for (const bar of bars) {
    if (bar.past || !bar.complete || bar.worst == null) continue;
    if (
      best === null ||
      bar.worst > (best.worst ?? -1) ||
      (bar.worst === best.worst && Math.abs(minutesOf(bar.time) - chosen) < Math.abs(minutesOf(best.time) - chosen))
    ) {
      best = bar;
    }
  }
  return best;
}
