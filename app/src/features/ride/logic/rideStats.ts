import type { RideSummary } from './summary';

// The statistics of "Ture", from the rides kept on this phone: totals for
// this week, month and year, and the records. Pure, so it is tested on its
// own; periods follow the phone's own calendar (weeks start on Monday).

export interface StatRide {
  id: string;
  startedAt: number;
  summary: RideSummary;
}

export interface PeriodTotals {
  rides: number;
  distanceM: number;
  movingMs: number;
}

export type Period = 'week' | 'month' | 'year' | 'all';

export interface RideRecord {
  id: string;
  startedAt: number;
  value: number;
}

export interface RideRecords {
  /** The longest ride, metres. */
  longest: RideRecord | null;
  /** The most time riding in one ride, ms. */
  longestTime: RideRecord | null;
  /** Top speed, m/s. */
  fastest: RideRecord | null;
  /** The most lean, degrees (rides with a course from the GPS only). */
  leanest: RideRecord | null;
}

/** When the period around `nowMs` started, on the phone's clock. */
export function periodStart(period: Period, nowMs: number): number {
  if (period === 'all') return Number.NEGATIVE_INFINITY;
  const d = new Date(nowMs);
  if (period === 'year') return new Date(d.getFullYear(), 0, 1).getTime();
  if (period === 'month') return new Date(d.getFullYear(), d.getMonth(), 1).getTime();
  // Monday is the first day of the week here.
  const sinceMonday = (d.getDay() + 6) % 7;
  return new Date(d.getFullYear(), d.getMonth(), d.getDate() - sinceMonday).getTime();
}

export function periodTotals(rides: readonly StatRide[], period: Period, nowMs: number): PeriodTotals {
  const from = periodStart(period, nowMs);
  const inside = rides.filter((r) => r.startedAt >= from && r.startedAt <= nowMs);
  return {
    rides: inside.length,
    distanceM: inside.reduce((sum, r) => sum + r.summary.distanceM, 0),
    movingMs: inside.reduce((sum, r) => sum + r.summary.movingMs, 0),
  };
}

function best(rides: readonly StatRide[], valueOf: (s: RideSummary) => number | undefined): RideRecord | null {
  let top: RideRecord | null = null;
  for (const r of rides) {
    const value = valueOf(r.summary);
    if (value === undefined || !Number.isFinite(value) || value <= 0) continue;
    if (!top || value > top.value) top = { id: r.id, startedAt: r.startedAt, value };
  }
  return top;
}

export function rideRecords(rides: readonly StatRide[]): RideRecords {
  return {
    longest: best(rides, (s) => s.distanceM),
    longestTime: best(rides, (s) => s.movingMs),
    fastest: best(rides, (s) => s.topSpeedMs),
    leanest: best(rides, (s) => s.maxLeanDeg),
  };
}
