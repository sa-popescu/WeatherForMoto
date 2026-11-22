import { elapsedMs, type RideClock } from './clock';
import { foldLean, hasLean, type LeanState } from './lean';
import type { GapRoads, RideTotals } from './track';
import { foldPoints, type RidePoint } from './track';

// A finished ride's totals, kept with it so the list of rides (and the
// statistics on "Ture") need no points.

export interface RideSummary {
  distanceM: number;
  movingDistanceM: number;
  movingMs: number;
  /** Ride time without the pauses. */
  elapsedMs: number;
  topSpeedMs: number;
  /** The most lean on either side, degrees; absent without a course from the GPS, or in older rides. */
  maxLeanDeg?: number;
}

export function summaryOf(totals: RideTotals, clock: RideClock, lean?: Pick<LeanState, 'maxLeftDeg' | 'maxRightDeg'>): RideSummary {
  return {
    distanceM: totals.distanceM,
    movingDistanceM: totals.movingDistanceM,
    movingMs: totals.movingMs,
    // A finished clock does not move any more, so any "now" gives the same.
    elapsedMs: elapsedMs(clock, clock.finishedAt ?? Date.now()),
    topSpeedMs: totals.topSpeedMs,
    ...(lean && hasLean(lean) ? { maxLeanDeg: Math.max(lean.maxLeftDeg, lean.maxRightDeg) } : {}),
  };
}

/** For rides finished before summaries were kept: the totals again from the points. */
export function summaryFromPoints(points: readonly RidePoint[], roads: GapRoads, clock: RideClock): RideSummary {
  return summaryOf(foldPoints(points, roads).totals, clock, foldLean(points));
}
