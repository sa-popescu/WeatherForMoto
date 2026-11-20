import { useMemo } from 'react';
import { buildTimeline, originOffsetSec, routeStats, segmentScores } from './plan';
import { bestBar, departureSweep } from './sweep';
import { deviceOffsetSec } from './timing';
import type { Departure } from './types';
import type { PlanState } from './useRoutePlan';

// Everything the results show, recomputed from the loaded hourly data when
// the departure or the speed changes (no new requests).

export function useDerivedPlan(state: PlanState, departure: Departure, speed: number) {
  const { points, weather } = state;
  return useMemo(() => {
    const fallback = deviceOffsetSec();
    const rows = buildTimeline(points, weather, departure, speed, fallback);
    const sweepPoints = points.map((p) => ({ km: p.km, name: p.name, forecast: weather[p.id]?.forecast ?? null }));
    const originOffset = originOffsetSec(points, weather, fallback);
    const bars = departureSweep({
      points: sweepPoints,
      date: departure.date,
      speedKmh: speed,
      originOffsetSec: originOffset,
      nowMs: Date.now(),
    });
    return {
      rows,
      // For the day comparison, which sweeps the other days only once it is opened.
      sweepPoints,
      originOffset,
      stats: routeStats(rows),
      segments: segmentScores(rows),
      bars,
      best: bestBar(bars, departure.time),
      settled: points.filter((p) => weather[p.id] && weather[p.id]?.status !== 'loading').length,
      failed: points.filter((p) => weather[p.id]?.status === 'error').length,
    };
  }, [points, weather, departure, speed]);
}

export type DerivedPlan = ReturnType<typeof useDerivedPlan>;
