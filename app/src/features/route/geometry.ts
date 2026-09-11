import { distanceKm } from '../../lib/geo';
import type { LatLon } from './types';

// Pure geometry along a polyline: cumulative distance, interpolation,
// weather sampling positions and splitting into coloured segments.

export const SAMPLE_SPACING_KM = 40;
export const MAX_SAMPLES = 12;

/** Distance along the line at every vertex, km (first is 0). */
export function cumulativeKm(line: readonly LatLon[]): number[] {
  const out: number[] = [];
  let total = 0;
  for (let i = 0; i < line.length; i += 1) {
    if (i > 0) total += distanceKm(line[i - 1], line[i]);
    out.push(total);
  }
  return out;
}

/** Index of the last vertex whose distance is <= km (binary search). */
function vertexBefore(cum: readonly number[], km: number): number {
  let lo = 0;
  let hi = cum.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (cum[mid] <= km) lo = mid;
    else hi = mid;
  }
  return lo;
}

/** Position at a given distance along the line, clamped to its ends. */
export function pointAtKm(line: readonly LatLon[], cum: readonly number[], km: number): LatLon {
  const last = line.length - 1;
  if (last < 0) throw new Error('pointAtKm: empty line');
  if (km <= cum[0]) return { lat: line[0].lat, lon: line[0].lon };
  if (km >= cum[last]) return { lat: line[last].lat, lon: line[last].lon };
  const i = vertexBefore(cum, km);
  const a = line[i];
  const b = line[i + 1];
  const span = cum[i + 1] - cum[i];
  const t = span > 0 ? (km - cum[i]) / span : 0;
  return { lat: a.lat + (b.lat - a.lat) * t, lon: a.lon + (b.lon - a.lon) * t };
}

export interface SampleSpec {
  km: number;
  stopIndex: number | null;
}

/**
 * Where to sample the weather: every stop, plus points every ~spacingKm in
 * between, never more than `cap` in total. When the budget is short, extra
 * points go to the legs with the widest gaps first.
 */
export function planSamples(stopKm: readonly number[], spacingKm: number = SAMPLE_SPACING_KM, cap: number = MAX_SAMPLES): SampleSpec[] {
  const stops: SampleSpec[] = stopKm.map((km, i) => ({ km, stopIndex: i }));
  const legs = stopKm.slice(1).map((km, i) => Math.max(0, km - stopKm[i]));
  const wanted = legs.map((len) => Math.max(0, Math.ceil(len / spacingKm) - 1));
  const counts = legs.map(() => 0);
  let budget = Math.max(0, cap - stops.length);
  while (budget > 0) {
    let pick = -1;
    let widest = 0;
    legs.forEach((len, i) => {
      if (counts[i] >= wanted[i]) return;
      const gap = len / (counts[i] + 1);
      if (gap > widest) {
        widest = gap;
        pick = i;
      }
    });
    if (pick < 0) break;
    counts[pick] += 1;
    budget -= 1;
  }
  const between: SampleSpec[] = [];
  counts.forEach((n, i) => {
    for (let j = 1; j <= n; j += 1) between.push({ km: stopKm[i] + (legs[i] * j) / (n + 1), stopIndex: null });
  });
  return [...stops, ...between].sort((a, b) => a.km - b.km || (a.stopIndex ?? 99) - (b.stopIndex ?? 99));
}

/**
 * Splits the line at the given distances. Always returns breaks.length + 1
 * pieces; neighbouring pieces share their boundary point.
 */
export function splitLineAtKm(line: readonly LatLon[], cum: readonly number[], breaks: readonly number[]): LatLon[][] {
  if (line.length === 0) return [];
  const sorted = [...breaks].sort((a, b) => a - b);
  const pieces: LatLon[][] = [];
  let current: LatLon[] = [line[0]];
  let b = 0;
  for (let i = 1; i < line.length; i += 1) {
    while (b < sorted.length && sorted[b] <= cum[i]) {
      const cut = pointAtKm(line, cum, sorted[b]);
      current.push(cut);
      pieces.push(current);
      current = [cut];
      b += 1;
    }
    current.push(line[i]);
  }
  const end = line[line.length - 1];
  for (; b < sorted.length; b += 1) {
    pieces.push(current);
    current = [end];
  }
  pieces.push(current);
  return pieces;
}

/** Closest vertex of the line to a point: its distance along the route and how far off it lies. */
export function nearestOnLine(line: readonly LatLon[], cum: readonly number[], p: LatLon): { km: number; offKm: number } {
  let best = { km: 0, offKm: Number.POSITIVE_INFINITY };
  for (let i = 0; i < line.length; i += 1) {
    const d = distanceKm(line[i], p);
    if (d < best.offKm) best = { km: cum[i], offKm: d };
  }
  return best;
}
