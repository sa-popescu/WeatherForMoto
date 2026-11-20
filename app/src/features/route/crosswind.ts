import { distanceKm } from '../../lib/geo';
import { pointAtKm } from './geometry';
import type { TimelineRow } from './plan';
import type { LatLon, RouteLine } from './types';

// Wind against the road. At each weather point, the direction of travel and
// the wind of the hour you get there say where the gusts hit you from. A gust
// from the side pushes the bike across the lane; one from the front or the
// back only slows it down or speeds it up.

/** Gusts below this are not worth naming a direction, km/h. */
export const GUST_NOTE_KMH = 30;
/** The side part of a gust from which the road gets a warning, km/h. */
export const CROSS_WARN_KMH = 40;
/** The direction of travel is read over this much road on each side of the point, km. */
const HEADING_HALF_WINDOW_KM = 3;
/** A stretch shorter than this (the very end of a short route) has no useful direction, km. */
const MIN_HEADING_SPAN_KM = 0.5;
/**
 * Straight line over road length below which the stretch winds too much to
 * have one direction (hairpins): the wind then comes from every side in turn.
 */
const MIN_STRAIGHTNESS = 0.5;
/** A wind within this many degrees of the road's axis counts as head or tail wind. */
const AXIS_DEG = 45;

export type WindSide = 'head' | 'tail' | 'cross';

export interface RoadWind {
  side: WindSide;
  gustKmh: number;
  /** The part of the gust that blows across the road, km/h. */
  crossKmh: number;
}

const RAD = Math.PI / 180;

/** Initial compass bearing from a to b, degrees 0 to 360 (0 is north, 90 east). */
export function bearingDeg(a: LatLon, b: LatLon): number {
  const lat1 = a.lat * RAD;
  const lat2 = b.lat * RAD;
  const dLon = (b.lon - a.lon) * RAD;
  const y = Math.sin(dLon) * Math.cos(lat2);
  const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
  return (Math.atan2(y, x) / RAD + 360) % 360;
}

/** The direction of travel around `km`, or null where the road winds too much to have one. */
export function headingAt(route: RouteLine, km: number): number | null {
  const from = Math.max(0, km - HEADING_HALF_WINDOW_KM);
  const to = Math.min(route.distanceKm, km + HEADING_HALF_WINDOW_KM);
  if (route.coords.length < 2 || to - from < MIN_HEADING_SPAN_KM) return null;
  const a = pointAtKm(route.coords, route.cumKm, from);
  const b = pointAtKm(route.coords, route.cumKm, to);
  if (distanceKm(a, b) < (to - from) * MIN_STRAIGHTNESS) return null;
  return bearingDeg(a, b);
}

/**
 * Where the wind hits a rider heading `headingDeg`. `windFromDeg` is where the
 * wind blows FROM, as forecasts give it. Null when the gusts are light or a
 * value is missing.
 */
export function windOnRoad(headingDeg: number | null, windFromDeg: number | null, gustKmh: number | null): RoadWind | null {
  if (headingDeg === null || windFromDeg === null || gustKmh === null || gustKmh < GUST_NOTE_KMH) return null;
  // 0 when the wind comes straight at the rider, 180 when it comes from behind.
  const offAxis = Math.abs((((windFromDeg - headingDeg) % 360) + 540) % 360 - 180);
  const crossKmh = gustKmh * Math.sin(offAxis * RAD);
  const side: WindSide = offAxis <= AXIS_DEG ? 'head' : offAxis >= 180 - AXIS_DEG ? 'tail' : 'cross';
  return { side, gustKmh, crossKmh };
}

/** The wind against the road at every row, in the same order. */
export function roadWinds(route: RouteLine, rows: readonly TimelineRow[]): Array<RoadWind | null> {
  return rows.map((row) =>
    windOnRoad(headingAt(route, row.point.km), row.slot?.wind_direction_10m ?? null, row.slot?.wind_gusts_kmh ?? null),
  );
}

export interface CrossWarning {
  /** The row with the strongest side gust. */
  row: TimelineRow;
  gustKmh: number;
  /** Other rows over the threshold. */
  more: number;
}

/** The strongest side gust over the warning threshold, or null when the road has none. */
export function crossWarning(rows: readonly TimelineRow[], winds: ReadonlyArray<RoadWind | null>): CrossWarning | null {
  const strong = rows
    .map((row, i) => ({ row, wind: winds[i] }))
    .filter((x): x is { row: TimelineRow; wind: RoadWind } => x.wind?.side === 'cross' && x.wind.crossKmh >= CROSS_WARN_KMH);
  if (strong.length === 0) return null;
  const worst = strong.reduce((a, b) => (b.wind.crossKmh > a.wind.crossKmh ? b : a));
  return { row: worst.row, gustKmh: worst.wind.gustKmh, more: strong.length - 1 };
}
