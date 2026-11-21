import type { FeatureCollection, LineString, Point } from 'geojson';
import { splitLineAtKm } from './geometry';
import type { TimelineRow } from './plan';
import { tierClass } from './routeFormat';
import type { LatLon, RouteLine } from './types';

// What the Traseu map draws, as plain GeoJSON ([lon, lat]) built from the
// plan, kept apart from MapLibre so it can be tested.

export type PointKind = 'origin' | 'via' | 'destination' | 'sample';
export type TierName = ReturnType<typeof tierClass>;

const toCoord = (p: LatLon): [number, number] => [p.lon, p.lat];

/** The route split at the sample points, each stretch with the tier of its worst score. */
export function routeStretches(route: RouteLine, rows: readonly TimelineRow[], segments: ReadonlyArray<number | null>): FeatureCollection<LineString, { tier: TierName }> {
  const pieces = rows.length >= 2 ? splitLineAtKm(route.coords, route.cumKm, rows.slice(1, -1).map((r) => r.point.km)) : [route.coords];
  return {
    type: 'FeatureCollection',
    features: pieces
      .filter((piece) => piece.length >= 2)
      .map((piece, i) => ({ type: 'Feature', properties: { tier: tierClass(segments[i]) }, geometry: { type: 'LineString', coordinates: piece.map(toCoord) } })),
  };
}

export function pointKind(row: TimelineRow, i: number, count: number): PointKind {
  if (row.point.stopIndex === null) return 'sample';
  if (i === 0) return 'origin';
  return i === count - 1 ? 'destination' : 'via';
}

/** The stops and the sample points, with what the map needs to draw each. */
export function routePoints(rows: readonly TimelineRow[]): FeatureCollection<Point, { kind: PointKind; tier: TierName }> {
  return {
    type: 'FeatureCollection',
    features: rows.map((row, i) => ({
      type: 'Feature',
      properties: { kind: pointKind(row, i, rows.length), tier: tierClass(row.slot?.moto_score) },
      geometry: { type: 'Point', coordinates: [row.point.lon, row.point.lat] },
    })),
  };
}

/** [west, south, east, north] around the route. */
export function routeBounds(route: RouteLine): [number, number, number, number] {
  let west = Infinity;
  let south = Infinity;
  let east = -Infinity;
  let north = -Infinity;
  for (const p of route.coords) {
    west = Math.min(west, p.lon);
    east = Math.max(east, p.lon);
    south = Math.min(south, p.lat);
    north = Math.max(north, p.lat);
  }
  return [west, south, east, north];
}
