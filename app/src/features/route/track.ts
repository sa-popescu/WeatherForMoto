import { distanceKm } from '../../lib/geo';
import { cumulativeKm, nearestOnLine } from './geometry';
import { GpxError, type GpxPoint, type ParsedGpx } from './gpx';
import { MAX_STOPS } from './stops';
import type { LatLon, RouteLine } from './types';

// Decides what an imported GPX becomes. A drawn road (a track, or a route
// with points close together) is kept exactly as it is: the weather is read
// along that line and no router is asked. A file with only a few points has
// no road in it, so its points become stops and the road is calculated.

/** Longest line kept; denser recordings are thinned evenly. */
export const MAX_TRACK_POINTS = 20_000;
/** Route points closer than this on average are a drawn road, not via points. */
const DENSE_ROUTE_KM = 1;
/** A waypoint this close to the line belongs to the ride. */
const NEAR_KM = 1;
/** Stops closer than this along the line count as the same stop. */
const MIN_STOP_GAP_KM = 0.5;
const MAX_WAYPOINTS_CHECKED = 300;

/** A stop taken from the file; `name` is null when the file does not name it. */
export interface ImportedStop extends LatLon {
  name: string | null;
}

export type GpxPlan =
  /** The road of the file, with its stops placed along it. Only the two ends can be unnamed. */
  | { kind: 'track'; route: RouteLine; stops: ImportedStop[] }
  /** No road in the file: these points go to the router as stops. */
  | { kind: 'stops'; stops: ImportedStop[] };

/** At most `max` items, spread evenly, always keeping the first and the last. */
export function pickEvenly<T>(list: readonly T[], max: number): T[] {
  if (list.length <= max) return [...list];
  if (max <= 1) return list.slice(0, Math.max(0, max));
  const step = (list.length - 1) / (max - 1);
  return Array.from({ length: max }, (_, i) => list[Math.round(i * step)]);
}

/** Drops repeated points and thins a very dense line. */
export function thinLine(line: readonly LatLon[], max: number = MAX_TRACK_POINTS): LatLon[] {
  const distinct = line.filter((p, i) => i === 0 || p.lat !== line[i - 1].lat || p.lon !== line[i - 1].lon);
  return pickEvenly(distinct, max).map((p) => ({ lat: p.lat, lon: p.lon }));
}

function trackPlan(line: readonly LatLon[], waypoints: readonly GpxPoint[]): GpxPlan {
  const coords = thinLine(line);
  const cumKm = cumulativeKm(coords);
  const total = cumKm[cumKm.length - 1] ?? 0;
  if (coords.length < 2 || !(total > 0)) throw new GpxError('empty');

  const named = waypoints.filter((w) => w.name).slice(0, MAX_WAYPOINTS_CHECKED);
  const first = coords[0];
  const last = coords[coords.length - 1];
  // On a loop the same waypoint can name both ends, which is what the rider expects.
  const startName = named.find((w) => distanceKm(w, first) <= NEAR_KM)?.name ?? null;
  const endName = [...named].reverse().find((w) => distanceKm(w, last) <= NEAR_KM)?.name ?? null;

  const along = named
    .map((w) => ({ point: w, ...nearestOnLine(coords, cumKm, w) }))
    .filter((c) => c.offKm <= NEAR_KM && c.km > MIN_STOP_GAP_KM && c.km < total - MIN_STOP_GAP_KM)
    .sort((a, b) => a.km - b.km);
  const spaced: typeof along = [];
  for (const c of along) {
    const previous = spaced[spaced.length - 1];
    if (!previous || c.km - previous.km >= MIN_STOP_GAP_KM) spaced.push(c);
  }
  const vias = pickEvenly(spaced, MAX_STOPS - 2);

  return {
    kind: 'track',
    // No road names: those come from the router, and an imported line has none.
    route: { coords, cumKm, distanceKm: total, stopKm: [0, ...vias.map((v) => v.km), total], roads: [] },
    stops: [
      { lat: first.lat, lon: first.lon, name: startName },
      ...vias.map((v) => ({ lat: v.point.lat, lon: v.point.lon, name: v.point.name })),
      { lat: last.lat, lon: last.lon, name: endName },
    ],
  };
}

/** Throws GpxError('empty') when the file has neither a road nor two points to route between. */
export function planImport(gpx: ParsedGpx): GpxPlan {
  if (gpx.track.length >= 2) return trackPlan(gpx.track, gpx.waypoints);
  const route = gpx.routePoints;
  if (route.length >= 2) {
    const cum = cumulativeKm(route);
    if (cum[cum.length - 1] / (route.length - 1) <= DENSE_ROUTE_KM) return trackPlan(route, gpx.waypoints);
  }
  const sparse = route.length >= 2 ? route : gpx.waypoints;
  if (sparse.length < 2) throw new GpxError('empty');
  return { kind: 'stops', stops: pickEvenly(sparse, MAX_STOPS).map((p) => ({ lat: p.lat, lon: p.lon, name: p.name || null })) };
}
