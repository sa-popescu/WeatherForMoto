import type { LatLon, RouteLine } from './types';

// Where to fill up on a route: the stations near the road, from the monthly
// OpenStreetMap file (public/data/fuel-stations.json, made by the "Fuel
// stations" workflow), and the stops that keep the tank above a reserve.
//
// The plan starts with a full tank and fills up before the rider has used
// USABLE_SHARE of the range, at the last station before that point, so the
// stops are as few as possible.

export interface FuelStation {
  lat: number;
  lon: number;
  /** Brand or name, may be empty. */
  label: string;
}

export interface StationOnRoute extends FuelStation {
  /** Distance along the route, km. */
  km: number;
  /** How far off the road it stands, km. */
  offKm: number;
}

export interface FuelGap {
  fromKm: number;
  toKm: number;
  /** Longer than the whole range: the rider cannot make it on one tank. */
  tooLong: boolean;
}

export interface FuelPlan {
  stops: StationOnRoute[];
  gaps: FuelGap[];
}

/** The ranges offered in the settings, km. */
export const FUEL_RANGES = [150, 200, 250, 300, 350, 400] as const;
export type FuelRange = (typeof FUEL_RANGES)[number];
/** Fill up before this share of the range is used: the rest is the reserve. */
export const USABLE_SHARE = 0.8;
/** A station further than this from the road is not "on the way". */
export const MAX_OFF_ROUTE_KM = 1;
/** A station right at the start or at the last stop is not a new stop. */
const MIN_STEP_KM = 1;

export const FUEL_FILE_URL = '/data/fuel-stations.json';

/** The stations in the file, or an error when it is not the expected shape. */
export function parseFuelFile(body: unknown): FuelStation[] {
  const rows = (body as { stations?: unknown })?.stations;
  if (!Array.isArray(rows)) throw new Error('fuel stations: no list in the file');
  const out: FuelStation[] = [];
  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    const [lat, lon, label] = row as unknown[];
    if (typeof lat === 'number' && typeof lon === 'number') out.push({ lat, lon, label: typeof label === 'string' ? label : '' });
  }
  return out;
}

const KM_PER_DEG = 111.32;

/** The stations within maxOffKm of the route, with their distance along it, in route order. */
export function stationsAlong(route: Pick<RouteLine, 'coords' | 'cumKm'>, stations: readonly FuelStation[], maxOffKm: number = MAX_OFF_ROUTE_KM): StationOnRoute[] {
  const { coords, cumKm } = route;
  if (coords.length < 2) return [];
  const margin = maxOffKm / KM_PER_DEG + 0.005;
  let south = Infinity;
  let north = -Infinity;
  let west = Infinity;
  let east = -Infinity;
  for (const p of coords) {
    south = Math.min(south, p.lat);
    north = Math.max(north, p.lat);
    west = Math.min(west, p.lon);
    east = Math.max(east, p.lon);
  }
  const lonMargin = margin / Math.cos((((south + north) / 2) * Math.PI) / 180);
  const out: StationOnRoute[] = [];
  for (const s of stations) {
    if (s.lat < south - margin || s.lat > north + margin || s.lon < west - lonMargin || s.lon > east + lonMargin) continue;
    const hit = project(coords, cumKm, s, margin, lonMargin);
    if (hit && hit.offKm <= maxOffKm) out.push({ ...s, km: hit.km, offKm: hit.offKm });
  }
  return out.sort((a, b) => a.km - b.km);
}

/** The nearest point of the route to p, as distance along it and off it; null when no segment is near. */
function project(coords: readonly LatLon[], cumKm: readonly number[], p: LatLon, latMargin: number, lonMargin: number): { km: number; offKm: number } | null {
  const kx = KM_PER_DEG * Math.cos((p.lat * Math.PI) / 180);
  let best: { km: number; offKm: number } | null = null;
  for (let i = 0; i + 1 < coords.length; i += 1) {
    const a = coords[i];
    const b = coords[i + 1];
    // Cheap skip for segments nowhere near.
    if ((a.lat < p.lat - latMargin && b.lat < p.lat - latMargin) || (a.lat > p.lat + latMargin && b.lat > p.lat + latMargin)) continue;
    if ((a.lon < p.lon - lonMargin && b.lon < p.lon - lonMargin) || (a.lon > p.lon + lonMargin && b.lon > p.lon + lonMargin)) continue;
    const ax = (a.lon - p.lon) * kx;
    const ay = (a.lat - p.lat) * KM_PER_DEG;
    const dx = (b.lon - p.lon) * kx - ax;
    const dy = (b.lat - p.lat) * KM_PER_DEG - ay;
    const len2 = dx * dx + dy * dy;
    const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, -(ax * dx + ay * dy) / len2));
    const offKm = Math.hypot(ax + t * dx, ay + t * dy);
    if (!best || offKm < best.offKm) best = { km: cumKm[i] + t * (cumKm[i + 1] - cumKm[i]), offKm };
  }
  return best;
}

/** Where to fill up on a route of distanceKm with this range, starting on a full tank. */
export function planFuel(along: readonly StationOnRoute[], distanceKm: number, rangeKm: number): FuelPlan {
  const usable = rangeKm * USABLE_SHARE;
  const stops: StationOnRoute[] = [];
  const gaps: FuelGap[] = [];
  let at = 0;
  while (distanceKm - at > usable) {
    const reachable = along.filter((s) => s.km >= at + MIN_STEP_KM && s.km <= at + usable);
    if (reachable.length > 0) {
      const stop = reachable[reachable.length - 1];
      stops.push(stop);
      at = stop.km;
      continue;
    }
    // Nothing before the reserve: the first station after it is the stop,
    // and the stretch up to it is a warning.
    const next = along.find((s) => s.km >= at + MIN_STEP_KM);
    const toKm = next && next.km < distanceKm ? next.km : distanceKm;
    if (toKm - at > usable) gaps.push({ fromKm: at, toKm, tooLong: toKm - at > rangeKm });
    if (!next || next.km >= distanceKm) break;
    stops.push(next);
    at = next.km;
  }
  return { stops, gaps };
}
