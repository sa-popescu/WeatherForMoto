import type { LatLon } from '../../route/types';
import type { AheadPoint } from './ahead';
import { projectOnRoute, type RideRoute } from './follow';
import type { RouteTurn } from './turns';

// A new route from where the rider is, after leaving the one from "Drum":
// the same destination, name and pace, the new line and turns, and the
// weather points of the old plan placed again along the new line.

/** Rerouting waits this long between tries, and stops after this many in one ride. */
export const REROUTE_GAP_MS = 30_000;
export const MAX_REROUTES = 20;
/** A weather point further than this from the new line says nothing about it any more. */
const AHEAD_KEEP_M = 5_000;

export function mayReroute(lastAt: number | null, count: number, now: number): boolean {
  return count < MAX_REROUTES && (lastAt === null || now - lastAt >= REROUTE_GAP_MS);
}

/** The weather points along a new line, by their place; points stored without one are dropped. */
export function remapAhead(points: readonly AheadPoint[] | undefined, route: RideRoute): AheadPoint[] {
  const out: AheadPoint[] = [];
  for (const p of points ?? []) {
    if (p.lat === undefined || p.lon === undefined) continue;
    const at = projectOnRoute(route, { lat: p.lat, lon: p.lon }, null);
    if (at.offM <= AHEAD_KEEP_M) out.push({ ...p, km: at.km });
  }
  return out.sort((a, b) => a.km - b.km);
}

export interface NewLine {
  coords: LatLon[];
  cumKm: number[];
  distanceKm: number;
  turns: RouteTurn[];
}

export function reroutedRoute(old: RideRoute, line: NewLine): RideRoute {
  const base: RideRoute = { ...old, coords: line.coords, cumKm: line.cumKm, distanceKm: line.distanceKm, turns: line.turns };
  return { ...base, ahead: remapAhead(old.ahead, base) };
}
