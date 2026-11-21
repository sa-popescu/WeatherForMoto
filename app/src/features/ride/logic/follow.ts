import type { LatLon } from '../../route/types';
import { MAX_ACCURACY_M, type RideFix } from './track';

// Following a planned route or an imported GPX: where along it the rider is,
// how far off it, and when that is worth saying out loud. All distances along
// the route are in km, matching RouteLine.cumKm.

export interface RideRoute {
  /** Where the route ends, for "Ai ajuns la …". */
  name: string;
  coords: LatLon[];
  /** Distance along the route at each vertex, km. */
  cumKm: number[];
  distanceKm: number;
  /** The planner's average speed, for the arrival time before the ride has its own average. */
  plannedKmh: number;
}

export interface RouteFix {
  /** Distance along the route of the closest point on it. */
  km: number;
  /** How far the position is from the route, in metres. */
  offM: number;
}

export interface FollowState {
  /** Progress along the route; null until the first usable fix. */
  km: number | null;
  offM: number | null;
  /** When the rider first went further than OFF_ROUTE_M, while still unannounced. */
  offSince: number | null;
  /** Announced as off the route. */
  off: boolean;
  arrived: boolean;
}

export type FollowEvent = 'off' | 'back' | 'arrived';

export const NEW_FOLLOW: FollowState = { km: null, offM: null, offSince: null, off: false, arrived: false };

/** The search looks this far back and ahead of the last progress first, so a route that passes near itself does not jump. */
const WINDOW_BACK_KM = 0.3;
const WINDOW_AHEAD_KM = 3;
/** Further than this from the windowed match, the whole route is searched (the rider rejoined elsewhere). */
const REJOIN_SEARCH_M = 150;
/**
 * Inside the window, every km away from the last progress counts as this many
 * metres off the road, so the leg the rider is on beats a hairpin or a way
 * back that the GPS happens to place a little closer.
 */
const ALONG_PENALTY_M_PER_KM = 20;
/** Off the route beyond this (plus the fix's own accuracy, capped) ... */
const OFF_ROUTE_M = 80;
const OFF_ACCURACY_CAP_M = 40;
/** ... for this long, it is said out loud. A wide junction or a fuel stop is shorter. */
const OFF_HOLD_MS = 8_000;
/** Back within this (plus accuracy) counts as back on the route. Lower than OFF_ROUTE_M, so it does not flicker. */
const BACK_ON_M = 40;
const BACK_ACCURACY_CAP_M = 30;
/** Within this of the end, on the route, is arrived. */
const ARRIVE_KM = 0.15;

const M_PER_DEG_LAT = 110_540;
const M_PER_DEG_LON_EQUATOR = 111_320;

/** First vertex index whose cumKm is at least km. */
function indexAtKm(cumKm: readonly number[], km: number): number {
  let lo = 0;
  let hi = cumKm.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cumKm[mid] < km) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/**
 * The closest point on segments [from, to) of the route, on a local flat
 * projection (fine at road scale). With a hint, distance along the route from
 * it weighs in (ALONG_PENALTY_M_PER_KM).
 */
function closestOnSegments(route: RideRoute, p: LatLon, from: number, to: number, hintKm: number | null = null): RouteFix {
  const kx = M_PER_DEG_LON_EQUATOR * Math.cos((p.lat * Math.PI) / 180);
  let best: RouteFix = { km: 0, offM: Number.POSITIVE_INFINITY };
  let bestCost = Number.POSITIVE_INFINITY;
  for (let i = Math.max(0, from); i < Math.min(to, route.coords.length - 1); i += 1) {
    const a = route.coords[i];
    const b = route.coords[i + 1];
    const ax = (a.lon - p.lon) * kx;
    const ay = (a.lat - p.lat) * M_PER_DEG_LAT;
    const bx = (b.lon - p.lon) * kx;
    const by = (b.lat - p.lat) * M_PER_DEG_LAT;
    const dx = bx - ax;
    const dy = by - ay;
    const len2 = dx * dx + dy * dy;
    const t = len2 > 0 ? Math.min(1, Math.max(0, -(ax * dx + ay * dy) / len2)) : 0;
    const off = Math.hypot(ax + t * dx, ay + t * dy);
    const km = route.cumKm[i] + t * (route.cumKm[i + 1] - route.cumKm[i]);
    const cost = hintKm === null ? off : off + Math.abs(km - hintKm) * ALONG_PENALTY_M_PER_KM;
    if (cost < bestCost) {
      bestCost = cost;
      best = { km, offM: off };
    }
  }
  return best;
}

/** Where a position falls on the route, searched near `hintKm` first when there is one. */
export function projectOnRoute(route: RideRoute, p: LatLon, hintKm: number | null): RouteFix {
  if (route.coords.length < 2) return { km: 0, offM: Number.POSITIVE_INFINITY };
  if (hintKm !== null) {
    const from = Math.max(0, indexAtKm(route.cumKm, hintKm - WINDOW_BACK_KM) - 1);
    const to = indexAtKm(route.cumKm, hintKm + WINDOW_AHEAD_KM) + 1;
    const near = closestOnSegments(route, p, from, to, hintKm);
    if (near.offM <= REJOIN_SEARCH_M) return near;
  }
  return closestOnSegments(route, p, 0, route.coords.length - 1);
}

/** The route's own line between two distances along it. */
export function routeSlice(route: RideRoute, fromKm: number, toKm: number): LatLon[] {
  const inner = route.coords.filter((_, i) => route.cumKm[i] > fromKm && route.cumKm[i] < toKm);
  return [pointAt(route, fromKm), ...inner, pointAt(route, toKm)];
}

function pointAt(route: RideRoute, km: number): LatLon {
  const i = Math.min(route.coords.length - 1, Math.max(1, indexAtKm(route.cumKm, km)));
  const a = route.coords[i - 1];
  const b = route.coords[i];
  const span = route.cumKm[i] - route.cumKm[i - 1];
  const t = span > 0 ? Math.min(1, Math.max(0, (km - route.cumKm[i - 1]) / span)) : 0;
  return { lat: a.lat + t * (b.lat - a.lat), lon: a.lon + t * (b.lon - a.lon) };
}

/** Feeds one fix: the new progress, and an event when something should be said. */
export function stepFollow(state: FollowState, route: RideRoute, fix: RideFix): { state: FollowState; event: FollowEvent | null } {
  if (fix.acc > MAX_ACCURACY_M) return { state, event: null };
  const at = projectOnRoute(route, fix, state.km);
  const offLimit = OFF_ROUTE_M + Math.min(fix.acc, OFF_ACCURACY_CAP_M);
  const backLimit = BACK_ON_M + Math.min(fix.acc, BACK_ACCURACY_CAP_M);
  let next: FollowState = { ...state, offM: at.offM };
  let event: FollowEvent | null = null;

  // Progress moves only with a position that is on the route.
  if (at.offM <= offLimit) next.km = at.km;

  if (at.offM > offLimit) {
    const since = state.offSince ?? fix.t;
    next.offSince = since;
    if (!state.off && fix.t - since >= OFF_HOLD_MS) {
      next = { ...next, off: true };
      event = 'off';
    }
  } else {
    next.offSince = null;
    if (state.off && at.offM <= backLimit) {
      next = { ...next, off: false };
      event = 'back';
    }
  }

  if (!next.arrived && !next.off && next.km !== null && next.km >= route.distanceKm - ARRIVE_KM && at.offM <= offLimit) {
    next = { ...next, arrived: true };
    event = 'arrived';
  }
  return { state: next, event };
}

/** Both ends of a gap within this of the route: the route itself is the road taken. */
const ROUTE_FILL_M = 60;

/** The road across a gap when the rider stayed on the route: its own line, no request needed. */
export function fillFromRoute(route: RideRoute, from: LatLon, to: LatLon, hintKm: number | null): { coords: LatLon[]; roadM: number } | null {
  const a = projectOnRoute(route, from, hintKm);
  const b = projectOnRoute(route, to, a.km);
  if (a.offM > ROUTE_FILL_M || b.offM > ROUTE_FILL_M || b.km <= a.km) return null;
  return { coords: routeSlice(route, a.km, b.km), roadM: (b.km - a.km) * 1000 };
}

/** Riding average counts for the arrival time once there is this much of it. */
const OWN_AVERAGE_AFTER_MS = 3 * 60_000;
const MIN_OWN_AVERAGE_KMH = 10;

/** Arrival time on the epoch clock: the ride's own moving average once it means something, else the planner's speed. */
export function arrivalAt(remainingKm: number, ownAvgKmh: number | null, movingMs: number, plannedKmh: number, now: number): number {
  const own = ownAvgKmh !== null && movingMs >= OWN_AVERAGE_AFTER_MS && ownAvgKmh >= MIN_OWN_AVERAGE_KMH;
  const kmh = own ? ownAvgKmh : plannedKmh;
  return now + (Math.max(0, remainingKm) / Math.max(1, kmh)) * 3_600_000;
}
