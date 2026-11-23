import { api, ApiError, isAbort } from '../../lib/api';
import { fetchRouteWithTime } from '../route/osrm';
import { DEFAULT_ROUTE_OPTIONS, getRouteOptions, isDefaultOptions, optionsQuery, type RouteOptions } from '../route/routeOptions';
import type { LatLon } from '../route/types';
import { fromOsrm, fromUrban, type DriveRoute } from './logic/driveRoute';

// Where "Drum", the ride and the planner get their routes: TomTom with live
// traffic through our server (/route/urban), with the rider's route options
// (route/routeOptions.ts), and OSRM without traffic or options when TomTom
// cannot answer (no key, the day's budget spent, TomTom down, our server out
// of reach). Options TomTom cannot follow on a given trip (a route with
// curves over 900 km, the shortest route or no motorways on a long one) give
// way once to the default route, and the screen says so.

export interface TrafficQuery {
  alternatives: number;
  /** A later departure (UTC ms), for predicted traffic; null for now. */
  departMs: number | null;
  /** The rider's route options; the default ones when left out. */
  options?: RouteOptions;
  /** False for a plan made ahead: the usual speeds of the roads, not today's traffic. */
  traffic?: boolean;
}

export interface TrafficRoutes {
  routes: DriveRoute[];
  attribution: string;
  /** The options the routes follow: the ones asked for, or the default ones when those found nothing. */
  options: RouteOptions;
  /** The options asked for found nothing, so these routes follow the default ones. */
  relaxed: boolean;
}

/**
 * Whether a refusal may be the options' doing, worth one more try with the
 * default ones: no route (TomTom's 400), or TomTom failing on a long request.
 * Not a spent budget, a missing key or no network: the default route would
 * fail the same way.
 */
export function worthRelaxing(err: unknown): boolean {
  if (!(err instanceof ApiError) || err.isNetwork) return false;
  return err.status === 404 || (err.status === 503 && err.detail === 'upstream');
}

type Attempt = { ok: true; routes: DriveRoute[]; attribution: string } | { ok: false; error: unknown };

async function askTomTom(points: readonly LatLon[], query: TrafficQuery, options: RouteOptions, signal: AbortSignal): Promise<Attempt> {
  try {
    const res = await api.urbanRoute(
      points,
      { alternatives: query.alternatives, departMs: query.departMs, route: { ...optionsQuery(options) }, traffic: query.traffic },
      signal,
    );
    const routes = res.routes.map((r, i) => fromUrban(r, i)).filter((r): r is DriveRoute => r !== null);
    return routes.length > 0 ? { ok: true, routes, attribution: res.attribution } : { ok: false, error: new Error('no usable route') };
  } catch (err) {
    if (isAbort(err)) throw err;
    return { ok: false, error: err };
  }
}

function why(err: unknown): string {
  return err instanceof ApiError ? `${err.status} ${err.detail}` : String(err);
}

/** TomTom's routes through `points` (start, stops, destination), or null when it cannot answer; the reason goes to the console only. */
export async function fetchTrafficRoutes(points: readonly LatLon[], query: TrafficQuery, signal: AbortSignal): Promise<TrafficRoutes | null> {
  const asked = query.options ?? DEFAULT_ROUTE_OPTIONS;
  const first = await askTomTom(points, query, asked, signal);
  if (first.ok) return { routes: first.routes, attribution: first.attribution, options: asked, relaxed: false };
  if (!isDefaultOptions(asked) && worthRelaxing(first.error)) {
    console.warn('[route] nothing with the rider\'s options, trying the default route:', why(first.error));
    const second = await askTomTom(points, query, DEFAULT_ROUTE_OPTIONS, signal);
    if (second.ok) return { routes: second.routes, attribution: second.attribution, options: DEFAULT_ROUTE_OPTIONS, relaxed: true };
  }
  console.warn('[route] no traffic route:', why(first.error));
  return null;
}

/** One route with the rider's options, TomTom first and OSRM after it; throws when neither has one (RouteError from OSRM). */
export async function fetchOneRoute(origin: LatLon, destination: LatLon, signal: AbortSignal): Promise<DriveRoute> {
  const traffic = await fetchTrafficRoutes([origin, destination], { alternatives: 0, departMs: null, options: getRouteOptions() }, signal);
  if (traffic) return traffic.routes[0];
  const { line, durationS } = await fetchRouteWithTime([origin, destination], signal);
  return fromOsrm(line, durationS);
}
