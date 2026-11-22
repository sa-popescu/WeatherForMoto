import { api, ApiError, isAbort } from '../../lib/api';
import { fetchRouteWithTime } from '../route/osrm';
import type { LatLon } from '../route/types';
import { fromOsrm, fromUrban, type DriveRoute } from './logic/driveRoute';

// Where "Drum" and the ride get their routes: TomTom with live traffic through
// our server (/route/urban), and OSRM without traffic when TomTom cannot
// answer (no key, the day's budget spent, TomTom down, our server out of
// reach).

export interface TrafficQuery {
  alternatives: number;
  /** A later departure (UTC ms), for predicted traffic; null for now. */
  departMs: number | null;
}

/** TomTom's routes, or null when it cannot answer; the reason goes to the console only. */
export async function fetchTrafficRoutes(
  origin: LatLon,
  destination: LatLon,
  query: TrafficQuery,
  signal: AbortSignal,
): Promise<{ routes: DriveRoute[]; attribution: string } | null> {
  try {
    const res = await api.urbanRoute([origin, destination], query, signal);
    const routes = res.routes.map((r, i) => fromUrban(r, i)).filter((r): r is DriveRoute => r !== null);
    return routes.length > 0 ? { routes, attribution: res.attribution } : null;
  } catch (err) {
    if (isAbort(err)) throw err;
    const reason = err instanceof ApiError ? `${err.status} ${err.detail}` : String(err);
    console.warn('[drive] no traffic route:', reason);
    return null;
  }
}

/** One route, TomTom first and OSRM after it; throws when neither has one (RouteError from OSRM). */
export async function fetchOneRoute(origin: LatLon, destination: LatLon, signal: AbortSignal): Promise<DriveRoute> {
  const traffic = await fetchTrafficRoutes(origin, destination, { alternatives: 0, departMs: null }, signal);
  if (traffic) return traffic.routes[0];
  const { line, durationS } = await fetchRouteWithTime([origin, destination], signal);
  return fromOsrm(line, durationS);
}
