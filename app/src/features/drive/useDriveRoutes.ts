import { useEffect, useState } from 'react';
import { api, ApiError, isAbort } from '../../lib/api';
import { fetchRouteWithTime, RouteError } from '../route/osrm';
import type { LatLon } from '../route/types';
import { fromOsrm, fromUrban, type DriveRoute, type RouteSource } from './logic/driveRoute';

// The routes from where the rider is to where they go: first TomTom, with
// live traffic and up to two alternatives (backend /route/urban); when that
// cannot answer (no key, the daily budget spent, TomTom down, no network to
// our server), one route from OSRM without traffic. Each new pair of ends
// cancels the request before it.

export type RoutesState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'ready'; routes: DriveRoute[]; source: RouteSource; attribution: string | null }
  | { status: 'error'; kind: 'no-route' | 'network' };

/** Ends this close count as the same, so a GPS fix moving a few metres does not ask again. */
const KEY_DECIMALS = 4;

function keyOf(p: LatLon | null): string | null {
  return p ? `${p.lat.toFixed(KEY_DECIMALS)},${p.lon.toFixed(KEY_DECIMALS)}` : null;
}

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

async function trafficRoutes(origin: LatLon, destination: LatLon, departMs: number | null, signal: AbortSignal): Promise<RoutesState | null> {
  const found = await fetchTrafficRoutes(origin, destination, { alternatives: 2, departMs }, signal);
  // Nothing from TomTom leaves OSRM to try.
  return found ? { status: 'ready', routes: found.routes, source: 'tomtom', attribution: found.attribution } : null;
}

async function plainRoute(origin: LatLon, destination: LatLon, signal: AbortSignal): Promise<RoutesState> {
  try {
    const { line, durationS } = await fetchRouteWithTime([origin, destination], signal);
    return { status: 'ready', routes: [fromOsrm(line, durationS)], source: 'osrm', attribution: null };
  } catch (err) {
    if (isAbort(err)) throw err;
    if (err instanceof RouteError && err.kind === 'no-route') return { status: 'error', kind: 'no-route' };
    console.warn('[drive] OSRM route failed', err);
    return { status: 'error', kind: 'network' };
  }
}

/**
 * `departMs` is a later departure (null for now); `attempt` grows with every
 * "try again", which asks again for the same ends.
 */
export function useDriveRoutes(origin: LatLon | null, destination: LatLon | null, departMs: number | null, attempt: number): RoutesState {
  const [state, setState] = useState<RoutesState>({ status: 'idle' });
  const originKey = keyOf(origin);
  const destinationKey = keyOf(destination);

  useEffect(() => {
    if (!originKey || !destinationKey) {
      setState({ status: 'idle' });
      return undefined;
    }
    const [oLat, oLon] = originKey.split(',').map(Number);
    const [dLat, dLon] = destinationKey.split(',').map(Number);
    const from = { lat: oLat, lon: oLon };
    const to = { lat: dLat, lon: dLon };
    const controller = new AbortController();
    setState({ status: 'loading' });
    const run = async (): Promise<void> => {
      const result = (await trafficRoutes(from, to, departMs, controller.signal)) ?? (await plainRoute(from, to, controller.signal));
      if (!controller.signal.aborted) setState(result);
    };
    run().catch((err: unknown) => {
      if (!isAbort(err)) console.warn('[drive] route lookup failed', err);
    });
    return () => controller.abort();
  }, [originKey, destinationKey, departMs, attempt]);

  return state;
}
