import { useEffect, useRef, useState } from 'react';
import { isAbort } from '../../lib/api';
import { noteUse } from '../../lib/usage';
import { fetchRouteWithTime, RouteError } from '../route/osrm';
import { queryKey, type RouteOptions } from '../route/routeOptions';
import type { LatLon } from '../route/types';
import { fromOsrm, type DriveRoute, type RouteSource } from './logic/driveRoute';
import { fetchTrafficRoutes } from './routeService';

// The routes from where the rider is to where they go: first TomTom, with
// live traffic and up to two alternatives (backend /route/urban); when that
// cannot answer (no key, the daily budget spent, TomTom down, no network to
// our server), one route from OSRM without traffic. The rider's route options
// go with every request (routeService.ts); new ends or new options cancel the
// request before them.

export type RoutesState =
  | { status: 'idle' }
  | { status: 'loading' }
  | {
      status: 'ready';
      routes: DriveRoute[];
      source: RouteSource;
      attribution: string | null;
      /** The options the routes follow (null from OSRM, which follows none). */
      options: RouteOptions | null;
      /** The rider's options found nothing, so TomTom's routes follow the default ones. */
      relaxed: boolean;
    }
  | { status: 'error'; kind: 'no-route' | 'network' };

/** Ends this close count as the same, so a GPS fix moving a few metres does not ask again. */
const KEY_DECIMALS = 4;

function keyOf(p: LatLon | null): string | null {
  return p ? `${p.lat.toFixed(KEY_DECIMALS)},${p.lon.toFixed(KEY_DECIMALS)}` : null;
}

async function trafficRoutes(
  origin: LatLon,
  destination: LatLon,
  departMs: number | null,
  options: RouteOptions,
  signal: AbortSignal,
): Promise<RoutesState | null> {
  const found = await fetchTrafficRoutes([origin, destination], { alternatives: 2, departMs, options }, signal);
  // Nothing from TomTom leaves OSRM to try.
  return found
    ? { status: 'ready', routes: found.routes, source: 'tomtom', attribution: found.attribution, options: found.options, relaxed: found.relaxed }
    : null;
}

async function plainRoute(origin: LatLon, destination: LatLon, signal: AbortSignal): Promise<RoutesState> {
  try {
    const { line, durationS } = await fetchRouteWithTime([origin, destination], signal);
    return { status: 'ready', routes: [fromOsrm(line, durationS)], source: 'osrm', attribution: null, options: null, relaxed: false };
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
export function useDriveRoutes(
  origin: LatLon | null,
  destination: LatLon | null,
  departMs: number | null,
  attempt: number,
  options: RouteOptions,
): RoutesState {
  const [state, setState] = useState<RoutesState>({ status: 'idle' });
  const originKey = keyOf(origin);
  const destinationKey = keyOf(destination);
  // The same route asked for keeps the same key, so a new object of equal options asks nothing again.
  const optionsKey = queryKey(options);
  const optionsRef = useRef(options);
  optionsRef.current = options;

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
      const result = (await trafficRoutes(from, to, departMs, optionsRef.current, controller.signal)) ?? (await plainRoute(from, to, controller.signal));
      if (controller.signal.aborted) return;
      setState(result);
      if (result.status === 'ready') noteUse('drive:routes');
    };
    run().catch((err: unknown) => {
      if (!isAbort(err)) console.warn('[drive] route lookup failed', err);
    });
    return () => controller.abort();
  }, [originKey, destinationKey, departMs, attempt, optionsKey]);

  return state;
}
