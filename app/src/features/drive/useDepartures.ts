import { useEffect, useRef, useState } from 'react';
import { isAbort } from '../../lib/api';
import { runPool } from '../route/pool';
import { queryKey, type RouteOptions } from '../route/routeOptions';
import type { LatLon } from '../route/types';
import { departureTimes } from './logic/departures';
import type { DriveRoute, RouteSource } from './logic/driveRoute';
import { fetchTrafficRoutes } from './routeService';

// The later departures of "Plecarea perfectă": for each, TomTom's best route
// with the traffic predicted for that moment (one request each, two at a
// time), with the same route options as the routes for now. Asked once per
// pair of ends and options, only when TomTom answered for now.

export interface LaterDeparture {
  departMs: number;
  status: 'loading' | 'ready' | 'error';
  route: DriveRoute | null;
}

export interface Departures {
  /** When the comparison was set up: the "now" the later times count from. */
  baseMs: number;
  later: LaterDeparture[];
}

const CONCURRENCY = 2;
const KEY_DECIMALS = 4;

const keyOf = (p: LatLon | null): string | null => (p ? `${p.lat.toFixed(KEY_DECIMALS)},${p.lon.toFixed(KEY_DECIMALS)}` : null);

/**
 * `source` is who drew the route for leaving now: TomTom is worth asking
 * about later too; with OSRM the later departures ride the same route. Null
 * until that route is known. `options` are the ones the routes for now
 * follow, so every departure compares the same kind of route.
 */
export function useDepartures(origin: LatLon | null, destination: LatLon | null, source: RouteSource | null, options: RouteOptions | null): Departures {
  const [state, setState] = useState<Departures>(() => ({ baseMs: Date.now(), later: [] }));
  const originKey = keyOf(origin);
  const destinationKey = keyOf(destination);
  const optionsKey = options ? queryKey(options) : null;
  const optionsRef = useRef(options);
  optionsRef.current = options;

  useEffect(() => {
    const baseMs = Date.now();
    if (!originKey || !destinationKey || source === null) {
      setState({ baseMs, later: [] });
      return undefined;
    }
    const times = departureTimes(baseMs);
    if (source === 'osrm') {
      setState({ baseMs, later: times.map((departMs) => ({ departMs, status: 'ready', route: null })) });
      return undefined;
    }
    const [oLat, oLon] = originKey.split(',').map(Number);
    const [dLat, dLon] = destinationKey.split(',').map(Number);
    const controller = new AbortController();
    setState({ baseMs, later: times.map((departMs) => ({ departMs, status: 'loading', route: null })) });
    const settle = (departMs: number, next: Partial<LaterDeparture>): void => {
      if (controller.signal.aborted) return;
      setState((s) => ({ ...s, later: s.later.map((d) => (d.departMs === departMs ? { ...d, ...next } : d)) }));
    };
    void runPool(
      times,
      CONCURRENCY,
      async (departMs) => {
        try {
          const found = await fetchTrafficRoutes(
            [
              { lat: oLat, lon: oLon },
              { lat: dLat, lon: dLon },
            ],
            { alternatives: 0, departMs, options: optionsRef.current ?? undefined },
            controller.signal,
          );
          const route = found?.routes[0];
          settle(departMs, route ? { status: 'ready', route: { ...route, id: `at-${departMs}` } } : { status: 'error' });
        } catch (err) {
          if (!isAbort(err)) settle(departMs, { status: 'error' });
        }
      },
      controller.signal,
    );
    return () => controller.abort();
  }, [originKey, destinationKey, source, optionsKey]);

  return state;
}
