import { useEffect, useMemo, useState, useSyncExternalStore } from 'react';
import { KEYS, readString, writeString } from '../../lib/storage';
import { FUEL_FILE_URL, FUEL_RANGES, parseFuelFile, planFuel, stationsAlong, USABLE_SHARE, type FuelPlan, type FuelRange, type FuelStation } from './fuel';
import type { RouteLine } from './types';

// The rider's range (kept on this phone only, set in Eu › Motocicletă), the
// stations file, loaded once and only when a route needs it, and the plan
// for a route.

const listeners = new Set<() => void>();

function readRange(): FuelRange | null {
  const value = Number(readString(KEYS.fuelRange));
  return (FUEL_RANGES as readonly number[]).includes(value) ? (value as FuelRange) : null;
}

export function setFuelRange(km: FuelRange | null): void {
  writeString(KEYS.fuelRange, km === null ? null : String(km));
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The range set on this phone, km, or null when the rider has not set one. */
export function useFuelRange(): FuelRange | null {
  return useSyncExternalStore(subscribe, readRange, () => null);
}

let loading: Promise<FuelStation[]> | null = null;

function loadStations(): Promise<FuelStation[]> {
  loading ??= fetch(FUEL_FILE_URL)
    .then((response) => {
      if (!response.ok) throw new Error(`fuel stations: HTTP ${response.status}`);
      return response.json() as Promise<unknown>;
    })
    .then(parseFuelFile)
    .catch((err: unknown) => {
      loading = null; // try again next time
      throw err;
    });
  return loading;
}

type StationsState = { status: 'idle' | 'loading' | 'error' } | { status: 'ready'; stations: FuelStation[] };

function useFuelStations(enabled: boolean): StationsState {
  const [state, setState] = useState<StationsState>({ status: 'idle' });
  useEffect(() => {
    if (!enabled) return undefined;
    let live = true;
    setState((current) => (current.status === 'ready' ? current : { status: 'loading' }));
    loadStations()
      .then((stations) => {
        if (live) setState({ status: 'ready', stations });
      })
      .catch((err: unknown) => {
        console.warn('[fuel] the stations could not be loaded', err);
        if (live) setState({ status: 'error' });
      });
    return () => {
      live = false;
    };
  }, [enabled]);
  return state;
}

export type FuelPlanState =
  | { status: 'off' }
  | { status: 'short'; range: FuelRange }
  | { status: 'loading' }
  | { status: 'error' }
  | { status: 'ready'; range: FuelRange; plan: FuelPlan };

/**
 * Where to fill up on the route: "off" without a range, "short" when one
 * tank covers it, otherwise the plan once the stations are in.
 */
export function useFuelPlan(route: RouteLine | null): FuelPlanState {
  const range = useFuelRange();
  const needed = route !== null && range !== null && route.distanceKm > range * USABLE_SHARE;
  const stations = useFuelStations(needed);
  return useMemo((): FuelPlanState => {
    if (range === null || route === null) return { status: 'off' };
    if (!needed) return { status: 'short', range };
    if (stations.status === 'error') return { status: 'error' };
    if (stations.status !== 'ready') return { status: 'loading' };
    return { status: 'ready', range, plan: planFuel(stationsAlong(route, stations.stations), route.distanceKm, range) };
  }, [range, route, needed, stations]);
}
