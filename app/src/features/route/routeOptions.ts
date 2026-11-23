import { useSyncExternalStore } from 'react';
import { KEYS, readJson, writeJson } from '../../lib/storage';

// The rider's route options: the kind of route, what to avoid and the
// motorcycle's top speed. They go to TomTom (backend/urban_route.py) for
// every route the app asks for: Drum's routes, the departure times it
// compares, a new route on a ride and the planner's. OSRM, the fallback when
// TomTom cannot answer, knows none of them. Kept on this phone only.

/** TomTom's names, so they pass to the backend as they are. */
export const ROUTE_TYPES = ['fastest', 'short', 'shortest', 'eco', 'thrilling'] as const;
export type RouteType = (typeof ROUTE_TYPES)[number];

export const AVOIDABLE = ['motorways', 'tollRoads', 'ferries', 'unpavedRoads', 'tunnels', 'borderCrossings'] as const;
export type Avoidable = (typeof AVOIDABLE)[number];

export const LEVELS = ['low', 'normal', 'high'] as const;
export type Level = (typeof LEVELS)[number];

/** Top speeds offered, km/h; null is no limit (the motorcycle's own). */
export const TOP_SPEEDS = [null, 45, 70, 90, 110] as const;
export type TopSpeed = (typeof TOP_SPEEDS)[number];

export interface RouteOptions {
  type: RouteType;
  /** In AVOIDABLE's order. */
  avoid: Avoidable[];
  /** How winding and how hilly a route with curves should be; only for 'thrilling'. */
  windingness: Level;
  hilliness: Level;
  topSpeedKmh: TopSpeed;
}

/** The route the app made before it had options: the fastest, avoiding unpaved roads. */
export const DEFAULT_ROUTE_OPTIONS: RouteOptions = {
  type: 'fastest',
  avoid: ['unpavedRoads'],
  windingness: 'normal',
  hilliness: 'normal',
  topSpeedKmh: null,
};

const includes = <T>(list: readonly T[], value: unknown): value is T => list.includes(value as T);

/** Saved options, checked one by one: anything unknown falls back to the default. */
export function normalizeOptions(value: unknown): RouteOptions {
  const v = (typeof value === 'object' && value !== null ? value : {}) as Partial<Record<keyof RouteOptions, unknown>>;
  const avoid = Array.isArray(v.avoid) ? AVOIDABLE.filter((a) => (v.avoid as unknown[]).includes(a)) : DEFAULT_ROUTE_OPTIONS.avoid;
  return {
    type: includes(ROUTE_TYPES, v.type) ? v.type : DEFAULT_ROUTE_OPTIONS.type,
    avoid,
    windingness: includes(LEVELS, v.windingness) ? v.windingness : 'normal',
    hilliness: includes(LEVELS, v.hilliness) ? v.hilliness : 'normal',
    topSpeedKmh: includes(TOP_SPEEDS, v.topSpeedKmh) ? v.topSpeedKmh : null,
  };
}

/** Whether these options ask for the same route as the default ones. */
export function isDefaultOptions(options: RouteOptions): boolean {
  return queryKey(options) === queryKey(DEFAULT_ROUTE_OPTIONS);
}

/** What /route/urban takes (backend/urban_route.py parse_options). */
export interface RouteOptionsQuery {
  route_type: RouteType;
  /** 'none' avoids nothing: an empty value would be dropped from the address. */
  avoid: string;
  windingness?: Level;
  hilliness?: Level;
  top_speed?: number;
}

export function optionsQuery(options: RouteOptions): RouteOptionsQuery {
  const query: RouteOptionsQuery = { route_type: options.type, avoid: options.avoid.length > 0 ? options.avoid.join(',') : 'none' };
  if (options.type === 'thrilling') {
    query.windingness = options.windingness;
    query.hilliness = options.hilliness;
  }
  if (options.topSpeedKmh !== null) query.top_speed = options.topSpeedKmh;
  return query;
}

/** One text per different route asked for: what effects and caches compare. */
export function queryKey(options: RouteOptions): string {
  return JSON.stringify(optionsQuery(options));
}

// ---- The store --------------------------------------------------------------

const listeners = new Set<() => void>();
let current: RouteOptions | null = null;

export function getRouteOptions(): RouteOptions {
  current ??= normalizeOptions(readJson<unknown>(KEYS.routeOptions, null));
  return current;
}

export function setRouteOptions(options: RouteOptions): void {
  current = normalizeOptions(options);
  writeJson(KEYS.routeOptions, current);
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The options, re-rendering when they change anywhere in the app. */
export function useRouteOptions(): RouteOptions {
  return useSyncExternalStore(subscribe, getRouteOptions, getRouteOptions);
}
