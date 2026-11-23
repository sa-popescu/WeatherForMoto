import { fetchTrafficRoutes } from '../drive/routeService';
import { fetchRoute } from './osrm';
import { isDefaultOptions, type RouteOptions } from './routeOptions';
import type { LatLon, RouteLine } from './types';

// The planner's road through its stops: TomTom with the rider's route options
// (the usual road speeds, not today's traffic, since a plan is often for
// later), and OSRM when TomTom cannot answer. The ETA stays the planner's
// own, from the rider's average speed; only the road comes from here.

/** Who drew the planner's road, and whether the rider's route options shaped it. */
export interface PlannedRouteInfo {
  source: 'tomtom' | 'osrm';
  /** TomTom's credit, to show with its road. */
  attribution: string | null;
  /** The options found nothing, so the road is TomTom's fastest. */
  relaxed: boolean;
  /** Options other than the default were asked for and the road does not follow them (OSRM, or relaxed). */
  optionsDropped: boolean;
}

/** The road through `places`; throws RouteError (from OSRM) when neither service has one. */
export async function fetchPlannedRoute(places: readonly LatLon[], options: RouteOptions, signal: AbortSignal): Promise<{ route: RouteLine; info: PlannedRouteInfo }> {
  const found = await fetchTrafficRoutes(places, { alternatives: 0, departMs: null, options, traffic: false }, signal);
  const best = found?.routes[0];
  // Each stop needs its place along the road (the legs' lengths); a server without them gets OSRM's road.
  if (found && best && best.line.stopKm.length === places.length) {
    return {
      route: { ...best.line, instructions: best.instructions },
      info: { source: 'tomtom', attribution: found.attribution, relaxed: found.relaxed, optionsDropped: found.relaxed },
    };
  }
  const route = await fetchRoute(places, signal);
  return { route, info: { source: 'osrm', attribution: null, relaxed: false, optionsDropped: !isDefaultOptions(options) } };
}
