import { useCallback, useEffect, useMemo, useState } from 'react';
import type { Place } from '../../lib/types';
import type { NameBetween } from '../route/samples';
import type { SamplePoint } from '../route/types';
import { bestDeparture, type DepartureOption } from './logic/departures';
import { routeKmh, type DriveRoute } from './logic/driveRoute';
import { planDriveWeather, routeOutlook, weatherForRoute, type RouteOutlook, type WeatherById } from './logic/driveWeather';
import { useDepartures, type LaterDeparture } from './useDepartures';
import { useDriveRoutes, type RoutesState } from './useDriveRoutes';
import { useDriveWeather } from './useDriveWeather';

// Everything "Drum" knows about one trip: the routes for the departure on
// screen, the fastest one for leaving now, the later departures of
// "Plecarea perfectă", the weather along all of them (loaded once per place),
// and what each route and each departure meets on the way.

const NO_POINTS: SamplePoint[] = [];

export interface DrivePlanInput {
  /** Where the routes start; null while there is no destination or the position is still coming. */
  origin: Place | null;
  destination: Place | null;
  /** The departure on screen, UTC ms, or null for now. */
  departMs: number | null;
  attempt: number;
  /** The name of the start in the weather points ("Poziția mea"). */
  originName: string;
  nameBetween: NameBetween;
}

export interface DrivePlan {
  routes: RoutesState;
  /** By route id, for the routes on screen. */
  outlooks: Readonly<Record<string, RouteOutlook>>;
  /** Now first, then the later departures. */
  departures: DepartureOption[];
  /** Index in `departures` of the one to recommend, once all are known. */
  best: number | null;
  /** The "now" the departures count from. */
  baseMs: number;
  /** A route's weather points and their forecasts, for the start of a ride on it. */
  pointsOf: (routeId: string) => { points: SamplePoint[]; weather: WeatherById } | null;
}

function fastest(routes: readonly DriveRoute[]): DriveRoute {
  return routes.reduce((a, b) => (b.durationS < a.durationS ? b : a));
}

function tripKeyOf(origin: Place | null, destination: Place | null): string {
  return origin && destination ? `${origin.lat.toFixed(3)},${origin.lon.toFixed(3)}>${destination.lat.toFixed(4)},${destination.lon.toFixed(4)}` : '';
}

export function useDrivePlan({ origin, destination, departMs, attempt, originName, nameBetween }: DrivePlanInput): DrivePlan {
  const routes = useDriveRoutes(origin, destination, departMs, attempt);
  const ready = routes.status === 'ready' ? routes : null;

  // The fastest way when leaving now, kept while a later departure is on screen.
  const [nowRoute, setNowRoute] = useState<DriveRoute | null>(null);
  useEffect(() => {
    if (departMs === null) setNowRoute(ready && ready.routes.length > 0 ? { ...fastest(ready.routes), id: 'now' } : null);
  }, [ready, departMs]);
  const departures = useDepartures(origin, destination, nowRoute?.source ?? null);

  const allRoutes = useMemo(() => {
    const list: DriveRoute[] = [...(ready?.routes ?? [])];
    if (nowRoute) list.push(nowRoute);
    for (const d of departures.later) if (d.route) list.push(d.route);
    return list;
  }, [ready, nowRoute, departures.later]);

  const plan = useMemo(() => {
    const start = allRoutes[0]?.line.coords[0];
    if (!destination || !start) return null;
    return planDriveWeather(allRoutes, { name: originName, lat: start.lat, lon: start.lon }, destination, nameBetween);
  }, [allRoutes, destination, originName, nameBetween]);
  const weather = useDriveWeather(plan?.shared ?? NO_POINTS, tripKeyOf(origin, destination));

  const outlookOf = useCallback(
    (route: DriveRoute, atMs: number): RouteOutlook | null => {
      const rp = plan?.routes.find((r) => r.routeId === route.id);
      return rp ? routeOutlook(rp.points, weatherForRoute(rp, weather), routeKmh(route), route.durationS / 60, atMs, route.line) : null;
    },
    [plan, weather],
  );

  const outlooks = useMemo(() => {
    const out: Record<string, RouteOutlook> = {};
    for (const route of ready?.routes ?? []) {
      const found = outlookOf(route, departMs ?? departures.baseMs);
      if (found) out[route.id] = found;
    }
    return out;
  }, [ready, outlookOf, departMs, departures.baseMs]);

  const options = useMemo(() => {
    if (!nowRoute) return [];
    const option = (at: number | null, route: DriveRoute | null, status: LaterDeparture['status']): DepartureOption =>
      route && status === 'ready'
        ? { departMs: at, status, durationS: route.durationS, delayS: route.delayS, outlook: outlookOf(route, at ?? departures.baseMs) }
        : { departMs: at, status, durationS: null, delayS: 0, outlook: null };
    // Without traffic every departure rides the route of now; only the weather changes.
    const sameRoad = nowRoute.source === 'osrm' ? nowRoute : null;
    return [option(null, nowRoute, 'ready'), ...departures.later.map((d) => option(d.departMs, d.route ?? sameRoad, d.status))];
  }, [nowRoute, departures, outlookOf]);

  const pointsOf = useCallback(
    (routeId: string) => {
      const rp = plan?.routes.find((r) => r.routeId === routeId);
      return rp ? { points: rp.points, weather: weatherForRoute(rp, weather) } : null;
    },
    [plan, weather],
  );

  return { routes, outlooks, departures: options, best: bestDeparture(options, departures.baseMs), baseMs: departures.baseMs, pointsOf };
}
