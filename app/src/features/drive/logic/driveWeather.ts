import { localNowIso } from '../../../lib/format';
import { distanceKm } from '../../../lib/geo';
import type { Place } from '../../../lib/types';
import { aheadPoints, warningAhead, type AheadWarning } from '../../ride/logic/ahead';
import { darkAt } from '../../route/daylight';
import { buildSamplePoints, type NameBetween } from '../../route/samples';
import { glareStretches } from '../../route/sunGlare';
import { pickSlot } from '../../route/timing';
import type { PointWeather, RouteLine, SamplePoint } from '../../route/types';
import type { DriveRoute } from './driveRoute';

// The weather along the routes "Drum" offers. Each route gets the planner's
// weather points (the ends, and one every 40 km or so); points of different
// routes that lie close together share one forecast, so three alternatives
// through the same town cost one request there, not three. What a route then
// shows is the first thing worth a warning at the moment the rider passes
// each point, the same rule the ride uses on the way (ride/logic/ahead.ts).

/** Points closer than this share one forecast: the models' grid is a few km wide. */
export const SAME_WEATHER_KM = 6;

export type WeatherById = Readonly<Record<string, PointWeather | undefined>>;

export interface RouteWeatherPoints {
  routeId: string;
  /** The route's own points; ids are unique across routes. */
  points: SamplePoint[];
  /** Route point id -> the shared point whose forecast it uses. */
  shared: Record<string, string>;
}

export interface DriveWeatherPlan {
  /** The points to load, each once. */
  shared: SamplePoint[];
  routes: RouteWeatherPoints[];
}

export function planDriveWeather(routes: readonly DriveRoute[], origin: Place, destination: Place, nameBetween: NameBetween): DriveWeatherPlan {
  const shared: SamplePoint[] = [];
  const perRoute = routes.map((route) => {
    const points = buildSamplePoints(route.line, [origin, destination], nameBetween).map((p) => ({ ...p, id: `${route.id}:${p.id}` }));
    const map: Record<string, string> = {};
    for (const p of points) {
      const near = shared.find((s) => distanceKm(s, p) < SAME_WEATHER_KM);
      if (near) {
        map[p.id] = near.id;
        continue;
      }
      const own: SamplePoint = { ...p, id: `w${shared.length}` };
      shared.push(own);
      map[p.id] = own.id;
    }
    return { routeId: route.id, points, shared: map };
  });
  return { shared, routes: perRoute };
}

/** A route's points with the forecasts of the shared points they use. */
export function weatherForRoute(route: RouteWeatherPoints, weather: WeatherById): Record<string, PointWeather | undefined> {
  return Object.fromEntries(route.points.map((p) => [p.id, weather[route.shared[p.id]]]));
}

export interface RouteOutlook {
  /** Every point has its answer (a forecast, or a failure). */
  settled: boolean;
  /** The first thing worth a warning on the way, timed to when the rider gets there. */
  warning: AheadWarning | null;
  /** The lowest moto score met on the way, or null before any forecast. */
  worstScore: number | null;
  /** Some point is reached before sunrise or after sunset. */
  dark: boolean;
  /** The first stretch ridden into a low sun (route/sunGlare.ts), with the wall clock where it starts; null without one. */
  glare: { fromKm: number; toKm: number; localIso: string } | null;
}

/**
 * The first stretch of `line` ridden into a low sun, leaving at `departMs` at
 * `speedKmh`. The cloud cover is the nearest point's for that hour; a point
 * without its forecast counts as overcast, so nothing is said too early.
 */
function firstGlare(line: RouteLine, points: readonly SamplePoint[], weather: WeatherById, speedKmh: number, departMs: number): RouteOutlook['glare'] {
  if (points.length === 0 || speedKmh <= 0) return null;
  const nearest = (km: number): SamplePoint => points.reduce((best, p) => (Math.abs(p.km - km) < Math.abs(best.km - km) ? p : best));
  const timeAtKm = (km: number): number => departMs + (km / speedKmh) * 3_600_000;
  const cloudAt = (km: number, ms: number): number | null => {
    const forecast = weather[nearest(km).id]?.forecast;
    if (!forecast) return 100;
    return pickSlot(forecast.hourly, localNowIso(forecast.utcOffsetSeconds, ms))?.cloud_cover ?? 100;
  };
  const [first] = glareStretches(line, timeAtKm, cloudAt);
  if (!first) return null;
  const offset = weather[nearest(first.fromKm).id]?.forecast?.utcOffsetSeconds ?? 0;
  return { fromKm: first.fromKm, toKm: first.toKm, localIso: localNowIso(offset, first.atMs) };
}

/**
 * What the weather does along one route for a departure at `departMs`, with
 * the route's own pace (`speedKmh`) and length in minutes. With the route's
 * `line`, also where the rider meets a low sun ahead.
 */
export function routeOutlook(
  points: readonly SamplePoint[],
  weather: WeatherById,
  speedKmh: number,
  rideMin: number,
  departMs: number,
  line?: RouteLine,
): RouteOutlook {
  const settled = points.every((p) => {
    const status = weather[p.id]?.status;
    return status === 'ready' || status === 'error';
  });
  const ahead = aheadPoints(points, weather, departMs);
  const warning = speedKmh > 0 ? warningAhead(ahead, 0, speedKmh, departMs, Math.ceil(rideMin) + 1) : null;
  const reachedAt = (km: number): number => departMs + (speedKmh > 0 ? (km / speedKmh) * 3_600_000 : 0);
  let worstScore: number | null = null;
  for (const p of ahead) {
    const score = pickSlot(p.slots, localNowIso(p.utcOffsetSeconds, reachedAt(p.km)))?.score ?? null;
    if (score !== null) worstScore = worstScore === null ? score : Math.min(worstScore, score);
  }
  const dark = points.some((p) => {
    const forecast = weather[p.id]?.forecast;
    return forecast ? darkAt(localNowIso(forecast.utcOffsetSeconds, reachedAt(p.km)), forecast.sun) !== null : false;
  });
  const glare = line && settled ? firstGlare(line, points, weather, speedKmh, departMs) : null;
  return { settled, warning, worstScore, dark, glare };
}
