import { isAbort } from '../../lib/api';
import { pointAtKm } from './geometry';
import type { LatLon, RouteLine } from './types';

// The highest point of a route. Weather points sit about 40 km apart, so a
// mountain pass between two of them goes unseen, and 2000 m up it can be ten
// degrees colder than either. The ground height along the road comes from
// Open-Meteo's elevation API (Copernicus 90 m DEM), straight from the browser
// like the routing. A pass well above every weather point gets a point of its
// own; anything less is already covered by the points there are.

const ELEVATION_URL = 'https://api.open-meteo.com/v1/elevation';
/** The API answers at most this many coordinates per request. */
const MAX_PER_REQUEST = 100;
/** Ground height is read about every kilometre of road... */
const PROFILE_SPACING_KM = 1;
/** ...at no more points than this: a longer route is read more sparsely. */
const MAX_PROFILE_POINTS = 400;
/** The profile gives up after this long, so the weather never waits on it. */
const TIMEOUT_MS = 5000;
/** A summit this much higher than every weather point gets a point of its own, metres. */
export const SUMMIT_MIN_RISE_M = 300;
/** Coordinates in the request are rounded to about 10 m, plenty for a 90 m grid. */
const COORD_DECIMALS = 4;

export interface Summit {
  km: number;
  lat: number;
  lon: number;
  elevationM: number;
}

/** Where along the road to read the ground height, km, ends included. */
export function profileKms(distanceKm: number): number[] {
  if (!(distanceKm > 0)) return [];
  const count = Math.min(MAX_PROFILE_POINTS, Math.max(2, Math.floor(distanceKm / PROFILE_SPACING_KM) + 1));
  return Array.from({ length: count }, (_, i) => (distanceKm * i) / (count - 1));
}

function nearestIndex(kms: readonly number[], km: number): number {
  let best = 0;
  for (let i = 1; i < kms.length; i += 1) if (Math.abs(kms[i] - km) < Math.abs(kms[best] - km)) best = i;
  return best;
}

/**
 * The highest point of the profile, when it rises at least SUMMIT_MIN_RISE_M
 * above the highest weather point (each read at its nearest profile point).
 */
export function findSummit(
  kms: readonly number[],
  heights: readonly number[],
  sampleKms: readonly number[],
): { km: number; elevationM: number } | null {
  if (kms.length === 0 || kms.length !== heights.length || sampleKms.length === 0) return null;
  // A missing height (sea, a gap in the model) never wins.
  const height = (i: number): number => (Number.isFinite(heights[i]) ? heights[i] : Number.NEGATIVE_INFINITY);
  let top = 0;
  for (let i = 1; i < kms.length; i += 1) if (height(i) > height(top)) top = i;
  const highestSample = Math.max(...sampleKms.map((km) => height(nearestIndex(kms, km))));
  if (!Number.isFinite(height(top)) || height(top) - highestSample < SUMMIT_MIN_RISE_M) return null;
  return { km: kms[top], elevationM: heights[top] };
}

function elevationUrl(points: readonly LatLon[]): string {
  const lats = points.map((p) => p.lat.toFixed(COORD_DECIMALS)).join(',');
  const lons = points.map((p) => p.lon.toFixed(COORD_DECIMALS)).join(',');
  return `${ELEVATION_URL}?latitude=${lats}&longitude=${lons}`;
}

/** Ground height of every point, metres, in the same order. Throws when any request fails. */
async function fetchElevations(points: readonly LatLon[], signal: AbortSignal): Promise<number[]> {
  const chunks: LatLon[][] = [];
  for (let i = 0; i < points.length; i += MAX_PER_REQUEST) chunks.push(points.slice(i, i + MAX_PER_REQUEST));
  const answers = await Promise.all(
    chunks.map(async (chunk) => {
      const res = await fetch(elevationUrl(chunk), { signal });
      if (!res.ok) throw new Error(`elevation HTTP ${res.status}`);
      const body = (await res.json()) as { elevation?: unknown };
      if (!Array.isArray(body.elevation) || body.elevation.length !== chunk.length) throw new Error('elevation: unexpected answer');
      return body.elevation.map((v) => (typeof v === 'number' ? v : Number.NaN));
    }),
  );
  return answers.flat();
}

/**
 * The summit that deserves its own weather point, or null when there is none
 * or the heights did not come in time. Rethrows only when `signal` aborts,
 * meaning a newer calculation took over.
 */
export async function routeSummit(route: RouteLine, sampleKms: readonly number[], signal: AbortSignal): Promise<Summit | null> {
  const kms = profileKms(route.distanceKm);
  if (kms.length < 2 || route.coords.length < 2) return null;
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  signal.addEventListener('abort', stop);
  const timer = window.setTimeout(stop, TIMEOUT_MS);
  try {
    const heights = await fetchElevations(kms.map((km) => pointAtKm(route.coords, route.cumKm, km)), controller.signal);
    const top = findSummit(kms, heights, sampleKms);
    if (!top) return null;
    const at = pointAtKm(route.coords, route.cumKm, top.km);
    return { km: top.km, lat: at.lat, lon: at.lon, elevationM: Math.round(top.elevationM) };
  } catch (err) {
    if (signal.aborted) throw err;
    // Too slow or no answer: the route works as before, without the extra point.
    console.warn(isAbort(err) ? '[route] elevation profile timed out' : '[route] elevation profile failed', err);
    return null;
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', stop);
  }
}
