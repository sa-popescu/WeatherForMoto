import { KEYS, readJson, writeJson } from './storage';
import type { Place, WeatherResponse } from './types';

/**
 * The last weather that actually arrived, kept on the device.
 *
 * The app has three ways to get weather and all of them can fail at once: the
 * API, the browser-direct call to the same upstream the API depends on, and the
 * service worker's copy (which iOS evicts whenever it feels like it). When that
 * happens the rider used to get an error card. An hour-old forecast is not an
 * error, so it is kept here and shown, labelled with its age.
 */

/** Past this an old forecast says more about yesterday than about the ride. */
export const LAST_GOOD_MAX_AGE_MS = 3 * 3600_000;
/** Same place, near enough (about a kilometre at these latitudes). */
const SAME_PLACE_DEG = 0.01;

interface Stored {
  savedAt: number;
  lat: number;
  lon: number;
  data: WeatherResponse;
}

function isStored(value: unknown): value is Stored {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  return typeof v.savedAt === 'number' && typeof v.lat === 'number' && typeof v.lon === 'number' && !!v.data;
}

export function rememberLastGood(place: Place, data: WeatherResponse, nowMs: number = Date.now()): void {
  writeJson(KEYS.lastWeather, { savedAt: nowMs, lat: place.lat, lon: place.lon, data } satisfies Stored);
}

/** What was last seen for this place, with its age, or null when there is nothing worth showing. */
export function lastGoodFor(
  place: Place,
  nowMs: number = Date.now(),
): { data: WeatherResponse; ageMin: number } | null {
  const stored = readJson<Stored | null>(KEYS.lastWeather, null, (v): v is Stored | null => v === null || isStored(v));
  if (!stored) return null;
  const age = nowMs - stored.savedAt;
  if (age < 0 || age > LAST_GOOD_MAX_AGE_MS) return null;
  if (Math.abs(stored.lat - place.lat) > SAME_PLACE_DEG || Math.abs(stored.lon - place.lon) > SAME_PLACE_DEG) return null;
  return { data: stored.data, ageMin: Math.round(age / 60_000) };
}
