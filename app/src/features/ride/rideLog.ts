import { api } from '../../lib/api';
import { NOMINATIM_GAP_MS, reverseGeocode } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';
import type { RideSummary } from './logic/summary';
import type { RidePoint } from './logic/track';

// Saving a recorded ride to the account's ride log (POST /me/rides/log): the
// distance and time as measured, between the places it started and ended,
// named by OpenStreetMap Nominatim (one lookup per point, within its limit).

const wait = (ms: number): Promise<void> => new Promise((resolve) => window.setTimeout(resolve, ms));

/** A place for the log: its name, or its coordinates when nothing names it (the log wants text). */
async function placeOf(p: RidePoint, lang: Lang): Promise<string> {
  const name = await reverseGeocode(p.lat, p.lon, lang);
  return (name ?? `${p.lat.toFixed(3)}, ${p.lon.toFixed(3)}`).slice(0, 120);
}

/** Sends the ride; returns its id in the log. Throws what the API throws. */
export async function logRideToAccount(token: string, points: readonly RidePoint[], summary: RideSummary, routeName: string | null, lang: Lang): Promise<number> {
  const first = points[0];
  const last = points[points.length - 1];
  const start = await placeOf(first, lang);
  await wait(NOMINATIM_GAP_MS);
  const end = await placeOf(last, lang);
  const result = await api.logRide(token, {
    route_name: routeName ? routeName.slice(0, 80) : null,
    start_city: start,
    end_city: end,
    // The log refuses zero: a ride that barely moved still counts as 100 m and a minute.
    distance_km: Math.min(5000, Math.max(0.1, Math.round(summary.distanceM / 100) / 10)),
    duration_min: Math.min(24 * 60, Math.max(1, Math.round(summary.elapsedMs / 60_000))),
    avg_moto_score: null,
    max_wind_gust: null,
    max_precip: null,
  });
  return result.ride_id;
}
