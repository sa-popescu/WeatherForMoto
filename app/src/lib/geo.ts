import { GPS_TIMEOUT_MS } from './config';
import type { Lang } from './i18n';
import type { Place } from './types';

// Location helpers. Suggestions come from Open-Meteo geocoding (built for
// search-as-you-type); Nominatim is used only for one reverse lookup per GPS
// fix, which its usage policy allows.

export interface Position {
  lat: number;
  lon: number;
  accuracy: number;
}

export class GeoError extends Error {
  readonly reason: 'unsupported' | 'denied' | 'timeout' | 'unavailable';

  constructor(reason: GeoError['reason']) {
    super(reason);
    this.name = 'GeoError';
    this.reason = reason;
  }
}

/**
 * Current position with a hard timeout. The Geolocation timeout option only
 * starts after the user answers the permission prompt, so an ignored prompt
 * would otherwise hang forever.
 */
export function getCurrentPosition(timeoutMs: number = GPS_TIMEOUT_MS): Promise<Position> {
  return new Promise((resolve, reject) => {
    if (!('geolocation' in navigator)) {
      reject(new GeoError('unsupported'));
      return;
    }
    const timer = window.setTimeout(() => reject(new GeoError('timeout')), timeoutMs);
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        window.clearTimeout(timer);
        resolve({ lat: pos.coords.latitude, lon: pos.coords.longitude, accuracy: pos.coords.accuracy });
      },
      (err) => {
        window.clearTimeout(timer);
        reject(new GeoError(err.code === err.PERMISSION_DENIED ? 'denied' : err.code === err.TIMEOUT ? 'timeout' : 'unavailable'));
      },
      { enableHighAccuracy: false, maximumAge: 5 * 60_000, timeout: timeoutMs },
    );
  });
}

export async function geolocationPermission(): Promise<PermissionState | 'unsupported'> {
  if (!('geolocation' in navigator)) return 'unsupported';
  try {
    const status = await navigator.permissions.query({ name: 'geolocation' });
    return status.state;
  } catch {
    return 'prompt';
  }
}

const reverseCache = new Map<string, string | null>();

/** Human name for coordinates ("Floreasca", "Sibiu"), or null if unknown. */
export async function reverseGeocode(lat: number, lon: number, lang: Lang, signal?: AbortSignal): Promise<string | null> {
  const key = `${lat.toFixed(3)},${lon.toFixed(3)},${lang}`;
  if (reverseCache.has(key)) return reverseCache.get(key) ?? null;
  try {
    const url = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&zoom=14&lat=${lat}&lon=${lon}&accept-language=${lang}`;
    const res = await fetch(url, { signal, headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = (await res.json()) as { address?: Record<string, string> };
    const a = body.address ?? {};
    const name = a.suburb || a.city_district || a.city || a.town || a.village || a.municipality || a.county || null;
    reverseCache.set(key, name);
    return name;
  } catch (err) {
    if (!(err instanceof DOMException && err.name === 'AbortError')) console.warn('[geo] reverse geocoding failed', err);
    return null;
  }
}

export interface PlaceSuggestion extends Place {
  region: string | null;
  country: string | null;
  countryCode: string | null;
}

interface OpenMeteoPlace {
  name: string;
  latitude: number;
  longitude: number;
  admin1?: string;
  admin2?: string;
  country?: string;
  country_code?: string;
}

function fold(text: string): string {
  return text.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
}

/** A point the search can lean on when two places share a name. */
export interface NearPoint {
  lat: number;
  lon: number;
}

/**
 * Orders geocoding hits: Romania first, as the API returned them. The one
 * case where that order says nothing useful is a true homonym: Romania has
 * several places with exactly the same name (Cheia sits in both Prahova and
 * Brașov), and the API cannot know which one the rider means. Only then does
 * this reorder, putting the homonyms first and, when `near` is known (the
 * neighbouring stop), the closest of them on top. A single exact match is left
 * where the API put it, so "Cluj" does not jump over "Cluj-Napoca".
 */
export function rankSuggestions(list: readonly PlaceSuggestion[], name: string, near?: NearPoint | null): PlaceSuggestion[] {
  const ranked = [...list.filter((p) => p.countryCode === 'RO'), ...list.filter((p) => p.countryCode !== 'RO')];
  const wanted = fold(name);
  const exact = ranked.filter((p) => fold(p.name) === wanted);
  if (exact.length < 2) return ranked;
  const rest = ranked.filter((p) => fold(p.name) !== wanted);
  if (near) exact.sort((a, b) => distanceKm(near, a) - distanceKm(near, b));
  return [...exact, ...rest];
}

/**
 * Place suggestions for a search box. Supports the Romanian "village, county"
 * form: "Sâmbăta, Brașov" keeps only results whose region matches the county.
 * `near` breaks ties between places with the same name.
 */
export async function searchPlaces(query: string, lang: Lang, signal?: AbortSignal, near?: NearPoint | null): Promise<PlaceSuggestion[]> {
  const [namePart, regionPart] = query.split(',').map((s) => s.trim());
  if (!namePart || namePart.length < 2) return [];
  const url = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(namePart)}&count=10&language=${lang}&format=json`;
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`Geocoding HTTP ${res.status}`);
  const body = (await res.json()) as { results?: OpenMeteoPlace[] };
  let results = body.results ?? [];
  if (regionPart) {
    const wanted = fold(regionPart);
    const filtered = results.filter((r) => fold(`${r.admin1 ?? ''} ${r.admin2 ?? ''}`).includes(wanted));
    if (filtered.length) results = filtered;
  }
  const suggestions = results.map((r) => ({
    name: r.name,
    lat: r.latitude,
    lon: r.longitude,
    region: r.admin1 ?? null,
    country: r.country ?? null,
    countryCode: r.country_code ?? null,
  }));
  return rankSuggestions(suggestions, namePart, near).slice(0, 8);
}

/** Great-circle distance in km. */
export function distanceKm(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const rad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * rad;
  const dLon = (b.lon - a.lon) * rad;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a.lat * rad) * Math.cos(b.lat * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * 6371 * Math.asin(Math.min(1, Math.sqrt(h)));
}
