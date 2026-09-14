import type { PlaceSuggestion } from './geo';
import type { Lang } from './i18n';

// Place search over Photon (photon.komoot.io): OpenStreetMap data built for
// search-as-you-type, so it finds neighbourhoods, streets and landmarks
// ("Floreasca", "Promenada Mall"), not only towns. Results near the place on
// screen rank higher. Parsing is kept pure so it can be tested offline.

export const PHOTON_URL = 'https://photon.komoot.io/api/';
/** Ask for more than we show: noise is filtered out and duplicates merged. */
const FETCH_LIMIT = 15;
export const MAX_SUGGESTIONS = 8;
/** Same name closer than this is one place (street segments, a district and its main road). */
const SAME_PLACE_KM = 1.5;

interface PhotonProperties {
  name?: string;
  type?: string;
  osm_key?: string;
  osm_value?: string;
  housenumber?: string;
  street?: string;
  locality?: string;
  district?: string;
  city?: string;
  county?: string;
  state?: string;
  country?: string;
  countrycode?: string;
}

export interface PhotonFeature {
  geometry?: { coordinates?: number[] };
  properties?: PhotonProperties;
}

// Map objects that share a name with a place but are never somewhere to ride to.
const SKIPPED_KEYS: ReadonlySet<string> = new Set(['railway', 'public_transport', 'waterway', 'power', 'barrier', 'entrance']);
const SKIPPED_VALUES: Readonly<Record<string, ReadonlySet<string>>> = {
  highway: new Set(['bus_stop', 'platform', 'stop', 'crossing', 'traffic_signals', 'street_lamp']),
  amenity: new Set(['parking', 'parking_entrance', 'parking_space', 'bicycle_parking', 'bench', 'waste_basket']),
  building: new Set(['apartments', 'residential', 'house', 'detached', 'garage', 'garages']),
};

export function foldText(text: string): string {
  return text.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
}

export function photonUrl(query: string, lang: Lang, near?: { lat: number; lon: number }): string {
  const params = new URLSearchParams({ q: query, limit: String(FETCH_LIMIT) });
  // Photon has no Romanian labels; its default is the local (Romanian) name.
  if (lang === 'en') params.set('lang', 'en');
  if (near) {
    // About 1 km is plenty for ranking and says little about where the rider is.
    params.set('lat', near.lat.toFixed(2));
    params.set('lon', near.lon.toFixed(2));
  }
  return `${PHOTON_URL}?${params.toString()}`;
}

interface Candidate {
  suggestion: PlaceSuggestion;
  key: string;
  type: string;
  /** Folded admin names, for the "name, county" filter. */
  area: string;
}

function isSkipped(p: PhotonProperties): boolean {
  if (!p.osm_key) return false;
  return SKIPPED_KEYS.has(p.osm_key) || Boolean(SKIPPED_VALUES[p.osm_key]?.has(p.osm_value ?? ''));
}

/** Where the place is, most specific first: "Sector 1, București". */
function contextOf(p: PhotonProperties, name: string): string | null {
  const seen = new Set([foldText(name)]);
  const parts: string[] = [];
  for (const part of [p.district ?? p.locality, p.city, p.county]) {
    if (!part || seen.has(foldText(part))) continue;
    seen.add(foldText(part));
    parts.push(part);
  }
  return parts.slice(0, 2).join(', ') || null;
}

/** Flat-earth distance, accurate enough to merge results a kilometre apart. */
function nearKm(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const x = (b.lon - a.lon) * Math.cos((((a.lat + b.lat) / 2) * Math.PI) / 180);
  return Math.hypot(x, b.lat - a.lat) * 111.32;
}

function toCandidate(feature: PhotonFeature): Candidate | null {
  const p = feature.properties ?? {};
  const [lon, lat] = feature.geometry?.coordinates ?? [];
  if (typeof lat !== 'number' || typeof lon !== 'number' || !Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (isSkipped(p)) return null;
  const name = (p.name ?? [p.street, p.housenumber].filter(Boolean).join(' ')).trim();
  if (!name) return null;
  return {
    suggestion: {
      name,
      lat,
      lon,
      region: contextOf(p, name),
      country: p.country ?? null,
      countryCode: p.countrycode ? p.countrycode.toUpperCase() : null,
    },
    key: foldText(name),
    type: p.type ?? '',
    area: foldText([p.district, p.locality, p.city, p.county, p.state].filter(Boolean).join(' ')),
  };
}

/**
 * Suggestions from a Photon answer: noise removed, duplicates merged, Romania
 * first (keeping Photon's relevance order inside each group). `regionHint` is
 * the part after a comma ("Sâmbăta, Brașov"); it narrows the list when it
 * matches something.
 */
export function parsePhoton(features: readonly PhotonFeature[], regionHint?: string): PlaceSuggestion[] {
  let candidates = features.map(toCandidate).filter((c): c is Candidate => c !== null);

  // "Sibiu" the county next to "Sibiu" the town is only noise.
  candidates = candidates.filter(
    (c) => !(c.type === 'county' || c.type === 'state') || !candidates.some((o) => o !== c && o.key === c.key && o.type !== c.type),
  );

  if (regionHint) {
    const wanted = foldText(regionHint);
    const inRegion = candidates.filter((c) => c.area.includes(wanted));
    if (inRegion.length) candidates = inRegion;
  }

  const kept: Candidate[] = [];
  for (const c of candidates) {
    if (!kept.some((k) => k.key === c.key && nearKm(k.suggestion, c.suggestion) < SAME_PLACE_KM)) kept.push(c);
  }

  const romanian = kept.filter((c) => c.suggestion.countryCode === 'RO');
  const others = kept.filter((c) => c.suggestion.countryCode !== 'RO');
  return [...romanian, ...others].slice(0, MAX_SUGGESTIONS).map((c) => c.suggestion);
}
