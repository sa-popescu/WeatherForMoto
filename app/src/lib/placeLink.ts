import type { Place } from './types';

// Shareable link to a place. The coordinates travel with the name, so a
// landmark ("Promenada Mall" exists in several cities) opens exactly where it
// was shared. Links with only ?q= (older app versions) are resolved by name.

export const SHARE_BASE_URL = 'https://weatherformoto.bluemouse.cc/';
const MAX_NAME_LENGTH = 80;
const COORDS = /^(-?\d{1,2}(?:\.\d{1,7})?),(-?\d{1,3}(?:\.\d{1,7})?)$/;

export function placeLinkUrl(place: Place): string {
  const params = new URLSearchParams({ q: place.name, ll: `${place.lat.toFixed(4)},${place.lon.toFixed(4)}` });
  return `${SHARE_BASE_URL}?${params.toString()}`;
}

export type SharedPlace = { kind: 'place'; place: Place } | { kind: 'name'; name: string };

/** The place in a shared link (?q= and optional ?ll=), or null when there is none. */
export function parsePlaceLink(params: URLSearchParams): SharedPlace | null {
  const name = (params.get('q') ?? '').trim().slice(0, MAX_NAME_LENGTH);
  if (!name) return null;
  const match = COORDS.exec((params.get('ll') ?? '').trim());
  if (match) {
    const lat = Number(match[1]);
    const lon = Number(match[2]);
    if (Math.abs(lat) <= 90 && Math.abs(lon) <= 180) return { kind: 'place', place: { name, lat, lon } };
  }
  return { kind: 'name', name };
}
