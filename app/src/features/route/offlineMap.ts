import type { LatLon } from './types';

// Which map tiles to download so a route can be ridden without signal: a
// band along the road, wide at the overview zooms and narrow at street level.
//
// OpenFreeMap allows an app to download at most 3,000 tiles or 50 MB at a
// time, with at most 4 requests in parallel. A longer route keeps the full
// band at the lower zooms and gives up detail at the top: first a narrower
// band at zoom 14, then zoom 14 altogether (the map then enlarges zoom 13).

export const MAX_TILES = 3000;
export const MAX_BYTES = 50 * 1024 * 1024;
export const PARALLEL = 4;
/** What a vector tile weighs on the wire, on average, for the size shown before the download. */
export const AVERAGE_TILE_BYTES = 30 * 1024;
/** The highest zoom OpenFreeMap has; the map enlarges it beyond. */
export const TOP_ZOOM = 14;

export interface Tile {
  z: number;
  x: number;
  y: number;
}

interface Band {
  z: number;
  /** How far either side of the road, km. */
  bufferKm: number;
}

/** The overview zooms, below TOP_ZOOM; zoom 14 is added by planTiles, as wide as the limit allows. */
const BANDS: readonly Band[] = [
  { z: 5, bufferKm: 20 },
  { z: 6, bufferKm: 20 },
  { z: 7, bufferKm: 15 },
  { z: 8, bufferKm: 15 },
  { z: 9, bufferKm: 10 },
  { z: 10, bufferKm: 8 },
  { z: 11, bufferKm: 5 },
  { z: 12, bufferKm: 3 },
  { z: 13, bufferKm: 1.5 },
];

const KM_PER_DEG = 111.32;
const MAX_LAT = 85.05112878;

/** The tile holding a point at a zoom (Web Mercator, as every web map). */
export function tileOf(lat: number, lon: number, z: number): { x: number; y: number } {
  const n = 2 ** z;
  const clamped = Math.max(-MAX_LAT, Math.min(MAX_LAT, lat));
  const rad = (clamped * Math.PI) / 180;
  const x = Math.floor(((lon + 180) / 360) * n);
  const y = Math.floor(((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * n);
  return { x: Math.min(n - 1, Math.max(0, x)), y: Math.min(n - 1, Math.max(0, y)) };
}

/** Points along the line no further apart than stepKm, so no tile in the band is skipped. */
function densify(line: readonly LatLon[], stepKm: number): LatLon[] {
  const out: LatLon[] = [];
  for (let i = 0; i < line.length; i += 1) {
    const a = line[i];
    out.push(a);
    const b = line[i + 1];
    if (!b) break;
    const km = Math.hypot((b.lat - a.lat) * KM_PER_DEG, (b.lon - a.lon) * KM_PER_DEG * Math.cos((a.lat * Math.PI) / 180));
    const steps = Math.floor(km / stepKm);
    for (let k = 1; k <= steps; k += 1) {
      const t = k / (steps + 1);
      out.push({ lat: a.lat + (b.lat - a.lat) * t, lon: a.lon + (b.lon - a.lon) * t });
    }
  }
  return out;
}

/** The tiles of one band, in the order the road meets them. */
export function bandTiles(line: readonly LatLon[], band: Band): Tile[] {
  const seen = new Set<string>();
  const out: Tile[] = [];
  for (const p of densify(line, Math.max(0.25, band.bufferKm / 2))) {
    const dLat = band.bufferKm / KM_PER_DEG;
    const dLon = band.bufferKm / (KM_PER_DEG * Math.cos((p.lat * Math.PI) / 180));
    const nw = tileOf(p.lat + dLat, p.lon - dLon, band.z);
    const se = tileOf(p.lat - dLat, p.lon + dLon, band.z);
    for (let x = nw.x; x <= se.x; x += 1) {
      for (let y = nw.y; y <= se.y; y += 1) {
        const key = `${x}/${y}`;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({ z: band.z, x, y });
      }
    }
  }
  return out;
}

export interface TilePlan {
  /** Overview zooms first, then street level along the road. */
  tiles: Tile[];
  /** The highest zoom downloaded. */
  topZoom: number;
  /** Less detail than a short route gets, to stay within the limit. */
  reduced: boolean;
}

/** The tiles to download for a route, within maxTiles. */
export function planTiles(line: readonly LatLon[], maxTiles: number = MAX_TILES): TilePlan {
  const lower = BANDS.flatMap((b) => bandTiles(line, b));
  for (const [i, bufferKm] of [1, 0.4].entries()) {
    const top = bandTiles(line, { z: TOP_ZOOM, bufferKm });
    if (lower.length + top.length <= maxTiles) return { tiles: [...lower, ...top], topZoom: TOP_ZOOM, reduced: i > 0 };
  }
  // Without zoom 14. A route long enough to need more still gets its first
  // maxTiles: the overview everywhere, and zoom 13 as far as it goes.
  return { tiles: lower.slice(0, maxTiles), topZoom: TOP_ZOOM - 1, reduced: true };
}

/** A short, stable id for a route's download: the same road gives the same id. */
export function routeMapId(line: readonly LatLon[]): string {
  let hash = 0x811c9dc5;
  const step = Math.max(1, Math.floor(line.length / 200));
  for (let i = 0; i < line.length; i += step) {
    const text = `${line[i].lat.toFixed(4)},${line[i].lon.toFixed(4)};`;
    for (let k = 0; k < text.length; k += 1) {
      hash ^= text.charCodeAt(k);
      hash = Math.imul(hash, 0x01000193);
    }
  }
  const last = line[line.length - 1];
  return `${(hash >>> 0).toString(36)}-${line.length}-${last ? Math.round(last.lat * 100) : 0}`;
}
