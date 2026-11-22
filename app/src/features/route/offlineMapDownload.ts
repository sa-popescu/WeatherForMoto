import { OFFLINE_MAP_CACHE_PREFIX, offlineKey } from '../../lib/offlineMapKeys';
import { AVERAGE_TILE_BYTES, MAX_BYTES, PARALLEL, planTiles, routeMapId, tileOf, type Tile } from './offlineMap';
import { runPool } from './pool';
import type { LatLon } from './types';

// Downloading the map along a route into its own cache in the Cache Storage,
// where the service worker finds it with no signal (sw.ts). The list of what
// is on the phone, and the downloads under way, are in offlineMapStore.ts.

/** One downloaded map, as the list keeps it. */
export interface OfflineMap {
  id: string;
  /** "From → to", as the planner or Drum named the route. */
  name: string;
  savedAt: number;
  files: number;
  bytes: number;
  /** Less detail at street level than a short route gets (planTiles). */
  reduced: boolean;
  /** Stopped at the size limit: the end of the route has less map. */
  partial: boolean;
}

export type OfflineMapErrorKind = 'unsupported' | 'space' | 'network';

export class OfflineMapError extends Error {
  readonly kind: OfflineMapErrorKind;

  constructor(kind: OfflineMapErrorKind, detail?: string) {
    super(detail ?? kind);
    this.name = 'OfflineMapError';
    this.kind = kind;
  }
}

const STYLE_URLS = ['https://tiles.openfreemap.org/styles/dark', 'https://tiles.openfreemap.org/styles/liberty'];
/** Latin, Latin Extended (ă, â, î), the comma-below letters (ș, ț) and punctuation. */
const FONT_RANGES = ['0-255', '256-511', '512-767', '8192-8447'];
/** The labels of both themes, whatever the app's own style changes pick. */
const FONT_STACKS = ['Noto Sans Regular', 'Noto Sans Bold', 'Noto Sans Italic'];
/** Share of files that may fail (a tile over the sea, a blip) before the download counts as failed. */
const MAX_FAILED_SHARE = 0.05;

export function offlineMapsSupported(): boolean {
  return typeof caches !== 'undefined' && typeof navigator !== 'undefined' && 'serviceWorker' in navigator;
}

export function estimateBytes(line: readonly LatLon[]): { tiles: number; bytes: number; reduced: boolean } {
  const plan = planTiles(line);
  return { tiles: plan.tiles.length, bytes: Math.min(MAX_BYTES, plan.tiles.length * AVERAGE_TILE_BYTES), reduced: plan.reduced };
}

async function fetchJson(url: string, signal: AbortSignal): Promise<unknown> {
  const response = await fetch(url, { signal, credentials: 'omit' });
  if (!response.ok) throw new OfflineMapError('network', `HTTP ${response.status} for ${url}`);
  return response.json();
}

interface MapSources {
  /** The styles, the tile index, fonts and sprites: every file besides the tiles. */
  files: string[];
  vectorTemplate: string;
  /** The shaded relief under the overview zooms, if the style has it. */
  reliefTemplate: string | null;
  reliefMaxZoom: number;
}

/** What the two OpenFreeMap styles need besides the tiles, read from the styles themselves. */
async function mapSources(signal: AbortSignal): Promise<MapSources> {
  const styles = (await Promise.all(STYLE_URLS.map((url) => fetchJson(url, signal)))) as Array<{
    glyphs?: string;
    sprite?: string | Array<{ url: string }>;
    sources?: Record<string, { type?: string; url?: string; tiles?: string[]; maxzoom?: number }>;
  }>;
  const files = new Set<string>(STYLE_URLS);
  let indexUrl: string | null = null;
  let reliefTemplate: string | null = null;
  let reliefMaxZoom = 0;
  for (const style of styles) {
    if (style.glyphs) {
      for (const stack of FONT_STACKS) {
        for (const range of FONT_RANGES) files.add(style.glyphs.replace('{fontstack}', encodeURIComponent(stack)).replace('{range}', range));
      }
    }
    const sprites = typeof style.sprite === 'string' ? [style.sprite] : (style.sprite ?? []).map((s) => s.url);
    for (const base of sprites) for (const suffix of ['.json', '.png', '@2x.json', '@2x.png']) files.add(base + suffix);
    for (const source of Object.values(style.sources ?? {})) {
      if (source.type === 'vector' && source.url) indexUrl = source.url;
      if (source.type === 'raster' && source.tiles?.[0]) {
        reliefTemplate = source.tiles[0];
        reliefMaxZoom = source.maxzoom ?? 6;
      }
    }
  }
  if (!indexUrl) throw new OfflineMapError('network', 'the map style names no vector tiles');
  files.add(indexUrl);
  const index = (await fetchJson(indexUrl, signal)) as { tiles?: string[] };
  const vectorTemplate = index.tiles?.[0];
  if (!vectorTemplate) throw new OfflineMapError('network', 'the tile index names no tiles');
  return { files: [...files], vectorTemplate, reliefTemplate, reliefMaxZoom };
}

function tileUrl(template: string, t: Tile): string {
  return template.replace('{z}', String(t.z)).replace('{x}', String(t.x)).replace('{y}', String(t.y));
}

/** The relief tiles over the route's box, up to the relief's own top zoom: a handful. */
function reliefTiles(line: readonly LatLon[], maxZoom: number): Tile[] {
  const lats = line.map((p) => p.lat);
  const lons = line.map((p) => p.lon);
  const out: Tile[] = [];
  for (let z = 0; z <= maxZoom; z += 1) {
    const nw = tileOf(Math.max(...lats), Math.min(...lons), z);
    const se = tileOf(Math.min(...lats), Math.max(...lons), z);
    for (let x = nw.x; x <= se.x; x += 1) for (let y = nw.y; y <= se.y; y += 1) out.push({ z, x, y });
  }
  return out;
}

/**
 * Download the map along the route. Progress is (files done, files in all).
 * Aborting stops it and removes what it had saved.
 */
export async function downloadRouteMap(
  line: readonly LatLon[],
  name: string,
  onProgress: (done: number, total: number) => void,
  signal: AbortSignal,
): Promise<OfflineMap> {
  if (!offlineMapsSupported()) throw new OfflineMapError('unsupported');
  const plan = planTiles(line);
  const estimate = Math.min(MAX_BYTES, plan.tiles.length * AVERAGE_TILE_BYTES);
  const storage = await navigator.storage?.estimate?.().catch(() => undefined);
  if (storage?.quota !== undefined && storage.quota - (storage.usage ?? 0) < estimate * 1.5) throw new OfflineMapError('space');
  // Ask the browser not to clear it under storage pressure; a refusal changes nothing else.
  void navigator.storage?.persist?.().catch(() => false);

  const id = routeMapId(line);
  const cacheName = OFFLINE_MAP_CACHE_PREFIX + id;
  await caches.delete(cacheName);
  const cache = await caches.open(cacheName);
  try {
    const sources = await mapSources(signal);
    const relief = sources.reliefTemplate ? reliefTiles(line, sources.reliefMaxZoom).map((t) => tileUrl(sources.reliefTemplate!, t)) : [];
    const urls = [...sources.files, ...relief, ...plan.tiles.map((t) => tileUrl(sources.vectorTemplate, t))];
    let done = 0;
    let saved = 0;
    let failed = 0;
    let bytes = 0;
    let partial = false;
    onProgress(0, urls.length);
    await runPool(
      urls,
      PARALLEL,
      async (url) => {
        if (bytes >= MAX_BYTES) {
          partial = true;
        } else {
          try {
            const response = await fetch(url, { signal, credentials: 'omit' });
            // An empty tile (open sea) answers 204 or 404: nothing to keep.
            if (response.ok && response.status !== 204) {
              const size = Number(response.headers.get('content-length')) || (await response.clone().blob()).size;
              await cache.put(offlineKey(url), response);
              bytes += size;
              saved += 1;
            } else if (response.status !== 204 && response.status !== 404) {
              failed += 1;
            }
          } catch (err) {
            if (signal.aborted) throw err;
            failed += 1;
          }
        }
        done += 1;
        onProgress(done, urls.length);
      },
      signal,
    );
    if (signal.aborted) throw new DOMException('The download was stopped.', 'AbortError');
    if (failed > urls.length * MAX_FAILED_SHARE) throw new OfflineMapError('network', `${failed} of ${urls.length} files failed`);
    return { id, name, savedAt: Date.now(), files: saved, bytes, reduced: plan.reduced, partial };
  } catch (err) {
    await caches.delete(cacheName);
    throw err;
  }
}
