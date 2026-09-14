import { TILE_SIZE, type FieldGeometry } from './mercator';

// Meteosat Third Generation imagery from EUMETSAT's open map service
// (EUMETView, WMS, free, no key): lightning seen from space every 5 minutes
// and infrared cloud pictures (about 2 km over Romania) every 10 minutes.
// Pure helpers only.

export const EUMETVIEW_WMS = 'https://view.eumetsat.int/geoserver/ows';
export const EUMETSAT_ATTRIBUTION = '<a href="https://view.eumetsat.int/" target="_blank" rel="noopener">EUMETSAT</a>';

export interface SatelliteProduct {
  layer: string;
  style: string;
  /** Time between two images. */
  stepSec: number;
}

/** MTG Lightning Imager, accumulated flash area over the last 5 minutes. */
export const LIGHTNING: SatelliteProduct = { layer: 'mtg_fd:li_afa', style: '', stepSec: 300 };
/** MTG FCI infrared 10.5 µm, high resolution, as grey levels (cold cloud tops bright). */
export const IR_CLOUDS: SatelliteProduct = { layer: 'mtg_fd:ir105_hrfi', style: 'mtg_fd:mtg_fd_ir105_hrfi_grayscale', stepSec: 600 };
/**
 * Meteosat cloud mask (about 4 km over Romania): cloud white, clear land green,
 * clear sea blue. Infrared alone misses low, warm cloud, which over Romania is
 * most of it; the mask says where cloud is, the infrared how high and thick.
 */
export const CLOUD_MASK: SatelliteProduct = { layer: 'msg_fes:clm', style: '', stepSec: 900 };
/** The mask is requested at a third of the canvas size and smoothed up: its own cells are that coarse. */
export const CLOUD_MASK_SCALE = 1 / 3;

/** Colour the lightning layer is drawn in, for the legend. */
export const LIGHTNING_COLOR = '#fef9bd';

/** The layer's own capabilities (a few KB): its time dimension names the newest image. */
export function capabilitiesUrl(product: SatelliteProduct): string {
  const [workspace, name] = product.layer.split(':');
  return `https://view.eumetsat.int/geoserver/${workspace}/${name}/ows?service=WMS&version=1.3.0&request=GetCapabilities`;
}

/** Newest image time in a capabilities document, unix seconds, or null. */
export function latestFromCapabilities(xml: string): number | null {
  const tag = /<Dimension\b[^>]*\bname="time"[^>]*>/.exec(xml)?.[0];
  const value = tag ? /\bdefault="([^"]+)"/.exec(tag)?.[1] : undefined;
  const ms = value ? Date.parse(value) : Number.NaN;
  return Number.isNaN(ms) ? null : Math.round(ms / 1000);
}

/** The image for moment t: the last one at or before t, never newer than the newest there is. */
export function imageTime(product: SatelliteProduct, tSec: number, latestSec: number | null): number {
  const snapped = Math.floor(tSec / product.stepSec) * product.stepSec;
  return latestSec === null ? snapped : Math.min(snapped, latestSec);
}

/** The `count` newest image times, oldest first. */
export function recentTimes(product: SatelliteProduct, latestSec: number, count: number): number[] {
  return Array.from({ length: count }, (_, i) => latestSec - (count - 1 - i) * product.stepSec);
}

export function wmsTime(sec: number): string {
  return new Date(sec * 1000).toISOString().replace('.000Z', 'Z');
}

const HALF_WORLD_M = 20037508.342789244;

/** The canvas in Web Mercator metres (west, south, east, north), for a WMS request in EPSG:3857. */
export function mercatorBbox(geometry: FieldGeometry): string {
  const world = TILE_SIZE * 2 ** geometry.z;
  const mx = (px: number): number => (px / world) * 2 * HALF_WORLD_M - HALF_WORLD_M;
  const my = (py: number): number => HALF_WORLD_M - (py / world) * 2 * HALF_WORLD_M;
  return [mx(geometry.x0), my(geometry.y0 + geometry.height), mx(geometry.x0 + geometry.width), my(geometry.y0)]
    .map((v) => v.toFixed(2))
    .join(',');
}

/** One image of the product covering exactly the canvas, pixel for pixel (or at `scale` of its size). */
export function imageUrl(product: SatelliteProduct, geometry: FieldGeometry, timeSec: number, scale = 1): string {
  const params = new URLSearchParams({
    service: 'WMS',
    version: '1.3.0',
    request: 'GetMap',
    layers: product.layer,
    styles: product.style,
    crs: 'EPSG:3857',
    bbox: mercatorBbox(geometry),
    width: String(Math.max(1, Math.round(geometry.width * scale))),
    height: String(Math.max(1, Math.round(geometry.height * scale))),
    format: 'image/png',
    transparent: 'true',
    time: wmsTime(timeSec),
  });
  return `${EUMETVIEW_WMS}?${params.toString()}`;
}

// ---- Satellite pictures to clouds on the map ------------------------------------

/**
 * Grey levels of the infrared picture, measured against the cloud mask
 * (September 2026): clear ground sits at 20-60, low cloud 40-100, high cold
 * tops up to 180.
 */
const WARM_GREY = 60;
const COLD_GREY = 165;
const SLATE = [148, 163, 184];
const WHITE = [241, 245, 249];
/** A low cloud the infrared barely sees still shows as a veil this opaque. */
const LOW_CLOUD_ALPHA = 90;

/** How high and cold a cloud top is, 0 (warm, low) to 1 (cold, high), with soft ends. */
export function cloudShare(grey: number): number {
  const t = Math.max(0, Math.min(1, (grey - WARM_GREY) / (COLD_GREY - WARM_GREY)));
  return t * t * (3 - 2 * t);
}

/** How surely the mask pixel is cloud, 0 to 1: white is cloud, green land and blue sea are clear. */
export function maskShare(r: number, g: number, b: number, a: number): number {
  return a === 0 ? 0 : Math.min(r, g, b) / 255;
}

/**
 * Clouds over the map, written into `ir` in place. The mask decides where
 * cloud is, including the low cloud infrared misses; the infrared decides how
 * it looks, from a slate veil for low cloud to near white for high tops. A
 * cold top the mask has not caught yet still shows. The opacity carries the
 * cloud amount, which is what the motion is measured on.
 */
export function paintClouds(ir: Uint8ClampedArray, mask: Uint8ClampedArray): void {
  for (let i = 0; i < ir.length; i += 4) {
    const height = ir[i + 3] === 0 ? 0 : cloudShare(ir[i]);
    const presence = Math.max(maskShare(mask[i], mask[i + 1], mask[i + 2], mask[i + 3]), height * height);
    const alpha = presence * (LOW_CLOUD_ALPHA + (230 - LOW_CLOUD_ALPHA) * height);
    if (alpha < 8) {
      ir[i + 3] = 0;
      continue;
    }
    ir[i] = SLATE[0] + (WHITE[0] - SLATE[0]) * height;
    ir[i + 1] = SLATE[1] + (WHITE[1] - SLATE[1]) * height;
    ir[i + 2] = SLATE[2] + (WHITE[2] - SLATE[2]) * height;
    ir[i + 3] = alpha;
  }
}
