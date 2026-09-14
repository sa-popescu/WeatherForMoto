// Web Mercator pixel maths, so a canvas painted here lines up exactly with the
// map's own tiles: world pixels at zoom z, the grid Leaflet and RainViewer use.
// Pure helpers only.

export const TILE_SIZE = 256;
/** RainViewer serves radar tiles up to this zoom; ICON-EU cells are ~7 km, so more adds nothing. */
export const FIELD_MAX_ZOOM = 7;
/** Largest canvas side painted per frame: sharp on a phone, cheap enough to redraw while playing. */
export const FIELD_MAX_PX = 1400;
/** Extra room around the view, as a share of its size, so a small pan stays covered. */
export const FIELD_PAD = 0.2;
/** Never narrower than this: rain moving in has to be on the canvas before it arrives. */
export const FIELD_MIN_SPAN_KM = 600;

const MAX_LAT = 85.05112878;
const KM_PER_DEGREE = 111.32;

export interface Box {
  south: number;
  west: number;
  north: number;
  east: number;
}

/** A canvas aligned to the world pixels of zoom `z`. */
export interface FieldGeometry {
  z: number;
  /** World pixel of the top-left corner. */
  x0: number;
  y0: number;
  width: number;
  height: number;
  /** Geographic corners of exactly those pixels. */
  bounds: Box;
}

const worldSize = (z: number): number => TILE_SIZE * 2 ** z;

export function lonToX(lon: number, z: number): number {
  return ((lon + 180) / 360) * worldSize(z);
}

export function latToY(lat: number, z: number): number {
  const clamped = Math.max(-MAX_LAT, Math.min(MAX_LAT, lat));
  const sin = Math.sin((clamped * Math.PI) / 180);
  return (0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI)) * worldSize(z);
}

export function xToLon(x: number, z: number): number {
  return (x / worldSize(z)) * 360 - 180;
}

export function yToLat(y: number, z: number): number {
  const n = Math.PI - (2 * Math.PI * y) / worldSize(z);
  return (Math.atan(Math.sinh(n)) * 180) / Math.PI;
}

/** The view with its padding, grown to the minimum span around its centre. */
function widen(view: Box): Box {
  const latPad = (view.north - view.south) * FIELD_PAD;
  const lonPad = (view.east - view.west) * FIELD_PAD;
  let south = view.south - latPad;
  let north = view.north + latPad;
  let west = view.west - lonPad;
  let east = view.east + lonPad;
  const midLat = (south + north) / 2;
  const minLat = FIELD_MIN_SPAN_KM / KM_PER_DEGREE;
  const minLon = FIELD_MIN_SPAN_KM / (KM_PER_DEGREE * Math.max(0.2, Math.cos((midLat * Math.PI) / 180)));
  if (north - south < minLat) {
    const centre = (north + south) / 2;
    south = centre - minLat / 2;
    north = centre + minLat / 2;
  }
  if (east - west < minLon) {
    const centre = (east + west) / 2;
    west = centre - minLon / 2;
    east = centre + minLon / 2;
  }
  return { south: Math.max(-MAX_LAT, south), west: Math.max(-180, west), north: Math.min(MAX_LAT, north), east: Math.min(180, east) };
}

/**
 * The canvas for a view: its padded box at the zoom the radar tiles on screen
 * use (never above FIELD_MAX_ZOOM), stepping down a zoom level while the box
 * would be wider or taller than FIELD_MAX_PX.
 */
export function fieldGeometry(view: Box, mapZoom: number): FieldGeometry {
  const box = widen(view);
  let z = Math.max(0, Math.min(FIELD_MAX_ZOOM, Math.floor(mapZoom)));
  const tooBig = (zoom: number): boolean =>
    lonToX(box.east, zoom) - lonToX(box.west, zoom) > FIELD_MAX_PX || latToY(box.south, zoom) - latToY(box.north, zoom) > FIELD_MAX_PX;
  while (z > 0 && tooBig(z)) z -= 1;
  const x0 = Math.floor(lonToX(box.west, z));
  const y0 = Math.floor(latToY(box.north, z));
  const x1 = Math.ceil(lonToX(box.east, z));
  const y1 = Math.ceil(latToY(box.south, z));
  return {
    z,
    x0,
    y0,
    width: x1 - x0,
    height: y1 - y0,
    bounds: { north: yToLat(y0, z), south: yToLat(y1, z), west: xToLon(x0, z), east: xToLon(x1, z) },
  };
}

/** Whether a canvas still serves a view: it covers it and has the zoom the view would pick. */
export function geometryServes(geometry: FieldGeometry, view: Box, mapZoom: number): boolean {
  const b = geometry.bounds;
  const covers = view.south >= b.south && view.north <= b.north && view.west >= b.west && view.east <= b.east;
  return covers && fieldGeometry(view, mapZoom).z === geometry.z;
}

export function geometryKey(geometry: FieldGeometry): string {
  return `${geometry.z}/${geometry.x0}/${geometry.y0}/${geometry.width}x${geometry.height}`;
}

export interface TileSlot {
  /** Tile address at the geometry's zoom. */
  x: number;
  y: number;
  /** Where the tile's top-left corner lands on the canvas. */
  left: number;
  top: number;
}

/** Every tile that touches the canvas, with its position on it. */
export function tilesFor(geometry: FieldGeometry): TileSlot[] {
  const count = 2 ** geometry.z;
  const slots: TileSlot[] = [];
  for (let ty = Math.floor(geometry.y0 / TILE_SIZE); ty * TILE_SIZE < geometry.y0 + geometry.height; ty += 1) {
    if (ty < 0 || ty >= count) continue;
    for (let tx = Math.floor(geometry.x0 / TILE_SIZE); tx * TILE_SIZE < geometry.x0 + geometry.width; tx += 1) {
      slots.push({ x: ((tx % count) + count) % count, y: ty, left: tx * TILE_SIZE - geometry.x0, top: ty * TILE_SIZE - geometry.y0 });
    }
  }
  return slots;
}
