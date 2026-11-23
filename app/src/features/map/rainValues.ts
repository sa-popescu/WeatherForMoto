import { lonToX, type FieldGeometry } from './mercator';
import type { LevelOf } from './nowcast';
import { bilinearAt, gridColumns, gridRow, type GridBox } from './rainPaint';

// Weather as values, not pictures. Every source (observed radar, the radar
// moved on, the model; the satellite clouds and lightning the same way, see
// cloudValues.ts and lightningValues.ts) becomes a field over the canvas, and
// the map colours it itself, per screen pixel (valueLayer.ts). Pure helpers.
//
// A field has two bytes per pixel. For rain: the strength as a code (0 for
// nothing, else the reflectivity in thirds of a dBZ, see dbzToCode) and how
// surely it is snow (0 to 255). A third of a dBZ is finer than any colour
// step, and one byte keeps the textures small.

export const CHANNELS = 2;

/** A field: CHANNELS bytes per pixel, row by row over a FieldGeometry. */
export type ValueField = Uint8ClampedArray<ArrayBuffer>;

const CODES_PER_DBZ = 3;
/** Code 0 is no rain; code 3 is -10 dBZ, the weakest echo RainViewer draws; 255 is 74 dBZ. */
const DBZ_AT_CODE_ZERO = -11;
const MIN_DBZ = -10;

export function dbzToCode(dbz: number): number {
  if (!Number.isFinite(dbz) || dbz < MIN_DBZ) return 0;
  return Math.min(255, Math.round((dbz - DBZ_AT_CODE_ZERO) * CODES_PER_DBZ));
}

export function codeToDbz(code: number): number {
  return code / CODES_PER_DBZ + DBZ_AT_CODE_ZERO;
}

/** Rain rate from reflectivity (Marshall-Palmer, Z = 200 R^1.6). */
export function dbzToMm(dbz: number): number {
  return (10 ** (dbz / 10) / 200) ** (1 / 1.6);
}

export function mmToDbz(mm: number): number {
  return 10 * Math.log10(200 * mm ** 1.6);
}

/** mm/h for every code; code 0 is dry. */
const MM_OF_CODE = Float32Array.from({ length: 256 }, (_, code) => (code === 0 ? 0 : dbzToMm(codeToDbz(code))));

export function mmToCode(mm: number): number {
  return mm > 0 ? dbzToCode(mmToDbz(mm)) : 0;
}

export function codeToMm(code: number): number {
  return MM_OF_CODE[code];
}

// ---- RainViewer tiles to values ------------------------------------------------

/**
 * RainViewer's Universal Blue colours (RRGGBBAA), one per dBZ from -10 up,
 * from its public colour table: rain to 74 dBZ, then snow from -9 dBZ. Below
 * 15 dBZ the rain colours are a see-through warm grey: drizzle and clutter.
 */
const RAIN_COLOURS =
  '63615914 66635a19 69665c1e 6c685d24 6f6b5f29 726e612e 75706234 78736439 7c75653e 7f786744 827b6949 857d6a4e 88806c54 8b826d59 ' +
  '8e856f5e 92887164 9e93756e aa9e7978 b6a97e82 c2b4828c cec08796 d2c48ba0 d6c88faa dacc93b4 ded097be 88ddeeff 6cd1ebff 51c5e8ff ' +
  '36bae5ff 1baee2ff 00a3e0ff 009ad5ff 0091caff 0088bfff 007fb4ff 0077aaff 0070a3ff 00699cff 006295ff 005b8eff 005588ff 005180ff ' +
  '004e78ff 004a70ff 004768ff ffee00ff ffe000ff ffd200ff ffc500ff ffb700ff ffaa00ff ff9f00ff ff9500ff ff8b00ff ff8100ff ff4400ff ' +
  'f23600ff e62800ff d91b00ff cd0d00ff c10000ff a80000ff 8f0000ff 760000ff 5d0000ff ffaaffff ff9fffff ff95ffff ff8bffff ff81ffff ' +
  'ff77ffff ff6cffff ff62ffff ff58ffff ff4effff ffffffff ffffffff ffffffff ffffffff ffffffff ffffffff ffffffff ffffffff ffffffff ffffffff';
const SNOW_COLOURS =
  'ceffff0c cdffff19 ccffff26 cbffff33 cbffff3f caffff4c c9ffff59 c8ffff66 c7ffff72 c7ffff7f c6ffff8c c5ffff99 c4ffffa5 c3ffffb2 ' +
  'c3ffffbf c2ffffcc c1ffffd8 c0ffffe5 bffffff2 bfffffff b8f8ffff b2f2ffff abebffff a5e5ffff 9fdfffff 98d8ffff 92d2ffff 8bcbffff ' +
  '85c5ffff 7fbfffff 78b8ffff 72b2ffff 6babffff 65a5ffff 5f9fffff 5b9bffff 5898ffff 5595ffff 5292ffff 4f8fffff 4b8bffff 4888ffff ' +
  '4585ffff 4282ffff 3f7fffff 3b7bffff 3878ffff 3575ffff 3272ffff 2f6fffff 2b6bffff 2868ffff 2565ffff 2262ffff 1f5fffff 1b5bffff ' +
  '1858ffff 1555ffff 1252ffff 0f4fffff 0c4bffff 0948ffff 0645ffff 0242ffff 003fffff 003bffff 0038ffff 0035ffff 0032ffff 002fffff ' +
  '002bffff 0028ffff 0025ffff 0022ffff 001fffff 001bffff 0018ffff 0015ffff 0012ffff 000fffff 000cffff 0009ffff 0006ffff 0002ffff';

interface PaletteEntry {
  r: number;
  g: number;
  b: number;
  a: number;
  /** Strength code and snow byte, packed as decodePixels writes them. */
  packed: number;
}

function entries(list: string, firstDbz: number, snow: number): PaletteEntry[] {
  return list.split(' ').map((hex, i) => ({
    r: Number.parseInt(hex.slice(0, 2), 16),
    g: Number.parseInt(hex.slice(2, 4), 16),
    b: Number.parseInt(hex.slice(4, 6), 16),
    a: Number.parseInt(hex.slice(6, 8), 16),
    packed: dbzToCode(firstDbz + i) | (snow << 8),
  }));
}

const PALETTE: readonly PaletteEntry[] = [...entries(RAIN_COLOURS, -10, 0), ...entries(SNOW_COLOURS, -9, 255)];

/**
 * The palette entry closest to a tile colour. A canvas keeps colours
 * premultiplied, so a see-through pixel comes back with its red, green and
 * blue rounded off; those count for less the more see-through it is, while
 * its opacity, which comes back exact, counts in full.
 */
function nearestEntry(r: number, g: number, b: number, a: number): number {
  const trust = (a / 255) ** 2;
  let best = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const e of PALETTE) {
    const distance = ((r - e.r) ** 2 + (g - e.g) ** 2 + (b - e.b) ** 2) * trust + 64 * (a - e.a) ** 2;
    if (distance < bestDistance) {
      bestDistance = distance;
      best = e.packed;
    }
  }
  return best;
}

/** Tile colours already matched; unsmoothed tiles hold only a few hundred. */
const matched = new Map<number, number>();
const MATCHED_KEPT = 65_536;

/** RGBA pixels of RainViewer tiles (unsmoothed, snow shown) into a rain field. */
export function decodePixels(rgba: Uint8ClampedArray, out: ValueField): void {
  const count = Math.min(rgba.length / 4, out.length / CHANNELS);
  for (let p = 0; p < count; p += 1) {
    const i = p * 4;
    const a = rgba[i + 3];
    const o = p * CHANNELS;
    if (a === 0) {
      out[o] = 0;
      out[o + 1] = 0;
      continue;
    }
    const key = ((rgba[i] << 24) | (rgba[i + 1] << 16) | (rgba[i + 2] << 8) | a) >>> 0;
    let packed = matched.get(key);
    if (packed === undefined) {
      packed = nearestEntry(rgba[i], rgba[i + 1], rgba[i + 2], a);
      if (matched.size >= MATCHED_KEPT) matched.clear();
      matched.set(key, packed);
    }
    out[o] = packed & 255;
    out[o + 1] = packed >> 8;
  }
}

/**
 * Blurs one plane of an image in place (bytes `offset`, `offset + stride`, …):
 * `passes` box blurs of `radius` pixels each way, which together come close
 * to a Gaussian. Edges repeat their last pixel.
 */
export function blurPlane(
  data: Uint8ClampedArray,
  width: number,
  height: number,
  stride: number,
  offset: number,
  radius: number,
  passes: number,
): void {
  const size = 2 * radius + 1;
  const line = new Float32Array(Math.max(width, height));
  const run = (start: number, step: number, count: number): void => {
    for (let i = 0; i < count; i += 1) line[i] = data[start + i * step];
    let sum = line[0] * (radius + 1);
    for (let i = 1; i <= radius; i += 1) sum += line[Math.min(count - 1, i)];
    for (let i = 0; i < count; i += 1) {
      data[start + i * step] = sum / size;
      sum += line[Math.min(count - 1, i + radius + 1)] - line[Math.max(0, i - radius)];
    }
  };
  for (let pass = 0; pass < passes; pass += 1) {
    for (let y = 0; y < height; y += 1) run(y * width * stride + offset, stride, width);
    for (let x = 0; x < width; x += 1) run(x * stride + offset, width * stride, height);
  }
}

/** Blurs one channel of a field in place (see blurPlane). */
export function blurChannel(field: ValueField, width: number, height: number, channel: number, radius: number, passes: number): void {
  blurPlane(field, width, height, CHANNELS, channel, radius, passes);
}

/**
 * Smooths a radar field read from tiles, in place. The strength gets a light
 * blur (about a kilometre), or radar speckle and the coarse cells of some
 * neighbouring radars would show once drawn. The snow mark gets a wide one:
 * RainViewer marks snow on a coarse grid, and its steps would show where snow
 * turns to rain.
 */
export function smoothRadarField(field: ValueField, width: number, height: number): void {
  blurChannel(field, width, height, 0, 1, 2);
  blurChannel(field, width, height, 1, 3, 2);
}

// ---- Motion -------------------------------------------------------------------

/** Echo strength for motion, as nowcast.ts measures it: dBZ above 10 from 15 dBZ up, else nothing. */
const LEVEL_OF_CODE = Float32Array.from({ length: 256 }, (_, code) => {
  const dbz = codeToDbz(code);
  return code > 0 && dbz >= 15 ? dbz - 10 : 0;
});

/** A field laid out as RGBA (strength in red, snow in green), the form motion is measured on. */
export function fieldToRgba(field: Uint8ClampedArray): Uint8ClampedArray<ArrayBuffer> {
  const count = field.length / CHANNELS;
  const rgba = new Uint8ClampedArray(count * 4);
  for (let p = 0; p < count; p += 1) {
    const code = field[p * CHANNELS];
    rgba[p * 4] = code;
    rgba[p * 4 + 1] = field[p * CHANNELS + 1];
    rgba[p * 4 + 3] = code > 0 ? 255 : 0;
  }
  return rgba;
}

/** Motion level of a field in RGBA form (see fieldToRgba). */
export const codeLevel: LevelOf = (r, _g, _b, a) => (a === 0 ? 0 : LEVEL_OF_CODE[r]);

// ---- The model ------------------------------------------------------------------

/** How many canvas pixels one cell of the model grid spans, west to east. */
function cellPixels(grid: GridBox, geometry: FieldGeometry): number {
  return (lonToX(grid.east, geometry.z) - lonToX(grid.west, geometry.z)) / grid.cols;
}

/**
 * A model field (values on its own latitude-longitude grid) over the canvas:
 * each pixel read between the model's cells, then turned into the field's
 * first byte by `toByte`. The second byte stays 0. Read straight between
 * cells, a coarse grid leaves corners and straight edges where its cells
 * meet, so the field is then blurred over about a third of a cell.
 */
export function gridToField(grid: GridBox, values: Float32Array, geometry: FieldGeometry, toByte: (value: number) => number): ValueField {
  const field: ValueField = new Uint8ClampedArray(geometry.width * geometry.height * CHANNELS);
  gridIntoField(field, 0, grid, values, geometry, toByte);
  return field;
}

/** Like gridToField, into one byte of a field there already is (the second: the share of snow). */
export function gridIntoField(
  field: ValueField,
  channel: number,
  grid: GridBox,
  values: Float32Array,
  geometry: FieldGeometry,
  toByte: (value: number) => number,
): void {
  const { width, height } = geometry;
  const us = gridColumns(grid, geometry, width, 0.5);
  for (let y = 0; y < height; y += 1) {
    const v = gridRow(grid, geometry, y, 0.5);
    if (v < -0.5 || v > grid.rows - 0.5) continue;
    for (let x = 0; x < width; x += 1) {
      const value = bilinearAt(values, grid.cols, grid.rows, us[x], v);
      if (!Number.isNaN(value)) field[(y * width + x) * CHANNELS + channel] = toByte(value);
    }
  }
  const radius = Math.round(cellPixels(grid, geometry) / 3);
  if (radius >= 1) blurPlane(field, width, height, CHANNELS, channel, radius, 2);
}

/** Model rain (mm/h) as a field; the model has no snow here. */
export function modelField(grid: GridBox, values: Float32Array, geometry: FieldGeometry): ValueField {
  return gridToField(grid, values, geometry, mmToCode);
}

/** The model showing through a field by `weight` (0 keeps the field, 1 is all model), both bytes mixed straight, in place. */
export function blendLinear(field: ValueField, model: Uint8ClampedArray, weight: number): void {
  const keep = 1 - weight;
  for (let i = 0; i < field.length; i += 1) field[i] = field[i] * keep + model[i] * weight;
}

/**
 * The model showing through a field by `weight` (0 keeps the field, 1 is all
 * model), in place. Mixed as rain amounts, so rain fading out gets lighter on
 * its way, as it would.
 */
export function blendFields(field: ValueField, model: Uint8ClampedArray, weight: number): void {
  const keep = 1 - weight;
  for (let i = 0; i < field.length; i += CHANNELS) {
    const a = field[i];
    const b = model[i];
    if (a === 0 && b === 0) continue;
    field[i] = mmToCode(MM_OF_CODE[a] * keep + MM_OF_CODE[b] * weight);
    field[i + 1] = field[i + 1] * keep + model[i + 1] * weight;
  }
}
