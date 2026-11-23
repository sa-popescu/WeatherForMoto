import { rainRgba, type Rgba } from './forecast';
import { xToLon, yToLat, type FieldGeometry } from './mercator';

// Painting model fields (rain, cloud cover) onto the same Mercator canvas as
// the extrapolated radar or satellite picture, and fading one into the other.
// Pure helpers only.

/** A regular latitude-longitude grid: outer cell edges and size, row 0 in the north. */
export interface GridBox {
  west: number;
  south: number;
  east: number;
  north: number;
  cols: number;
  rows: number;
}

/** Cell by cell (1 - weight) * a + weight * b; a missing value takes the other one. */
export function mixValues(a: Float32Array, b: Float32Array, weight: number): Float32Array {
  const out = new Float32Array(a.length);
  for (let i = 0; i < a.length; i += 1) {
    const va = a[i];
    const vb = b[i];
    if (Number.isNaN(va)) out[i] = vb;
    else if (Number.isNaN(vb)) out[i] = va;
    else out[i] = va * (1 - weight) + vb * weight;
  }
  return out;
}

/**
 * The colours of one row of pixel corners: each corner placed back on the
 * model's latitude-longitude grid (Mercator rows are not evenly spaced in
 * latitude), the value interpolated there and only then coloured. Null where
 * the grid has no value.
 */
function cornerRow(
  out: Array<Rgba | null>,
  grid: GridBox,
  values: Float32Array,
  us: Float32Array,
  v: number,
  color: (value: number) => Rgba,
): void {
  const { cols, rows } = grid;
  out.fill(null);
  if (v < -0.5 || v > rows - 0.5) return;
  const r0 = Math.max(0, Math.min(rows - 1, Math.floor(v)));
  const r1 = Math.min(rows - 1, r0 + 1);
  const fy = Math.max(0, Math.min(1, v - r0));
  for (let x = 0; x < us.length; x += 1) {
    const u = us[x];
    if (u < -0.5 || u > cols - 0.5) continue;
    const c0 = Math.max(0, Math.min(cols - 1, Math.floor(u)));
    const c1 = Math.min(cols - 1, c0 + 1);
    const fx = Math.max(0, Math.min(1, u - c0));
    // Written out rather than looped over an array: this runs for every pixel of every frame.
    let total = 0;
    let weight = 0;
    const v00 = values[r0 * cols + c0];
    const v10 = values[r0 * cols + c1];
    const v01 = values[r1 * cols + c0];
    const v11 = values[r1 * cols + c1];
    const w00 = (1 - fx) * (1 - fy);
    const w10 = fx * (1 - fy);
    const w01 = (1 - fx) * fy;
    const w11 = fx * fy;
    if (w00 > 0 && !Number.isNaN(v00)) {
      total += v00 * w00;
      weight += w00;
    }
    if (w10 > 0 && !Number.isNaN(v10)) {
      total += v10 * w10;
      weight += w10;
    }
    if (w01 > 0 && !Number.isNaN(v01)) {
      total += v01 * w01;
      weight += w01;
    }
    if (w11 > 0 && !Number.isNaN(v11)) {
      total += v11 * w11;
      weight += w11;
    }
    if (weight > 0) out[x] = color(total / weight);
  }
}

/**
 * A field on the canvas. Values are interpolated and only then coloured, so
 * colour bands keep their own colours instead of melting into a halo. Each
 * pixel takes the colours of its four corners: inside a band they agree and
 * the pixel is that colour; on the edge between bands it gets their blend,
 * which smooths the edge over exactly one pixel instead of a staircase.
 * The colour function hands out the same array for the same band, which is
 * how corners are told apart cheaply.
 */
export function paintModelField(
  grid: GridBox,
  values: Float32Array,
  geometry: FieldGeometry,
  color: (value: number) => Rgba,
): Uint8ClampedArray<ArrayBuffer> {
  const { width, height, x0, y0, z } = geometry;
  const pixels = new Uint8ClampedArray(width * height * 4);
  const cellLon = (grid.east - grid.west) / grid.cols;
  const cellLat = (grid.north - grid.south) / grid.rows;
  const us = new Float32Array(width + 1);
  for (let x = 0; x <= width; x += 1) us[x] = (xToLon(x0 + x, z) - grid.west) / cellLon - 0.5;
  const rowV = (y: number): number => (grid.north - yToLat(y0 + y, z)) / cellLat - 0.5;

  let above: Array<Rgba | null> = Array.from({ length: width + 1 }, () => null);
  let below: Array<Rgba | null> = Array.from({ length: width + 1 }, () => null);
  cornerRow(above, grid, values, us, rowV(0), color);
  for (let y = 0; y < height; y += 1) {
    cornerRow(below, grid, values, us, rowV(y + 1), color);
    for (let x = 0; x < width; x += 1) {
      const a = above[x];
      const b = above[x + 1];
      const c = below[x];
      const d = below[x + 1];
      const i = (y * width + x) * 4;
      if (a === b && a === c && a === d) {
        if (a === null || a[3] === 0) continue;
        pixels[i] = a[0];
        pixels[i + 1] = a[1];
        pixels[i + 2] = a[2];
        pixels[i + 3] = a[3];
        continue;
      }
      // An edge: the four corners blended. Colours count in proportion to their opacity, so a
      // clear corner thins the colour out instead of darkening it. Written out for speed.
      let red = 0;
      let green = 0;
      let blue = 0;
      let alpha = 0;
      if (a !== null) {
        red += a[0] * a[3];
        green += a[1] * a[3];
        blue += a[2] * a[3];
        alpha += a[3];
      }
      if (b !== null) {
        red += b[0] * b[3];
        green += b[1] * b[3];
        blue += b[2] * b[3];
        alpha += b[3];
      }
      if (c !== null) {
        red += c[0] * c[3];
        green += c[1] * c[3];
        blue += c[2] * c[3];
        alpha += c[3];
      }
      if (d !== null) {
        red += d[0] * d[3];
        green += d[1] * d[3];
        blue += d[2] * d[3];
        alpha += d[3];
      }
      if (alpha === 0) continue;
      pixels[i] = red / alpha;
      pixels[i + 1] = green / alpha;
      pixels[i + 2] = blue / alpha;
      pixels[i + 3] = alpha / 4;
    }
    [above, below] = [below, above];
  }
  return pixels;
}

/** Model rain in the radar's colours. */
export function paintModelRain(grid: GridBox, values: Float32Array, geometry: FieldGeometry): Uint8ClampedArray<ArrayBuffer> {
  return paintModelField(grid, values, geometry, rainRgba);
}

/**
 * Fades `top` into `base`, in place: weight 0 keeps base, 1 is all top. Colours
 * are mixed in proportion to their opacity, so rain fading out does not turn
 * dark on its way to transparent.
 */
export function crossFade(base: Uint8ClampedArray, top: Uint8ClampedArray, weight: number): void {
  const keep = 1 - weight;
  for (let i = 0; i < base.length; i += 4) {
    const aBase = base[i + 3] * keep;
    const aTop = top[i + 3] * weight;
    const alpha = aBase + aTop;
    if (alpha < 0.5) {
      base[i + 3] = 0;
      continue;
    }
    base[i] = (base[i] * aBase + top[i] * aTop) / alpha;
    base[i + 1] = (base[i + 1] * aBase + top[i + 1] * aTop) / alpha;
    base[i + 2] = (base[i + 2] * aBase + top[i + 2] * aTop) / alpha;
    base[i + 3] = alpha;
  }
}
