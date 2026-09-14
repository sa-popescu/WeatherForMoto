import { rainRgba } from './forecast';
import type { RainGrid } from './iconEu';
import { xToLon, yToLat, type FieldGeometry } from './mercator';

// Painting model rain onto the same Mercator canvas as the extrapolated radar,
// and fading one into the other. Pure helpers only.

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
 * A rain field on the canvas. Each pixel is placed back on the model's
 * latitude-longitude grid (Mercator rows are not evenly spaced in latitude),
 * the millimetres are interpolated there and only then coloured, so the bands
 * keep the crisp edges of the radar instead of melting into a halo.
 */
export function paintModelRain(grid: RainGrid, values: Float32Array, geometry: FieldGeometry): Uint8ClampedArray {
  const { width, height, x0, y0, z } = geometry;
  const pixels = new Uint8ClampedArray(width * height * 4);
  const { cols, rows } = grid;
  const cellLon = (grid.east - grid.west) / cols;
  const cellLat = (grid.north - grid.south) / rows;
  const us = new Float32Array(width);
  for (let x = 0; x < width; x += 1) us[x] = (xToLon(x0 + x + 0.5, z) - grid.west) / cellLon - 0.5;

  for (let y = 0; y < height; y += 1) {
    const v = (grid.north - yToLat(y0 + y + 0.5, z)) / cellLat - 0.5;
    if (v < -0.5 || v > rows - 0.5) continue;
    const r0 = Math.max(0, Math.min(rows - 1, Math.floor(v)));
    const r1 = Math.min(rows - 1, r0 + 1);
    const fy = Math.max(0, Math.min(1, v - r0));
    for (let x = 0; x < width; x += 1) {
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
      if (weight <= 0) continue;
      const color = rainRgba(total / weight);
      if (color[3] === 0) continue;
      const i = (y * width + x) * 4;
      pixels[i] = color[0];
      pixels[i + 1] = color[1];
      pixels[i + 2] = color[2];
      pixels[i + 3] = color[3];
    }
  }
  return pixels;
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
