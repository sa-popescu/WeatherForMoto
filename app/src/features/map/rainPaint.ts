import { xToLon, yToLat, type FieldGeometry } from './mercator';

// Reading model fields (rain, cloud cover) on their latitude-longitude grid
// from the Mercator canvas the other weather values sit on (rainValues.ts
// turns them into fields). Pure helpers only.

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
 * The grid's value at column `u`, row `v` (cell centres at whole numbers, row
 * 0 in the north), interpolated between the four cells around it; a missing
 * cell leaves the others to decide. NaN off the grid or with no value near.
 */
export function bilinearAt(values: Float32Array, cols: number, rows: number, u: number, v: number): number {
  if (u < -0.5 || u > cols - 0.5 || v < -0.5 || v > rows - 0.5) return Number.NaN;
  const r0 = Math.max(0, Math.min(rows - 1, Math.floor(v)));
  const r1 = Math.min(rows - 1, r0 + 1);
  const fy = Math.max(0, Math.min(1, v - r0));
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
  return weight > 0 ? total / weight : Number.NaN;
}

/** Grid columns of the canvas's pixel edges (`edge` 0) or centres (`edge` 0.5), left to right. */
export function gridColumns(grid: GridBox, geometry: FieldGeometry, count: number, edge: number): Float32Array {
  const cellLon = (grid.east - grid.west) / grid.cols;
  const us = new Float32Array(count);
  for (let x = 0; x < count; x += 1) us[x] = (xToLon(geometry.x0 + x + edge, geometry.z) - grid.west) / cellLon - 0.5;
  return us;
}

/** Grid row of canvas row `y` (`edge` as in gridColumns). */
export function gridRow(grid: GridBox, geometry: FieldGeometry, y: number, edge: number): number {
  const cellLat = (grid.north - grid.south) / grid.rows;
  return (grid.north - yToLat(geometry.y0 + y + edge, geometry.z)) / cellLat - 0.5;
}
