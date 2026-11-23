import { describe, expect, it } from 'vitest';
import { latToY, lonToX, type FieldGeometry } from './mercator';
import { bilinearAt, mixValues } from './rainPaint';
import { CHANNELS, gridToField } from './rainValues';

/** A canvas over exactly 25-27 E, 44-46 N at zoom 6. */
function geometryOver(): FieldGeometry {
  const z = 6;
  const x0 = Math.floor(lonToX(25, z));
  const y0 = Math.floor(latToY(46, z));
  const x1 = Math.ceil(lonToX(27, z));
  const y1 = Math.ceil(latToY(44, z));
  return { z, x0, y0, width: x1 - x0, height: y1 - y0, bounds: { south: 44, west: 25, north: 46, east: 27 } };
}

describe('bilinearAt', () => {
  const values = Float32Array.from([0, 10, 20, Number.NaN]);

  it('reads between cell centres, and a missing cell leaves the others to decide', () => {
    expect(bilinearAt(values, 2, 2, 0, 0)).toBe(0);
    expect(bilinearAt(values, 2, 2, 0.5, 0)).toBe(5);
    expect(bilinearAt(values, 2, 2, 0, 0.5)).toBe(10);
    // Near the missing cell: only its three neighbours count, in their own proportions.
    expect(bilinearAt(values, 2, 2, 0.75, 0.75)).toBeCloseTo((0.1875 * 10 + 0.1875 * 20) / 0.4375, 6);
  });

  it('has nothing off the grid', () => {
    expect(bilinearAt(values, 2, 2, -0.6, 0)).toBeNaN();
    expect(bilinearAt(values, 2, 2, 0, 1.6)).toBeNaN();
  });
});

describe('a model field on the canvas', () => {
  it('places rows by latitude, not by pixel share', () => {
    // A value in the whole northern row: the edge sits near 45 N, which is not the canvas middle in Mercator.
    const grid = { west: 25, south: 44, east: 27, north: 46, cols: 2, rows: 2 };
    const g = geometryOver();
    const field = gridToField(grid, Float32Array.from([5, 5, 0, 0]), g, (v) => (v > 0.1 ? 200 : 0));
    const column = Math.floor(g.width / 2);
    // The canvas starts on a whole pixel just north of the grid, so the scan starts at the value.
    let edge = -1;
    let seen = false;
    for (let y = 0; y < g.height; y += 1) {
      // Halfway down the value: the softened edge stays centred on where it was.
      if (field[(y * g.width + column) * CHANNELS] > 100) seen = true;
      else if (seen) {
        edge = y;
        break;
      }
    }
    // Interpolation moves the edge south of the cell centre line by design; it must sit
    // within the second degree band, measured in latitude.
    expect(edge).toBeGreaterThan(Math.floor(latToY(45.3, g.z) - g.y0));
    expect(edge).toBeLessThan(Math.ceil(latToY(44.4, g.z) - g.y0));
  });
});

describe('mixValues', () => {
  it('mixes hourly totals and lets a missing value take the other', () => {
    expect(Array.from(mixValues(Float32Array.from([1, Number.NaN, 4]), Float32Array.from([3, 2, Number.NaN]), 0.5))).toEqual([2, 2, 4]);
  });
});
