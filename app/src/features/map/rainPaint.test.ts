import { describe, expect, it } from 'vitest';
import { rainRgba } from './forecast';
import type { RainGrid } from './iconEu';
import { latToY, lonToX, type FieldGeometry } from './mercator';
import { crossFade, mixValues, paintModelRain } from './rainPaint';

/** A canvas over exactly 25-27 E, 44-46 N at zoom 6. */
function geometryOver(): FieldGeometry {
  const z = 6;
  const x0 = Math.floor(lonToX(25, z));
  const y0 = Math.floor(latToY(46, z));
  const x1 = Math.ceil(lonToX(27, z));
  const y1 = Math.ceil(latToY(44, z));
  return { z, x0, y0, width: x1 - x0, height: y1 - y0, bounds: { south: 44, west: 25, north: 46, east: 27 } };
}

describe('paintModelRain', () => {
  // 2 x 2 cells of one degree: only the north-west cell has rain.
  const grid: RainGrid = { west: 25, south: 44, east: 27, north: 46, cols: 2, rows: 2, values: Float32Array.from([3, 0, 0, 0]) };

  it('paints the rain where the model has it, in the radar colours, and nothing elsewhere', () => {
    const g = geometryOver();
    const pixels = paintModelRain(grid, grid.values, g);
    const at = (lon: number, lat: number): number[] => {
      const x = Math.floor(lonToX(lon, g.z) - g.x0);
      const y = Math.floor(latToY(lat, g.z) - g.y0);
      const i = (y * g.width + x) * 4;
      return Array.from(pixels.slice(i, i + 4));
    };
    expect(at(25.1, 45.9)).toEqual(rainRgba(3));
    expect(at(26.9, 44.1)).toEqual([0, 0, 0, 0]);
  });

  it('places rows by latitude, not by pixel share', () => {
    // Rain in the whole northern row: the edge sits near 45 N, which is not the canvas middle in Mercator.
    const north: RainGrid = { ...grid, values: Float32Array.from([5, 5, 0, 0]) };
    const g = geometryOver();
    const pixels = paintModelRain(north, north.values, g);
    const column = Math.floor(g.width / 2);
    // The canvas starts on a whole pixel just north of the grid, so the scan starts at the rain.
    let edge = -1;
    let seenRain = false;
    for (let y = 0; y < g.height; y += 1) {
      const alpha = pixels[(y * g.width + column) * 4 + 3];
      if (alpha > 0) seenRain = true;
      else if (seenRain) {
        edge = y;
        break;
      }
    }
    // Interpolation moves the 0.1 mm edge south of the cell centre line by design; it must sit
    // within the second degree band, measured in latitude.
    expect(edge).toBeGreaterThan(Math.floor(latToY(45.3, g.z) - g.y0));
    expect(edge).toBeLessThan(Math.ceil(latToY(44.4, g.z) - g.y0));
  });
});

describe('mixValues and crossFade', () => {
  it('mixes hourly totals and lets a missing value take the other', () => {
    expect(Array.from(mixValues(Float32Array.from([1, Number.NaN, 4]), Float32Array.from([3, 2, Number.NaN]), 0.5))).toEqual([2, 2, 4]);
  });

  it('fades between two frames without darkening colours', () => {
    const rain = Uint8ClampedArray.from([0, 85, 136, 255]);
    const clear = Uint8ClampedArray.from([0, 0, 0, 0]);
    const kept = Uint8ClampedArray.from(rain);
    crossFade(kept, clear, 0);
    expect(Array.from(kept)).toEqual([0, 85, 136, 255]);
    const fading = Uint8ClampedArray.from(rain);
    crossFade(fading, clear, 0.5);
    expect(Array.from(fading)).toEqual([0, 85, 136, 128]);
    const appearing = Uint8ClampedArray.from(clear);
    crossFade(appearing, rain, 1);
    expect(Array.from(appearing)).toEqual([0, 85, 136, 255]);
  });
});
