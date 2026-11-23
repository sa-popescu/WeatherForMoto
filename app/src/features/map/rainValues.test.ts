import { describe, expect, it } from 'vitest';
import { latToY, lonToX, type FieldGeometry } from './mercator';
import { extrapolate, type MotionField } from './nowcast';
import {
  blendFields,
  blurChannel,
  CHANNELS,
  codeLevel,
  codeToDbz,
  codeToMm,
  dbzToCode,
  decodePixels,
  fieldToRgba,
  mmToCode,
  modelField,
  smoothRadarField,
  type ValueField,
} from './rainValues';

const field = (codes: number[], snow: number[] = []): ValueField =>
  Uint8ClampedArray.from(codes.flatMap((code, i) => [code, snow[i] ?? 0]));

describe('strength codes', () => {
  it('keeps 0 for no rain and a third of a dBZ per step', () => {
    expect(dbzToCode(-10)).toBe(3);
    expect(dbzToCode(74)).toBe(255);
    expect(dbzToCode(-20)).toBe(0);
    expect(dbzToCode(Number.NaN)).toBe(0);
    expect(codeToDbz(dbzToCode(32.33))).toBeCloseTo(32.33, 0);
  });

  it('turns rain rates into codes and back', () => {
    expect(mmToCode(0)).toBe(0);
    expect(codeToMm(0)).toBe(0);
    for (const mm of [0.3, 1, 4, 20]) expect(codeToMm(mmToCode(mm)) / mm).toBeCloseTo(1, 1);
    // Marshall-Palmer: 30 dBZ is about 2.7 mm/h.
    expect(codeToMm(dbzToCode(30))).toBeCloseTo(2.73, 1);
  });
});

describe('decodePixels', () => {
  it('reads RainViewer colours back into reflectivity and snow', () => {
    const rgba = Uint8ClampedArray.from([
      0x88, 0xdd, 0xee, 0xff, // rain, 15 dBZ
      0xff, 0xee, 0x00, 0xff, // rain, 35 dBZ
      0x9f, 0xdf, 0xff, 0xff, // snow, 15 dBZ
      0, 0, 0, 0, // nothing
    ]);
    const out: ValueField = new Uint8ClampedArray(4 * CHANNELS);
    decodePixels(rgba, out);
    expect([out[0], out[1]]).toEqual([dbzToCode(15), 0]);
    expect([out[2], out[3]]).toEqual([dbzToCode(35), 0]);
    expect([out[4], out[5]]).toEqual([dbzToCode(15), 255]);
    expect([out[6], out[7]]).toEqual([0, 0]);
  });

  it('places a see-through drizzle colour by its opacity, though a canvas rounds its colour off', () => {
    // 0 dBZ is 827b69 at opacity 0x49; premultiplied and back it comes out a little off.
    const out: ValueField = new Uint8ClampedArray(CHANNELS);
    decodePixels(Uint8ClampedArray.from([0x84, 0x7a, 0x6b, 0x49]), out);
    expect(out[0]).toBe(dbzToCode(0));
    expect(out[1]).toBe(0);
  });
});

describe('blurChannel and smoothRadarField', () => {
  const strength = (f: ValueField): number[] => Array.from(f).filter((_, i) => i % CHANNELS === 0);

  it('spreads a lone cell evenly and keeps its total', () => {
    const lone = field(Array.from({ length: 25 }, (_, i) => (i === 12 ? 180 : 0)));
    blurChannel(lone, 5, 5, 0, 1, 1);
    const s = strength(lone);
    expect(s[12]).toBe(20);
    expect(s[6]).toBe(20);
    expect(s[0]).toBe(0);
    expect(s.reduce((a, b) => a + b, 0)).toBe(180);
  });

  it('leaves an even field as it is, and only the channel asked for', () => {
    const even = field(Array(9).fill(90), Array(9).fill(255));
    blurChannel(even, 3, 3, 0, 1, 2);
    expect(strength(even)).toEqual(Array(9).fill(90));
    const snowEdge = field(Array(16).fill(100), [255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0]);
    smoothRadarField(snowEdge, 4, 4);
    const snow = Array.from(snowEdge).filter((_, i) => i % CHANNELS === 1);
    // The snow mark turns into a slope, not a step.
    expect(snow[1]).toBeGreaterThan(snow[2]);
    expect(snow[1]).toBeLessThan(255);
    expect(snow[2]).toBeGreaterThan(0);
    expect(strength(snowEdge)).toEqual(Array(16).fill(100));
  });
});

describe('motion and extrapolation on values', () => {
  it('shows strength in red and rain only from 15 dBZ to the motion', () => {
    const rgba = fieldToRgba(field([0, dbzToCode(10), dbzToCode(30)], [0, 0, 255]));
    expect(Array.from(rgba.slice(8, 12))).toEqual([dbzToCode(30), 255, 0, 255]);
    expect(codeLevel(rgba[0], rgba[1], rgba[2], rgba[3])).toBe(0);
    expect(codeLevel(rgba[4], rgba[5], rgba[6], rgba[7])).toBe(0);
    expect(codeLevel(rgba[8], rgba[9], rgba[10], rgba[11])).toBeCloseTo(20, 0);
  });

  it('moves a field on without mixing its values', () => {
    const width = 8;
    const source = new Uint8ClampedArray(width * width * CHANNELS);
    source[(2 * width + 2) * CHANNELS] = 150;
    source[(2 * width + 2) * CHANNELS + 1] = 255;
    // Everything moves one pixel east per interval.
    const motion: MotionField = { cols: 1, rows: 1, blockPx: width, dx: Float32Array.of(1), dy: Float32Array.of(0) };
    const moved = extrapolate(source, width, width, motion, 2, CHANNELS);
    expect(moved[(2 * width + 4) * CHANNELS]).toBe(150);
    expect(moved[(2 * width + 4) * CHANNELS + 1]).toBe(255);
    expect(moved[(2 * width + 2) * CHANNELS]).toBe(0);
  });
});

describe('the model as values', () => {
  /** A canvas over exactly 25-27 E, 44-46 N at zoom 6. */
  function geometryOver(): FieldGeometry {
    const z = 6;
    const x0 = Math.floor(lonToX(25, z));
    const y0 = Math.floor(latToY(46, z));
    const x1 = Math.ceil(lonToX(27, z));
    const y1 = Math.ceil(latToY(44, z));
    return { z, x0, y0, width: x1 - x0, height: y1 - y0, bounds: { south: 44, west: 25, north: 46, east: 27 } };
  }

  it('reads the model rate under every pixel, with no snow', () => {
    const g = geometryOver();
    const grid = { west: 25, south: 44, east: 27, north: 46, cols: 2, rows: 2 };
    const at = (lon: number, lat: number): number => {
      const x = Math.floor(lonToX(lon, g.z) - g.x0);
      const y = Math.floor(latToY(lat, g.z) - g.y0);
      return (y * g.width + x) * CHANNELS;
    };
    const even = modelField(grid, Float32Array.from([3, 3, 3, 3]), g);
    expect(codeToMm(even[at(26, 45)])).toBeCloseTo(3, 0);
    expect(even[at(26, 45) + 1]).toBe(0);
    // One wet cell: its middle is wet, the far corner of the opposite cell dry.
    const one = modelField(grid, Float32Array.from([3, 0, 0, 0]), g);
    expect(codeToMm(one[at(25.5, 45.5)])).toBeGreaterThan(1);
    expect(one[at(26.9, 44.1)]).toBe(0);
  });

  it('lets the model show through as rain amounts, so fading rain gets lighter', () => {
    const extrapolated = field([mmToCode(4), mmToCode(4), 0], [255, 0, 0]);
    blendFields(extrapolated, field([0, mmToCode(4), mmToCode(2)]), 0.5);
    expect(codeToMm(extrapolated[0])).toBeCloseTo(2, 0);
    expect(extrapolated[1]).toBe(128);
    expect(codeToMm(extrapolated[2])).toBeCloseTo(4, 0);
    expect(codeToMm(extrapolated[4])).toBeCloseTo(1, 0);
  });
});
