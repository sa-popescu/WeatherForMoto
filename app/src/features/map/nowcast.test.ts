import { describe, expect, it } from 'vitest';
import { fieldGeometry } from './mercator';
import {
  alphaLevel,
  echoLevel,
  estimateMotion,
  extrapolate,
  modelWeight,
  motionOptions,
  NOWCAST_STEP_S,
  regularize,
  type MotionOptions,
  type RadarPicture,
} from './nowcast';

const SIZE = 240;

/** A rain cell in radar colours: red core, yellow ring, blue edge. */
function cellPicture(cx: number, cy: number, timeSec: number): RadarPicture {
  const rgba = new Uint8ClampedArray(SIZE * SIZE * 4);
  for (let y = 0; y < SIZE; y += 1) {
    for (let x = 0; x < SIZE; x += 1) {
      const r = Math.hypot(x - cx, y - cy);
      // An elongated ring pattern, so a shift is never ambiguous.
      const stretched = Math.hypot((x - cx) * 0.8, y - cy);
      const color = r < 9 ? [255, 68, 0] : stretched < 20 ? [255, 238, 0] : r < 32 ? [0, 85, 136] : null;
      if (!color) continue;
      const i = (y * SIZE + x) * 4;
      rgba.set([...color, 255], i);
    }
  }
  return { rgba, timeSec };
}

function redCentre(rgba: Uint8ClampedArray): { x: number; y: number } {
  let sx = 0;
  let sy = 0;
  let n = 0;
  for (let y = 0; y < SIZE; y += 1) {
    for (let x = 0; x < SIZE; x += 1) {
      const i = (y * SIZE + x) * 4;
      if (rgba[i] === 255 && rgba[i + 1] === 68 && rgba[i + 3] === 255) {
        sx += x;
        sy += y;
        n += 1;
      }
    }
  }
  return { x: sx / n, y: sy / n };
}

const OPTIONS: MotionOptions = { factor: 2, blockCells: 12, radiusCells: 6 };

describe('echoLevel', () => {
  it('reads radar colours back as echo strength', () => {
    expect(echoLevel(0x88, 0xdd, 0xee, 255)).toBe(5);
    expect(echoLevel(0xff, 0xee, 0x00, 255)).toBe(25);
    expect(echoLevel(0x87, 0xdc, 0xef, 255)).toBe(5);
    expect(echoLevel(0, 0, 0, 0)).toBe(0);
    // Drizzle and clutter are drawn half transparent: nothing to follow.
    expect(echoLevel(0x82, 0x7b, 0x69, 0x49)).toBe(0);
  });
});

describe('alphaLevel', () => {
  it('follows cloud pictures by their opacity', () => {
    expect(alphaLevel(241, 245, 249, 230)).toBeGreaterThan(alphaLevel(148, 163, 184, 60));
    expect(alphaLevel(0, 0, 0, 0)).toBe(0);
  });
});

describe('modelWeight', () => {
  it('keeps the radar alone for 20 minutes, then fades into the model', () => {
    expect(modelWeight(10 * 60)).toBe(0);
    expect(modelWeight(20 * 60)).toBe(0);
    expect(modelWeight(60 * 60)).toBe(0.5);
    expect(modelWeight(100 * 60)).toBe(1);
  });
});

describe('estimateMotion and extrapolate', () => {
  // The cell moves 6 px right and 4 px up every 10 minutes.
  const pictures = [cellPicture(90, 130, 0), cellPicture(96, 126, NOWCAST_STEP_S), cellPicture(102, 122, 2 * NOWCAST_STEP_S)];
  const motion = estimateMotion(pictures, SIZE, SIZE, OPTIONS);

  it('measures how the rain moves', () => {
    const block = (x: number, y: number): number => Math.floor(y / motion.blockPx) * motion.cols + Math.floor(x / motion.blockPx);
    const at = block(102, 122);
    expect(motion.dx[at]).toBeCloseTo(6, 0);
    expect(motion.dy[at]).toBeCloseTo(-4, 0);
  });

  it('moves the newest picture on along that motion, keeping its colours', () => {
    const later = extrapolate(pictures[2].rgba, SIZE, SIZE, motion, 3);
    const centre = redCentre(later);
    expect(centre.x).toBeGreaterThan(102 + 18 - 3);
    expect(centre.x).toBeLessThan(102 + 18 + 3);
    expect(centre.y).toBeGreaterThan(122 - 12 - 3);
    expect(centre.y).toBeLessThan(122 - 12 + 3);
  });

  it('stands still when the pictures do not move, and without rain', () => {
    const still = estimateMotion([cellPicture(100, 100, 0), cellPicture(100, 100, NOWCAST_STEP_S)], SIZE, SIZE, OPTIONS);
    expect(Math.max(...Array.from(still.dx, Math.abs), ...Array.from(still.dy, Math.abs))).toBeLessThan(0.5);
    const empty = { rgba: new Uint8ClampedArray(SIZE * SIZE * 4), timeSec: 0 };
    const none = estimateMotion([empty, { ...empty, timeSec: NOWCAST_STEP_S }], SIZE, SIZE, OPTIONS);
    expect(Array.from(none.dx).every((v) => v === 0)).toBe(true);
  });
});

describe('regularize', () => {
  it('replaces an outlier and fills blocks that had nothing to match', () => {
    const cols = 5;
    const rows = 5;
    const dx = new Float32Array(cols * rows).fill(2);
    const dy = new Float32Array(cols * rows).fill(-1);
    const valid = new Uint8Array(cols * rows).fill(1);
    dx[12] = 9; // the centre block disagrees with all its neighbours
    for (let i = 0; i < cols; i += 1) valid[i] = 0; // the top row had no rain
    const field = regularize({ cols, rows, dx, dy, valid });
    expect(field.dx[12]).toBeCloseTo(2, 5);
    expect(field.dx[0]).toBeCloseTo(2, 5);
    expect(field.dy[2]).toBeCloseTo(-1, 5);
  });
});

describe('motionOptions', () => {
  it('keeps the working grid near 3.5 km and searches up to 120 km/h', () => {
    const options = motionOptions(fieldGeometry({ south: 41.5, west: 14, north: 52, east: 33 }, 6));
    expect(options.factor).toBe(2);
    expect(options.radiusCells).toBeGreaterThanOrEqual(6);
    expect(options.blockCells).toBeGreaterThanOrEqual(6);
  });
});
