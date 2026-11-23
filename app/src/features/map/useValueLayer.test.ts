import { describe, expect, it } from 'vitest';
import { fieldGeometry } from './mercator';
import { flowGap } from './useValueLayer';

const canvas = fieldGeometry({ south: 43.6, west: 20.5, north: 48.2, east: 29.9 }, 7);
const other = fieldGeometry({ south: 44.6, west: 21.5, north: 49.2, east: 30.9 }, 7);
const T = 1_795_000_000;

describe('flowGap', () => {
  it('joins two radar moments on the same canvas along the motion, either way along the band', () => {
    expect(flowGap({ timeSec: T, geometry: canvas }, { timeSec: T + 600, geometry: canvas })).toBe(600);
    expect(flowGap({ timeSec: T + 600, geometry: canvas }, { timeSec: T, geometry: canvas })).toBe(-600);
  });

  it('blends in place across a new canvas, across model hours and with nothing before', () => {
    expect(flowGap({ timeSec: T, geometry: other }, { timeSec: T + 600, geometry: canvas })).toBeNull();
    expect(flowGap({ timeSec: T, geometry: canvas }, { timeSec: T + 3600, geometry: canvas })).toBeNull();
    expect(flowGap(null, { timeSec: T, geometry: canvas })).toBeNull();
  });
});
