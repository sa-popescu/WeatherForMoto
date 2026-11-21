import { describe, expect, it } from 'vitest';
import { needsFill, plausibleRoad } from './gapFill';
import type { TrackGap } from './logic/track';

function gap(straightM: number): TrackGap {
  return { t: 1, from: { lat: 44.4, lon: 26.1 }, to: { lat: 44.41, lon: 26.1 }, straightM };
}

describe('gap fills', () => {
  it('asks for a road only when the straight line could be wrong', () => {
    expect(needsFill(gap(20))).toBe(false);
    expect(needsFill(gap(600))).toBe(true);
    expect(needsFill(gap(400_000))).toBe(false);
  });

  it('takes a road that winds, and refuses one that goes somewhere else', () => {
    expect(plausibleRoad(gap(1000), 1400)).toBe(true);
    // Short gaps get some slack: a block around in the city is still the road.
    expect(plausibleRoad(gap(300), 1600)).toBe(true);
    expect(plausibleRoad(gap(10_000), 45_000)).toBe(false);
    expect(plausibleRoad(gap(1000), 500)).toBe(false);
  });
});
