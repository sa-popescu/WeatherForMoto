import { describe, expect, it } from 'vitest';
import { findSummit, profileKms, SUMMIT_MIN_RISE_M } from './elevation';
import { SUMMIT_POINT_ID, withSummit } from './samples';
import type { SamplePoint } from './types';

describe('elevation profile', () => {
  it('reads about every kilometre, never more than 400 points', () => {
    const short = profileKms(12.5);
    expect(short).toHaveLength(13);
    expect(short[0]).toBe(0);
    expect(short[short.length - 1]).toBe(12.5);
    expect(profileKms(1200)).toHaveLength(400);
    expect(profileKms(0)).toEqual([]);
  });

  it('finds a pass that stands well above every weather point', () => {
    // A climb from 600 m to 2145 m at km 6, back down to 900 m; weather points at km 0 and 10.
    const kms = [0, 2, 4, 6, 8, 10];
    const heights = [600, 1200, 1800, 2145, 1500, 900];
    expect(findSummit(kms, heights, [0, 10])).toEqual({ km: 6, elevationM: 2145 });
  });

  it('stays quiet when a weather point already sits near the top', () => {
    const kms = [0, 2, 4, 6, 8, 10];
    const heights = [600, 1200, 1800, 2145, 1500, 900];
    expect(findSummit(kms, heights, [0, 5.5, 10])).toBeNull();
    // Just under the threshold above the highest point.
    const gentle = [600, 700, 800, 600 + SUMMIT_MIN_RISE_M - 1, 700, 600];
    expect(findSummit(kms, gentle, [0, 10])).toBeNull();
  });

  it('ignores missing heights and mismatched input', () => {
    expect(findSummit([0, 1, 2], [100, Number.NaN, 120], [0, 2])).toBeNull();
    expect(findSummit([0, 1, 2], [100, Number.NaN, 900], [0])).toEqual({ km: 2, elevationM: 900 });
    expect(findSummit([0, 1], [100], [0])).toBeNull();
    expect(findSummit([0, 1], [100, 900], [])).toBeNull();
  });
});

describe('summit point', () => {
  const points: SamplePoint[] = [
    { id: 'p0', lat: 45.18, lon: 23.68, km: 0, name: 'Novaci', stopIndex: 0 },
    { id: 'p1', lat: 45.75, lon: 23.6, km: 90, name: 'Șugag', stopIndex: 1 },
  ];

  it('slots in along the way with its height in the name', () => {
    const out = withSummit(points, { km: 34, lat: 45.33, lon: 23.62, elevationM: 2145 }, (m) => `Punctul cel mai înalt · ${m} m`);
    expect(out.map((p) => p.id)).toEqual(['p0', SUMMIT_POINT_ID, 'p1']);
    expect(out[1]).toMatchObject({ name: 'Punctul cel mai înalt · 2145 m', km: 34, stopIndex: null });
  });

  it('leaves the points alone when there is no summit', () => {
    expect(withSummit(points, null, () => 'x')).toEqual(points);
  });
});
