import { describe, expect, it } from 'vitest';
import type { Lightning } from '../../../lib/types';
import { compassPoint, lightningTone } from './lightning';

const base: Lightning = {
  count: 0, near_count: 0, nearest_km: null, bearing_deg: null, last_minutes_ago: null,
  radius_km: 30, window_min: 30, observed_until: null, attribution: '© EUMETSAT',
};

describe('lightningTone', () => {
  it('shows nothing without flashes or without the source', () => {
    expect(lightningTone(null)).toBeNull();
    expect(lightningTone(undefined)).toBeNull();
    expect(lightningTone(base)).toBeNull();
  });
  it('is near when one struck within 10 km', () => {
    expect(lightningTone({ ...base, count: 4, near_count: 0, nearest_km: 22 })).toBe('around');
    expect(lightningTone({ ...base, count: 4, near_count: 1, nearest_km: 6 })).toBe('near');
  });
});

describe('compassPoint', () => {
  it('rounds to the nearest of eight points', () => {
    expect(compassPoint(0)).toBe('N');
    expect(compassPoint(22)).toBe('N');
    expect(compassPoint(23)).toBe('NE');
    expect(compassPoint(90)).toBe('E');
    expect(compassPoint(200)).toBe('S');
    expect(compassPoint(338)).toBe('N');
    expect(compassPoint(-90)).toBe('W');
  });
});
