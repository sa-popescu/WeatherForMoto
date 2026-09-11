import { describe, expect, it } from 'vitest';
import { ApiError } from '../../lib/api';
import type { Hazard } from '../../lib/types';
import {
  clampSeverity,
  hazardLabelKey,
  hazardTypeOf,
  isDescriptionValid,
  MAX_HAZARD_RADIUS_KM,
  mergeHazards,
  MIN_HAZARD_RADIUS_KM,
  parseApiTime,
  radiusFromBounds,
  relativeSpan,
  reportFailure,
  severityKey,
  severityTone,
} from './hazards';

const NOW = Date.UTC(2026, 8, 11, 12, 0);

function hazard(id: number, overrides: Partial<Hazard> = {}): Hazard {
  return {
    id,
    lat: 44.43,
    lon: 26.1,
    hazard_type: 'gravel',
    severity: 3,
    description: 'pietriș',
    distance_km: 1,
    created_at: '2026-09-11T10:00:00+00:00',
    expires_at: '2026-09-11T16:00:00+00:00',
    ...overrides,
  };
}

describe('hazard type metadata', () => {
  it('maps known types to their labels and anything else to "other"', () => {
    expect(hazardTypeOf('ice')).toBe('ice');
    expect(hazardLabelKey('roadworks')).toBe('typeRoadworks');
    expect(hazardTypeOf('meteorite')).toBe('other');
    expect(hazardLabelKey('')).toBe('typeOther');
  });

  it('turns severity into words and colours', () => {
    expect(severityKey(1)).toBe('sev1');
    expect(severityKey(9)).toBe('sev5');
    expect(clampSeverity(0)).toBe(1);
    expect(clampSeverity(Number.NaN)).toBe(3);
    expect([1, 2, 3, 4, 5].map((level) => severityTone(level))).toEqual(['low', 'low', 'mid', 'high', 'high']);
  });
});

describe('radiusFromBounds', () => {
  it('reaches the corner of the visible map', () => {
    // Half a degree each way from Bucharest: about 68 km to the corner.
    const radius = radiusFromBounds({ lat: 44.43, lon: 26.1 }, { lat: 44.93, lon: 26.6 });
    expect(radius).toBeGreaterThanOrEqual(66);
    expect(radius).toBeLessThanOrEqual(70);
  });

  it('caps at 150 km and never goes under 5 km', () => {
    expect(radiusFromBounds({ lat: 45, lon: 25 }, { lat: 50, lon: 32 })).toBe(MAX_HAZARD_RADIUS_KM);
    expect(radiusFromBounds({ lat: 45, lon: 25 }, { lat: 45.001, lon: 25.001 })).toBe(MIN_HAZARD_RADIUS_KM);
  });
});

describe('mergeHazards', () => {
  it('dedupes by id, lets the newer copy win and drops expired reports', () => {
    const known = new Map([
      [1, hazard(1)],
      [2, hazard(2, { expires_at: '2026-09-11T11:59:00+00:00' })],
    ]);
    const merged = mergeHazards(known, [hazard(1, { description: 'actualizat' }), hazard(3), hazard(3)], NOW);
    expect([...merged.keys()].sort((a, b) => a - b)).toEqual([1, 3]);
    expect(merged.get(1)?.description).toBe('actualizat');
  });

  it('leaves the map it was given untouched', () => {
    const known = new Map([[1, hazard(1)]]);
    mergeHazards(known, [hazard(2)], NOW);
    expect(known.size).toBe(1);
  });
});

describe('time helpers', () => {
  it('reads timestamps with and without an offset as UTC', () => {
    expect(parseApiTime('2026-09-11T10:00:00+00:00')).toBe(Date.UTC(2026, 8, 11, 10));
    expect(parseApiTime('2026-09-11T13:00:00+03:00')).toBe(Date.UTC(2026, 8, 11, 10));
    expect(parseApiTime('2026-09-11T10:00:00')).toBe(Date.UTC(2026, 8, 11, 10));
    expect(Number.isNaN(parseApiTime('garbage'))).toBe(true);
  });

  it('sizes spans in minutes, hours or days', () => {
    expect(relativeSpan(5 * 60_000)).toEqual({ n: 5, unit: 'min' });
    expect(relativeSpan(-59.6 * 60_000)).toEqual({ n: 1, unit: 'h' });
    expect(relativeSpan(26 * 3_600_000)).toEqual({ n: 1, unit: 'd' });
  });
});

describe('report form rules', () => {
  it('needs 3 to 220 characters once trimmed', () => {
    expect(isDescriptionValid('  ab ')).toBe(false);
    expect(isDescriptionValid('abc')).toBe(true);
    expect(isDescriptionValid('x'.repeat(220))).toBe(true);
    expect(isDescriptionValid('x'.repeat(221))).toBe(false);
  });

  it('classifies API failures for the right message', () => {
    expect(reportFailure(new ApiError(401, 'x'))).toBe('auth');
    expect(reportFailure(new ApiError(403, 'x'))).toBe('auth');
    expect(reportFailure(new ApiError(429, 'x'))).toBe('rate');
    expect(reportFailure(new ApiError(422, 'x'))).toBe('invalid');
    expect(reportFailure(new ApiError(0, 'timeout'))).toBe('network');
    expect(reportFailure(new ApiError(500, 'x'))).toBe('other');
    expect(reportFailure(new Error('x'))).toBe('other');
  });
});
