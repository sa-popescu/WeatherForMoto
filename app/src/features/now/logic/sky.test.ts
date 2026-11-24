import { describe, expect, it } from 'vitest';
import { skyOf } from './sky';

const dry = { precipitation_mm: 0, rain_intensity: 'none' as const };

describe('skyOf', () => {
  it('reads the WMO code', () => {
    expect(skyOf({ ...dry, weather_code: 0, is_day: true })).toEqual({ kind: 'clear', night: false });
    expect(skyOf({ ...dry, weather_code: 2, is_day: false })).toEqual({ kind: 'partly', night: true });
    expect(skyOf({ ...dry, weather_code: 3, is_day: true }).kind).toBe('overcast');
    expect(skyOf({ ...dry, weather_code: 45, is_day: true }).kind).toBe('fog');
    expect(skyOf({ ...dry, weather_code: 53, is_day: true }).kind).toBe('drizzle');
    expect(skyOf({ ...dry, weather_code: 81, is_day: true }).kind).toBe('rain');
    expect(skyOf({ ...dry, weather_code: 73, is_day: true }).kind).toBe('snow');
    expect(skyOf({ ...dry, weather_code: 95, is_day: true }).kind).toBe('storm');
  });

  it('shows rain the sources measure under a dry code', () => {
    expect(skyOf({ weather_code: 3, is_day: true, precipitation_mm: 0.4, rain_intensity: 'slaba' }).kind).toBe('drizzle');
    expect(skyOf({ weather_code: 2, is_day: true, precipitation_mm: 2, rain_intensity: 'moderata' }).kind).toBe('rain');
    expect(skyOf({ weather_code: 1, is_day: true, precipitation_mm: 0.07, rain_intensity: 'urme' }).kind).toBe('clear');
  });

  it('falls back to a few clouds without a code, and to day without is_day', () => {
    expect(skyOf({ ...dry, weather_code: null, is_day: null })).toEqual({ kind: 'partly', night: false });
  });
});
