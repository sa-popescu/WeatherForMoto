import { describe, expect, it } from 'vitest';
import { precipOf, skyOf } from './sky';

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

describe('precipOf', () => {
  const calm = { precipitation_mm: 0, wind_speed_kmh: 0, wind_gusts_kmh: 0, wind_direction_deg: 0 };

  it('falls thicker with a heavier code or more water', () => {
    const light = precipOf({ ...calm, weather_code: 71 }, 'snow').amount;
    expect(precipOf({ ...calm, weather_code: 75 }, 'snow').amount).toBeGreaterThan(light);
    expect(precipOf({ ...calm, weather_code: 71, precipitation_mm: 3 }, 'snow').amount).toBeGreaterThan(light);
    expect(precipOf({ ...calm, weather_code: 65 }, 'rain').amount).toBeGreaterThan(precipOf({ ...calm, weather_code: 61 }, 'rain').amount);
    expect(precipOf({ ...calm, weather_code: 3, precipitation_mm: 8 }, 'rain').amount).toBe(1);
  });

  it('drifts with the wind: from the east to the left, from the west to the right', () => {
    const east = precipOf({ ...calm, weather_code: 73, wind_speed_kmh: 30, wind_direction_deg: 90 }, 'snow');
    const west = precipOf({ ...calm, weather_code: 73, wind_speed_kmh: 30, wind_direction_deg: 270 }, 'snow');
    expect(east.wind).toBeLessThan(-0.5);
    expect(west.wind).toBeGreaterThan(0.5);
    expect(precipOf({ ...calm, weather_code: 73 }, 'snow').wind).toBe(0);
  });

  it('reads gusts above the mean wind', () => {
    expect(precipOf({ ...calm, weather_code: 63, wind_speed_kmh: 15, wind_gusts_kmh: 45 }, 'rain').gust).toBe(1);
  });
});
