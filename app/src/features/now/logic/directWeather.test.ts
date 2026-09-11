import { describe, expect, it } from 'vitest';
import { dailyScore, frostRisk, mapOpenMeteo, roadSurfaceTemp, scoreBreakdown, type OpenMeteoForecast, type ScoreInput } from '../../../lib/directWeather';
import { addMinutesLocal } from '../../../lib/format';
import { hours } from './testFixtures';

// The direct Open-Meteo fallback: local port of the backend scoring rules.

const calm: ScoreInput = { feelsLike: 22, gustsKmh: 15, precipitationMm: 0, weatherCode: 1, probability: 0 };
const score = (over: Partial<ScoreInput>): number | null => scoreBreakdown({ ...calm, ...over }).score;

describe('scoreBreakdown (backend rules)', () => {
  it('gives 100 with nothing to penalize', () => {
    expect(scoreBreakdown(calm)).toEqual({ score: 100, factors: [] });
    expect(scoreBreakdown({ feelsLike: null, gustsKmh: null, precipitationMm: null, weatherCode: null, probability: null }).score).toBeNull();
  });

  it('caps rain by probability x intensity and adds the amount penalty', () => {
    const { score: s, factors } = scoreBreakdown({ ...calm, precipitationMm: 3.2, probability: 80, weatherCode: 63 });
    expect(factors).toEqual([{ factor: 'rain', penalty: 29, cap: 39, detail: expect.any(String) }]);
    expect(s).toBe(39);
    expect(score({ precipitationMm: 0.2, probability: 70, weatherCode: 51 })).toBe(84);
  });

  it('rounds halves to even like Python', () => {
    expect(scoreBreakdown({ ...calm, probability: 5 }).factors).toEqual([]);
    expect(score({ probability: 25 })).toBe(98);
  });

  it('counts storm water once but keeps the storm cap', () => {
    const { score: s, factors } = scoreBreakdown({ ...calm, weatherCode: 95, probability: 90, precipitationMm: 12 });
    expect(factors.map((f) => [f.factor, f.penalty, f.cap])).toEqual([['rain', 44, 39], ['storm', 0, 39]]);
    expect(s).toBe(39);
    expect(score({ weatherCode: 95, probability: 5, precipitationMm: 0 })).toBe(100);
  });

  it('applies gust, temperature, fog and frost rules', () => {
    expect(score({ gustsKmh: 55 })).toBe(85);
    expect(score({ feelsLike: 3 })).toBe(72);
    expect(score({ feelsLike: -2 })).toBe(45);
    expect(score({ feelsLike: 37 })).toBe(90);
    expect(score({ visibilityM: 300 })).toBe(59);
    expect(score({ frostRisk: true })).toBe(59);
    expect(score({ feelsLike: 3, probability: 40, precipitationMm: 0.3 })).toBe(52);
  });
});

describe('road and frost estimates', () => {
  it('warms dry asphalt in the sun and cools it on clear nights', () => {
    expect(roadSurfaceTemp(20, 50, 0, 0, true)).toBe(29);
    expect(roadSurfaceTemp(2, 80, 0, 0, false)).toBe(0.5);
    expect(roadSurfaceTemp(10, 90, 61, 1.2, true)).toBe(9);
  });

  it('needs a cold surface plus moisture for frost', () => {
    expect(frostRisk(2, 0.5, 0, 0, 0)).toBe(true);
    expect(frostRisk(2, 0.5, -5, 0, 0)).toBe(false);
    expect(frostRisk(5, 4, 3, 0, 0)).toBe(false);
    expect(frostRisk(1, 0.8, null, 0.3, 61)).toBe(true);
  });
});

describe('dailyScore', () => {
  it('mixes the worst quarter with the mean and applies a sustained hazard cap only', () => {
    const one = hours('2026-09-12T08:00', 8, (i) => (i === 3 ? { moto_score: 39, precipitation_mm: 3.2, precipitation_probability: 80 } : { moto_score: 100 }));
    // Worst quarter = 2 hours (39, 100) -> 69.5; mean 92.4; mixed 80.9. One wet hour of eight is not sustained.
    expect(dailyScore(one)).toBe(81);
    const two = hours('2026-09-12T08:00', 8, (i) => (i === 3 || i === 4 ? { moto_score: 39, precipitation_mm: 3.2, precipitation_probability: 80 } : { moto_score: 100 }));
    expect(dailyScore(two)).toBe(39);
  });
});

function fixture(): OpenMeteoForecast {
  const time = Array.from({ length: 48 }, (_, i) => addMinutesLocal('2026-09-11T00:00', i * 60));
  const rain = [0.6, 3.2, 2.4];
  const precipitation = time.map((_, i) => (i >= 16 && i <= 18 ? rain[i - 16] : 0));
  const same = (value: number): number[] => time.map(() => value);
  return {
    latitude: 44.43,
    longitude: 26.1,
    timezone: 'Europe/Bucharest',
    utc_offset_seconds: 10_800,
    current: { temperature_2m: 24, apparent_temperature: 24, relative_humidity_2m: 50, precipitation: 0, weather_code: 1, wind_speed_10m: 10, wind_gusts_10m: 20, wind_direction_10m: 200, surface_pressure: 1005, pressure_msl: 1015, visibility: 24_000, is_day: 1 },
    hourly: {
      time,
      temperature_2m: same(22),
      apparent_temperature: same(22),
      precipitation_probability: time.map((_, i) => (i >= 15 && i <= 19 ? 80 : 5)),
      precipitation,
      weather_code: precipitation.map((p) => (p >= 2.5 ? 63 : p > 0 ? 61 : 1)),
      wind_speed_10m: same(10),
      wind_gusts_10m: time.map((_, i) => (i === 12 ? 55 : 20)),
      wind_direction_10m: same(200),
      uv_index: same(3),
      relative_humidity_2m: same(60),
      surface_pressure: same(1005),
      pressure_msl: same(1015),
      dew_point_2m: same(12),
      cloud_cover: same(30),
      visibility: time.map((_, i) => (i === 30 ? 300 : 24_000)),
      is_day: time.map((_, i) => (i % 24 >= 7 && i % 24 < 20 ? 1 : 0)),
    },
    daily: {
      time: ['2026-09-11', '2026-09-12'],
      weather_code: [63, 45],
      temperature_2m_max: [25, 23],
      temperature_2m_min: [15, 14],
      apparent_temperature_max: [25, 23],
      apparent_temperature_min: [15, 14],
      precipitation_sum: [6.2, 0],
      precipitation_probability_max: [80, 5],
      wind_speed_10m_max: [14, 12],
      wind_gusts_10m_max: [55, 20],
      sunrise: ['2026-09-11T06:49', '2026-09-12T06:50'],
      sunset: ['2026-09-11T19:33', '2026-09-12T19:31'],
    },
  };
}

describe('mapOpenMeteo', () => {
  const nowMs = Date.UTC(2026, 8, 11, 11, 30); // 14:30 at UTC+3
  const data = mapOpenMeteo(fixture(), { name: 'București', lat: 44.43, lon: 26.1 }, nowMs);

  it('maps and scores every hour', () => {
    expect(data.city).toBe('București');
    expect(data.hourly).toHaveLength(48);
    expect(data.hourly[17]).toMatchObject({ time: '2026-09-11T17:00', moto_score: 39, moto_label: 'EVITĂ', rain_intensity: 'moderata', is_day: true });
    expect(data.hourly[12].moto_score).toBe(85);
    expect(data.hourly[30]).toMatchObject({ moto_score: 59, is_day: false });
  });

  it('builds the current block with its breakdown', () => {
    expect(data.current).toMatchObject({ moto_score: 100, wind_direction: 'S', beaufort: 2, visibility_km: 24, sources: ['open-meteo'], score_breakdown: [] });
  });

  it('scores today from the daylight hours still ahead, with a sustained rain cap', () => {
    expect(data.daily[0]).toMatchObject({ date: '2026-09-11', moto_score: 59, precipitation_max_mm_h: 3.2, rain_intensity_max: 'moderata', weather_code: 63 });
    expect(data.daily[1]).toMatchObject({ moto_score: 100, sunset: '2026-09-12T19:31' });
  });
});
