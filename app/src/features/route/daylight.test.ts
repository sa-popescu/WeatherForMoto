import { describe, expect, it } from 'vitest';
import type { DailyWeather } from '../../lib/types';
import { darkAt, firstDark, sunDays } from './daylight';
import { buildTimeline } from './plan';
import { dayForecast } from './testData';
import type { PointWeather, SamplePoint, SunDay } from './types';

const SUN: SunDay[] = [
  { date: '2026-11-22', sunrise: '2026-11-22T07:21', sunset: '2026-11-22T16:52' },
  { date: '2026-11-23', sunrise: '2026-11-23T07:22', sunset: '2026-11-23T16:51' },
];

describe('riding in the dark', () => {
  it('keeps the sun times of every day', () => {
    const daily = [{ date: '2026-11-22', sunrise: '2026-11-22T07:21', sunset: '2026-11-22T16:52' }] as DailyWeather[];
    expect(sunDays(daily)).toEqual([SUN[0]]);
  });

  it('tells daylight from before sunrise and after sunset', () => {
    expect(darkAt('2026-11-22T07:00', SUN)).toEqual({ kind: 'before-sunrise', sun: '2026-11-22T07:21' });
    expect(darkAt('2026-11-22T07:21', SUN)).toBeNull();
    expect(darkAt('2026-11-22T16:51', SUN)).toBeNull();
    expect(darkAt('2026-11-22T16:52', SUN)).toEqual({ kind: 'after-sunset', sun: '2026-11-22T16:52' });
    expect(darkAt('2026-11-23T06:00', SUN)).toEqual({ kind: 'before-sunrise', sun: '2026-11-23T07:22' });
  });

  it('says nothing when the day or its sun times are missing', () => {
    expect(darkAt('2026-11-24T22:00', SUN)).toBeNull();
    expect(darkAt('2026-11-22T22:00', [{ date: '2026-11-22', sunrise: null, sunset: null }])).toBeNull();
  });

  it('finds the first point reached after sunset', () => {
    // 300 km at 60 km/h from 13:00: Pitești at 15:00 in daylight, Sibiu at 18:00 after sunset.
    const points: SamplePoint[] = [
      { id: 'a', lat: 44.43, lon: 26.1, km: 0, name: 'București', stopIndex: 0 },
      { id: 'b', lat: 44.86, lon: 24.87, km: 120, name: 'Pitești', stopIndex: 1 },
      { id: 'c', lat: 45.8, lon: 24.13, km: 300, name: 'Sibiu', stopIndex: 2 },
    ];
    const forecast = { ...dayForecast('2026-11-22', 7200, () => 70), sun: SUN };
    const weather: Record<string, PointWeather> = Object.fromEntries(points.map((p) => [p.id, { status: 'ready', forecast }]));
    const rows = buildTimeline(points, weather, { date: '2026-11-22', time: '13:00' }, 60, 7200);
    expect(rows.map((r) => r.dark?.kind ?? null)).toEqual([null, null, 'after-sunset']);
    expect(firstDark(rows)).toMatchObject({ point: { name: 'Sibiu' }, etaLocal: '2026-11-22T18:00' });

    const early = buildTimeline(points, weather, { date: '2026-11-22', time: '06:00' }, 60, 7200);
    expect(firstDark(early)).toMatchObject({ point: { name: 'București' }, dark: { kind: 'before-sunrise' } });
    expect(firstDark(buildTimeline(points.slice(0, 2), weather, { date: '2026-11-22', time: '09:00' }, 60, 7200))).toBeNull();
  });
});
