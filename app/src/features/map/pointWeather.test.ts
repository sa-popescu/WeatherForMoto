import { describe, expect, it } from 'vitest';
import type { HourlyWeather, WeatherResponse } from '../../lib/types';
import { current, hours } from '../now/logic/testFixtures';
import { hourIndexAt, inViewerClock, rainSentence, summarizePoint } from './pointWeather';

// Bucharest in November: UTC+2. 21 Nov 2026, 10:20 local is 08:20 UTC.
const OFFSET_S = 2 * 3600;
const NOW_MS = Date.parse('2026-11-21T08:20:00Z');
const utcSec = (iso: string): number => Date.parse(iso) / 1000;

function weather(over: (i: number) => Partial<HourlyWeather> = () => ({})): WeatherResponse {
  return {
    city: 'Brașov',
    latitude: 45.65,
    longitude: 25.6,
    timezone: 'Europe/Bucharest',
    utc_offset_seconds: OFFSET_S,
    current: current(),
    hourly: hours('2026-11-21T00:00', 48, (i) => ({ moto_score: 60 + (i % 10), ...over(i) })),
    daily: [],
  };
}

describe('hourIndexAt', () => {
  it('finds the local hour that holds the moment', () => {
    // 13:30 local = 11:30 UTC, the 13:00 entry (index 13).
    expect(hourIndexAt(weather(), utcSec('2026-11-21T11:30:00Z'), NOW_MS)).toBe(13);
  });

  it('shows the current hour for a moment already past', () => {
    expect(hourIndexAt(weather(), utcSec('2026-11-21T06:00:00Z'), NOW_MS)).toBe(10);
  });

  it('gives -1 past the end of the forecast', () => {
    expect(hourIndexAt(weather(), utcSec('2026-11-25T12:00:00Z'), NOW_MS)).toBe(-1);
  });
});

describe('summarizePoint', () => {
  it('reads score, air and gusts at that hour', () => {
    const data = weather((i) => (i === 13 ? { temperature: 8.4, wind_gusts_kmh: 31 } : {}));
    const summary = summarizePoint(data, utcSec('2026-11-21T11:30:00Z'), 'ro', NOW_MS);
    expect(summary).toMatchObject({ hourIso: '2026-11-21T13:00', isNow: false, score: 63, temperature: 8.4, gusts: 31 });
  });

  it('says when the hour shown is now', () => {
    expect(summarizePoint(weather(), NOW_MS / 1000, 'ro', NOW_MS)?.isNow).toBe(true);
  });

  it('is null when the forecast does not reach the moment', () => {
    expect(summarizePoint(weather(), utcSec('2026-11-30T12:00:00Z'), 'ro', NOW_MS)).toBeNull();
  });
});

describe('inViewerClock', () => {
  it('moves the hours to the viewer zone, so the card and the band agree', () => {
    // A computer on UTC+1 looking at Romania (UTC+2): 13:00 there is 12:00 here.
    const shifted = inViewerClock(weather(), 3600);
    expect(shifted.utc_offset_seconds).toBe(3600);
    expect(shifted.hourly[13].time).toBe('2026-11-21T12:00');
    // 11:30 UTC is 12:30 on the band: the card shows the hour that holds it.
    expect(summarizePoint(shifted, utcSec('2026-11-21T11:30:00Z'), 'ro', NOW_MS)?.hourIso).toBe('2026-11-21T12:00');
  });

  it('leaves the forecast alone in the same zone', () => {
    const data = weather();
    expect(inViewerClock(data, OFFSET_S)).toBe(data);
  });
});

describe('rainSentence', () => {
  it('names the dry twelve hours, a few percent included', () => {
    expect(rainSentence(weather().hourly, 10, '2026-11-21T10:20', 'ro')).toBe('Fără ploaie în următoarele 12 ore.');
    const slight = weather((i) => (i === 12 ? { precipitation_probability: 8 } : {}));
    expect(rainSentence(slight.hourly, 10, '2026-11-21T10:20', 'ro')).toBe('Fără ploaie în următoarele 12 ore.');
  });

  it('names a real chance that brings no amount', () => {
    const chance = weather((i) => (i === 12 ? { precipitation_probability: 25 } : {}));
    expect(rainSentence(chance.hourly, 10, '2026-11-21T10:20', 'ro')).toContain('25%');
  });

  it('names when the rain comes and how much', () => {
    const data = weather((i) => (i >= 12 && i <= 14 ? { precipitation_probability: 80, precipitation_mm: 0.6 } : {}));
    const text = rainSentence(data.hourly, 10, '2026-11-21T10:20', 'ro');
    expect(text.startsWith('Ploaie 12:00 – 15:00.')).toBe(true);
    expect(text).toContain('80%');
    expect(text).toContain('0,6 mm/h');
  });

  it('starts with "acum" in lower case when it is already raining', () => {
    const data = weather((i) => (i >= 10 && i <= 11 ? { precipitation_probability: 90, precipitation_mm: 1.2 } : {}));
    expect(rainSentence(data.hourly, 10, '2026-11-21T10:20', 'ro').startsWith('Ploaie acum – 12:00.')).toBe(true);
  });
});
