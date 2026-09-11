import { describe, expect, it } from 'vitest';
import type { DailyWeather } from '../../../lib/types';
import { addDays, bestDay, pickWeekend, rainLine, rankWeekend, weekendDates, type WeekendDay } from './weekend';

const WORDS = { noRain: 'fără ploaie', bands: { urme: 'urme', slaba: 'slabă', moderata: 'moderată', puternica: 'puternică' } };

function daily(date: string, score: number | null, probability: number | null = 0, band: DailyWeather['rain_intensity_max'] = 'none'): DailyWeather {
  return {
    date, weather_code: null, icon: null, description: null, temp_max: null, temp_min: null, feels_max: null, feels_min: null,
    precipitation_mm: null, precipitation_probability: probability, precipitation_max_mm_h: null, rain_intensity_max: band,
    wind_max_kmh: null, wind_gusts_kmh: null, moto_score: score, moto_label: null, sunrise: null, sunset: null,
  };
}

function day(date: string, score: number | null): WeekendDay {
  return { date, score, probability: 0, band: 'none' };
}

describe('weekendDates', () => {
  // 2026-09-11 is a Friday.
  it('picks the coming Saturday and Sunday on weekdays', () => {
    expect(weekendDates('2026-09-11')).toEqual(['2026-09-12', '2026-09-13']);
    expect(weekendDates('2026-09-07')).toEqual(['2026-09-12', '2026-09-13']);
  });

  it('uses today and tomorrow on Saturday, only today on Sunday', () => {
    expect(weekendDates('2026-09-12')).toEqual(['2026-09-12', '2026-09-13']);
    expect(weekendDates('2026-09-13')).toEqual(['2026-09-13']);
  });

  it('crosses month and year ends', () => {
    expect(weekendDates('2026-12-31')).toEqual(['2027-01-02', '2027-01-03']);
    expect(addDays('2026-02-28', 1)).toBe('2026-03-01');
  });
});

describe('pickWeekend', () => {
  it('reads the weekend rows and tolerates missing days', () => {
    const rows = [daily('2026-09-11', 70), daily('2026-09-12', 88, 40, 'urme')];
    expect(pickWeekend(rows, '2026-09-11')).toEqual([
      { date: '2026-09-12', score: 88, probability: 40, band: 'urme' },
      { date: '2026-09-13', score: null, probability: null, band: 'none' },
    ]);
  });
});

describe('ranking', () => {
  it('finds the best day', () => {
    expect(bestDay([day('a', 60), day('b', 81)])?.date).toBe('b');
    expect(bestDay([day('a', null)])).toBeNull();
  });

  it('orders by best day, then average, then name; no data last', () => {
    const ranked = rankWeekend([
      { place: { name: 'Sibiu', lat: 0, lon: 0 }, days: [day('s', 80), day('d', 40)] },
      { place: { name: 'Brașov', lat: 0, lon: 0 }, days: [day('s', 80), day('d', 70)] },
      { place: { name: 'Cluj', lat: 0, lon: 0 }, days: [day('s', null), day('d', null)] },
      { place: { name: 'Arad', lat: 0, lon: 0 }, days: [day('s', 91), day('d', 20)] },
    ]);
    expect(ranked.map((r) => r.place.name)).toEqual(['Arad', 'Brașov', 'Sibiu', 'Cluj']);
    expect(ranked[3].best).toBeNull();
  });
});

describe('rainLine', () => {
  it('always pairs the chance with an amount word', () => {
    expect(rainLine(40, 'urme', WORDS)).toBe('40% · urme');
    expect(rainLine(85, 'moderata', WORDS)).toBe('85% · moderată');
    expect(rainLine(null, 'slaba', WORDS)).toBe('slabă');
  });

  it('says no rain when nothing measurable is expected', () => {
    expect(rainLine(20, 'none', WORDS)).toBe('fără ploaie');
    expect(rainLine(60, 'none', WORDS)).toBe('fără ploaie');
    expect(rainLine(null, 'none', WORDS)).toBe('fără ploaie');
  });

  it('treats a high chance without amount as traces, like the backend', () => {
    expect(rainLine(70.4, 'none', WORDS)).toBe('70% · urme');
  });
});
