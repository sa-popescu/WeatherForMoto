import { describe, expect, it } from 'vitest';
import { day, hours } from './testFixtures';
import { buildTimeline, hourReadout, peakRain } from './timeline';

const NOW = '2026-11-19T16:00';
const DAYS = [day('2026-11-19', { sunrise: '2026-11-19T07:15', sunset: '2026-11-19T16:45' }), day('2026-11-20', { sunrise: '2026-11-20T07:16', sunset: '2026-11-20T16:44' })];

/** 24 hours from 16:00: dry now, a small chance at 17:00, a real one at 18:00, rain from 19:00 to 21:00. */
const bars = buildTimeline(
  hours(NOW, 24, (i) => {
    if (i === 0) return { temperature: 11.4, feels_like: 8.6, wind_gusts_kmh: 22.4, precipitation_probability: 5, moto_score: 78 };
    if (i === 1) return { precipitation_probability: 15 };
    if (i === 2) return { precipitation_probability: 45, moto_score: 66 };
    if (i === 3) return { precipitation_mm: 0.3, precipitation_probability: 55, moto_score: 52 };
    if (i === 4) return { precipitation_mm: 1.2, precipitation_probability: 80, wind_gusts_kmh: 41, moto_score: 38 };
    if (i === 5) return { precipitation_mm: 2.6, precipitation_probability: null, moto_score: 24 };
    return {};
  }),
  0,
  DAYS,
);

describe('hourReadout', () => {
  it('says what the current hour is doing without a tap', () => {
    const now = hourReadout(bars[0], true, NOW, 'ro');
    expect(now.when).toBe('Acum · 16:00');
    expect(now.score).toBe(78);
    expect(now.temp).toBe('11°');
    expect(now.feels).toBe('resimțit 9°');
    expect(now.gust).toBe('22');
    expect(now.gustWord).toBe('din E, moderate');
    expect(now.road).toEqual({ state: 'dry', temp: '28°', word: 'uscat' });
  });

  it('names the rain first and gives its numbers after', () => {
    const wet = hourReadout(bars[4], false, NOW, 'ro');
    expect(wet.when).toBe('20:00');
    expect(wet.rain).toEqual({ state: 'wet', value: 'Slabă', sub: '1,2 mm/h · 80% șanse' });
    expect(wet.road).toMatchObject({ state: 'wet', word: 'ud' });
    expect(wet.gustWord).toBe('din E, puternice');
    expect(hourReadout(bars[3], false, NOW, 'ro').rain).toEqual({ state: 'wet', value: 'Urme', sub: '0,3 mm/h · 55% șanse' });
  });

  it('shows the amount alone when the chance is unknown', () => {
    expect(hourReadout(bars[5], false, NOW, 'ro').rain).toEqual({ state: 'wet', value: 'Moderată', sub: '2,6 mm/h' });
  });

  it('tells a real chance from a small one, and keeps quiet about noise', () => {
    expect(hourReadout(bars[2], false, NOW, 'ro').rain).toEqual({ state: 'possible', value: 'Posibil', sub: '45% șanse, cantitate neglijabilă' });
    expect(hourReadout(bars[1], false, NOW, 'ro').rain).toEqual({ state: 'unlikely', value: 'Puțin probabil', sub: '15% șanse' });
    // 5% is not worth a number: it reads as plain "no rain".
    expect(hourReadout(bars[0], true, NOW, 'ro').rain).toEqual({ state: 'none', value: 'Nu plouă', sub: '' });
    expect(hourReadout(bars[0], true, NOW, 'en').rain).toEqual({ state: 'none', value: 'No rain', sub: '' });
  });

  it('warns about ice on the road before anything else', () => {
    const icy = buildTimeline(hours(NOW, 1, () => ({ frost_risk: true, precipitation_mm: 0.4, road_surface_temp: -1 })), 0, DAYS)[0];
    expect(hourReadout(icy, true, NOW, 'ro').road).toEqual({ state: 'frost', temp: '-1°', word: 'risc de polei' });
  });

  it('marks the hours of the next day', () => {
    expect(hourReadout(bars[10], false, NOW, 'ro').when).toBe('mâine 02:00');
    expect(hourReadout(bars[10], false, NOW, 'en').when).toBe('tomorrow 02:00');
  });

  it('reads as one sentence for a screen reader', () => {
    expect(hourReadout(bars[0], true, NOW, 'ro').sentence).toBe('Acum · 16:00: scor 78 OK, 11°, fără ploaie, rafale 22 km/h, asfalt 28°, uscat');
    expect(hourReadout(bars[4], false, NOW, 'en').sentence).toBe(
      '20:00: score 38 AVOID, 22°, 80% chance, up to 1.2 mm/h, light rain, gusts 41 km/h, asphalt 28°, wet',
    );
  });

  it('survives an hour with nothing in it', () => {
    const empty = buildTimeline(
      hours(NOW, 1, () => ({
        temperature: null,
        feels_like: null,
        wind_gusts_kmh: null,
        wind_direction_10m: null,
        road_surface_temp: null,
        precipitation_mm: null,
        precipitation_probability: null,
        moto_score: null,
      })),
      0,
      DAYS,
    )[0];
    const readout = hourReadout(empty, true, NOW, 'ro');
    expect(readout.score).toBeNull();
    expect(readout.temp).toBe('–');
    expect(readout.gust).toBe('–');
    expect(readout.road).toEqual({ state: 'dry', temp: '–', word: 'uscat' });
    expect(readout.rain).toEqual({ state: 'none', value: 'Nu plouă', sub: '' });
  });
});

describe('peakRain', () => {
  it('finds the wettest hour', () => {
    expect(peakRain(bars)?.time).toBe('2026-11-19T21:00');
  });

  it('is null on a dry day, traces below the measurable floor included', () => {
    const dry = buildTimeline(hours(NOW, 24, (i) => (i === 3 ? { precipitation_mm: 0.04 } : {})), 0, DAYS);
    expect(peakRain(dry)).toBeNull();
  });
});
