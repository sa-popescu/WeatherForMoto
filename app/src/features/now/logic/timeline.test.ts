import { describe, expect, it } from 'vitest';
import { day, hours } from './testFixtures';
import { buildTimeline, hourReadout, peakRain } from './timeline';

const NOW = '2026-11-19T16:00';
const DAYS = [day('2026-11-19', { sunrise: '2026-11-19T07:15', sunset: '2026-11-19T16:45' }), day('2026-11-20', { sunrise: '2026-11-20T07:16', sunset: '2026-11-20T16:44' })];

/** 24 hours from 16:00: dry now, a chance at 18:00, rain from 19:00 to 21:00. */
const bars = buildTimeline(
  hours(NOW, 24, (i) => {
    if (i === 0) return { temperature: 11.4, feels_like: 8.6, wind_gusts_kmh: 22.4, precipitation_probability: 5, moto_score: 78 };
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
    expect(now.rain).toEqual({ state: 'dry', value: 'Uscat', sub: 'șanse 5%' });
    expect(now.gust).toBe('22');
    expect(now.gustWord).toBe('moderate');
  });

  it('gives a rain hour its chance, its amount and its band together', () => {
    const wet = hourReadout(bars[4], false, NOW, 'ro');
    expect(wet.when).toBe('20:00');
    expect(wet.rain).toEqual({ state: 'wet', value: '80%', sub: '1,2 mm/h · slabă' });
    expect(wet.gustWord).toBe('puternice');
  });

  it('does not call an hour dry when rain is a real possibility', () => {
    expect(hourReadout(bars[2], false, NOW, 'ro').rain).toEqual({ state: 'possible', value: 'Posibil', sub: 'șanse 45%' });
  });

  it('shows the amount when the chance is unknown', () => {
    expect(hourReadout(bars[5], false, NOW, 'ro').rain).toEqual({ state: 'wet', value: '2,6 mm/h', sub: 'moderată' });
  });

  it('marks the hours of the next day', () => {
    expect(hourReadout(bars[10], false, NOW, 'ro').when).toBe('mâine 02:00');
    expect(hourReadout(bars[10], false, NOW, 'en').when).toBe('tomorrow 02:00');
  });

  it('reads as one sentence for a screen reader', () => {
    expect(hourReadout(bars[0], true, NOW, 'ro').sentence).toBe('Acum · 16:00: scor 78 OK, 11°, fără ploaie, rafale 22 km/h');
    expect(hourReadout(bars[4], false, NOW, 'en').sentence).toBe('20:00: score 38 AVOID, 22°, 80% chance, up to 1.2 mm/h, light rain, gusts 41 km/h');
  });

  it('survives an hour with nothing in it', () => {
    const empty = buildTimeline(
      hours(NOW, 1, () => ({ temperature: null, feels_like: null, wind_gusts_kmh: null, precipitation_mm: null, precipitation_probability: null, moto_score: null })),
      0,
      DAYS,
    )[0];
    const readout = hourReadout(empty, true, NOW, 'ro');
    expect(readout.score).toBeNull();
    expect(readout.temp).toBe('–');
    expect(readout.gust).toBe('–');
    expect(readout.rain).toEqual({ state: 'dry', value: 'Uscat', sub: '' });
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
