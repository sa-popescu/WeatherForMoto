import { describe, expect, it } from 'vitest';
import type { HourlyWeather } from '../../../lib/types';
import { buildHeadline } from './headline';
import { current, day, hours } from './testFixtures';
import { computeVerdict } from './verdict';

const DAILY = [day('2026-09-11'), day('2026-09-12')];
const scenario = (over: (i: number) => Partial<HourlyWeather>): HourlyWeather[] => hours('2026-09-11T00:00', 48, over);

describe('computeVerdict', () => {
  it('says go when the rest of the daylight stays good', () => {
    const hourly = scenario(() => ({ moto_score: 94 }));
    expect(computeVerdict({ nowScore: 94, hourly, startIndex: 10, daily: DAILY })).toEqual({ kind: 'go', tier: 'ideal', night: false });
  });

  it('says go until the first daylight hour that drops below OK', () => {
    const hourly = scenario((i) => ({ moto_score: i >= 16 && i <= 20 ? 34 : 80 }));
    expect(computeVerdict({ nowScore: 72, hourly, startIndex: 10, daily: DAILY })).toEqual({ kind: 'goUntil', until: '2026-09-11T16:00' });
  });

  it('ignores a drop that only happens after sunset', () => {
    const hourly = scenario((i) => ({ moto_score: i >= 21 ? 30 : 80 }));
    expect(computeVerdict({ nowScore: 80, hourly, startIndex: 10, daily: DAILY }).kind).toBe('go');
  });

  it('says not now, from the first hour that stays good for two hours', () => {
    const hourly = scenario((i) => ({ moto_score: i < 14 ? 30 : i === 14 ? 70 : i === 15 ? 40 : 88 }));
    expect(computeVerdict({ nowScore: 30, hourly, startIndex: 10, daily: DAILY })).toEqual({ kind: 'notNow', from: '2026-09-11T16:00', tomorrow: false });
  });

  it('says not today and points to the best window tomorrow', () => {
    const hourly = scenario((i) => ({ moto_score: i < 24 ? 22 : i >= 34 && i < 40 ? 88 : 70 }));
    expect(computeVerdict({ nowScore: 22, hourly, startIndex: 10, daily: DAILY })).toMatchObject({
      kind: 'notToday',
      tomorrow: { from: '2026-09-12T10:00', strong: true, hours: 6 },
    });
  });

  it('after sunset with a bad score, says not now and names tomorrow morning', () => {
    const hourly = scenario((i) => ({ moto_score: i < 24 ? 30 : 86 }));
    expect(computeVerdict({ nowScore: 30, hourly, startIndex: 21, daily: DAILY })).toEqual({ kind: 'notNow', from: '2026-09-12T07:00', tomorrow: true });
  });

  it('flags a good score at night', () => {
    const hourly = scenario(() => ({ moto_score: 90 }));
    expect(computeVerdict({ nowScore: 90, hourly, startIndex: 22, daily: DAILY })).toEqual({ kind: 'go', tier: 'ideal', night: true });
  });

  it('calls it day right after sunrise, even though the hour slot is flagged night', () => {
    const daily = [day('2026-09-11', { sunrise: '2026-09-11T07:01' }), day('2026-09-12')];
    const hourly = scenario((i) => (i === 7 ? { moto_score: 90, is_day: false } : { moto_score: 90 }));
    expect(computeVerdict({ nowScore: 90, hourly, startIndex: 7, daily, nowLocal: '2026-09-11T07:03' })).toEqual({ kind: 'go', tier: 'ideal', night: false });
    expect(computeVerdict({ nowScore: 90, hourly, startIndex: 7, daily, nowLocal: '2026-09-11T07:00' })).toEqual({ kind: 'go', tier: 'ideal', night: true });
  });

  it('calls it night right after sunset, inside an hour flagged day', () => {
    const hourly = scenario(() => ({ moto_score: 90 }));
    expect(computeVerdict({ nowScore: 90, hourly, startIndex: 19, daily: DAILY, nowLocal: '2026-09-11T19:40' })).toEqual({ kind: 'go', tier: 'ideal', night: true });
  });

  it('is unknown without a score', () => {
    expect(computeVerdict({ nowScore: null, hourly: scenario(() => ({})), startIndex: 10, daily: DAILY }).kind).toBe('unknown');
  });
});

describe('buildHeadline', () => {
  const rainyAfternoon = scenario((i) =>
    i >= 16 && i <= 20 ? { moto_score: 34, precipitation_mm: 3.2, precipitation_probability: 80, weather_code: 63 } : { moto_score: 80 },
  );

  it('names the rain with probability, amount and intensity (RO)', () => {
    const h = buildHeadline({ current: current({ moto_score: 72 }), hourly: rainyAfternoon, startIndex: 10, daily: DAILY, lang: 'ro' });
    expect(h.title).toBe('Da, până la 16:00.');
    expect(h.sub).toBe('De la 16:00: 80% șanse, până la 3,2 mm/h (moderată).');
    expect(h.keyTime).toBe('2026-09-11T16:00');
  });

  it('names the rain with probability, amount and intensity (EN)', () => {
    const h = buildHeadline({ current: current({ moto_score: 72 }), hourly: rainyAfternoon, startIndex: 10, daily: DAILY, lang: 'en' });
    expect(h.title).toBe('Yes, until 16:00.');
    expect(h.sub).toBe('From 16:00: 80% chance, up to 3.2 mm/h (moderate).');
  });

  it('calls a high chance of traces what it is', () => {
    const now = current({
      moto_score: 80,
      precipitation_mm: 0.2,
      precipitation_probability: 70,
      score_breakdown: [{ factor: 'rain', penalty: 10, cap: 84, detail: '' }],
    });
    const h = buildHeadline({ current: now, hourly: scenario(() => ({ moto_score: 80 })), startIndex: 13, daily: DAILY, lang: 'ro' });
    expect(h.title).toBe('Da. Cel mult câteva picături.');
    expect(h.sub).toBe('70% șanse, dar doar urme (0,2 mm/h).');
  });

  it('says not today with the storm and tomorrow’s window', () => {
    const hourly = scenario((i) =>
      i < 24 ? { moto_score: 22, weather_code: 95, precipitation_probability: 90, precipitation_mm: 12, wind_gusts_kmh: 68 } : { moto_score: 84 },
    );
    const now = current({ moto_score: 22, weather_code: 95, precipitation_probability: 90, precipitation_mm: 12, score_breakdown: null });
    const h = buildHeadline({ current: now, hourly, startIndex: 10, daily: DAILY, lang: 'ro' });
    expect(h.title).toBe('Nu azi.');
    expect(h.sub).toBe('Furtună, 90% șanse, până la 12 mm/h (puternică). Mâine de la 07:00 arată bine (84).');
  });

  it('names the coming sunrise from the exact time, not the hour slot', () => {
    const daily = [day('2026-09-11', { sunrise: '2026-09-11T07:01' }), day('2026-09-12')];
    const hourly = scenario(() => ({ moto_score: 95 }));
    const h = buildHeadline({ current: current(), hourly, startIndex: 6, daily, lang: 'ro', nowLocal: '2026-09-11T06:40' });
    expect(h.title).toBe('Da, dar e noapte.');
    expect(h.sub).toBe('E întuneric până la 07:01: vizibilitate redusă, fii văzut și atent la animale.');
  });

  it('describes a calm day by what matters on the bike', () => {
    const h = buildHeadline({ current: current(), hourly: scenario(() => ({})), startIndex: 10, daily: DAILY, lang: 'ro' });
    expect(h.title).toBe('Da. Drum liber.');
    expect(h.sub).toBe('Uscat până la apus (19:33), rafale de cel mult 15 km/h.');
  });
});
