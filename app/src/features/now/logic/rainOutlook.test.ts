import { describe, expect, it } from 'vitest';
import { rainConsequence, rainOutlook, rainPeakLine, rainPeriod } from './rainOutlook';
import { hours } from './testFixtures';

const START = '2026-09-11T10:00';

describe('rainOutlook', () => {
  it('reports a dry day', () => {
    const o = rainOutlook(hours(START, 24), 0);
    expect(o.kind).toBe('dry');
    expect(rainConsequence(o, 'ro')).toBe('Fără ploaie în următoarele 24 de ore.');
  });

  it('keeps a low isolated chance honest', () => {
    const o = rainOutlook(hours(START, 24, (i) => ({ precipitation_probability: i === 5 ? 5 : 0 })), 0);
    expect(o).toMatchObject({ kind: 'dry', peakProbability: 5, peakProbabilityTime: '2026-09-11T15:00' });
    expect(rainConsequence(o, 'en')).toBe('No measurable amount: the 5% is an isolated scenario, the road stays dry.');
  });

  it('ignores a trace the models give no chance to', () => {
    const o = rainOutlook(hours(START, 24, (i) => (i === 3 ? { precipitation_mm: 0.1, precipitation_probability: 0 } : {})), 0);
    expect(o).toMatchObject({ kind: 'dry', peakMm: 0 });
    expect(rainConsequence(o, 'ro')).toBe('Fără ploaie în următoarele 24 de ore.');
  });

  it('finds the next episode with its peaks and total', () => {
    const mm = [0.6, 3.2, 2.4, 1.1, 0.3, 0.1];
    const o = rainOutlook(hours(START, 24, (i) => (i >= 6 && i < 12 ? { precipitation_mm: mm[i - 6], precipitation_probability: 80 } : {})), 0);
    expect(o).toMatchObject({
      kind: 'episode',
      start: '2026-09-11T16:00',
      end: '2026-09-11T22:00',
      startsNow: false,
      peakProbability: 80,
      peakMm: 3.2,
      peakMmTime: '2026-09-11T17:00',
      totalMm: 7.7,
      hours: 6,
      band: 'moderata',
    });
    expect(rainConsequence(o, 'ro')).toBe('Aproximativ 7,7 mm în total, în 6 h. Asfalt ud și vizibilitate redusă: costum de ploaie obligatoriu.');
  });

  it('treats a high chance of traces as traces', () => {
    const o = rainOutlook(hours(START, 24, (i) => (i < 5 ? { precipitation_mm: 0.2, precipitation_probability: 70 } : {})), 0);
    expect(o).toMatchObject({ startsNow: true, band: 'urme', totalMm: 1 });
    expect(rainConsequence(o, 'ro')).toContain('fără costum de ploaie');
  });

  it('bridges one dry hour but not two', () => {
    const wet = new Set([2, 4, 7]);
    const o = rainOutlook(hours(START, 24, (i) => (wet.has(i) ? { precipitation_mm: 1, precipitation_probability: 60 } : {})), 0);
    expect(o.start).toBe('2026-09-11T12:00');
    expect(o.end).toBe('2026-09-11T15:00');
  });

  it('flags an unlikely but heavy shower', () => {
    const o = rainOutlook(hours(START, 24, (i) => (i === 3 ? { precipitation_mm: 3, precipitation_probability: 20 } : {})), 0);
    expect(rainConsequence(o, 'ro')).toMatch(/^Puțin probabil \(20%\)\. Dacă totuși plouă: aproximativ 3,0 mm/);
  });
});

describe('the rain line under the bars', () => {
  const NOW_ISO = '2026-09-11T22:00';
  const night = (over: (i: number) => Partial<import('../../../lib/types').HourlyWeather>) => rainOutlook(hours(NOW_ISO, 24, over), 0);

  it('gives the period, the chance and the peak of a rain that is coming', () => {
    const o = night((i) => (i < 6 ? { precipitation_mm: [0.2, 0.3, 0.4, 0.6, 0.4, 1.4][i], precipitation_probability: [70, 75, 80, 85, 90, 85][i] } : {}));
    expect(rainPeriod(o, NOW_ISO, 'ro')).toBe('Acum – mâine 04:00');
    expect(rainPeakLine(o, NOW_ISO, 'ro')).toBe('Până la 90% șanse, maxim 1,4 mm/h (slabă) la mâine 03:00.');
    expect(rainPeakLine(o, NOW_ISO, 'en')).toBe('Up to 90% chance, peaking at 1.4 mm/h (light) at tomorrow 03:00.');
  });

  it('starts the period later when the rain does', () => {
    const o = night((i) => (i >= 2 && i < 4 ? { precipitation_mm: 1, precipitation_probability: 80 } : {}));
    expect(rainPeriod(o, NOW_ISO, 'ro')).toBe('mâine 00:00 – mâine 02:00');
  });

  it('says nothing more on a dry day, or when there is no amount to give', () => {
    const dry = night(() => ({}));
    expect(rainPeriod(dry, NOW_ISO, 'ro')).toBeNull();
    expect(rainPeakLine(dry, NOW_ISO, 'ro')).toBeNull();
    const chanceOnly = night((i) => (i < 3 ? { precipitation_probability: 40 } : {}));
    expect(rainPeriod(chanceOnly, NOW_ISO, 'ro')).toBe('Acum – mâine 01:00');
    expect(rainPeakLine(chanceOnly, NOW_ISO, 'ro')).toBeNull();
  });

  it('leaves an unlikely rain to the advice, which already gives the chance', () => {
    const unlikely = night((i) => (i < 3 ? { precipitation_mm: 1, precipitation_probability: 20 } : {}));
    expect(rainPeakLine(unlikely, NOW_ISO, 'ro')).toBeNull();
    expect(rainConsequence(unlikely, 'ro')).toMatch(/^Puțin probabil \(20%\)/);
  });
});
