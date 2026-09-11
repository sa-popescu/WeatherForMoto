import { describe, expect, it } from 'vitest';
import { currentHourIndex, localNowIso } from './format';
import { rainBandOf, rainImpact, tierOf } from './scoring';

describe('tierOf', () => {
  it('uses the unified 85 / 60 / 40 thresholds', () => {
    expect(tierOf(85)).toBe('ideal');
    expect(tierOf(84)).toBe('ok');
    expect(tierOf(60)).toBe('ok');
    expect(tierOf(59)).toBe('atentie');
    expect(tierOf(40)).toBe('atentie');
    expect(tierOf(39)).toBe('evita');
    expect(tierOf(null)).toBeNull();
  });
});

describe('rain bands and impact', () => {
  it('classifies mm/h into bands', () => {
    expect(rainBandOf(0)).toBe('none');
    expect(rainBandOf(0.1)).toBe('urme');
    expect(rainBandOf(0.5)).toBe('slaba');
    expect(rainBandOf(3.2)).toBe('moderata');
    expect(rainBandOf(12)).toBe('puternica');
  });

  it('treats a high chance of traces as low impact, not real rain', () => {
    expect(rainImpact(70, 'urme')).toBe('low');
    expect(rainImpact(70, 'none')).toBe('low');
    expect(rainImpact(50, 'none')).toBe('none');
  });

  it('matches the backend matrix for real rain', () => {
    expect(rainImpact(80, 'moderata')).toBe('high');
    expect(rainImpact(45, 'slaba')).toBe('low');
    expect(rainImpact(20, 'puternica')).toBe('medium');
    expect(rainImpact(null, 'slaba')).toBe('medium');
  });
});

describe('local time helpers', () => {
  it('reads the wall clock at the location', () => {
    const noonUtc = Date.UTC(2026, 8, 11, 12, 0);
    expect(localNowIso(3 * 3600, noonUtc)).toBe('2026-09-11T15:00');
  });

  it('finds the current hour slot', () => {
    const hourly = ['2026-09-11T14:00', '2026-09-11T15:00', '2026-09-11T16:00'].map((time) => ({ time }));
    const at1530Local = Date.UTC(2026, 8, 11, 12, 30);
    expect(currentHourIndex(hourly, 3 * 3600, at1530Local)).toBe(1);
  });
});
