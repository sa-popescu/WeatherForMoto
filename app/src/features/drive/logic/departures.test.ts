import { describe, expect, it } from 'vitest';
import { bestDeparture, departureCost, departureTimes, type DepartureOption } from './departures';
import type { RouteOutlook } from './driveWeather';

/** 09:12 UTC. */
const NOW = Date.UTC(2026, 10, 22, 9, 12);
const at = (h: number, m: number): number => Date.UTC(2026, 10, 22, h, m);

const dry: RouteOutlook = { settled: true, warning: null, worstScore: 80, dark: false, glare: null };
const wet: RouteOutlook = {
  settled: true,
  warning: { kind: 'rain', band: 'moderata', minutes: 40, km: 60, name: 'Pitești', value: null, key: 'rain|60.0' },
  worstScore: 45,
  dark: false,
  glare: null,
};

function option(departMs: number | null, durationMin: number, outlook: RouteOutlook | null, status: DepartureOption['status'] = 'ready'): DepartureOption {
  return { departMs, status, durationS: durationMin * 60, delayS: 0, outlook };
}

describe('departureTimes', () => {
  it('starts at the half hour at least 20 minutes away, then half hours, then hours', () => {
    expect(departureTimes(NOW)).toEqual([at(10, 0), at(10, 30), at(11, 0), at(12, 0), at(13, 0)]);
    // 09:05 + 20 min is 09:25: the first one is 09:30.
    expect(departureTimes(at(9, 5))[0]).toBe(at(9, 30));
  });
});

describe('bestDeparture', () => {
  it('waits out the rain when a later departure stays dry', () => {
    const options = [option(null, 150, wet), option(at(10, 0), 148, wet), option(at(12, 0), 155, dry), option(at(13, 0), 152, dry)];
    expect(bestDeparture(options, NOW)).toBe(2);
  });

  it('leaves now when nothing is gained by waiting', () => {
    const options = [option(null, 150, dry), option(at(10, 0), 150, dry), option(at(11, 0), 145, dry)];
    expect(bestDeparture(options, NOW)).toBe(0);
  });

  it('picks the hour with less traffic when the weather is the same', () => {
    // Rush hour now: 40 minutes lost in traffic, gone an hour later.
    const options = [option(null, 95, dry), option(at(10, 0), 70, dry), option(at(10, 30), 56, dry)];
    expect(bestDeparture(options, NOW)).toBe(2);
  });

  it('weighs riding in the dark', () => {
    const night = { ...dry, dark: true };
    expect(departureCost(option(null, 60, night), NOW)).toBeGreaterThan(departureCost(option(null, 60, dry), NOW)!);
  });

  it('waits for every departure before recommending one, and skips those that failed', () => {
    expect(bestDeparture([option(null, 60, dry), option(at(10, 0), 50, null, 'loading')], NOW)).toBeNull();
    expect(bestDeparture([option(null, 60, wet), option(at(10, 0), 50, null, 'error')], NOW)).toBe(0);
    expect(bestDeparture([option(null, 60, { ...dry, settled: false })], NOW)).toBeNull();
  });
});
