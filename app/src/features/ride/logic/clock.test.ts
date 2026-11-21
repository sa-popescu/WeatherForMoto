import { describe, expect, it } from 'vitest';
import { averageKmh, elapsedMs, finishClock, NEW_CLOCK, pauseClock, resumeClock, type RideClock } from './clock';
import { fmtDuration, fmtKm, fmtKmh } from './rideFormat';

const started: RideClock = { ...NEW_CLOCK, startedAt: 1_000_000 };

describe('ride clock', () => {
  it('is zero until the first point', () => {
    expect(elapsedMs(NEW_CLOCK, 5_000_000)).toBe(0);
  });

  it('leaves the pauses out, the one in progress too', () => {
    const paused = pauseClock(started, 1_000_000 + 60_000);
    expect(elapsedMs(paused, 1_000_000 + 90_000)).toBe(60_000);
    const resumed = resumeClock(paused, 1_000_000 + 120_000);
    expect(resumed.pausedMs).toBe(60_000);
    expect(elapsedMs(resumed, 1_000_000 + 180_000)).toBe(120_000);
  });

  it('does not count a pause taken before the first point', () => {
    const resumed = resumeClock(pauseClock(NEW_CLOCK, 10), 50_000);
    expect(resumed.pausedMs).toBe(0);
  });

  it('stops at the end, closing a pause in progress', () => {
    const done = finishClock(pauseClock(started, 1_000_000 + 60_000), 1_000_000 + 100_000);
    expect(done.pausedAt).toBeNull();
    expect(elapsedMs(done, 9_999_999_999)).toBe(60_000);
  });
});

describe('averageKmh', () => {
  it('waits for half a minute of riding', () => {
    expect(averageKmh(400, 20_000)).toBeNull();
    expect(averageKmh(30_000, 1_800_000)).toBeCloseTo(60, 5);
  });
});

describe('ride formats', () => {
  it('writes durations with seconds, and hours once there is one', () => {
    expect(fmtDuration(42_000)).toBe('0:42');
    expect(fmtDuration(724_000)).toBe('12:04');
    expect(fmtDuration(3_912_000)).toBe('1:05:12');
  });

  it('writes km with one decimal under 100 km, and km/h whole', () => {
    expect(fmtKm(12_430, 'ro')).toBe('12,4');
    expect(fmtKm(134_600, 'en')).toBe('135');
    expect(fmtKmh(47.6, 'ro')).toBe('48');
    expect(fmtKmh(null, 'ro')).toBe('–');
  });
});
