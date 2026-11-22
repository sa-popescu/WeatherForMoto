import { describe, expect, it } from 'vitest';
import type { ScoreFactor } from '../../../lib/types';
import { FORGIVE, learnTolerance, MIN_RIDES, personalScore, type Tolerance } from './personal';
import type { RideStory } from './story';

function story(minFeelsC: number, maxFeelsC: number, gust: number): RideStory {
  return {
    rainMin: 0,
    rainFrom: null,
    rainTo: null,
    rainBand: 'none',
    coldest: null,
    minTempC: minFeelsC + 2,
    warmestC: maxFeelsC - 2,
    minFeelsC,
    maxFeelsC,
    gust: { kmh: gust, at: 0 },
    darkMin: 0,
    highestM: null,
    samples: 10,
  };
}

const factor = (name: string, penalty: number, cap: number | null = null): ScoreFactor => ({ factor: name, penalty, cap, detail: '' });

describe('learnTolerance', () => {
  it('waits for enough rides, then keeps the second most extreme of each', () => {
    const rides = [story(8, 20, 30), story(3, 24, 45), story(6, 31, 52), story(10, 22, 25)];
    expect(learnTolerance(rides)).toEqual({ rides: 4, tolerance: null });
    const learned = learnTolerance([...rides, story(-2, 28, 60), null]);
    expect(learned.rides).toBe(MIN_RIDES);
    // -2° was once: the second coldest, 3°, is what counts. Same for the gusts and the heat.
    expect(learned.tolerance).toEqual({ coldFeelsC: 3, gustKmh: 52, heatFeelsC: 28 });
  });

  it('leaves out rides whose story found no weather', () => {
    const empty = { ...story(0, 0, 0), samples: 0 };
    expect(learnTolerance([empty, empty, empty, empty, empty]).tolerance).toBeNull();
  });
});

describe('personalScore', () => {
  const tolerance: Tolerance = { coldFeelsC: 3, gustKmh: 52, heatFeelsC: 30 };

  it('gives back part of the cold and wind penalties within what the rider has ridden in', () => {
    const now = { moto_score: 57, feels_like: 6, wind_gusts_kmh: 40, score_breakdown: [factor('cold', 28), factor('wind', 15)] };
    expect(personalScore(now, tolerance)).toBe(Math.round(57 + (28 + 15) * FORGIVE));
    // Colder than ever ridden: the cold counts in full.
    expect(personalScore({ ...now, feels_like: 1 }, tolerance)).toBe(Math.round(57 + 15 * FORGIVE));
  });

  it('never goes above a cap set by rain, a storm or frost', () => {
    const wet = { moto_score: 59, feels_like: 6, wind_gusts_kmh: 30, score_breakdown: [factor('cold', 28), factor('rain', 10, 59)] };
    expect(personalScore(wet, tolerance)).toBe(59);
  });

  it('says nothing without the breakdown', () => {
    expect(personalScore({ moto_score: 70, feels_like: 6, wind_gusts_kmh: 30, score_breakdown: null }, tolerance)).toBeNull();
  });
});
