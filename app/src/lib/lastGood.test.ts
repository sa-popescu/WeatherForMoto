import { describe, expect, it, beforeEach } from 'vitest';
import { LAST_GOOD_MAX_AGE_MS, lastGoodFor, rememberLastGood } from './lastGood';
import type { Place, WeatherResponse } from './types';

const PLACE: Place = { name: 'Floreasca', lat: 44.47, lon: 26.1 };
const DATA = { city: 'Floreasca', latitude: 44.47, longitude: 26.1 } as unknown as WeatherResponse;
const NOW = 1_800_000_000_000;

// The suite runs without a DOM, and storage.ts only needs these three.
const store = new Map<string, string>();
beforeEach(() => store.clear());
Object.defineProperty(globalThis, 'localStorage', {
  configurable: true,
  value: {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
  },
});

describe('the last weather that actually arrived', () => {

  it('comes back with its age', () => {
    rememberLastGood(PLACE, DATA, NOW - 20 * 60_000);
    expect(lastGoodFor(PLACE, NOW)).toEqual({ data: DATA, ageMin: 20 });
  });

  it('is dropped once it says more about yesterday than about the ride', () => {
    rememberLastGood(PLACE, DATA, NOW - LAST_GOOD_MAX_AGE_MS - 1);
    expect(lastGoodFor(PLACE, NOW)).toBeNull();
  });

  it('is not offered for another place', () => {
    rememberLastGood(PLACE, DATA, NOW);
    expect(lastGoodFor({ name: 'Brașov', lat: 45.65, lon: 25.6 }, NOW)).toBeNull();
    // A few hundred metres away is the same place.
    expect(lastGoodFor({ name: 'Floreasca', lat: 44.475, lon: 26.104 }, NOW)).not.toBeNull();
  });

  it('says nothing when nothing was ever stored', () => {
    expect(lastGoodFor(PLACE, NOW)).toBeNull();
  });
});
