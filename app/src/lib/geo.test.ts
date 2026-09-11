import { describe, expect, it } from 'vitest';
import { distanceKm, rankSuggestions } from './geo';
import type { PlaceSuggestion } from './geo';

const ro = (name: string, region: string, lat: number, lon: number): PlaceSuggestion => ({
  name,
  lat,
  lon,
  region,
  country: 'România',
  countryCode: 'RO',
});

// The two Romanian villages called Cheia, and the neighbouring stop that tells
// them apart: Vălenii de Munte, in Prahova.
const cheiaBrasov = ro('Cheia', 'Brașov', 45.44, 25.3);
const cheiaPrahova = ro('Cheia', 'Prahova', 45.42, 25.94);
const valenii = { lat: 45.19, lon: 26.04 };

describe('distanceKm', () => {
  it('measures a known distance', () => {
    expect(distanceKm({ lat: 44.43, lon: 26.1 }, { lat: 45.79, lon: 24.13 })).toBeCloseTo(220, -1);
  });
});

describe('rankSuggestions', () => {
  it('puts the homonym closest to the neighbouring stop first', () => {
    const ranked = rankSuggestions([cheiaBrasov, cheiaPrahova], 'Cheia', valenii);
    expect(ranked.map((p) => p.region)).toEqual(['Prahova', 'Brașov']);
  });

  it('keeps the API order for homonyms when there is no reference', () => {
    const ranked = rankSuggestions([cheiaBrasov, cheiaPrahova], 'Cheia', null);
    expect(ranked.map((p) => p.region)).toEqual(['Brașov', 'Prahova']);
  });

  it('ignores diacritics and case when matching the typed name', () => {
    const ranked = rankSuggestions([cheiaBrasov, cheiaPrahova], 'cheia', valenii);
    expect(ranked[0].region).toBe('Prahova');
  });

  it('leaves a single exact match where the API put it', () => {
    const clujNapoca = ro('Cluj-Napoca', 'Cluj', 46.77, 23.6);
    const cluj = ro('Cluj', 'Cluj', 46.9, 23.5);
    const ranked = rankSuggestions([clujNapoca, cluj], 'Cluj', { lat: 46.9, lon: 23.5 });
    expect(ranked.map((p) => p.name)).toEqual(['Cluj-Napoca', 'Cluj']);
  });

  it('keeps Romania before the homonyms abroad', () => {
    const abroad: PlaceSuggestion = { name: 'Cheia', lat: 43.2, lon: 25.1, region: 'Veliko Tarnovo', country: 'Bulgaria', countryCode: 'BG' };
    const ranked = rankSuggestions([abroad, cheiaBrasov, cheiaPrahova], 'Cheia', valenii);
    expect(ranked.map((p) => p.countryCode)).toEqual(['RO', 'RO', 'BG']);
    expect(ranked[0].region).toBe('Prahova');
  });
});
