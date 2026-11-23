import { describe, expect, it } from 'vitest';
import { causeFromHours, causeSentence } from './cause';
import { hour } from './testFixtures';

// The reason a score is low, now including the road after rain (backend/wet_road.py).

describe('the road after rain', () => {
  it('names a road still wet after rain as the reason', () => {
    const hours = [hour('2026-11-24T10:00'), hour('2026-11-24T11:00', { wet_road: 'wet' })];
    const cause = causeFromHours(hours);
    expect(cause?.factor).toBe('wet_road');
    expect(cause?.from).toBe('2026-11-24T11:00');
    expect(causeSentence(cause!, 'ro')).toBe('Drum probabil încă ud după ploaie.');
    expect(causeSentence(cause!, 'en')).toBe('The road is probably still wet after rain.');
  });

  it('puts the rain before the first rain warning when they cost the same, and explains both', () => {
    const wet = hour('2026-11-24T12:00', { precipitation_mm: 1.2, precipitation_probability: 90, weather_code: 61, wet_road: 'first_rain' });
    const cause = causeFromHours([wet]);
    // The rain itself costs more than the slippery first hour.
    expect(cause?.factor).toBe('rain');
    const onlyFirst = causeFromHours([hour('2026-11-24T12:00', { precipitation_mm: 0.2, precipitation_probability: 10, wet_road: 'first_rain' })]);
    expect(onlyFirst?.factor).toBe('first_rain');
    expect(causeSentence(onlyFirst!, 'ro')).toMatch(/^Prima ploaie după zile uscate, asfalt alunecos/);
  });
});
