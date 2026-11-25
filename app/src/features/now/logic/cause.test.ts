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

describe('the cold later today', () => {
  it('says "down to" when the coldest feel is not the first hour (Rahova, 25 Nov 19:30)', () => {
    const hours = [
      hour('2026-11-25T19:00', { feels_like: -0.9 }),
      hour('2026-11-25T22:00', { feels_like: -2.1 }),
      hour('2026-11-25T23:00', { feels_like: -3.0 }),
    ];
    const cause = causeFromHours(hours);
    expect(cause?.factor).toBe('cold');
    expect(causeSentence(cause!, 'ro')).toBe('Frig, resimțit până la -3°.');
    expect(causeSentence(cause!, 'en')).toBe('Cold, feeling down to -3°.');
  });

  it('keeps "resimțit" when the first hour is already the coldest', () => {
    const cause = causeFromHours([hour('2026-11-25T19:00', { feels_like: -3.0 }), hour('2026-11-25T20:00', { feels_like: -2.0 })]);
    expect(causeSentence(cause!, 'ro')).toBe('Frig, resimțit -3°.');
  });
});
