import { describe, expect, it } from 'vitest';
import type { HourlyWeather } from '../../../lib/types';
import type { PointWeather, SamplePoint } from '../../route/types';
import { aheadLine, aheadSentence } from '../aheadText';
import { aheadPoints, warningAhead, type AheadPoint, type AheadSlot } from './ahead';

const OFFSET = 2 * 3600;
/** 10:00 local time at the points (08:00 UTC). */
const NOW = Date.UTC(2026, 10, 22, 8, 0);

function slot(hour: number, over: Partial<AheadSlot> = {}): AheadSlot {
  return { time: `2026-11-22T${String(hour).padStart(2, '0')}:00`, band: 'none', rainProb: 0, gustKmh: 20, frost: false, score: 80, ...over };
}

function point(km: number, name: string, slots: AheadSlot[]): AheadPoint {
  return { km, name, utcOffsetSeconds: OFFSET, slots };
}

const calmDay = [9, 10, 11, 12, 13].map((h) => slot(h));

describe('warningAhead', () => {
  it('finds rain at the point reached within the hour, at the hour of reaching it', () => {
    const points = [
      point(20, 'Găești', calmDay),
      // Rain from 11:00 only: reached at 10:40 at 60 km/h, it is still dry; at 11:20 it rains.
      point(40, 'Pitești', [slot(10), slot(11, { band: 'moderata', rainProb: 80 })]),
    ];
    expect(warningAhead(points, 0, 60, NOW)?.name).toBeUndefined();
    const later = warningAhead(points, 0, 60, NOW + 40 * 60_000);
    expect(later).toMatchObject({ kind: 'rain', band: 'moderata', name: 'Pitești', minutes: 40 });
  });

  it('looks no further than the horizon, and not behind', () => {
    const wet = [slot(10, { band: 'puternica', rainProb: 90 }), slot(11, { band: 'puternica', rainProb: 90 })];
    expect(warningAhead([point(90, 'Far', wet)], 0, 60, NOW)).toBeNull();
    expect(warningAhead([point(5, 'Behind', wet)], 10, 60, NOW)).toBeNull();
  });

  it('puts frost first, then heavy rain, gusts, light rain and a bad score', () => {
    const at = (s: AheadSlot) => warningAhead([point(10, 'X', [slot(10, s)])], 0, 60, NOW)?.kind;
    expect(at(slot(10, { frost: true, band: 'puternica', rainProb: 90 }))).toBe('frost');
    expect(at(slot(10, { band: 'moderata', rainProb: 80, gustKmh: 70 }))).toBe('rain');
    expect(at(slot(10, { band: 'slaba', rainProb: 80, gustKmh: 70 }))).toBe('gust');
    expect(at(slot(10, { band: 'slaba', rainProb: 80 }))).toBe('rain');
    expect(at(slot(10, { score: 30 }))).toBe('score');
    // A small chance of a little rain is not worth a word.
    expect(at(slot(10, { band: 'urme', rainProb: 20 }))).toBeUndefined();
  });
});

describe('aheadPoints', () => {
  it('keeps the next hours of each point with a forecast, in route order', () => {
    const hourly = Array.from({ length: 48 }, (_, i) => {
      const day = 22 + Math.floor(i / 24);
      return { time: `2026-11-${day}T${String(i % 24).padStart(2, '0')}:00`, rain_intensity: null, precipitation_mm: 0.6, precipitation_probability: 70, wind_gusts_kmh: 30, frost_risk: false, moto_score: 60 } as unknown as HourlyWeather;
    });
    const pts: SamplePoint[] = [
      { id: 'b', lat: 0, lon: 0, km: 50, name: 'B', stopIndex: null },
      { id: 'a', lat: 0, lon: 0, km: 0, name: 'A', stopIndex: 0 },
      { id: 'c', lat: 0, lon: 0, km: 90, name: 'C', stopIndex: 1 },
    ];
    const weather: Record<string, PointWeather> = {
      a: { status: 'ready', forecast: { hourly, utcOffsetSeconds: OFFSET, sun: [] } },
      b: { status: 'ready', forecast: { hourly, utcOffsetSeconds: OFFSET, sun: [] } },
      c: { status: 'error', forecast: null },
    };
    const out = aheadPoints(pts, weather, NOW);
    expect(out.map((p) => p.name)).toEqual(['A', 'B']);
    expect(out[0].slots[0].time).toBe('2026-11-22T09:00');
    expect(out[0].slots).toHaveLength(26);
    // The band comes from the amount when the forecast gives no intensity.
    expect(out[0].slots[0].band).toBe('slaba');
  });
});

describe('ahead words', () => {
  const rain = { kind: 'rain' as const, minutes: 20, km: 40, name: 'Pitești', band: 'moderata' as const, value: null, key: 'k' };

  it('writes a short line and says a sentence', () => {
    expect(aheadLine(rain, 'ro')).toBe('Ploaie moderată în ~20 min · Pitești');
    expect(aheadSentence(rain, 'ro')).toBe('Ploaie moderată în aproximativ 20 de minute, pe la Pitești.');
    expect(aheadSentence({ ...rain, kind: 'gust', value: 62, name: 'DN7', minutes: 1 }, 'ro')).toBe('Rafale de 62 de kilometri pe oră chiar acum, pe DN7.');
    expect(aheadLine({ ...rain, kind: 'frost', minutes: 35 }, 'en')).toBe('Ice risk in ~35 min · Pitești');
  });
});
