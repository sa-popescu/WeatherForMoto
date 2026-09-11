import { describe, expect, it } from 'vitest';
import { gearFor, rideConditions } from './gear';
import { hours } from './testFixtures';

const NOW = '2026-09-11T10:00';
const opts = { night: false, sunset: '2026-09-11T19:33', nowIso: NOW };

describe('gear checklist', () => {
  it('requires the rain suit for likely moderate rain, with the amount in the reason', () => {
    const ride = hours(NOW, 9, (i) => (i >= 6 ? { precipitation_mm: 3.2, precipitation_probability: 80 } : {}));
    const recs = gearFor(rideConditions(ride, opts), 'ro');
    const suit = recs.find((r) => r.id === 'rain_suit');
    expect(suit).toMatchObject({ urgency: 'required', reason: '80% șanse, până la 3,2 mm/h (moderată), de la 16:00' });
    expect(recs[0].urgency).toBe('required');
  });

  it('only suggests a visor cloth for traces', () => {
    const ride = hours(NOW, 6, () => ({ precipitation_mm: 0.2, precipitation_probability: 70 }));
    const ids = gearFor(rideConditions(ride, opts), 'ro').map((r) => r.id);
    expect(ids).toContain('pinlock');
    expect(ids).not.toContain('rain_suit');
  });

  it('dresses for the coldest ride hour', () => {
    const ride = hours(NOW, 6, (i) => ({ feels_like: i === 0 ? 8 : 16 }));
    const ids = gearFor(rideConditions(ride, opts), 'en').map((r) => r.id);
    expect(ids).toEqual(expect.arrayContaining(['jacket_liner', 'base', 'gloves_thermal', 'pants_cool']));
  });

  it('adds a visor and hydration for gusts and heat, sorted by urgency', () => {
    const ride = hours(NOW, 6, () => ({ feels_like: 31, wind_gusts_kmh: 55 }));
    const recs = gearFor(rideConditions(ride, opts), 'en');
    expect(recs.map((r) => r.id)).toEqual(expect.arrayContaining(['visor_full', 'jacket_mesh', 'hydration']));
    expect(recs.find((r) => r.id === 'visor_full')?.item).toBe('Fully closed visor and neck collar');
    const order = recs.map((r) => r.urgency);
    expect(order).toEqual([...order].sort((a, b) => ['required', 'recommended', 'advice'].indexOf(a) - ['required', 'recommended', 'advice'].indexOf(b)));
  });

  it('recommends a clear visor at night and before sunset', () => {
    const night = gearFor(rideConditions(hours(NOW, 3), { ...opts, night: true }), 'ro').find((r) => r.id === 'dark_visor');
    expect(night?.reason).toBe('e întuneric acum');
    const dusk = gearFor(rideConditions(hours('2026-09-11T18:00', 2), { ...opts, nowIso: '2026-09-11T18:00' }), 'ro').find((r) => r.id === 'dark_visor');
    expect(dusk?.reason).toBe('apus la 19:33');
  });
});
