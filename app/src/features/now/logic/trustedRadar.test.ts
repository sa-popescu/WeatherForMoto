import { describe, expect, it } from 'vitest';
import { trustedRadar } from './headline';
import type { RadarRain } from './radarRain';
import { current, hours } from './testFixtures';

const RAINING: RadarRain = { nowMmPerHour: 1.2, arrivesInMin: null, stopsInMin: 30, peakMmPerHour: 1.2, reachMin: 90, baseSec: 0 };
const COMING: RadarRain = { nowMmPerHour: 0, arrivesInMin: 20, stopsInMin: null, peakMmPerHour: 2, reachMin: 90, baseSec: 0 };
const dryVerdict = {
  precipitation: { state: 'dry' as const, probability: 5, confidence: 92, explanation: 'Nu plouă.', explanation_en: 'Dry.' },
  fog: null,
  storm: null,
  summary: 'Nu plouă.',
  summary_en: 'Dry.',
};

describe('trustedRadar', () => {
  const hourly = hours('2026-11-25T18:00', 4);

  it('does not let the radar say it rains under a sure dry verdict', () => {
    const out = trustedRadar({ radar: RAINING, current: current({ humidity: 80, verdict: dryVerdict }), hourly, startIndex: 0 });
    expect(out?.nowMmPerHour).toBe(0);
    expect(out?.stopsInMin).toBeNull();
  });

  it('drops a shower coming over dry air that no forecast backs', () => {
    const out = trustedRadar({ radar: COMING, current: current({ humidity: 50, precipitation_probability: 5 }), hourly, startIndex: 0 });
    expect(out?.arrivesInMin).toBeNull();
  });

  it('keeps a shower the forecast or damp air backs', () => {
    const damp = trustedRadar({ radar: COMING, current: current({ humidity: 85, precipitation_probability: 5 }), hourly, startIndex: 0 });
    expect(damp?.arrivesInMin).toBe(20);
    const backed = trustedRadar({
      radar: COMING,
      current: current({ humidity: 50 }),
      hourly: hours('2026-11-25T18:00', 4, () => ({ precipitation_probability: 60 })),
      startIndex: 0,
    });
    expect(backed?.arrivesInMin).toBe(20);
  });

  it('keeps rain overhead when the arbiter has no sure dry verdict', () => {
    const out = trustedRadar({ radar: RAINING, current: current({ humidity: 90 }), hourly, startIndex: 0 });
    expect(out?.nowMmPerHour).toBe(1.2);
  });
});
