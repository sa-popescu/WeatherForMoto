import { describe, expect, it } from 'vitest';
import { nowLine } from './nowLine';

const calm = { wind_speed_kmh: 8, wind_gusts_kmh: 15, wind_direction: 'NE', frost_risk: false, verdict: null };

describe('nowLine', () => {
  it('names the sky and a light wind with its direction', () => {
    expect(nowLine(calm, 'Senin', 'ro')).toBe('Senin, vânt slab din NE');
    expect(nowLine(calm, 'Clear', 'en')).toBe('Clear, light wind from the NE');
  });

  it('names strong gusts by their speed', () => {
    expect(nowLine({ ...calm, wind_gusts_kmh: 52.4 }, 'Înnorat', 'ro')).toBe('Înnorat, rafale de 52 km/h');
  });

  it('adds what matters on a bike: disputed rain, fog, ice', () => {
    const verdict = {
      precipitation: { state: 'uncertain' as const, probability: 50, confidence: 50, explanation: '', explanation_en: '' },
      fog: { state: 'fog' as const, probability: 80, confidence: 80, explanation: '', explanation_en: '' },
      storm: null,
      summary: '',
      summary_en: '',
    };
    expect(nowLine({ ...calm, frost_risk: true, verdict }, 'Înnorat', 'ro')).toBe(
      'Înnorat, posibil ploaie, ceață, vânt slab din NE, risc de polei',
    );
  });

  it('does not say fog twice', () => {
    const verdict = {
      precipitation: { state: 'dry' as const, probability: 2, confidence: 98, explanation: '', explanation_en: '' },
      fog: { state: 'fog' as const, probability: 80, confidence: 80, explanation: '', explanation_en: '' },
      storm: null,
      summary: '',
      summary_en: '',
    };
    expect(nowLine({ ...calm, verdict }, 'Ceață', 'ro', true)).toBe('Ceață, vânt slab din NE');
  });
});
