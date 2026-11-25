import { describe, expect, it } from 'vitest';
import { withObserved } from './storyWeather';

const model = { tempC: 12, feelsC: 10, precipMm: 0, gustKmh: 30, isDay: true };

describe('withObserved', () => {
  it('lets a measurement replace the models where one was logged', () => {
    const [out] = withObserved([model], [{ temp: 9, gusts: 45, raining: 1, sources: ['official'] }]);
    expect(out?.tempC).toBe(9);
    expect(out?.feelsC).toBe(7);
    expect(out?.gustKmh).toBe(45);
    expect(out?.precipMm).toBe(0.5);
  });

  it('dries a model shower nobody measured', () => {
    const [out] = withObserved([{ ...model, precipMm: 2 }], [{ raining: 0, sources: ['metar'] }]);
    expect(out?.precipMm).toBe(0);
    expect(out?.tempC).toBe(12);
  });

  it('keeps the models where nothing was measured', () => {
    expect(withObserved([model, null], [null])).toEqual([model, null]);
  });
});
