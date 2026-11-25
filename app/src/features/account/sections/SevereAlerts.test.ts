import { describe, expect, it } from 'vitest';
import { severeEnabled, withSevere } from './SevereAlerts';

describe('severe weather switch', () => {
  it('is on unless turned off', () => {
    expect(severeEnabled(null)).toBe(true);
    expect(severeEnabled('not json')).toBe(true);
    expect(severeEnabled('{"severe":false}')).toBe(false);
  });

  it('keeps the other switches', () => {
    expect(JSON.parse(withSevere('{"rain":false}', false))).toEqual({ rain: false, severe: false });
    expect(severeEnabled(withSevere(null, true))).toBe(true);
  });
});
