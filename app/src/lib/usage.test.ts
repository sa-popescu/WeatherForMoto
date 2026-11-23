import { describe, expect, it } from 'vitest';
import { usageAllowed, usageBody } from './usage';
import { isUsageEvent, USAGE_EVENTS } from './usageEvents';

describe('usage counts in the app', () => {
  const open = { dev: false, gpc: false, dnt: false, canBeacon: true };

  it('sends only from the built app, to a browser that does not ask to be left alone', () => {
    expect(usageAllowed(open)).toBe(true);
    expect(usageAllowed({ ...open, dev: true })).toBe(false);
    expect(usageAllowed({ ...open, gpc: true })).toBe(false);
    expect(usageAllowed({ ...open, dnt: true })).toBe(false);
    expect(usageAllowed({ ...open, canBeacon: false })).toBe(false);
  });

  it('sends each name once, in a stable order', () => {
    expect(usageBody(['view:drum', 'visit', 'view:drum'])).toBe('{"e":["view:drum","visit"]}');
  });

  it('has a unique, short name and a label for every part of the app', () => {
    const names = USAGE_EVENTS.map(([name]) => name);
    expect(new Set(names).size).toBe(names.length);
    for (const [name, label] of USAGE_EVENTS) {
      expect(name).toMatch(/^[a-z]+(:[a-z-]+)?$/);
      expect(label.length).toBeGreaterThan(3);
    }
    expect(isUsageEvent('visit')).toBe(true);
    expect(isUsageEvent('nope')).toBe(false);
  });

  it('fits the whole list in one report the Worker accepts', () => {
    expect(usageBody(USAGE_EVENTS.map(([name]) => name)).length).toBeLessThan(2048);
  });
});
