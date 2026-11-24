import { describe, expect, it } from 'vitest';
import { timeLabel, timePhrase } from './formatting';

describe('timeLabel', () => {
  const now = '2026-11-21T19:00'; // a Saturday

  it('gives the bare hour today', () => {
    expect(timeLabel('2026-11-21T22:00', now, 'ro')).toBe('22:00');
  });

  it('says tomorrow for the next day', () => {
    expect(timeLabel('2026-11-22T03:00', now, 'ro')).toBe('mâine 03:00');
    expect(timeLabel('2026-11-22T03:00', now, 'en')).toBe('tomorrow 03:00');
  });

  it('names the weekday further out, as it reads inside a sentence', () => {
    expect(timeLabel('2026-11-24T03:00', now, 'ro')).toBe('marți 03:00');
    expect(timeLabel('2026-11-24T03:00', now, 'en')).toBe('Tuesday 03:00');
  });

  it('crosses a month end', () => {
    expect(timeLabel('2026-12-01T10:00', '2026-11-30T20:00', 'ro')).toBe('mâine 10:00');
  });
});

describe('timePhrase', () => {
  const now = '2026-11-24T21:00';
  it('puts the day before the preposition, as Romanian says it', () => {
    expect(timePhrase('from', '2026-11-24T23:00', now, 'ro')).toBe('de la 23:00');
    expect(timePhrase('from', '2026-11-25T03:00', now, 'ro')).toBe('mâine de la 03:00');
    expect(timePhrase('at', '2026-11-25T03:00', now, 'ro')).toBe('mâine la 03:00');
    expect(timePhrase('from', '2026-11-25T03:00', now, 'en')).toBe('tomorrow from 03:00');
  });
});
