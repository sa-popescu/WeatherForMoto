import { describe, expect, it } from 'vitest';
import { hoursOfDay, tableDay } from './dayHours';
import { day, hours } from './testFixtures';

describe('hoursOfDay', () => {
  // Today from 14:00, then all of tomorrow and the day after.
  const hourly = hours('2026-11-21T14:00', 58);

  it('starts today at the current hour slot', () => {
    const today = hoursOfDay(hourly, '2026-11-21', '2026-11-21T14:00');
    expect(today[0].time).toBe('2026-11-21T14:00');
    expect(today.at(-1)?.time).toBe('2026-11-21T23:00');
    expect(today).toHaveLength(10);
  });

  it('drops the hours already gone when the clock moves on', () => {
    expect(hoursOfDay(hourly, '2026-11-21', '2026-11-21T20:00')[0].time).toBe('2026-11-21T20:00');
  });

  it('keeps every hour of a later day', () => {
    const tomorrow = hoursOfDay(hourly, '2026-11-22', '2026-11-21T14:00');
    expect(tomorrow).toHaveLength(24);
    expect(tomorrow[0].time).toBe('2026-11-22T00:00');
  });

  it('gives nothing for a day beyond the hourly forecast', () => {
    expect(hoursOfDay(hourly, '2026-11-30', '2026-11-21T14:00')).toEqual([]);
  });
});

describe('tableDay', () => {
  const daily = [day('2026-11-21'), day('2026-11-22'), day('2026-11-23')];

  it('shows today until a day is picked', () => {
    expect(tableDay(daily, null, '2026-11-21')?.date).toBe('2026-11-21');
  });

  it('shows the picked day', () => {
    expect(tableDay(daily, '2026-11-23', '2026-11-21')?.date).toBe('2026-11-23');
  });

  it('falls back to today once the picked day has left the forecast', () => {
    expect(tableDay(daily.slice(1), '2026-11-21', '2026-11-22')?.date).toBe('2026-11-22');
  });

  it('takes the first day when today is not in the list either', () => {
    expect(tableDay(daily, null, '2026-11-20')?.date).toBe('2026-11-21');
    expect(tableDay([], null, '2026-11-21')).toBeNull();
  });
});
