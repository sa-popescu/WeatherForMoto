import { describe, expect, it } from 'vitest';
import {
  daysBetween,
  defaultDeparture,
  departureUtcMs,
  etaUtcMs,
  forecastDaysFor,
  localIsoAt,
  nextDates,
  pickSlot,
  rideMinutes,
  timesBetween,
} from './timing';

const RO = 3 * 3600; // Europe/Bucharest in summer

describe('departure and ETA', () => {
  it('reads the departure as the origin wall clock', () => {
    expect(departureUtcMs({ date: '2026-09-12', time: '08:30' }, RO)).toBe(Date.UTC(2026, 8, 12, 5, 30));
  });

  it('uses distance over the chosen average speed', () => {
    const dep = Date.UTC(2026, 8, 12, 5, 30);
    expect(etaUtcMs(dep, 150, 75)).toBe(dep + 2 * 3_600_000);
    expect(rideMinutes(277, 75)).toBe(222);
  });

  it('converts an instant to each place own clock', () => {
    const instant = Date.UTC(2026, 8, 12, 9, 0);
    expect(localIsoAt(instant, RO)).toBe('2026-09-12T12:00');
    expect(localIsoAt(instant, 2 * 3600)).toBe('2026-09-12T11:00');
  });
});

describe('pickSlot', () => {
  const hourly = ['2026-09-12T10:00', '2026-09-12T11:00', '2026-09-12T12:00'].map((time, i) => ({ time, v: i }));

  it('picks the slot containing the local time', () => {
    expect(pickSlot(hourly, '2026-09-12T11:45')?.v).toBe(1);
    expect(pickSlot(hourly, '2026-09-12T12:00')?.v).toBe(2);
  });

  it('returns null outside the forecast', () => {
    expect(pickSlot(hourly, '2026-09-12T13:10')).toBeNull();
  });
});

describe('forecastDaysFor', () => {
  it('asks for 3 days when the ride ends inside them', () => {
    expect(forecastDaysFor(0, 20 * 60, 4)).toBe(3);
    expect(forecastDaysFor(1, 20 * 60, 4)).toBe(3);
  });

  it('asks for 7 days further out or when the ride spills past day 3', () => {
    expect(forecastDaysFor(3, 8 * 60, 2)).toBe(7);
    expect(forecastDaysFor(2, 20 * 60, 5)).toBe(7);
  });
});

describe('dates and times', () => {
  it('lists half-hour departures inclusive', () => {
    const list = timesBetween('05:00', '20:00');
    expect(list).toHaveLength(31);
    expect(list[0]).toBe('05:00');
    expect(list[1]).toBe('05:30');
    expect(list[30]).toBe('20:00');
  });

  it('lists the next seven dates at the given offset', () => {
    const lateEvening = Date.UTC(2026, 8, 11, 22, 30); // 01:30 on the 12th in Romania
    expect(nextDates(lateEvening, RO, 3)).toEqual(['2026-09-12', '2026-09-13', '2026-09-14']);
    expect(daysBetween('2026-09-12', '2026-09-14')).toBe(2);
  });

  it('defaults to the next half hour, or tomorrow morning late in the evening', () => {
    expect(defaultDeparture(Date.UTC(2026, 8, 11, 7, 10), RO)).toEqual({ date: '2026-09-11', time: '10:30' });
    expect(defaultDeparture(Date.UTC(2026, 8, 11, 19, 0), RO)).toEqual({ date: '2026-09-12', time: '08:00' });
  });
});
