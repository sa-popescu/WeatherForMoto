import { localNowIso } from '../../lib/format';
import type { Departure } from './types';

// Time arithmetic for the planner. Forecast times are local to each place
// ("YYYY-MM-DDTHH:MM"); instants in between are plain UTC milliseconds.

export const SPEEDS = [60, 75, 90, 110] as const;
export type Speed = (typeof SPEEDS)[number];
export const DEFAULT_SPEED: Speed = 75;
export const STEP_MIN = 30;
export const PLAN_DAYS = 7;

/** "2026-09-12T08:30" at a place with the given offset -> UTC ms. */
export function localIsoToUtcMs(localIso: string, offsetSec: number): number {
  const [datePart, timePart = '00:00'] = localIso.split('T');
  const [y, m, d] = datePart.split('-').map(Number);
  const [hh, mm] = timePart.split(':').map(Number);
  return Date.UTC(y, m - 1, d, hh, mm) - offsetSec * 1000;
}

/** Departure instant; the chosen time is the origin's wall clock. */
export function departureUtcMs(dep: Departure, originOffsetSec: number): number {
  return localIsoToUtcMs(`${dep.date}T${dep.time}`, originOffsetSec);
}

/** Arrival at a point `km` along the route at a steady motorcycle pace. */
export function etaUtcMs(depUtcMs: number, km: number, speedKmh: number): number {
  return depUtcMs + (km / speedKmh) * 3_600_000;
}

/** Wall-clock time at a place for a UTC instant. */
export function localIsoAt(utcMs: number, offsetSec: number): string {
  return localNowIso(offsetSec, utcMs);
}

/** The hourly slot that contains a local time, or null outside the forecast. */
export function pickSlot<T extends { time: string }>(hourly: readonly T[], localIso: string): T | null {
  const hour = localIso.slice(0, 13);
  return hourly.find((h) => h.time.slice(0, 13) === hour) ?? null;
}

export function minutesOf(time: string): number {
  const [hh, mm] = time.split(':').map(Number);
  return hh * 60 + mm;
}

export function timeOf(minutes: number): string {
  const wrapped = ((minutes % 1440) + 1440) % 1440;
  return `${String(Math.floor(wrapped / 60)).padStart(2, '0')}:${String(wrapped % 60).padStart(2, '0')}`;
}

/** "HH:MM" values from `from` to `to` inclusive, every `step` minutes. */
export function timesBetween(from: string, to: string, step: number = STEP_MIN): string[] {
  const out: string[] = [];
  for (let m = minutesOf(from); m <= minutesOf(to); m += step) out.push(timeOf(m));
  return out;
}

export function rideMinutes(km: number, speedKmh: number): number {
  return Math.max(1, Math.round((km / speedKmh) * 60));
}

/** Whole days between two "YYYY-MM-DD" dates. */
export function daysBetween(fromDate: string, toDate: string): number {
  return Math.round((localIsoToUtcMs(`${toDate}T00:00`, 0) - localIsoToUtcMs(`${fromDate}T00:00`, 0)) / 86_400_000);
}

/**
 * Forecast length to ask the API for: 3 days covers departures up to two days
 * out; anything that can end after the third day needs the 7-day forecast.
 */
export function forecastDaysFor(dayOffset: number, latestDepartureMin: number, rideHours: number): 3 | 7 {
  const lastHour = dayOffset * 24 + latestDepartureMin / 60 + rideHours;
  return lastHour < 72 ? 3 : 7;
}

/** The device's UTC offset, used only before any forecast has arrived. */
export function deviceOffsetSec(nowMs: number = Date.now()): number {
  return -new Date(nowMs).getTimezoneOffset() * 60;
}

/** The next `count` dates starting today, at the given offset. */
export function nextDates(nowMs: number, offsetSec: number, count: number = PLAN_DAYS): string[] {
  const today = localIsoAt(nowMs, offsetSec).slice(0, 10);
  const base = localIsoToUtcMs(`${today}T00:00`, 0);
  return Array.from({ length: count }, (_, i) => new Date(base + i * 86_400_000).toISOString().slice(0, 10));
}

/** Sensible first departure: the next half hour today, or 08:00 tomorrow late in the evening. */
export function defaultDeparture(nowMs: number, offsetSec: number): Departure {
  const local = localIsoAt(nowMs, offsetSec);
  const [today, tomorrow] = nextDates(nowMs, offsetSec, 2);
  const next = Math.ceil((minutesOf(local.slice(11, 16)) + 1) / STEP_MIN) * STEP_MIN;
  if (next > 21 * 60) return { date: tomorrow, time: '08:00' };
  return { date: today, time: timeOf(next) };
}
