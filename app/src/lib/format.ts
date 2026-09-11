import type { Lang } from './i18n';

// Formatting helpers. Forecast times from the API are LOCAL to the location
// ("YYYY-MM-DDTHH:MM", no offset), so they are handled as plain strings and
// never passed through the device's timezone.

const LOCALE: Record<Lang, string> = { ro: 'ro-RO', en: 'en-GB' };

export function fmtNumber(value: number | null | undefined, lang: Lang, decimals = 0): string {
  if (value == null || !Number.isFinite(value)) return '–';
  return value.toLocaleString(LOCALE[lang], { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

/** Rain amount: one decimal under 10 mm, whole numbers above ("0,2", "12"). */
export function fmtMm(value: number | null | undefined, lang: Lang): string {
  if (value == null || !Number.isFinite(value)) return '–';
  return fmtNumber(value, lang, value < 10 ? 1 : 0);
}

export function fmtTemp(value: number | null | undefined, lang: Lang): string {
  return value == null ? '–' : `${fmtNumber(Math.round(value), lang)}°`;
}

/** "2026-09-11T17:00" -> "17:00" */
export function hourOf(localIso: string): string {
  return localIso.slice(11, 16);
}

/** "2026-09-11T17:00" -> 17 */
export function hourNumber(localIso: string): number {
  return Number.parseInt(localIso.slice(11, 13), 10);
}

/** Current wall-clock time at the location, as a local ISO string. */
export function localNowIso(utcOffsetSeconds: number, nowMs: number = Date.now()): string {
  // Shift the clock by the location's offset and read it back as UTC.
  return new Date(nowMs + utcOffsetSeconds * 1000).toISOString().slice(0, 16);
}

/** Index of the hourly slot containing "now" at the location (or the next one). */
export function currentHourIndex(hourly: ReadonlyArray<{ time: string }>, utcOffsetSeconds: number, nowMs?: number): number {
  const nowHour = localNowIso(utcOffsetSeconds, nowMs).slice(0, 13);
  const exact = hourly.findIndex((h) => h.time.slice(0, 13) === nowHour);
  if (exact >= 0) return exact;
  const next = hourly.findIndex((h) => h.time.slice(0, 13) > nowHour);
  return next >= 0 ? next : Math.max(0, hourly.length - 1);
}

function dateFromLocalIso(localIso: string): Date {
  const [datePart, timePart = '00:00'] = localIso.split('T');
  const [y, m, d] = datePart.split('-').map(Number);
  const [hh, mm] = timePart.split(':').map(Number);
  return new Date(Date.UTC(y, m - 1, d, hh, mm));
}

/** Weekday name for a local date ("Sâm", "Saturday"). */
export function dayName(localDateIso: string, lang: Lang, style: 'short' | 'long' = 'short'): string {
  const text = dateFromLocalIso(localDateIso).toLocaleDateString(LOCALE[lang], { weekday: style, timeZone: 'UTC' });
  return text.charAt(0).toUpperCase() + text.slice(1).replace('.', '');
}

/** "11 sept" / "11 Sept" */
export function dayMonth(localDateIso: string, lang: Lang): string {
  return dateFromLocalIso(localDateIso)
    .toLocaleDateString(LOCALE[lang], { day: 'numeric', month: 'short', timeZone: 'UTC' })
    .replace('.', '');
}

/** Minutes between two local ISO strings of the same location. */
export function minutesBetween(fromLocalIso: string, toLocalIso: string): number {
  return Math.round((dateFromLocalIso(toLocalIso).getTime() - dateFromLocalIso(fromLocalIso).getTime()) / 60_000);
}

/** Adds minutes to a local ISO string, keeping the "YYYY-MM-DDTHH:MM" form. */
export function addMinutesLocal(localIso: string, minutes: number): string {
  return new Date(dateFromLocalIso(localIso).getTime() + minutes * 60_000).toISOString().slice(0, 16);
}

/** Relative age like "acum 5 min" is handled by callers; this returns whole minutes. */
export function ageMinutes(sinceMs: number, nowMs: number = Date.now()): number {
  return Math.max(0, Math.round((nowMs - sinceMs) / 60_000));
}
