import { fmtNumber } from '../../../lib/format';
import type { Lang } from '../../../lib/i18n';

// Number formats of the ride screen. Speeds come in m/s or km/h as noted, and
// are shown in whole km/h.

/** "0:42", "12:04", "1:05:12": seconds tick, hours appear once there is one. */
export function fmtDuration(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const sec = String(total % 60).padStart(2, '0');
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${sec}` : `${m}:${sec}`;
}

/** Time left, in whole minutes: "18 min", "1 h 05". */
export function fmtTimeLeft(ms: number): string {
  const minutes = Math.max(0, Math.round(ms / 60_000));
  if (minutes < 60) return `${minutes} min`;
  return `${Math.floor(minutes / 60)} h ${String(minutes % 60).padStart(2, '0')}`;
}

/** Distance in km: one decimal under 100 km ("12,4"), whole above ("134"). */
export function fmtKm(metres: number, lang: Lang): string {
  const km = metres / 1000;
  return fmtNumber(km, lang, km < 100 ? 1 : 0);
}

/** A speed in km/h, whole, or "–" when there is none. */
export function fmtKmh(kmh: number | null, lang: Lang): string {
  return kmh === null ? '–' : fmtNumber(Math.round(kmh), lang);
}

export function msToKmh(ms: number): number {
  return ms * 3.6;
}

/** Wall-clock time on this phone, "09:12". The ride happens where the phone is. */
export function fmtClockTime(epochMs: number, lang: Lang): string {
  return new Date(epochMs).toLocaleTimeString(lang === 'en' ? 'en-GB' : 'ro-RO', { hour: '2-digit', minute: '2-digit' });
}
