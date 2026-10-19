import { ApiError } from './api';

// When the weather cannot be loaded at all, the only diagnostic that ever
// reaches us is a photo of the phone screen. The card therefore carries the
// technical cause of each leg that failed: what our API said, and what
// Open-Meteo said when the app asked it directly. Neither is translated —
// it is meant to be read by whoever is fixing it, not by the rider.

/** Long enough for a status and an upstream message, short enough for a phone. */
export const DETAIL_MAX = 200;

function messageOf(err: unknown): string {
  if (err instanceof Error) return err.message;
  const text = String(err ?? '');
  return text || 'unknown';
}

/** What our own API did: a status when it answered, the cause when it did not. */
export function apiLeg(err: unknown): string {
  if (err instanceof ApiError) {
    return err.isNetwork ? `API unreachable: ${err.detail}` : `API HTTP ${err.status}: ${err.detail}`;
  }
  return `API: ${messageOf(err)}`;
}

/** What the browser's own call to Open-Meteo did. */
export function directLeg(err: unknown): string {
  const message = messageOf(err);
  return message.startsWith('Open-Meteo') ? `direct: ${message}` : `direct Open-Meteo: ${message}`;
}

/** One line naming both failures, for the error card and the stale banner. */
export function failureDetail(apiErr: unknown, directErr: unknown): string {
  const line = `${apiLeg(apiErr)} · ${directLeg(directErr)}`;
  return line.length > DETAIL_MAX ? `${line.slice(0, DETAIL_MAX - 1)}…` : line;
}
