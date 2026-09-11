import type { CheckNowResponse } from '../../../lib/types';

// Turns POST /alerts/check-now into one sentence for a toast. The backend
// answers {reason} when it skipped the check, otherwise counts and events
// (see _dispatch_for_user in backend/auth_alerts.py). Alerts go out by web
// push only, so the push device count is the only channel that matters.

export type CheckOutcome =
  | { kind: 'disabled' }
  | { kind: 'noLocation' }
  | { kind: 'quiet' }
  | { kind: 'delivered'; count: number }
  | { kind: 'nothing' }
  | { kind: 'noChannel' }
  | { kind: 'alreadySent'; count: number };

function toCount(value: unknown): number {
  const n = typeof value === 'number' ? value : typeof value === 'boolean' ? Number(value) : Number.NaN;
  return Number.isFinite(n) ? n : 0;
}

/** `pushDevices`: push subscriptions on the account (all devices). */
export function describeCheckNow(res: CheckNowResponse, pushDevices: number): CheckOutcome {
  if (res.reason === 'alerts_disabled') return { kind: 'disabled' };
  if (res.reason === 'missing_home_location') return { kind: 'noLocation' };
  if (res.reason === 'quiet_hours') return { kind: 'quiet' };
  const delivered = toCount(res.delivered);
  if (delivered > 0) return { kind: 'delivered', count: delivered };
  const events = Array.isArray(res.events) ? res.events.length : 0;
  if (events === 0) return { kind: 'nothing' };
  if (pushDevices === 0) return { kind: 'noChannel' };
  // Events exist but none went out: each type is sent once per cooldown window.
  return { kind: 'alreadySent', count: events };
}
