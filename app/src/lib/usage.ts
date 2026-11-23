import type { UsageEvent } from './usageEvents';

// Anonymous counts of what the app is used for, so the next work goes where
// riders actually are. The Worker (worker/usage.ts) keeps only totals per
// day: how many visits opened Drum, calculated a route, started a ride and
// so on. Each part counts once per visit (one page load): the names collect
// here and go out together in one small message when the page is hidden or
// closed, with no id, no cookie, no position and nothing about the account.
// Nothing is sent in development, or when the browser asks sites not to
// track it (Global Privacy Control, Do Not Track).

const USAGE_PATH = '/usage';

export interface PrivacySignals {
  dev: boolean;
  /** navigator.globalPrivacyControl */
  gpc: boolean;
  /** navigator.doNotTrack === '1' */
  dnt: boolean;
  canBeacon: boolean;
}

export function usageAllowed(signals: PrivacySignals): boolean {
  return !signals.dev && !signals.gpc && !signals.dnt && signals.canBeacon;
}

/** The message for the Worker: the names not sent yet, in a stable order. */
export function usageBody(events: Iterable<UsageEvent>): string {
  return JSON.stringify({ e: [...new Set(events)].sort() });
}

const pending = new Set<UsageEvent>();
const sent = new Set<UsageEvent>();
let allowed: boolean | null = null;
let listening = false;

function readAllowed(): boolean {
  // Checked first: tests and the dev server have no Worker to count anything.
  if (import.meta.env.DEV || typeof navigator === 'undefined') return false;
  const nav = navigator as Navigator & { globalPrivacyControl?: boolean };
  return usageAllowed({
    dev: false,
    gpc: nav.globalPrivacyControl === true,
    dnt: nav.doNotTrack === '1',
    canBeacon: typeof nav.sendBeacon === 'function',
  });
}

function flush(): void {
  if (pending.size === 0) return;
  const events = [...pending];
  try {
    // False when the browser's queue is full: the names wait for the next time the page is hidden.
    if (!navigator.sendBeacon(USAGE_PATH, usageBody(events))) return;
  } catch (err) {
    console.warn('[usage] the counts could not be sent', err);
    return;
  }
  for (const event of events) {
    pending.delete(event);
    sent.add(event);
  }
}

function listen(): void {
  if (listening) return;
  listening = true;
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flush();
  });
  window.addEventListener('pagehide', flush);
}

/** Notes that this visit used a part of the app; cheap, and safe to call as often as it happens. */
export function noteUse(event: UsageEvent): void {
  if (pending.has(event) || sent.has(event)) return;
  allowed ??= readAllowed();
  if (!allowed) return;
  pending.add(event);
  listen();
}

/** The visit itself, and whether it is in the installed app. */
export function noteVisit(): void {
  noteUse('visit');
  if (typeof window === 'undefined') return;
  const standalone = window.matchMedia?.('(display-mode: standalone)').matches || (navigator as Navigator & { standalone?: boolean }).standalone === true;
  if (standalone) noteUse('visit:installed');
}
