import { version as APP_VERSION } from '../../package.json';
import { API_BASE } from './config';

// Errors nobody caught, sent to the API (POST /client-errors) so they show
// up in its log and, once a day per error, by email. What leaves the phone:
// the message, a few stack lines without query strings, the app version, the
// tab and the browser. Never coordinates, the account or an email address.

export type ErrorKind = 'error' | 'rejection' | 'render';

export interface ErrorPayload {
  kind: ErrorKind;
  message: string;
  stack: string;
  version: string;
  tab: string;
  browser: string;
}

/** At most this many reports per page load, each distinct error once. */
export const MAX_REPORTS_PER_SESSION = 5;
const MAX_MESSAGE = 300;
const MAX_STACK_LINES = 8;

/** Not ours or not actionable: extensions, cross-origin scripts, the network
 *  dropping on a ride, a request the app cancelled itself. */
const NOISE = [
  /ResizeObserver loop/i,
  /^Script error\.?$/i,
  /Failed to fetch|Load failed|NetworkError|network connection was lost/i,
  /AbortError|The operation was aborted|signal is aborted/i,
];
const FOREIGN_SOURCE = /(chrome|moz|safari(-web)?)-extension:\/\//;
const URL_TAIL = /(https?:\/\/[^\s?#)]+)[?#][^\s)]*/g;

export const APP_BUILD = `${APP_VERSION}+${__APP_BUILD__}`;

/** The tab's name from the hash ("#/drum?..." is "drum"), nothing after it. */
export function tabFromHash(hash: string): string {
  const name = hash.replace(/^#\/?/, '').split(/[/?#&]/)[0] ?? '';
  return /^[a-z-]{1,24}$/.test(name) ? name : '';
}

export function stripUrlTails(text: string): string {
  return text.replace(URL_TAIL, '$1');
}

function lines(text: string): string[] {
  return stripUrlTails(text)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

function messageOf(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  if (typeof error === 'string') return error;
  try {
    return JSON.stringify(error) ?? String(error);
  } catch {
    return String(error);
  }
}

/** The report for one error, or null when it is noise. */
export function buildPayload(
  kind: ErrorKind,
  error: unknown,
  context: { hash: string; userAgent: string; extraStack?: string },
): ErrorPayload | null {
  const message = stripUrlTails(messageOf(error)).slice(0, MAX_MESSAGE).trim();
  if (!message || NOISE.some((pattern) => pattern.test(message))) return null;
  const errorStack = error instanceof Error ? (error.stack ?? '') : '';
  if (FOREIGN_SOURCE.test(errorStack)) return null;
  // A render error keeps a few lines of each: where it broke, and in which
  // components.
  const componentLines = lines(context.extraStack ?? '').slice(0, 3);
  const stack = [...lines(errorStack).slice(0, MAX_STACK_LINES - componentLines.length), ...componentLines].join('\n');
  return {
    kind,
    message,
    stack,
    version: APP_BUILD,
    tab: tabFromHash(context.hash),
    browser: context.userAgent.slice(0, 200),
  };
}

const sent = new Set<string>();

/** Report one error, unless it is noise, already sent or over the session cap. */
export function reportError(kind: ErrorKind, error: unknown, extraStack?: string): void {
  // In development the console already shows everything.
  if (import.meta.env.DEV) return;
  const payload = buildPayload(kind, error, {
    hash: window.location.hash,
    userAgent: navigator.userAgent,
    extraStack,
  });
  if (!payload) return;
  const key = `${payload.kind}|${payload.message}|${payload.stack.split('\n')[0] ?? ''}`;
  if (sent.has(key) || sent.size >= MAX_REPORTS_PER_SESSION) return;
  sent.add(key);
  // text/plain keeps it a simple request (no CORS preflight); keepalive lets
  // it finish when the error is followed by the page closing.
  fetch(`${API_BASE}/client-errors`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: JSON.stringify(payload),
    keepalive: true,
    credentials: 'omit',
  }).catch((err: unknown) => console.warn('[errors] could not send the report', err));
}

/** Listen for errors nothing else caught. Render errors come from React (main.tsx). */
export function installErrorReporting(): void {
  window.addEventListener('error', (event) => {
    // Failed <img> or <script> loads also arrive here, without an error object.
    if (!event.error && !event.message) return;
    reportError('error', event.error ?? event.message);
  });
  window.addEventListener('unhandledrejection', (event) => reportError('rejection', event.reason));
}
