// Anonymous usage counts (app/src/lib/usage.ts sends them), and the page that
// shows them.
//
//   POST /usage  {"e": ["visit", "view:drum", ...]}  -> 204, or 429 past the day's caps
//   GET  /stats                                     -> a form asking for the key
//   POST /stats  key=<STATS_KEY>                    -> the totals, as a page
//
// What is kept: for each day (Romania's calendar), how many visits used each
// part of the app, the names in lib/usageEvents.ts and nothing else. No id,
// no cookie, no position, nothing about an account. The address of a request
// is only hashed, in memory, to cap how often one address reports in a day,
// and is forgotten when the day ends or the object restarts. Days older than
// KEEP_DAYS are deleted. Everything sits in one Durable Object, so the
// counts cost one request and one row written per visit; the daily caps keep
// that well inside the free plan, next to the live location (live.ts).
//
// The page asks for the STATS_KEY secret, set in the Cloudflare dashboard
// (Workers & Pages > weatherformoto > Settings > Variables and Secrets) and
// never in this repository. Without it, or with one shorter than
// MIN_KEY_LENGTH, the page says the statistics are off. The key travels in
// the body of a POST, never in the address, so it stays out of history and logs.

import { isUsageEvent, USAGE_EVENTS, type UsageEvent } from '../lib/usageEvents';
import { sameText, sha256, type DurableNamespace } from './live';

// ---- The few Cloudflare types used here (trimmed) ------------------------

export interface UsageStorage {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  /** `end` is exclusive, as in Cloudflare's API. */
  list<T>(options: { prefix: string; start?: string; end?: string }): Promise<Map<string, T>>;
  delete(keys: string[]): Promise<number>;
}

export interface UsageState {
  storage: UsageStorage;
}

export interface UsageEnv {
  /** Missing until wrangler.jsonc binds the object: /usage then answers 503 and the app does not mind. */
  USAGE_STATS?: DurableNamespace;
  /** The key of the /stats page, a secret set in the dashboard. */
  STATS_KEY?: string;
}

// ---- Limits -----------------------------------------------------------------

/** Reports one address can make in a day: far more visits than one rider makes. */
export const REPORTS_PER_CLIENT = 30;
/** Reports in a day, in all; past it the day's counts stop growing. */
export const DAILY_REPORTS = 10_000;
/** Days kept; older ones are deleted when a new day starts. */
export const KEEP_DAYS = 400;
/** The longest period the page adds up. */
export const SHOWN_DAYS = 90;
/** A shorter STATS_KEY leaves the page off: a key that short could be guessed. */
export const MIN_KEY_LENGTH = 24;
const MAX_REPORT_BODY = 2048;
const MAX_FORM_BODY = 1024;
const MAX_EVENTS = USAGE_EVENTS.length;
const DELETE_BATCH = 128;

const COMMON_HEADERS = { 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' };
const PAGE_HEADERS = {
  ...COMMON_HEADERS,
  'Content-Type': 'text/html; charset=utf-8',
  'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
  'Referrer-Policy': 'no-referrer',
  'X-Content-Type-Options': 'nosniff',
};

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...COMMON_HEADERS },
  });
}

// ---- Days -------------------------------------------------------------------

const DAY_FORMAT = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Bucharest', year: 'numeric', month: '2-digit', day: '2-digit' });

/** The day in Romania at `ms`, as YYYY-MM-DD. */
export function dayOf(ms: number): string {
  return DAY_FORMAT.format(ms);
}

/** `day` moved by a number of calendar days. */
export function shiftDay(day: string, days: number): string {
  const [y, m, d] = day.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d + days)).toISOString().slice(0, 10);
}

const dayKey = (day: string): string => `d:${day}`;

export interface DayCounts {
  day: string;
  /** Reports counted that day (a visit can report more than once, when it is hidden and comes back). */
  reports: number;
  counts: Partial<Record<UsageEvent, number>>;
}

type DayRecord = Omit<DayCounts, 'day'>;

const emptyRecord = (): DayRecord => ({ reports: 0, counts: {} });

/** The counts of the whole app. Cloudflare keeps one instance, wherever it is asked from. */
export class UsageStats {
  private readonly storage: UsageStorage;
  private day = '';
  private record: DayRecord = emptyRecord();
  private readonly perClient = new Map<string, number>();

  constructor(state: UsageState) {
    this.storage = state.storage;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/add' && request.method === 'POST') return this.add(request);
    if (url.pathname === '/days') return this.days(Number(url.searchParams.get('n')));
    return json(404, { error: 'path' });
  }

  /** Today's record in memory; a new day forgets the addresses and deletes the days past KEEP_DAYS. */
  private async today(): Promise<string> {
    const day = dayOf(Date.now());
    if (day === this.day) return day;
    this.day = day;
    this.record = (await this.storage.get<DayRecord>(dayKey(day))) ?? emptyRecord();
    this.perClient.clear();
    const old = [...(await this.storage.list<DayRecord>({ prefix: 'd:', end: dayKey(shiftDay(day, -KEEP_DAYS + 1)) })).keys()];
    // Cloudflare deletes at most 128 keys a call.
    for (let i = 0; i < old.length; i += DELETE_BATCH) await this.storage.delete(old.slice(i, i + DELETE_BATCH));
    return day;
  }

  private async add(request: Request): Promise<Response> {
    const { client, events } = (await request.json()) as { client: string; events: unknown[] };
    const day = await this.today();
    if (this.record.reports >= DAILY_REPORTS) return json(429, { error: 'budget' });
    const used = this.perClient.get(client) ?? 0;
    if (used >= REPORTS_PER_CLIENT) return json(429, { error: 'client' });
    this.perClient.set(client, used + 1);
    this.record.reports += 1;
    for (const event of new Set(events.filter(isUsageEvent))) this.record.counts[event] = (this.record.counts[event] ?? 0) + 1;
    await this.storage.put(dayKey(day), this.record);
    return new Response(null, { status: 204, headers: COMMON_HEADERS });
  }

  /** The last `n` days (today first), each with its counts; days nobody visited are left out. */
  private async days(n: number): Promise<Response> {
    const count = Number.isFinite(n) ? Math.min(SHOWN_DAYS, Math.max(1, Math.floor(n))) : SHOWN_DAYS;
    const today = await this.today();
    const saved = await this.storage.list<DayRecord>({ prefix: 'd:', start: dayKey(shiftDay(today, -(count - 1))) });
    saved.set(dayKey(today), this.record);
    const days: DayCounts[] = [...saved.entries()]
      .map(([key, record]) => ({ day: key.slice(2), reports: record.reports, counts: record.counts }))
      .filter((d) => d.day <= today && (d.reports > 0 || d.day === today))
      .sort((a, b) => (a.day < b.day ? 1 : -1));
    return json(200, { today, days });
  }
}

// ---- The Worker's side ------------------------------------------------------

function statsStub(env: UsageEnv): { fetch(request: Request): Promise<Response> } {
  const ns = env.USAGE_STATS as DurableNamespace;
  return ns.get(ns.idFromName('usage'));
}

/** The names of a report, checked: only known ones, each once. Null when it is not a report. */
export function parseReport(text: string): UsageEvent[] | null {
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return null;
  }
  const list = (body as { e?: unknown } | null)?.e;
  if (!Array.isArray(list)) return null;
  const events = [...new Set(list.filter(isUsageEvent))].slice(0, MAX_EVENTS);
  return events.length > 0 ? events : null;
}

async function report(request: Request, env: UsageEnv, origin: string): Promise<Response> {
  if (request.method !== 'POST') return json(405, { error: 'method' });
  // Browsers send Origin on every POST: only the app's own pages report.
  if (request.headers.get('Origin') !== origin) return json(403, { error: 'origin' });
  if (!env.USAGE_STATS) return json(503, { error: 'off' });
  const text = await request.text();
  if (text.length > MAX_REPORT_BODY) return json(413, { error: 'too-large' });
  const events = parseReport(text);
  if (!events) return json(400, { error: 'events' });
  const client = await sha256(request.headers.get('CF-Connecting-IP') ?? 'unknown');
  return statsStub(env).fetch(new Request('https://usage/add', { method: 'POST', body: JSON.stringify({ client, events }) }));
}

// ---- The page ---------------------------------------------------------------

function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c] ?? c);
}

const STYLE = `
:root { color-scheme: light dark; --ink: #1a1a1a; --muted: #5c6470; --line: #d9dde3; --bg: #fafafa; --accent: #c2410c; }
@media (prefers-color-scheme: dark) { :root { --ink: #eceff3; --muted: #9aa3ae; --line: #343a42; --bg: #14171b; --accent: #fb923c; } }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 15px/1.5 system-ui, sans-serif; }
main { max-width: 760px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { font-size: 22px; margin: 0 0 4px; }
h2 { font-size: 17px; margin: 32px 0 8px; }
p { color: var(--muted); margin: 0 0 16px; }
.wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { padding: 6px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; }
th:first-child, td:first-child { text-align: left; white-space: normal; min-width: 11em; }
@media (max-width: 480px) { body { font-size: 14px; } }
thead th { color: var(--muted); font-weight: 600; }
tr.lead td { font-weight: 700; }
form { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
label { width: 100%; font-weight: 600; }
input { flex: 1 1 220px; padding: 10px; font: inherit; border: 1px solid var(--line); border-radius: 8px; background: transparent; color: inherit; }
button { padding: 10px 18px; font: inherit; font-weight: 600; border: 0; border-radius: 8px; background: var(--accent); color: #fff; cursor: pointer; }
.error { color: var(--accent); }
`;

function page(status: number, body: string): Response {
  const html = `<!doctype html><html lang="ro"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex"><title>Motonor · statistici</title><style>${STYLE}</style></head><body><main>${body}</main></body></html>`;
  return new Response(html, { status, headers: PAGE_HEADERS });
}

function keyForm(note: string | null): string {
  return `<h1>Statistici de utilizare</h1>
<p>Câte vizite au folosit fiecare parte a aplicației, fără date despre cine.</p>
${note ? `<p class="error">${escapeHtml(note)}</p>` : ''}
<form method="post" action="/stats">
<label for="key">Cheia statisticilor</label>
<input id="key" name="key" type="password" autocomplete="current-password" required>
<button type="submit">Arată</button>
</form>`;
}

/** A period of the summary: the days from `fromAgo` back to `toAgo` days ago, both included. */
const PERIODS: ReadonlyArray<{ label: string; fromAgo: number; toAgo: number }> = [
  { label: 'Azi', fromAgo: 0, toAgo: 0 },
  { label: 'Ieri', fromAgo: 1, toAgo: 1 },
  { label: '7 zile', fromAgo: 0, toAgo: 6 },
  { label: '30 de zile', fromAgo: 0, toAgo: 29 },
  { label: '90 de zile', fromAgo: 0, toAgo: 89 },
];

/** The totals of each part of the app over each period, in the order of usageEvents.ts. */
export function summary(today: string, days: readonly DayCounts[]): Array<{ event: UsageEvent; label: string; totals: number[] }> {
  const sums = PERIODS.map((p) => {
    const newest = shiftDay(today, -p.fromAgo);
    const oldest = shiftDay(today, -p.toAgo);
    return days.filter((d) => d.day <= newest && d.day >= oldest);
  });
  return USAGE_EVENTS.map(([event, label]) => ({
    event,
    label,
    totals: sums.map((inPeriod) => inPeriod.reduce((total, d) => total + (d.counts[event] ?? 0), 0)),
  }));
}

function statsBody(today: string, days: readonly DayCounts[]): string {
  const rows = summary(today, days)
    .map(
      (row) =>
        `<tr${row.event === 'visit' ? ' class="lead"' : ''}><td>${escapeHtml(row.label)}</td>${row.totals.map((t) => `<td>${t}</td>`).join('')}</tr>`,
    )
    .join('');
  const daily = days
    .slice(0, 30)
    .map((d) => `<tr><td>${d.day}</td><td>${d.counts.visit ?? 0}</td><td>${d.counts['view:drum'] ?? 0}</td><td>${d.counts['ride:start'] ?? 0}</td></tr>`)
    .join('');
  return `<h1>Statistici de utilizare</h1>
<p>Câte vizite au folosit fiecare parte a aplicației: o vizită numără o parte cel mult o dată. Zilele sunt după ora României; azi e ${today}.</p>
<div class="wrap"><table><thead><tr><th>Ce</th>${PERIODS.map((p) => `<th>${p.label}</th>`).join('')}</tr></thead><tbody>${rows}</tbody></table></div>
<h2>Pe zile</h2>
<div class="wrap"><table><thead><tr><th>Ziua</th><th>Vizite</th><th>Drum</th><th>Ture pornite</th></tr></thead><tbody>${daily}</tbody></table></div>`;
}

/** STATS_KEY when it is set and long enough, else null (the page is off). */
function statsKey(env: UsageEnv): string | null {
  const key = env.STATS_KEY?.trim() ?? '';
  return key.length >= MIN_KEY_LENGTH ? key : null;
}

async function keyFromForm(request: Request): Promise<string | null> {
  const text = await request.text();
  if (text.length > MAX_FORM_BODY) return null;
  return new URLSearchParams(text).get('key');
}

async function stats(request: Request, env: UsageEnv): Promise<Response> {
  const key = statsKey(env);
  if (!key || !env.USAGE_STATS) {
    return page(503, '<h1>Statistici de utilizare</h1><p>Statisticile sunt oprite: Worker-ul nu are încă cheia sau locul unde se adună.</p>');
  }
  if (request.method === 'GET' || request.method === 'HEAD') return page(200, keyForm(null));
  if (request.method !== 'POST') return page(405, keyForm(null));
  const given = await keyFromForm(request);
  // Hashing both first makes the comparison take the same time whatever the guess.
  if (!given || !sameText(await sha256(given), await sha256(key))) return page(403, keyForm('Cheie greșită.'));
  const answer = await statsStub(env).fetch(new Request(`https://usage/days?n=${SHOWN_DAYS}`));
  if (!answer.ok) throw new Error(`usage store answered ${answer.status}`);
  const { today, days } = (await answer.json()) as { today: string; days: DayCounts[] };
  return page(200, statsBody(today, days));
}

/** Requests to /usage and /stats, or null for anything else. */
export async function handleUsage(request: Request, env: UsageEnv): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/usage' && url.pathname !== '/stats') return null;
  try {
    return url.pathname === '/usage' ? await report(request, env, url.origin) : await stats(request, env);
  } catch (err) {
    console.error('[usage] request failed', err);
    return url.pathname === '/usage' ? json(503, { error: 'store' }) : page(503, '<h1>Statistici de utilizare</h1><p>Nu am putut citi totalurile. Încearcă din nou.</p>');
  }
}
