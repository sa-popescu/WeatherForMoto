import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { USAGE_EVENTS } from '../lib/usageEvents';
import type { DurableNamespace } from './live';
import {
  dayOf,
  DAILY_REPORTS,
  handleUsage,
  KEEP_DAYS,
  parseReport,
  REPORTS_PER_CLIENT,
  shiftDay,
  summary,
  UsageStats,
  type DayCounts,
  type UsageEnv,
  type UsageStorage,
} from './usage';

const ORIGIN = 'https://motonor.bluemouse.cc';
const KEY = 'a-long-random-key-for-the-stats-page-0123';

class FakeStorage implements UsageStorage {
  readonly data = new Map<string, unknown>();
  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.data.get(key)) as T | undefined;
  }
  async put<T>(key: string, value: T): Promise<void> {
    this.data.set(key, structuredClone(value));
  }
  async list<T>({ prefix, start, end }: { prefix: string; start?: string; end?: string }): Promise<Map<string, T>> {
    const keys = [...this.data.keys()].filter((k) => k.startsWith(prefix) && (start === undefined || k >= start) && (end === undefined || k < end)).sort();
    return new Map(keys.map((k) => [k, structuredClone(this.data.get(k)) as T]));
  }
  async delete(keys: string[]): Promise<number> {
    let deleted = 0;
    for (const key of keys) if (this.data.delete(key)) deleted += 1;
    return deleted;
  }
}

function setup(withKey = true): { env: UsageEnv; storage: FakeStorage } {
  const storage = new FakeStorage();
  let object: UsageStats | null = null;
  const ns: DurableNamespace = {
    idFromName: (name: string) => name,
    get: () => {
      object ??= new UsageStats({ storage });
      return object;
    },
  };
  return { env: { USAGE_STATS: ns, STATS_KEY: withKey ? KEY : undefined }, storage };
}

function reportRequest(events: unknown, { origin = ORIGIN, ip = '203.0.113.7' }: { origin?: string | null; ip?: string } = {}): Request {
  const headers: Record<string, string> = { 'Content-Type': 'text/plain;charset=UTF-8', 'CF-Connecting-IP': ip };
  if (origin) headers.Origin = origin;
  return new Request(`${ORIGIN}/usage`, { method: 'POST', headers, body: JSON.stringify({ e: events }) });
}

async function send(env: UsageEnv, events: unknown, options?: { origin?: string | null; ip?: string }): Promise<number> {
  return (await handleUsage(reportRequest(events, options), env))!.status;
}

function statsRequest(key: string | null): Request {
  if (key === null) return new Request(`${ORIGIN}/stats`);
  return new Request(`${ORIGIN}/stats`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ key }).toString(),
  });
}

describe('usage counts', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // Noon in Bucharest.
    vi.setSystemTime(new Date('2026-11-24T10:00:00Z'));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('counts each known part once per report and drops unknown names', async () => {
    const { env, storage } = setup();
    expect(await send(env, ['visit', 'view:drum', 'view:drum', 'made-up', 42])).toBe(204);
    expect(await send(env, ['visit', 'ride:start'], { ip: '198.51.100.1' })).toBe(204);
    expect(storage.data.get('d:2026-11-24')).toEqual({ reports: 2, counts: { visit: 2, 'view:drum': 1, 'ride:start': 1 } });
  });

  it('refuses reports from other sites, empty ones and other methods', async () => {
    const { env, storage } = setup();
    expect(await send(env, ['visit'], { origin: 'https://example.com' })).toBe(403);
    expect(await send(env, ['visit'], { origin: null })).toBe(403);
    expect(await send(env, ['made-up'])).toBe(400);
    expect((await handleUsage(new Request(`${ORIGIN}/usage`), env))!.status).toBe(405);
    expect(storage.data.size).toBe(0);
  });

  it('answers 503 without the binding, and ignores other paths', async () => {
    expect(await send({}, ['visit'])).toBe(503);
    expect(await handleUsage(new Request(`${ORIGIN}/live`), {})).toBeNull();
  });

  it('caps the reports of one address and of the whole day', async () => {
    const { env } = setup();
    for (let i = 0; i < REPORTS_PER_CLIENT; i += 1) expect(await send(env, ['visit'])).toBe(204);
    expect(await send(env, ['visit'])).toBe(429);
    expect(await send(env, ['visit'], { ip: '198.51.100.2' })).toBe(204);
    expect(DAILY_REPORTS).toBeGreaterThan(REPORTS_PER_CLIENT);
  });

  it('starts a new day after midnight in Romania and deletes days past the limit', async () => {
    const { env, storage } = setup();
    const ancient = shiftDay('2026-11-25', -KEEP_DAYS);
    const kept = shiftDay('2026-11-25', -KEEP_DAYS + 1);
    await storage.put(`d:${ancient}`, { reports: 1, counts: { visit: 1 } });
    await storage.put(`d:${kept}`, { reports: 1, counts: { visit: 1 } });
    await send(env, ['visit']);
    // 22:30 UTC is already the next day in Bucharest (UTC+2 in winter).
    vi.setSystemTime(new Date('2026-11-24T22:30:00Z'));
    for (let i = 0; i < REPORTS_PER_CLIENT; i += 1) expect(await send(env, ['visit'])).toBe(204);
    expect(storage.data.get('d:2026-11-25')).toEqual({ reports: REPORTS_PER_CLIENT, counts: { visit: REPORTS_PER_CLIENT } });
    expect(storage.data.get('d:2026-11-24')).toEqual({ reports: 1, counts: { visit: 1 } });
    expect(storage.data.has(`d:${ancient}`)).toBe(false);
    expect(storage.data.has(`d:${kept}`)).toBe(true);
  });

  it('shows the form, refuses a wrong key and shows the totals for the right one', async () => {
    const { env } = setup();
    await send(env, ['visit', 'view:drum']);
    const form = (await handleUsage(statsRequest(null), env))!;
    expect(form.status).toBe(200);
    expect(form.headers.get('Content-Security-Policy')).toContain("default-src 'none'");
    expect(await form.text()).toContain('type="password"');
    expect((await handleUsage(statsRequest('wrong'), env))!.status).toBe(403);
    const shown = (await handleUsage(statsRequest(KEY), env))!;
    expect(shown.status).toBe(200);
    const html = await shown.text();
    expect(html).toContain('Ecran: Drum');
    expect(html).toContain('2026-11-24');
    expect(html).not.toContain(KEY);
  });

  it('keeps the page off without a key, or with a short one', async () => {
    expect((await handleUsage(statsRequest(null), setup(false).env))!.status).toBe(503);
    const short = setup();
    short.env.STATS_KEY = 'short';
    expect((await handleUsage(statsRequest('short'), short.env))!.status).toBe(503);
  });
});

describe('usage helpers', () => {
  it('reads only the known names of a report, each once', () => {
    expect(parseReport('{"e":["visit","visit","nope"]}')).toEqual(['visit']);
    expect(parseReport('{"e":[]}')).toBeNull();
    expect(parseReport('not json')).toBeNull();
    expect(parseReport('{"e":"visit"}')).toBeNull();
  });

  it('gives the day in Romania and moves it by calendar days', () => {
    expect(dayOf(Date.parse('2026-11-24T21:59:00Z'))).toBe('2026-11-24');
    expect(dayOf(Date.parse('2026-11-24T22:01:00Z'))).toBe('2026-11-25');
    expect(shiftDay('2026-03-01', -1)).toBe('2026-02-28');
    expect(shiftDay('2026-12-31', 1)).toBe('2027-01-01');
  });

  it('adds the days of each period', () => {
    const days: DayCounts[] = [
      { day: '2026-11-24', reports: 2, counts: { visit: 2 } },
      { day: '2026-11-23', reports: 3, counts: { visit: 3, 'ride:start': 1 } },
      { day: '2026-11-10', reports: 5, counts: { visit: 5 } },
    ];
    const rows = summary('2026-11-24', days);
    expect(rows).toHaveLength(USAGE_EVENTS.length);
    expect(rows.find((r) => r.event === 'visit')!.totals).toEqual([2, 3, 5, 10, 10]);
    expect(rows.find((r) => r.event === 'ride:start')!.totals).toEqual([0, 1, 1, 1, 1]);
  });
});
