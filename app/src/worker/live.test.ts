import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DAILY_SHARES,
  handleLive,
  LiveBudget,
  LiveRide,
  MIN_UPDATE_MS,
  parseUpdate,
  SHARE_TTL_MS,
  SHARES_PER_CLIENT,
  type DurableNamespace,
  type DurableState,
  type DurableStorage,
  type LiveEnv,
  type LiveState,
} from './live';

const ORIGIN = 'https://motonor.bluemouse.cc';

class FakeStorage implements DurableStorage {
  readonly data = new Map<string, unknown>();
  alarm: number | null = null;
  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.data.get(key)) as T | undefined;
  }
  async put<T>(key: string, value: T): Promise<void> {
    this.data.set(key, structuredClone(value));
  }
  async deleteAll(): Promise<void> {
    this.data.clear();
  }
  async setAlarm(at: number): Promise<void> {
    this.alarm = at;
  }
  async deleteAlarm(): Promise<void> {
    this.alarm = null;
  }
}

type DurableObject = { fetch(request: Request): Promise<Response> };

function namespace(make: (state: DurableState) => DurableObject): DurableNamespace & { objects: Map<string, DurableObject> } {
  const objects = new Map<string, DurableObject>();
  return {
    objects,
    idFromName: (name: string) => name,
    get: (id: unknown) => {
      const name = String(id);
      if (!objects.has(name)) objects.set(name, make({ storage: new FakeStorage() }));
      return objects.get(name)!;
    },
  };
}

function env(): LiveEnv & { LIVE_RIDES: ReturnType<typeof namespace> } {
  return {
    LIVE_RIDES: namespace((state) => new LiveRide(state)),
    LIVE_BUDGET: namespace((state) => new LiveBudget(state)),
  };
}

function call(e: LiveEnv, method: string, path: string, init: { body?: unknown; key?: string; ip?: string; origin?: string } = {}) {
  const headers: Record<string, string> = { 'CF-Connecting-IP': init.ip ?? '1.2.3.4' };
  if (init.key) headers.Authorization = `Bearer ${init.key}`;
  if (method === 'POST') headers.Origin = init.origin ?? ORIGIN;
  const body = init.body === undefined ? undefined : JSON.stringify(init.body);
  return handleLive(new Request(`${ORIGIN}${path}`, { method, headers, body }), e) as Promise<Response>;
}

async function start(e: LiveEnv, ip?: string): Promise<{ id: string; key: string; expiresAt: number }> {
  const response = await call(e, 'POST', '/live', { ip });
  expect(response.status).toBe(201);
  return response.json() as Promise<{ id: string; key: string; expiresAt: number }>;
}

const HERE = { lat: 45.6, lon: 24.6, speedKmh: 62, dest: 'Bâlea Lac', etaMs: 0 };

describe('live location', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-11-28T09:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('starts a share with a link id and a separate key', async () => {
    const e = env();
    const share = await start(e);
    expect(share.id).toMatch(/^[A-Za-z0-9_-]{22}$/);
    expect(share.key).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(share.expiresAt).toBe(Date.now() + SHARE_TTL_MS);
    const state = (await (await call(e, 'GET', `/live/${share.id}`)).json()) as LiveState;
    expect(state.pos).toBeNull();
  });

  it('moves the position with the key, and only with it', async () => {
    const e = env();
    const { id, key } = await start(e);
    expect((await call(e, 'PUT', `/live/${id}`, { body: HERE })).status).toBe(401);
    const other = 'x'.repeat(43);
    expect((await call(e, 'PUT', `/live/${id}`, { body: HERE, key: other })).status).toBe(403);
    expect((await call(e, 'PUT', `/live/${id}`, { body: HERE, key })).status).toBe(204);
    const state = (await (await call(e, 'GET', `/live/${id}`)).json()) as LiveState;
    expect(state.pos).toMatchObject({ lat: 45.6, lon: 24.6, speedKmh: 62, dest: 'Bâlea Lac', etaMs: null });
    expect(state.trail).toEqual([[45.6, 24.6]]);
  });

  it('refuses updates faster than the phone sends them', async () => {
    const e = env();
    const { id, key } = await start(e);
    await call(e, 'PUT', `/live/${id}`, { body: HERE, key });
    expect((await call(e, 'PUT', `/live/${id}`, { body: HERE, key })).status).toBe(429);
    vi.advanceTimersByTime(MIN_UPDATE_MS);
    expect((await call(e, 'PUT', `/live/${id}`, { body: { ...HERE, lat: 45.61 }, key })).status).toBe(204);
    const state = (await (await call(e, 'GET', `/live/${id}`)).json()) as LiveState;
    expect(state.trail).toHaveLength(2);
  });

  it('deletes everything when the ride ends', async () => {
    const e = env();
    const { id, key } = await start(e);
    await call(e, 'PUT', `/live/${id}`, { body: HERE, key });
    expect((await call(e, 'DELETE', `/live/${id}`, { key })).status).toBe(204);
    expect((await call(e, 'GET', `/live/${id}`)).status).toBe(404);
  });

  it('is gone after 12 hours, and the alarm clears it', async () => {
    const e = env();
    const { id } = await start(e);
    vi.advanceTimersByTime(SHARE_TTL_MS);
    expect((await call(e, 'GET', `/live/${id}`)).status).toBe(404);
    const ride = e.LIVE_RIDES.objects.get(id) as LiveRide;
    await ride.alarm();
  });

  it('lets one address share a few rides a day, and everyone together a few more', async () => {
    const e = env();
    for (let i = 0; i < SHARES_PER_CLIENT; i += 1) await start(e, '9.9.9.9');
    const refused = await call(e, 'POST', '/live', { ip: '9.9.9.9' });
    expect(refused.status).toBe(429);
    expect(await refused.json()).toEqual({ error: 'client' });
    for (let i = SHARES_PER_CLIENT; i < DAILY_SHARES; i += 1) await start(e, `10.0.0.${i}`);
    expect(((await (await call(e, 'POST', '/live', { ip: '10.1.1.1' })).json()) as { error: string }).error).toBe('budget');
    vi.advanceTimersByTime(24 * 3600 * 1000);
    expect((await call(e, 'POST', '/live', { ip: '10.1.1.1' })).status).toBe(201);
  });

  it('only lets the app start sharing', async () => {
    expect((await call(env(), 'POST', '/live', { origin: 'https://evil.example' })).status).toBe(403);
  });

  it('answers 503 without the bindings, and leaves other paths alone', async () => {
    expect((await call({}, 'POST', '/live')).status).toBe(503);
    expect(await handleLive(new Request(`${ORIGIN}/s/abc`), env())).toBeNull();
    expect((await call(env(), 'GET', '/live/not-an-id')).status).toBe(404);
  });

  it('checks and rounds what the phone sends', () => {
    const now = Date.now();
    expect(parseUpdate({ lat: 91, lon: 0 }, now)).toBeNull();
    expect(parseUpdate({ lat: '45', lon: 24 }, now)).toBeNull();
    expect(parseUpdate({ lat: 45.123456789, lon: 24.987654321, speedKmh: 999, dest: ' x ', etaMs: now + 3600_000 }, now)).toEqual({
      lat: 45.12346,
      lon: 24.98765,
      speedKmh: null,
      dest: 'x',
      etaMs: now + 3600_000,
    });
  });
});
