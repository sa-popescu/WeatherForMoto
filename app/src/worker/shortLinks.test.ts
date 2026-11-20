import { describe, expect, it, vi } from 'vitest';
import { GONE_QUERY, handle, isRouteQuery, LINK_TTL_S, newId, type Env } from './shortLinks';

const ORIGIN = 'https://weatherformoto.bluemouse.cc';
const QUERY = 'r=1&t=2026-11-21T11:00&k=75&p=44.4335~26.0411~Militari&p=44.5343~26.187~Cre%C8%9Buleasca&n=3&l=ktmvIil--EyD9O0';

function fakeEnv() {
  const store = new Map<string, string>();
  const put = vi.fn(async (key: string, value: string) => {
    store.set(key, value);
  });
  const assets = vi.fn(async () => new Response('app'));
  const env: Env = { SHORT_LINKS: { get: async (key) => store.get(key) ?? null, put }, ASSETS: { fetch: assets } };
  return { env, store, put, assets };
}

const post = (body: unknown, origin: string | null = ORIGIN): Request =>
  new Request(`${ORIGIN}/s`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(origin ? { Origin: origin } : {}) },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });

describe('short links', () => {
  it('stores a plan for 90 days and opens it in the route tab', async () => {
    const { env, store, put } = fakeEnv();
    const created = await handle(post({ q: QUERY }), env);
    expect(created.status).toBe(201);
    const { url } = (await created.json()) as { url: string };
    expect(url).toMatch(new RegExp(`^${ORIGIN}/s/[A-Za-z0-9]{8}$`));
    expect(put).toHaveBeenCalledWith(url.slice(-8), QUERY, { expirationTtl: LINK_TTL_S });
    expect(store.size).toBe(1);

    const opened = await handle(new Request(url), env);
    expect(opened.status).toBe(302);
    expect(opened.headers.get('Location')).toBe(`${ORIGIN}/#/traseu?${QUERY}`);
  });

  it('sends an expired or made up link to a page that says so', async () => {
    const { env } = fakeEnv();
    for (const path of ['/s/Abcd1234', '/s/not-an-id']) {
      const res = await handle(new Request(`${ORIGIN}${path}`), env);
      expect(res.headers.get('Location')).toBe(`${ORIGIN}/#/traseu?${GONE_QUERY}`);
    }
  });

  it('stores only route plans, and only from the app itself', async () => {
    const { env, put } = fakeEnv();
    expect((await handle(post({ q: 'https://evil.example' }), env)).status).toBe(400);
    expect((await handle(post({ q: 'r=1&p=<script>' }), env)).status).toBe(400);
    expect((await handle(post('not json'), env)).status).toBe(400);
    expect((await handle(post({ q: `r=1&l=${'A'.repeat(20_000)}` }), env)).status).toBe(413);
    expect((await handle(post({ q: QUERY }, 'https://other.example'), env)).status).toBe(403);
    expect((await handle(new Request(`${ORIGIN}/s`), env)).status).toBe(405);
    expect(put).not.toHaveBeenCalled();
    // No Origin header (not a browser): the shape check still applies.
    expect((await handle(post({ q: QUERY }, null), env)).status).toBe(201);
  });

  it('leaves every other path to the static app', async () => {
    const { env, assets } = fakeEnv();
    const res = await handle(new Request(`${ORIGIN}/s/a/b`), env);
    expect(await res.text()).toBe('app');
    expect(assets).toHaveBeenCalledTimes(1);
  });

  it('makes ids of letters and digits', () => {
    const ids = new Set(Array.from({ length: 200 }, () => newId()));
    expect(ids.size).toBe(200);
    for (const id of ids) expect(id).toMatch(/^[A-Za-z0-9]{8}$/);
    expect(isRouteQuery(QUERY)).toBe(true);
    expect(isRouteQuery('r=2&p=1')).toBe(false);
  });
});
