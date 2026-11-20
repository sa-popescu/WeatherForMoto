import { afterEach, describe, expect, it, vi } from 'vitest';
import { SHARE_BASE_URL } from '../../lib/placeLink';
import { queryOf, SHORTEN_ABOVE, shortRouteLink } from './shortLink';

const SHORT = `${SHARE_BASE_URL}#/traseu?r=1&p=44.43~26.1~A&p=45.8~24.13~B`;
const LONG = `${SHARE_BASE_URL}#/traseu?r=1&p=44.43~26.1~A&p=45.8~24.13~B&n=200&l=${'Ab9_'.repeat(100)}`;

afterEach(() => vi.unstubAllGlobals());

describe('short route links', () => {
  it('reads the plan out of a route link', () => {
    expect(queryOf(SHORT)).toBe('r=1&p=44.43~26.1~A&p=45.8~24.13~B');
    expect(queryOf(`${SHARE_BASE_URL}?q=Sibiu`)).toBeNull();
  });

  it('shares a short link as it is, without asking the server', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    expect(SHORT.length).toBeLessThanOrEqual(SHORTEN_ABOVE);
    expect(await shortRouteLink(SHORT)).toBe(SHORT);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('swaps a long link for the short one the Worker hands back', async () => {
    const fetchMock = vi.fn(async () => Response.json({ url: `${SHARE_BASE_URL}s/K7f3qXa2` }, { status: 201 }));
    vi.stubGlobal('fetch', fetchMock);
    expect(await shortRouteLink(LONG)).toBe(`${SHARE_BASE_URL}s/K7f3qXa2`);
    const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(JSON.parse(String(init.body))).toEqual({ q: queryOf(LONG) });
  });

  it('falls back to the long link when the Worker fails or answers oddly', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('nope', { status: 503 })));
    expect(await shortRouteLink(LONG)).toBe(LONG);
    vi.stubGlobal('fetch', vi.fn(async () => Response.json({ url: 'https://evil.example/x' })));
    expect(await shortRouteLink(LONG)).toBe(LONG);
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw new TypeError('offline');
    }));
    expect(await shortRouteLink(LONG)).toBe(LONG);
  });
});
