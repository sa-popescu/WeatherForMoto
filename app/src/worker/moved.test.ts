import { describe, expect, it } from 'vitest';
import { movedRedirect, NEW_ORIGIN } from './moved';
import worker from './index';
import type { Env } from './shortLinks';

const OLD = 'https://weatherformoto.bluemouse.cc';

const request = (url: string, headers: Record<string, string>, method = 'GET') => new Request(url, { method, headers });
const pageLoad = (url: string) => request(url, { 'Sec-Fetch-Mode': 'navigate', Accept: 'text/html' });

describe('the old address', () => {
  it('sends page loads to the same path and query on the new one, for good', () => {
    const res = movedRedirect(pageLoad(`${OLD}/?p=44.43~26.1~Acasa`));
    expect(res?.status).toBe(301);
    expect(res?.headers.get('Location')).toBe(`${NEW_ORIGIN}/?p=44.43~26.1~Acasa`);
    expect(movedRedirect(pageLoad(`${OLD}/s/K7f3qXa2`))?.headers.get('Location')).toBe(`${NEW_ORIGIN}/s/K7f3qXa2`);
    expect(movedRedirect(pageLoad(`${OLD}/privacy-policy.html`))?.headers.get('Location')).toBe(`${NEW_ORIGIN}/privacy-policy.html`);
  });

  it('redirects a crawler without Fetch Metadata that asks for a page', () => {
    expect(movedRedirect(request(`${OLD}/`, { Accept: 'text/html,application/xhtml+xml' }))?.status).toBe(301);
  });

  it('keeps serving what a phone with the cached app still fetches there', () => {
    // The service worker's update, its precache of the shell, scripts and a short link POST.
    expect(movedRedirect(request(`${OLD}/sw.js`, { 'Sec-Fetch-Mode': 'same-origin', 'Sec-Fetch-Dest': 'serviceworker' }))).toBeNull();
    expect(movedRedirect(request(`${OLD}/index.html?__WB_REVISION__=abc`, { 'Sec-Fetch-Mode': 'same-origin', Accept: '*/*' }))).toBeNull();
    expect(movedRedirect(request(`${OLD}/assets/index-abc.js`, { 'Sec-Fetch-Mode': 'cors' }))).toBeNull();
    expect(movedRedirect(request(`${OLD}/s`, { 'Sec-Fetch-Mode': 'cors', Origin: OLD }, 'POST'))).toBeNull();
  });

  it('leaves the new address alone', () => {
    expect(movedRedirect(pageLoad(`${NEW_ORIGIN}/`))).toBeNull();
  });

  it('runs before the short links and the static app in the Worker', async () => {
    let served = 0;
    const env: Env = { ASSETS: { fetch: async () => { served += 1; return new Response('app'); } } };
    const moved = await worker.fetch(pageLoad(`${OLD}/`), env);
    expect(moved.status).toBe(301);
    expect(served).toBe(0);
    const here = await worker.fetch(pageLoad(`${NEW_ORIGIN}/`), env);
    expect(await here.text()).toBe('app');
    expect(served).toBe(1);
  });
});
