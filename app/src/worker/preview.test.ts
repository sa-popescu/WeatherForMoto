import { describe, expect, it } from 'vitest';
import { handleLiveLink, type LiveState } from './live';
import { isPreviewRobot, livePreview, previewPage, routePreview } from './preview';
import { handle, type Env } from './shortLinks';

const ORIGIN = 'https://motonor.bluemouse.cc';
const WHATSAPP = 'WhatsApp/2.23.20.0 A';
const CHROME = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Mobile Safari/537.36';
const QUERY = 'r=1&t=2026-11-22T08:00&k=75&p=44.4268~26.1025~Bucure%C8%99ti&p=44.8565~24.8692~Pite%C8%99ti&p=45.7983~24.1256~Sibiu';
const ID = 'AbCdEfGhIjKlMnOpQrSt_-';

function asked(path: string, userAgent: string): Request {
  return new Request(`${ORIGIN}${path}`, { headers: { 'User-Agent': userAgent } });
}

describe('link previews', () => {
  it('knows the chat apps\' robots from people', () => {
    expect(isPreviewRobot(asked('/', WHATSAPP))).toBe(true);
    expect(isPreviewRobot(asked('/', 'TelegramBot (like TwitterBot)'))).toBe(true);
    expect(isPreviewRobot(asked('/', 'facebookexternalhit/1.1 Facebot Twitterbot/1.0'))).toBe(true);
    expect(isPreviewRobot(asked('/', CHROME))).toBe(false);
  });

  it('names a route by its ends, its stops and its departure', () => {
    const preview = routePreview(QUERY, `${ORIGIN}/#/traseu?${QUERY}`);
    expect(preview.title).toBe('Traseu București → Sibiu, cu o oprire pe drum');
    expect(preview.description).toContain('Plecare 22 nov, 08:00.');
    expect(routePreview(QUERY, '', 'en').title).toBe('Route București → Sibiu, with one stop on the way');
    expect(routePreview('r=gone', '').title).toBe('Traseu expirat');
  });

  it('names a live ride by where it goes and when it arrives, or says it ended', () => {
    const now = Date.UTC(2026, 10, 23, 10, 0);
    const state: LiveState = {
      expiresAt: now + 3600_000,
      pos: { lat: 45, lon: 25, speedKmh: 80, dest: 'Sibiu', etaMs: Date.UTC(2026, 10, 23, 12, 30), at: now },
      trail: [],
    };
    const preview = livePreview(state, '', 'ro', now);
    expect(preview.title).toBe('Tură în direct spre Sibiu');
    // 12:30 UTC is 14:30 in Bucharest in November.
    expect(preview.description).toContain('Sosire estimată la 14:30.');
    expect(livePreview(null, '').title).toBe('Tura s-a încheiat');
    expect(livePreview({ ...state, expiresAt: now - 1 }, '', 'ro', now).title).toBe('Tura s-a încheiat');
  });

  it('writes a page with the preview, safe from whatever a name holds', async () => {
    const page = previewPage({ title: 'Spre <script>"x"</script>', description: 'a & b', target: `${ORIGIN}/#/live/${ID}` }, new URL(`${ORIGIN}/l/${ID}`));
    const html = await page.text();
    expect(page.headers.get('Content-Type')).toContain('text/html');
    expect(html).not.toContain('<script>');
    expect(html).toContain('&lt;script&gt;&quot;x&quot;');
    expect(html).toContain(`<meta property="og:image" content="${ORIGIN}/og-image.jpg">`);
    expect(html).toContain(`<meta property="og:url" content="${ORIGIN}/l/${ID}">`);
    expect(html).toContain(`<meta http-equiv="refresh" content="0; url=${ORIGIN}/#/live/${ID}">`);
  });

  it('answers a short route link\'s robot with the route, and people with the redirect', async () => {
    const env: Env = {
      SHORT_LINKS: { get: async () => QUERY, put: async () => undefined },
      ASSETS: { fetch: async () => new Response('app') },
    };
    const robot = await handle(asked('/s/Abcd1234', WHATSAPP), env);
    expect(robot.status).toBe(200);
    expect(await robot.text()).toContain('Traseu București → Sibiu');
    const person = await handle(asked('/s/Abcd1234', CHROME), env);
    expect(person.status).toBe(302);
    expect(person.headers.get('Location')).toBe(`${ORIGIN}/#/traseu?${QUERY}`);
  });

  it('sends a live link on to the live page, and tells a robot the ride ended when it cannot be read', async () => {
    const person = await handleLiveLink(asked(`/l/${ID}`, CHROME), {});
    expect(person?.status).toBe(302);
    expect(person?.headers.get('Location')).toBe(`${ORIGIN}/#/live/${ID}`);
    const robot = await handleLiveLink(asked(`/l/${ID}?lang=en`, WHATSAPP), {});
    expect(await robot?.text()).toContain('The ride has ended');
    expect(await handleLiveLink(asked('/l/short', CHROME), {})).toBeNull();
    expect(await handleLiveLink(asked('/drum', CHROME), {})).toBeNull();
  });
});
