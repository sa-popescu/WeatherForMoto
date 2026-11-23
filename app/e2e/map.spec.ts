import { expect, test, type Page } from '@playwright/test';
import { fileURLToPath } from 'node:url';

// The radar map draws: a band of radar frames from the fixtures, each tile one
// rain disc in a colour of RainViewer's table, has to come out as rain on the
// map. This goes through what unit tests cannot: reading the tiles in the
// worker, the WebGL layer and its shader. The base map is a plain dark
// background, and every other outside source is refused, as in the layout checks.

const FIXTURES = fileURLToPath(new URL('./fixtures/', import.meta.url));
const FIXTURE_TIME = new Date('2026-11-23T10:00:00Z');
const FRAMES = 13;

/** The newest frame five minutes before the fixture time, then every ten minutes back. */
function radarIndex(): object {
  const newest = Math.floor(FIXTURE_TIME.getTime() / 1000) - 300;
  const past = Array.from({ length: FRAMES }, (_, i) => ({ time: newest - (FRAMES - 1 - i) * 600, path: `/v2/radar/fixture${i}` }));
  return { version: '2.0', generated: newest, host: 'https://tilecache.rainviewer.com', radar: { past, nowcast: [] } };
}

const PLAIN_STYLE = {
  version: 8,
  sources: {},
  layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#000000' } }],
};

async function prepare(page: Page): Promise<void> {
  await page.clock.setFixedTime(FIXTURE_TIME);
  await page.addInitScript(() => {
    localStorage.setItem('motonorInstallInviteV1', JSON.stringify({ shown: 2, lastAt: Date.now() }));
  });
  // On the context, so the reading worker's requests are answered too.
  await page.context().route('**/*', async (route) => {
    const url = new URL(route.request().url());
    if (url.hostname === '127.0.0.1') return route.continue();
    if (url.hostname.endsWith('.run.app')) {
      if (url.pathname === '/weather') return route.fulfill({ path: `${FIXTURES}weather.json`, contentType: 'application/json' });
      if (url.pathname === '/meta/scoring') return route.fulfill({ path: `${FIXTURES}scoring.json`, contentType: 'application/json' });
      return route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"not in the map check"}' });
    }
    const cors = { 'access-control-allow-origin': '*' };
    if (url.hostname === 'tiles.openfreemap.org' && url.pathname.startsWith('/styles/')) {
      return route.fulfill({ json: PLAIN_STYLE, headers: cors });
    }
    if (url.hostname === 'api.rainviewer.com') return route.fulfill({ json: radarIndex(), headers: cors });
    if (url.hostname === 'tilecache.rainviewer.com') return route.fulfill({ path: `${FIXTURES}radar-tile.png`, contentType: 'image/png', headers: cors });
    return route.abort();
  });
}

/** Share of the map's pixels that are rain blue: far more blue than red, and bright enough to be drawn rain. */
async function rainShare(page: Page): Promise<number> {
  const shot = await page.locator('.map-canvas').screenshot();
  return page.evaluate(async (base64) => {
    const image = new Image();
    image.src = `data:image/png;base64,${base64}`;
    await image.decode();
    const canvas = document.createElement('canvas');
    canvas.width = image.width;
    canvas.height = image.height;
    const ctx = canvas.getContext('2d');
    if (!ctx) return 0;
    ctx.drawImage(image, 0, 0);
    const { data } = ctx.getImageData(0, 0, image.width, image.height);
    let blue = 0;
    for (let i = 0; i < data.length; i += 4) if (data[i + 2] > 90 && data[i + 2] > data[i] + 50) blue += 1;
    return blue / (data.length / 4);
  }, shot.toString('base64'));
}

test('the radar map draws the rain of the frame on screen', async ({ page }, info) => {
  test.skip(info.project.name !== 'desktop', 'one size is enough to see the drawing');
  const errors: string[] = [];
  page.on('pageerror', (err) => errors.push(err.message));
  page.on('console', (message) => {
    // Satellite clouds and lightning are refused here; the radar and the layers must work.
    if (/\[map\] (radar frame could not be read|the weather layer .* cannot be drawn|the .* layer could not be added)/.test(message.text())) errors.push(message.text());
  });
  await prepare(page);
  await page.goto('/#/harta');

  const scrubber = page.locator('.map-scrub input.map-range');
  await scrubber.waitFor();
  const pause = page.getByRole('button', { name: 'Oprește animația radarului' });
  if (await pause.isVisible()) await pause.click();
  // The newest radar frame: the last of the observed part of the band.
  await scrubber.fill(String(FRAMES - 1));

  await expect.poll(() => rainShare(page), { timeout: 20_000 }).toBeGreaterThan(0.05);
  expect(errors).toEqual([]);
});
