import { describe, expect, it } from 'vitest';
import { serveFile } from '../worker/staleAssets';
import { isStaleFileError } from './staleBuild';

const ORIGIN = 'https://motonor.bluemouse.cc';

/** Static files as the Worker sees them: "/" and one script, 404 for the rest. */
const assets = {
  fetch: async (request: Request): Promise<Response> => {
    const { pathname } = new URL(request.url);
    if (pathname === '/') return new Response('app', { headers: { 'Content-Type': 'text/html' } });
    if (pathname === '/assets/index-new.js') return new Response('js', { headers: { 'Content-Type': 'text/javascript' } });
    return new Response(null, { status: 404 });
  },
};

describe('a file of an older build', () => {
  it('is told apart by the messages browsers give', () => {
    // Safari and Edge on iOS, when the server sent the app's page instead.
    expect(isStaleFileError(new TypeError("'text/html' is not a valid JavaScript MIME type for module script 'https://x/assets/DriveScreen-B_gcvzpl.js'."))).toBe(true);
    expect(isStaleFileError(new TypeError('Failed to fetch dynamically imported module: https://x/assets/a.js'))).toBe(true);
    expect(isStaleFileError(new TypeError('Importing a module script failed.'))).toBe(true);
    expect(isStaleFileError(new Error('error loading dynamically imported module'))).toBe(true);
    expect(isStaleFileError(new Error('Unable to preload CSS for /assets/a.css'))).toBe(true);
    expect(isStaleFileError(new TypeError("Cannot read properties of undefined (reading 'lat')"))).toBe(false);
    expect(isStaleFileError(null)).toBe(false);
  });

  it('gets a plain 404 nobody keeps, not the app page', async () => {
    const missing = await serveFile(new Request(`${ORIGIN}/assets/DriveScreen-B_gcvzpl.js`), assets);
    expect(missing.status).toBe(404);
    expect(missing.headers.get('Cache-Control')).toBe('no-store');
    expect(await missing.text()).toBe('Not found');
  });

  it('leaves the files that exist alone, and opens the app for any other page', async () => {
    expect(await (await serveFile(new Request(`${ORIGIN}/assets/index-new.js`), assets)).text()).toBe('js');
    const page = await serveFile(new Request(`${ORIGIN}/acum`), assets);
    expect(page.status).toBe(200);
    expect(await page.text()).toBe('app');
    expect((await serveFile(new Request(`${ORIGIN}/nothing`, { method: 'POST' }), assets)).status).toBe(404);
  });
});
