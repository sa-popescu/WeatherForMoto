// The old address sends visitors to the new one (the move itself, with the
// phone's data, is app/src/features/move/move.ts).
//
// Only page loads are redirected. A phone that already used the old address
// has the app in its service worker, which answers page loads from its cache
// without asking the network, so it still opens there and shows the card that
// moves its settings and rides across. Everything else (the service worker's
// own updates, scripts, icons, short link POSTs from that cached app) is
// served as before, so those phones keep working until they move.

export const OLD_HOST = 'weatherformoto.bluemouse.cc';
export const NEW_ORIGIN = 'https://motonor.bluemouse.cc';

/** A browser loading a page, as opposed to a script, image or fetch. */
function isPageLoad(request: Request): boolean {
  if (request.method !== 'GET' && request.method !== 'HEAD') return false;
  const mode = request.headers.get('Sec-Fetch-Mode');
  if (mode !== null) return mode === 'navigate';
  // Clients without Fetch Metadata (crawlers, older browsers): go by what they accept.
  return (request.headers.get('Accept') ?? '').includes('text/html');
}

/** A permanent redirect to the same path and query on the new address, or null to serve the request. */
export function movedRedirect(request: Request): Response | null {
  const url = new URL(request.url);
  if (url.hostname !== OLD_HOST || !isPageLoad(request)) return null;
  // The fragment (#/drum, #/traseu?...) never reaches the server; browsers keep it across the redirect.
  return new Response(null, {
    status: 301,
    headers: { Location: `${NEW_ORIGIN}${url.pathname}${url.search}`, 'Cache-Control': 'public, max-age=86400' },
  });
}
