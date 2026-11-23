import type { AssetFetcher } from './shortLinks';

// The static files, for every request the Worker does not answer itself.
// wrangler.jsonc sets not_found_handling to "none", so a request that matches
// no file reaches the Worker, and this decides what it gets:
// - under /assets/, a plain 404 that nobody keeps. Those names change with
//   every build, so this is a file of another build, usually an older one a
//   page loaded before a deploy still asks for. The single-page fallback used
//   to answer it with the app's page as a 200, under the year-long immutable
//   cache public/_headers sets for /assets/*: that browser then kept the page
//   in place of the script, and the old page could not load the screen again.
//   A 404 lets the app load the new build instead (lib/staleBuild.ts).
// - any other page: the app itself, as the fallback did, so an old or mistyped
//   address still opens Motonor.

export function staleAsset(pathname: string): Response | null {
  if (!pathname.startsWith('/assets/')) return null;
  return new Response('Not found', {
    status: 404,
    headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' },
  });
}

export async function serveFile(request: Request, assets: AssetFetcher): Promise<Response> {
  const response = await assets.fetch(request);
  if (response.status !== 404) return response;
  const { pathname } = new URL(request.url);
  const stale = staleAsset(pathname);
  if (stale) return stale;
  if (request.method !== 'GET' && request.method !== 'HEAD') return response;
  return assets.fetch(new Request(new URL('/', request.url), { method: request.method, headers: request.headers }));
}
