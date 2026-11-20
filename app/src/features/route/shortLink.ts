import { SHARE_BASE_URL } from '../../lib/placeLink';
import { ROUTE_TAB_HASH } from './routeLink';

// A long route link (an imported GPX road makes it run to a few thousand
// characters) is swapped for a short one before it is shared:
// https://weatherformoto.bluemouse.cc/s/K7f3qXa2, kept for 90 days by the
// Worker in front of the app (src/worker/shortLinks.ts). A short link is not
// worth a server round trip; when the Worker cannot be reached (offline, a
// development server) the long link is shared, as before.

/** Links up to this long are shared as they are: a few stops and no imported road. */
export const SHORTEN_ABOVE = 300;
/**
 * The share sheet must open soon after the tap (Safari allows about 5 s), so
 * the Worker gets this long before the long link goes out instead.
 */
const TIMEOUT_MS = 3000;
const SHORTEN_URL = `${SHARE_BASE_URL}s`;
const SHORT_PREFIX = `${SHARE_BASE_URL}s/`;

/** The plan part of a route link (after "#/traseu?"), or null when it is not one. */
export function queryOf(link: string): string | null {
  const marker = `${ROUTE_TAB_HASH}?`;
  const at = link.indexOf(marker);
  return at < 0 ? null : link.slice(at + marker.length);
}

/** The link to share: a short one for a long plan, or `link` itself when it is short or the Worker fails. */
export async function shortRouteLink(link: string): Promise<string> {
  if (link.length <= SHORTEN_ABOVE) return link;
  const q = queryOf(link);
  if (!q) return link;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(SHORTEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ q }),
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = (await res.json()) as { url?: unknown };
    if (typeof body.url !== 'string' || !body.url.startsWith(SHORT_PREFIX)) throw new Error('unexpected answer');
    return body.url;
  } catch (err) {
    console.warn('[route] short link failed, sharing the full link', err);
    return link;
  } finally {
    clearTimeout(timer);
  }
}
