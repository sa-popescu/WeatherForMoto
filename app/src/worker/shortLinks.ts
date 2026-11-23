// Short links for shared ride plans, run by the Cloudflare Worker in front of
// the static app (wrangler.jsonc at the repo root). Only /s and /s/* reach
// this code; every other request is served as a static file.
//
//   POST /s       {"q": "r=1&t=...&p=...&n=...&l=..."}  ->  201 {"url": "https://<host>/s/K7f3qXa2"}
//   GET  /s/<id>                                        ->  302 to /#/traseu?<q>
//
// The plan (the query of a route link, see features/route/routeLink.ts) is
// kept in Workers KV for 90 days under a random id, and nothing else: no
// account, no IP address. The query must have the shape of a route link and
// is only ever sent back into the app's own route tab, so the store cannot be
// used to send anyone to another site. This file is not part of the app
// bundle: wrangler builds it on its own, through index.ts. Everything else
// that reaches the Worker goes to the static files (staleAssets.ts).

import { isPreviewRobot, previewLang, previewPage, routePreview } from './preview';
import { serveFile } from './staleAssets';

/** The two KV calls used here (Cloudflare's KVNamespace, trimmed). */
export interface KvStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
}

/** The static app, for requests under /s that are not short links. */
export interface AssetFetcher {
  fetch(request: Request): Promise<Response>;
}

export interface Env {
  /** Missing while wrangler.jsonc has no KV binding: short links are then off and the app shares full links. */
  SHORT_LINKS?: KvStore;
  ASSETS: AssetFetcher;
}

/** How long a short link works: 90 days, in seconds. */
export const LINK_TTL_S = 90 * 24 * 3600;
const ID_LENGTH = 8;
const ID_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
/** The largest multiple of the alphabet size under 256: bytes above it are drawn again, so every character is equally likely. */
const UNBIASED_LIMIT = 256 - (256 % ID_ALPHABET.length);
const ID_PATTERN = /^[A-Za-z0-9]{8}$/;
/** A route link's query: version 1 first, then only the characters routeLink.ts writes. */
const QUERY_PATTERN = /^r=1&[A-Za-z0-9%&=~._:-]+$/;
/** A link carries at most 10 stops and 1000 road points (about 12 KB of query). */
export const MAX_QUERY_LENGTH = 16_000;
const MAX_BODY_LENGTH = MAX_QUERY_LENGTH + 64;
const ROUTE_TAB = '/#/traseu?';
/** What a link that expired or never existed opens: the app says so (routeLink.ts). */
export const GONE_QUERY = 'r=gone';

const COMMON_HEADERS = { 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' };

export function isRouteQuery(value: unknown): value is string {
  return typeof value === 'string' && value.length <= MAX_QUERY_LENGTH && QUERY_PATTERN.test(value);
}

/** A random id of ID_LENGTH letters and digits. */
export function newId(): string {
  let id = '';
  while (id.length < ID_LENGTH) {
    for (const byte of crypto.getRandomValues(new Uint8Array(ID_LENGTH * 2))) {
      if (byte < UNBIASED_LIMIT && id.length < ID_LENGTH) id += ID_ALPHABET[byte % ID_ALPHABET.length];
    }
  }
  return id;
}

function json(status: number, body: unknown, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...COMMON_HEADERS, ...extra },
  });
}

function redirect(origin: string, query: string): Response {
  return new Response(null, { status: 302, headers: { Location: `${origin}${ROUTE_TAB}${query}`, ...COMMON_HEADERS } });
}

async function create(request: Request, env: Env, origin: string): Promise<Response> {
  // Browsers send Origin on a POST: only the app's own pages may store links.
  const from = request.headers.get('Origin');
  if (from !== null && from !== origin) return json(403, { error: 'origin' });
  const text = await request.text();
  if (text.length > MAX_BODY_LENGTH) return json(413, { error: 'too-large' });
  let query: unknown;
  try {
    query = (JSON.parse(text) as { q?: unknown }).q;
  } catch {
    return json(400, { error: 'json' });
  }
  if (!isRouteQuery(query)) return json(400, { error: 'not-a-route' });
  if (!env.SHORT_LINKS) return json(503, { error: 'store-missing' });
  const id = newId();
  try {
    await env.SHORT_LINKS.put(id, query, { expirationTtl: LINK_TTL_S });
  } catch (err) {
    console.error('[short-links] store failed', err);
    return json(503, { error: 'store' });
  }
  return json(201, { url: `${origin}/s/${id}` });
}

function unavailable(): Response {
  return new Response('Linkul nu poate fi deschis acum. Încearcă din nou peste câteva minute.', {
    status: 503,
    headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Retry-After': '60', ...COMMON_HEADERS },
  });
}

async function open(request: Request, id: string, env: Env, url: URL): Promise<Response> {
  if (!env.SHORT_LINKS) return unavailable();
  let query: string | null;
  try {
    query = await env.SHORT_LINKS.get(id);
  } catch (err) {
    console.error('[short-links] read failed', err);
    return unavailable();
  }
  const plan = isRouteQuery(query) ? query : GONE_QUERY;
  // A chat app's preview robot gets the route's ends and departure (preview.ts).
  if (isPreviewRobot(request)) {
    const lang = previewLang(url);
    return previewPage(routePreview(plan, `${url.origin}${ROUTE_TAB}${plan}`, lang), url, lang);
  }
  return redirect(url.origin, plan);
}

export async function handle(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === '/s' || url.pathname === '/s/') {
    return request.method === 'POST' ? create(request, env, url.origin) : json(405, { error: 'method' }, { Allow: 'POST' });
  }
  const match = /^\/s\/([^/]+)\/?$/.exec(url.pathname);
  if (match && (request.method === 'GET' || request.method === 'HEAD')) {
    return ID_PATTERN.test(match[1]) ? open(request, match[1], env, url) : redirect(url.origin, GONE_QUERY);
  }
  return serveFile(request, env.ASSETS);
}
