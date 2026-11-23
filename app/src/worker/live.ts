// Live location of a ride, shared by link (features/ride/liveShare.ts sends
// it, features/live/LiveView.tsx shows it). Positions never touch the app's
// database on Turso: each shared ride is a Durable Object here on Cloudflare,
// with its own small storage, deleted when the ride ends and after 12 hours
// at the latest. A second object, the budget, caps how many rides can be
// shared a day, overall and per address, so the free plan's daily limits
// (100,000 requests, 100,000 rows written) stay out of reach by use or by
// abuse; past the cap the app says sharing is off until the next day, and
// nothing else in the app is affected. The limits reset every night.
//
//   POST   /live                          -> 201 {id, key, expiresAt}
//   PUT    /live/<id>   Bearer <key>  {lat, lon, speedKmh, dest, etaMs} -> 204
//   DELETE /live/<id>   Bearer <key>      -> 204, everything deleted
//   GET    /live/<id>                     -> 200 the position and the trail, 404 once ended
//   GET    /l/<id>                        -> the link a rider sends: 302 to the app's
//                                            live page, or the ride's preview for a
//                                            chat app's robot (preview.ts)
//
// The id is in the link, so anyone with the link can watch; only the phone
// that started the sharing holds the key that moves the position or ends it.
// This file is not part of the app bundle: wrangler builds it through index.ts.

import { isPreviewRobot, livePreview, previewLang, previewPage } from './preview';

// ---- The few Cloudflare types used here (trimmed) ------------------------

export interface DurableStorage {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  deleteAll(): Promise<void>;
  setAlarm(scheduledTime: number): Promise<void>;
  deleteAlarm(): Promise<void>;
}

export interface DurableState {
  storage: DurableStorage;
}

export interface DurableStub {
  fetch(request: Request): Promise<Response>;
}

export interface DurableNamespace {
  idFromName(name: string): unknown;
  get(id: unknown): DurableStub;
}

export interface LiveEnv {
  /** Missing until wrangler.jsonc binds the objects: sharing then answers 503 and the app says it is off. */
  LIVE_RIDES?: DurableNamespace;
  LIVE_BUDGET?: DurableNamespace;
}

// ---- Limits -----------------------------------------------------------------

/** A shared ride ends by itself this long after it started. */
export const SHARE_TTL_MS = 12 * 3600 * 1000;
/** The phone sends every 30 s; anything faster than this is refused. */
export const MIN_UPDATE_MS = 25_000;
/**
 * Rides shared a day, in all. Even if every one ran the full 12 hours at the
 * fastest pace allowed, with viewers asking all the time (one read per
 * VIEW_CACHE_S), that is under 80,000 requests and 35,000 rows written.
 */
export const DAILY_SHARES = 20;
/** Rides one address can share a day. */
export const SHARES_PER_CLIENT = 3;
/** Points kept for the line behind the rider, about 100 m apart. */
export const MAX_TRAIL = 300;
const TRAIL_STEP_KM = 0.1;
/** Viewers are served the same answer for this long, so many viewers do not mean many reads. */
export const VIEW_CACHE_S = 20;
const MAX_UPDATE_BODY = 1024;
const MAX_DEST = 80;
/** An arrival estimate further out than this is not believed. */
const MAX_ETA_AHEAD_MS = 48 * 3600 * 1000;

const ID_PATTERN = /^[A-Za-z0-9_-]{22}$/;
const KEY_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const COMMON_HEADERS = { 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' };

// ---- Small helpers ----------------------------------------------------------

function json(status: number, body: unknown, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...COMMON_HEADERS, ...extra },
  });
}

function base64url(bytes: Uint8Array): string {
  let text = '';
  for (const byte of bytes) text += String.fromCharCode(byte);
  return btoa(text).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** A random id (16 bytes, 22 characters) or key (32 bytes, 43 characters). */
export function randomToken(bytes: 16 | 32): string {
  return base64url(crypto.getRandomValues(new Uint8Array(bytes)));
}

export async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return base64url(new Uint8Array(digest));
}

/** Compares two strings in time that does not depend on where they differ. */
export function sameText(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

function bearer(request: Request): string | null {
  const match = /^Bearer ([A-Za-z0-9_-]+)$/.exec(request.headers.get('Authorization') ?? '');
  return match && KEY_PATTERN.test(match[1]) ? match[1] : null;
}

function distanceKm(a: readonly [number, number], b: readonly [number, number]): number {
  const kx = 111.32 * Math.cos((a[0] * Math.PI) / 180);
  return Math.hypot((b[0] - a[0]) * 111.32, (b[1] - a[1]) * kx);
}

// ---- What a shared ride keeps ---------------------------------------------

export interface LivePosition {
  lat: number;
  lon: number;
  speedKmh: number | null;
  /** Where the rider is going, if the ride follows a route. */
  dest: string | null;
  etaMs: number | null;
  /** Server time of the update. */
  at: number;
}

interface RideRecord {
  keyHash: string;
  createdAt: number;
  expiresAt: number;
  pos: LivePosition | null;
  trail: Array<[number, number]>;
}

export interface LiveState {
  expiresAt: number;
  pos: LivePosition | null;
  trail: Array<[number, number]>;
}

function finiteIn(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
}

/** The phone's update, checked and rounded; null when it is not one. */
export function parseUpdate(body: unknown, now: number): Omit<LivePosition, 'at'> | null {
  if (typeof body !== 'object' || body === null) return null;
  const b = body as Record<string, unknown>;
  if (!finiteIn(b.lat, -90, 90) || !finiteIn(b.lon, -180, 180)) return null;
  const round = (v: number): number => Math.round(v * 1e5) / 1e5;
  return {
    lat: round(b.lat),
    lon: round(b.lon),
    speedKmh: finiteIn(b.speedKmh, 0, 400) ? Math.round(b.speedKmh) : null,
    dest: typeof b.dest === 'string' && b.dest.trim() ? b.dest.trim().slice(0, MAX_DEST) : null,
    etaMs: finiteIn(b.etaMs, now, now + MAX_ETA_AHEAD_MS) ? Math.round(b.etaMs) : null,
  };
}

/** One shared ride. Cloudflare keeps one instance per id, wherever it is asked from. */
export class LiveRide {
  private readonly storage: DurableStorage;

  constructor(state: DurableState) {
    this.storage = state.storage;
  }

  async fetch(request: Request): Promise<Response> {
    const { pathname } = new URL(request.url);
    const now = Date.now();
    if (pathname === '/init') return this.init(request, now);
    const ride = await this.storage.get<RideRecord>('ride');
    if (!ride || now >= ride.expiresAt) return json(404, { error: 'gone' });
    if (pathname === '/state') return json(200, { expiresAt: ride.expiresAt, pos: ride.pos, trail: ride.trail } satisfies LiveState);
    const key = request.headers.get('X-Live-Key') ?? '';
    if (!sameText(await sha256(key), ride.keyHash)) return json(403, { error: 'key' });
    if (pathname === '/end') {
      await this.storage.deleteAll();
      await this.storage.deleteAlarm();
      return new Response(null, { status: 204, headers: COMMON_HEADERS });
    }
    if (pathname === '/update') return this.update(request, ride, now);
    return json(404, { error: 'path' });
  }

  private async init(request: Request, now: number): Promise<Response> {
    const { keyHash } = (await request.json()) as { keyHash: string };
    const ride: RideRecord = { keyHash, createdAt: now, expiresAt: now + SHARE_TTL_MS, pos: null, trail: [] };
    await this.storage.put('ride', ride);
    await this.storage.setAlarm(ride.expiresAt);
    return json(201, { expiresAt: ride.expiresAt });
  }

  private async update(request: Request, ride: RideRecord, now: number): Promise<Response> {
    if (ride.pos && now - ride.pos.at < MIN_UPDATE_MS) return json(429, { error: 'too-soon' }, { 'Retry-After': '25' });
    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return json(400, { error: 'json' });
    }
    const update = parseUpdate(body, now);
    if (!update) return json(400, { error: 'position' });
    const point: [number, number] = [update.lat, update.lon];
    const last = ride.trail[ride.trail.length - 1];
    const trail = !last || distanceKm(last, point) >= TRAIL_STEP_KM ? [...ride.trail, point].slice(-MAX_TRAIL) : ride.trail;
    await this.storage.put('ride', { ...ride, pos: { ...update, at: now }, trail } satisfies RideRecord);
    return new Response(null, { status: 204, headers: COMMON_HEADERS });
  }

  /** The 12 hours are up: nothing of the ride is kept. */
  async alarm(): Promise<void> {
    await this.storage.deleteAll();
  }
}

/**
 * How many rides were shared today, in all and per address. One instance for
 * the whole app. The per-address counts live only in memory (a restart
 * forgets them, which only ever lets a rider share again); the total is kept.
 */
export class LiveBudget {
  private readonly storage: DurableStorage;
  private day = '';
  private total = 0;
  private loaded = false;
  private readonly perClient = new Map<string, number>();

  constructor(state: DurableState) {
    this.storage = state.storage;
  }

  async fetch(request: Request): Promise<Response> {
    const { client } = (await request.json()) as { client: string };
    const day = new Date().toISOString().slice(0, 10);
    if (!this.loaded) {
      const saved = await this.storage.get<{ day: string; total: number }>('day');
      if (saved) {
        this.day = saved.day;
        this.total = saved.total;
      }
      this.loaded = true;
    }
    if (day !== this.day) {
      this.day = day;
      this.total = 0;
      this.perClient.clear();
    }
    if (this.total >= DAILY_SHARES) return json(429, { error: 'budget' });
    const used = this.perClient.get(client) ?? 0;
    if (used >= SHARES_PER_CLIENT) return json(429, { error: 'client' });
    this.total += 1;
    this.perClient.set(client, used + 1);
    await this.storage.put('day', { day, total: this.total });
    return json(200, { left: DAILY_SHARES - this.total });
  }
}

// ---- The Worker's side ------------------------------------------------------

function rideStub(env: Required<LiveEnv>, id: string): DurableStub {
  return env.LIVE_RIDES.get(env.LIVE_RIDES.idFromName(id));
}

/** Cloudflare's shared cache on a custom domain; absent in tests and on workers.dev it does nothing. */
function edgeCache(): Cache | null {
  const store = (globalThis as { caches?: { default?: Cache } }).caches;
  return store?.default ?? null;
}

function viewKey(origin: string, id: string): string {
  return `${origin}/live/${id}?view`;
}

async function create(request: Request, env: Required<LiveEnv>, origin: string): Promise<Response> {
  // Browsers send Origin on a POST: only the app's own pages may start sharing.
  const from = request.headers.get('Origin');
  if (from !== null && from !== origin) return json(403, { error: 'origin' });
  const client = await sha256(request.headers.get('CF-Connecting-IP') ?? 'unknown');
  const budget = env.LIVE_BUDGET.get(env.LIVE_BUDGET.idFromName('budget'));
  const allowed = await budget.fetch(new Request('https://live/take', { method: 'POST', body: JSON.stringify({ client }) }));
  if (!allowed.ok) return json(429, { error: ((await allowed.json()) as { error?: string }).error ?? 'budget' });
  const id = randomToken(16);
  const key = randomToken(32);
  const started = await rideStub(env, id).fetch(
    new Request('https://live/init', { method: 'POST', body: JSON.stringify({ keyHash: await sha256(key) }) }),
  );
  if (!started.ok) return json(503, { error: 'store' });
  const { expiresAt } = (await started.json()) as { expiresAt: number };
  return json(201, { id, key, expiresAt });
}

async function forward(request: Request, env: Required<LiveEnv>, id: string, action: 'update' | 'end'): Promise<Response> {
  const key = bearer(request);
  if (!key) return json(401, { error: 'key' });
  let body: string | undefined;
  if (action === 'update') {
    body = await request.text();
    if (body.length > MAX_UPDATE_BODY) return json(413, { error: 'too-large' });
  }
  return rideStub(env, id).fetch(new Request(`https://live/${action}`, { method: 'POST', body, headers: { 'X-Live-Key': key } }));
}

async function view(env: Required<LiveEnv>, id: string, origin: string): Promise<Response> {
  const cache = edgeCache();
  const cacheKey = viewKey(origin, id);
  const cached = await cache?.match(cacheKey);
  if (cached) return cached;
  const answer = await rideStub(env, id).fetch(new Request('https://live/state'));
  const text = await answer.text();
  if (answer.ok && cache) {
    const headers = new Headers(answer.headers);
    headers.set('Cache-Control', `public, max-age=${VIEW_CACHE_S}`);
    await cache.put(cacheKey, new Response(text, { status: answer.status, headers }));
  }
  return new Response(text, { status: answer.status, headers: answer.headers });
}

/** Requests under /live, or null for anything else. */
export async function handleLive(request: Request, env: LiveEnv): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/live' && !url.pathname.startsWith('/live/')) return null;
  if (!env.LIVE_RIDES || !env.LIVE_BUDGET) return json(503, { error: 'off' });
  const bound = env as Required<LiveEnv>;
  try {
    if (url.pathname === '/live' || url.pathname === '/live/') {
      return request.method === 'POST' ? await create(request, bound, url.origin) : json(405, { error: 'method' }, { Allow: 'POST' });
    }
    const id = url.pathname.slice('/live/'.length);
    if (!ID_PATTERN.test(id)) return json(404, { error: 'gone' });
    if (request.method === 'GET') return await view(bound, id, url.origin);
    if (request.method === 'PUT') return await forward(request, bound, id, 'update');
    if (request.method === 'DELETE') {
      const answer = await forward(request, bound, id, 'end');
      if (answer.ok) await edgeCache()?.delete(viewKey(url.origin, id));
      return answer;
    }
    return json(405, { error: 'method' }, { Allow: 'GET, PUT, DELETE' });
  } catch (err) {
    console.error('[live] request failed', err);
    return json(503, { error: 'store' });
  }
}

const LIVE_LINK = /^\/l\/([A-Za-z0-9_-]{22})\/?$/;

/** The ride's state for a preview: null once it ended, or when it cannot be read. */
async function stateForPreview(env: LiveEnv, id: string): Promise<LiveState | null> {
  if (!env.LIVE_RIDES || !env.LIVE_BUDGET) return null;
  try {
    const answer = await rideStub(env as Required<LiveEnv>, id).fetch(new Request('https://live/state'));
    return answer.ok ? ((await answer.json()) as LiveState) : null;
  } catch (err) {
    console.error('[live] the preview could not read the ride', err);
    return null;
  }
}

/**
 * The link a rider sends, /l/<id>: people go on to the app's live page
 * (/#/live/<id>), and a chat app's preview robot gets the ride's destination
 * and arrival instead of the app's home page (preview.ts). Null for any
 * other path.
 */
export async function handleLiveLink(request: Request, env: LiveEnv): Promise<Response | null> {
  const url = new URL(request.url);
  const match = LIVE_LINK.exec(url.pathname);
  if (!match || (request.method !== 'GET' && request.method !== 'HEAD')) return null;
  const target = `${url.origin}/#/live/${match[1]}`;
  if (!isPreviewRobot(request)) return new Response(null, { status: 302, headers: { Location: target, ...COMMON_HEADERS } });
  const lang = previewLang(url);
  return previewPage(livePreview(await stateForPreview(env, match[1]), target, lang), url, lang);
}
