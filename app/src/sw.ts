/// <reference lib="webworker" />
import { CacheableResponsePlugin } from 'workbox-cacheable-response';
import { ExpirationPlugin } from 'workbox-expiration';
import { cleanupOutdatedCaches, createHandlerBoundToURL, precacheAndRoute } from 'workbox-precaching';
import { NavigationRoute, registerRoute } from 'workbox-routing';
import { CacheFirst, NetworkFirst, StaleWhileRevalidate } from 'workbox-strategies';

// MotoMeteo service worker (built by vite-plugin-pwa, injectManifest).
// - app shell precached, SPA navigations served from it
// - weather: network first, offline fallback up to 6 h with age headers
// - map and radar tiles: bounded caches
// - web push: shows the alert and opens the app on tap
// - replaces the legacy app's worker and deletes its caches

declare let self: ServiceWorkerGlobalScope;

// Bump on any change that must reach installed workers even when the bundle
// is unchanged (e.g. new headers such as the CSP served with sw.js): the
// browser only reinstalls a worker whose script bytes differ.
const SW_VERSION = '2026-09-14.1';

const WEATHER_CACHE = 'mm-weather-v1';
// Radar tiles are requested with CORS now, so the map can read their pixels to
// extrapolate the radar. The old cache held opaque copies, which a CORS request
// cannot use: it is dropped on activation.
const RADAR_TILE_CACHE = 'mm-radar-tiles-v2';
const RETIRED_CACHES = ['mm-radar-tiles-v1'];
const WEATHER_MAX_AGE_MS = 6 * 60 * 60 * 1000;
const STAMP_HEADER = 'x-sw-cached-at';
const LEGACY_CACHE_PREFIXES = ['motometeo-'];

precacheAndRoute(self.__WB_MANIFEST);
cleanupOutdatedCaches();

registerRoute(
  new NavigationRoute(createHandlerBoundToURL('/index.html'), {
    denylist: [/\/privacy-policy/, /\/legacy/],
  }),
);

// ---- Weather API --------------------------------------------------------

async function stamp(response: Response): Promise<Response> {
  const headers = new Headers(response.headers);
  headers.set(STAMP_HEADER, String(Date.now()));
  return new Response(await response.blob(), { status: response.status, statusText: response.statusText, headers });
}

async function withOfflineHeaders(cached: Response, ageMs: number): Promise<Response> {
  const headers = new Headers(cached.headers);
  headers.set('x-sw-offline', '1');
  headers.set('x-sw-age-min', String(Math.round(ageMs / 60_000)));
  return new Response(await cached.blob(), { status: 200, headers });
}

registerRoute(
  ({ url, request }) => request.method === 'GET' && /\/weather$/.test(url.pathname),
  async ({ request }) => {
    const cache = await caches.open(WEATHER_CACHE);
    try {
      const response = await fetch(request);
      if (response.ok) await cache.put(request, await stamp(response.clone()));
      return response;
    } catch (err) {
      const cached = await cache.match(request);
      const cachedAt = Number(cached?.headers.get(STAMP_HEADER) ?? 0);
      const age = Date.now() - cachedAt;
      if (cached && age <= WEATHER_MAX_AGE_MS) return withOfflineHeaders(cached, age);
      throw err;
    }
  },
);

// Scoring constants change rarely.
registerRoute(
  ({ url }) => /\/meta\/scoring$/.test(url.pathname),
  new StaleWhileRevalidate({ cacheName: 'mm-meta-v1' }),
);

// ---- Map tiles and radar ------------------------------------------------

registerRoute(
  ({ url }) => /basemaps\.cartocdn\.com$/.test(url.hostname) || /tile\.openstreetmap\.org$/.test(url.hostname),
  new CacheFirst({
    cacheName: 'mm-tiles-v1',
    plugins: [new CacheableResponsePlugin({ statuses: [0, 200] }), new ExpirationPlugin({ maxEntries: 600, maxAgeSeconds: 14 * 24 * 3600 })],
  }),
);

registerRoute(
  ({ url }) => url.hostname === 'api.rainviewer.com',
  new NetworkFirst({
    cacheName: 'mm-radar-index-v1',
    networkTimeoutSeconds: 5,
    plugins: [new ExpirationPlugin({ maxEntries: 2, maxAgeSeconds: 15 * 60 })],
  }),
);

registerRoute(
  ({ url }) => url.hostname === 'tilecache.rainviewer.com',
  new CacheFirst({
    cacheName: RADAR_TILE_CACHE,
    plugins: [new CacheableResponsePlugin({ statuses: [200] }), new ExpirationPlugin({ maxEntries: 300, maxAgeSeconds: 3 * 3600 })],
  }),
);

// ---- Lifecycle ------------------------------------------------------------

async function legacyCacheNames(): Promise<string[]> {
  const names = await caches.keys();
  return names.filter((name) => LEGACY_CACHE_PREFIXES.some((prefix) => name.startsWith(prefix)));
}

async function retiredCacheNames(): Promise<string[]> {
  const names = await caches.keys();
  return names.filter((name) => RETIRED_CACHES.includes(name));
}

self.addEventListener('install', (event) => {
  // Coming from the legacy app: take over at once, it has no update prompt.
  event.waitUntil(
    legacyCacheNames().then((legacy) => {
      if (legacy.length > 0) return self.skipWaiting();
      return undefined;
    }),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    Promise.all([legacyCacheNames(), retiredCacheNames()])
      .then(([legacy, retired]) => Promise.all([...legacy, ...retired].map((name) => caches.delete(name))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('message', (event) => {
  const type = event.data ? (event.data as { type?: string }).type : undefined;
  if (type === 'SKIP_WAITING') void self.skipWaiting();
  if (type === 'GET_VERSION') event.ports[0]?.postMessage(SW_VERSION);
});

// ---- Web push ---------------------------------------------------------------

interface PushPayload {
  title?: string;
  body?: string;
  url?: string;
  tag?: string;
  data?: { url?: string; [key: string]: unknown };
}

self.addEventListener('push', (event) => {
  let payload: PushPayload = {};
  try {
    payload = (event.data?.json() as PushPayload | undefined) ?? {};
  } catch {
    payload = { body: event.data?.text() };
  }
  const url = payload.data?.url ?? (payload.url && payload.url !== '/' ? payload.url : '/#/acum');
  event.waitUntil(
    self.registration.showNotification(payload.title ?? 'MotoMeteo', {
      body: payload.body ?? '',
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      tag: payload.tag,
      data: { url },
    }),
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const target = new URL((event.notification.data as { url?: string } | null)?.url ?? '/#/acum', self.location.origin).href;
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(async (clients) => {
      for (const client of clients) {
        if (new URL(client.url).origin === self.location.origin) {
          await client.focus();
          if ('navigate' in client) await (client as WindowClient).navigate(target);
          return;
        }
      }
      await self.clients.openWindow(target);
    }),
  );
});
