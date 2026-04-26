// MotoMeteo Service Worker — cache-first strategy for offline support
var CACHE = 'motometeo-v6';
var WEATHER_CACHE = 'motometeo-weather-v2';
var WEATHER_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours
var SNOOZED_UNTIL = 0;
var ASSETS = [
    '/',
    '/index.html',
    '/manifest.json',
    '/icon-192.png',
    '/icon-512.png',
    '/icon-apple.png',
    'https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Inter:wght@400;500;600&display=swap',
    'https://cdn.tailwindcss.com',
    'https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js',
];

var OFFLINE_HTML = [
    '<!DOCTYPE html>',
    '<html lang="ro">',
    '<head>',
    '  <meta charset="UTF-8">',
    '  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">',
    '  <title>MotoMeteo \u2014 Offline</title>',
    '  <style>',
    '    *{box-sizing:border-box;margin:0;padding:0}',
    '    body{background:#0b0c0f;color:#f3f4f6;font-family:system-ui,sans-serif;',
    '      display:flex;flex-direction:column;align-items:center;justify-content:center;',
    '      min-height:100vh;',
    '      padding:env(safe-area-inset-top,0) env(safe-area-inset-right,0)',
    '             env(safe-area-inset-bottom,0) env(safe-area-inset-left,0);',
    '      text-align:center;gap:1rem}',
    '    .icon{font-size:4rem}',
    '    .title{font-size:1.5rem;font-weight:700;letter-spacing:.1em;color:#f97316}',
    '    .sub{color:#6b7280;font-size:.95rem;max-width:280px;line-height:1.5}',
    '    button{margin-top:.5rem;background:#f97316;color:#000;border:none;',
    '      padding:.75rem 2rem;border-radius:.75rem;font-weight:700;',
    '      font-size:1rem;cursor:pointer;letter-spacing:.05em}',
    '  </style>',
    '</head>',
    '<body>',
    '  <div class="icon">\uD83C\uDFCD\uFE0F</div>',
    '  <div class="title">MOTO // METEO</div>',
    '  <div class="sub">Nu exist\u0103 conexiune la internet. Datele meteo nu pot fi actualizate momentan.</div>',
    '  <button onclick="location.reload()">Re\u00EEncerc\u0103</button>',
    '</body>',
    '</html>',
].join('\n');

self.addEventListener('install', function (e) {
    e.waitUntil(
        caches.open(CACHE).then(function (c) {
            // Cache core assets; ignore failures for CDN resources
            return Promise.allSettled(ASSETS.map(function (url) {
                return c.add(url).catch(function () { });
            }));
        }).then(function () { return self.skipWaiting(); })
    );
});

self.addEventListener('activate', function (e) {
    e.waitUntil(
        caches.keys().then(function (keys) {
            return Promise.all(keys.filter(function (k) { return k !== CACHE && k !== WEATHER_CACHE; }).map(function (k) { return caches.delete(k); }));
        }).then(function () { return self.clients.claim(); })
    );
});

self.addEventListener('fetch', function (e) {
    var url = new URL(e.request.url);

    // Always try network first for HTML navigation so users get latest JS/logic.
    if (e.request.mode === 'navigate') {
        e.respondWith(
            fetch(e.request).then(function (resp) {
                if (resp && resp.status === 200) {
                    var clone = resp.clone();
                    caches.open(CACHE).then(function (c) { c.put(e.request, clone); });
                }
                return resp;
            }).catch(function () {
                return caches.match(e.request).then(function (cached) {
                    return cached || new Response(OFFLINE_HTML, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
                });
            })
        );
        return;
    }

    // Network-first for weather API calls (always fresh data)
    var isApi = url.pathname.startsWith('/weather')
        || url.pathname.startsWith('/geocode')
        || url.pathname.startsWith('/route')
        || url.pathname.startsWith('/auth')
        || url.pathname.startsWith('/me')
        || url.pathname.startsWith('/alerts')
        || url.pathname.startsWith('/push')
        || url.hostname === 'api.open-meteo.com'
        || url.hostname.endsWith('.open-meteo.com')
        || url.hostname === 'geocoding-api.open-meteo.com'
        || url.hostname === 'api.openweathermap.org'
        || url.hostname === 'api.met.no'
        || url.hostname === 'nominatim.openstreetmap.org';

    // Weather endpoint: network-first with 6h offline fallback cache
    var isWeatherApi = url.pathname.startsWith('/weather') || url.pathname.startsWith('/geocode')
        || url.hostname.endsWith('open-meteo.com')
        || url.hostname === 'api.met.no';
    if (isWeatherApi && e.request.method === 'GET') {
        e.respondWith(
            fetch(e.request).then(function (resp) {
                if (resp && resp.status === 200) {
                    var clone = resp.clone();
                    caches.open(WEATHER_CACHE).then(function (c) {
                        // Store with timestamp header
                        resp.clone().blob().then(function(body) {
                            var headers = new Headers(resp.headers);
                            headers.set('x-sw-cached-at', String(Date.now()));
                            var stamped = new Response(body, { status: resp.status, statusText: resp.statusText, headers: headers });
                            c.put(e.request, stamped);
                        });
                    });
                    return clone;
                }
                return resp;
            }).catch(function () {
                return caches.open(WEATHER_CACHE).then(function (c) {
                    return c.match(e.request).then(function (cached) {
                        if (!cached) return new Response(JSON.stringify({ error: 'offline', cached: false }), { status: 503, headers: { 'Content-Type': 'application/json' } });
                        var cachedAt = parseInt(cached.headers.get('x-sw-cached-at') || '0', 10);
                        var age = Date.now() - cachedAt;
                        if (age > WEATHER_TTL_MS) {
                            return new Response(JSON.stringify({ error: 'cache_expired', cached: true, age_h: Math.round(age / 3600000) }), { status: 503, headers: { 'Content-Type': 'application/json' } });
                        }
                        // Clone with extra header so app knows it's cached
                        return cached.blob().then(function(body) {
                            var h = new Headers(cached.headers);
                            h.set('x-sw-offline', 'true');
                            h.set('x-sw-age-min', String(Math.round(age / 60000)));
                            return new Response(body, { status: 200, headers: h });
                        });
                    });
                });
            })
        );
        return;
    }

    if (isApi) {
        e.respondWith(
            fetch(e.request).catch(function () {
                return caches.match(e.request);
            })
        );
        return;
    }

    // Cache-first for static assets; fall back to offline page for navigation
    e.respondWith(
        caches.match(e.request).then(function (cached) {
            if (cached) return cached;
            return fetch(e.request).then(function (resp) {
                if (resp && resp.status === 200 && e.request.method === 'GET') {
                    var clone = resp.clone();
                    caches.open(CACHE).then(function (c) { c.put(e.request, clone); });
                }
                return resp;
            }).catch(function () {
                // Return offline page for HTML navigation requests
                if (e.request.mode === 'navigate') {
                    return new Response(OFFLINE_HTML, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
                }
            });
        })
    );
});

self.addEventListener('push', function (event) {
    if (Date.now() < SNOOZED_UNTIL) {
        return;
    }

    var payload = {};
    try {
        payload = event.data ? event.data.json() : {};
    } catch (_) {
        payload = { title: 'MotoMeteo', body: 'Ai o alerta meteo noua.' };
    }

    var title = payload.title || 'MotoMeteo';
    var options = {
        body: payload.body || 'Ai o actualizare meteo.',
        icon: payload.icon || '/icon-192.png',
        badge: payload.badge || '/icon-192.png',
        data: payload.data || { url: '/' },
        vibrate: [120, 60, 120],
        renotify: true,
        tag: (payload.data && payload.data.event && payload.data.event.type) || 'motometeo-alert',
        actions: [
            { action: 'open', title: 'Deschide' },
            { action: 'snooze', title: 'Amână 30m' }
        ],
    };

    event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function (event) {
    if (event.action === 'snooze') {
        SNOOZED_UNTIL = Date.now() + 30 * 60 * 1000;
        event.notification.close();
        return;
    }

    event.notification.close();
    var targetUrl = '/';
    if (event.notification && event.notification.data) {
        if (event.notification.data.url) targetUrl = event.notification.data.url;
        if (event.notification.data.event && event.notification.data.event.type) {
            targetUrl = '/#alerts';
        }
    }

    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function (clientList) {
            for (var i = 0; i < clientList.length; i++) {
                var client = clientList[i];
                if (client.url.indexOf(self.location.origin) === 0 && 'focus' in client) {
                    client.navigate(targetUrl);
                    return client.focus();
                }
            }
            if (clients.openWindow) return clients.openWindow(targetUrl);
        })
    );
});
