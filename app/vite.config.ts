/// <reference types="vitest/config" />
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import { VitePWA } from 'vite-plugin-pwa';

// The API lives on Cloud Run. In dev and preview the app calls "/api" and Vite
// proxies it server-side, so the browser never hits CORS on localhost.
const LIVE_API = 'https://weatherformoto-1056457771445.europe-west1.run.app';

/** The day of the build on Romanian time, so a deploy after midnight carries the new date: "2026.11.23". */
function buildDay(now: Date): string {
  try {
    const day = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Bucharest', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
    return day.replace(/-/g, '.');
  } catch (err) {
    // A Node built without time zone data: the UTC day is at most a day off.
    console.warn(`[vite] no Romanian time zone for the build date, using UTC: ${String(err)}`);
    return now.toISOString().slice(0, 10).replace(/-/g, '.');
  }
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', '');
  const proxy = {
    '/api': {
      target: env.API_PROXY_TARGET || LIVE_API,
      changeOrigin: true,
      secure: true,
      rewrite: (path: string) => path.replace(/^\/api/, ''),
    },
  };

  // The app's version (lib/appVersion.ts): the day it was built and the short
  // commit Workers Builds or Vercel built, "local" anywhere else. Every deploy
  // changes it, with no number to remember to raise.
  const build = (env.WORKERS_CI_COMMIT_SHA || env.VERCEL_GIT_COMMIT_SHA || 'local').slice(0, 7);

  return {
    define: { __APP_BUILD__: JSON.stringify(build), __APP_DATE__: JSON.stringify(buildDay(new Date())) },
    plugins: [
      react(),
      VitePWA({
        strategies: 'injectManifest',
        srcDir: 'src',
        filename: 'sw.ts',
        injectRegister: false,
        registerType: 'prompt',
        manifestFilename: 'manifest.webmanifest',
        manifest: {
          id: '/',
          name: 'Motonor — Vremea și drumul pentru motocicliști',
          short_name: 'Motonor',
          description: 'Vremea și drumul pentru motocicliști: scor moto pe ore, trasee cu trafic live și vremea pe care o prinzi pe drum.',
          lang: 'ro',
          dir: 'ltr',
          // No hash: the shell opens on its home view, "Drum" (App.tsx).
          start_url: '/',
          scope: '/',
          display: 'standalone',
          // Any, so a tablet can turn to landscape and get the desktop layout;
          // phones are kept upright from lib/orientation.ts where allowed.
          orientation: 'any',
          background_color: '#0b0c0a',
          theme_color: '#0b0c0a',
          categories: ['weather', 'navigation', 'lifestyle'],
          icons: [
            { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
            { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
            // The same dial, shrunk into the safe circle Android crops to.
            { src: '/icon-maskable-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' },
          ],
          shortcuts: [
            { name: 'Vreme', url: '/#/acum', icons: [{ src: '/icon-192.png', sizes: '192x192' }] },
            { name: 'Hartă radar', url: '/#/harta', icons: [{ src: '/icon-192.png', sizes: '192x192' }] },
            { name: 'Ture', url: '/#/ture', icons: [{ src: '/icon-192.png', sizes: '192x192' }] },
          ],
        },
        injectManifest: {
          globPatterns: ['**/*.{js,css,html,svg,png,woff2,webmanifest}'],
          globIgnores: ['**/splash-*.png', 'legacy.html', 'privacy-policy.html', 'privacy-policy-en.html'],
          maximumFileSizeToCacheInBytes: 3 * 1024 * 1024,
        },
        devOptions: { enabled: false },
      }),
    ],
    server: { proxy, port: 5173 },
    preview: { proxy, port: 4173 },
    build: {
      target: 'es2020',
      sourcemap: true,
      // MapLibre (the ride map) is about 1 MB on its own. It gets its own chunk,
      // so its hash, and the copy the service worker keeps, only change when
      // the library does, not with every change to the ride screen.
      chunkSizeWarningLimit: 1100,
      rollupOptions: {
        output: {
          manualChunks: (id: string) => (id.includes('node_modules/maplibre-gl/') ? 'maplibre' : undefined),
        },
      },
    },
    test: { environment: 'node', include: ['src/**/*.test.ts'] },
  };
});
