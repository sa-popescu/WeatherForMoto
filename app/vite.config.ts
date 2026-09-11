/// <reference types="vitest/config" />
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import { VitePWA } from 'vite-plugin-pwa';

// The API lives on Cloud Run. In dev and preview the app calls "/api" and Vite
// proxies it server-side, so the browser never hits CORS on localhost.
const LIVE_API = 'https://weatherformoto-1056457771445.europe-west1.run.app';

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

  return {
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
          name: 'MotoMeteo — Vremea pentru motocicliști',
          short_name: 'MotoMeteo',
          description: 'Pot să plec cu motocicleta? Scor moto pe ore, ploaie pe șanse și cantitate, vremea pe traseu.',
          lang: 'ro',
          dir: 'ltr',
          start_url: '/#/acum',
          scope: '/',
          display: 'standalone',
          orientation: 'portrait-primary',
          background_color: '#0b0c0a',
          theme_color: '#0b0c0a',
          categories: ['weather', 'navigation', 'lifestyle'],
          icons: [
            { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
            { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
            { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' },
          ],
          shortcuts: [
            { name: 'Traseu', url: '/#/traseu', icons: [{ src: '/icon-192.png', sizes: '192x192' }] },
            { name: 'Hartă', url: '/#/harta', icons: [{ src: '/icon-192.png', sizes: '192x192' }] },
          ],
        },
        injectManifest: {
          globPatterns: ['**/*.{js,css,html,svg,png,woff2,webmanifest}'],
          globIgnores: ['**/splash-*.png', 'legacy.html', 'privacy-policy.html'],
          maximumFileSizeToCacheInBytes: 3 * 1024 * 1024,
        },
        devOptions: { enabled: false },
      }),
    ],
    server: { proxy, port: 5173 },
    preview: { proxy, port: 4173 },
    build: { target: 'es2020', sourcemap: true },
    test: { environment: 'node', include: ['src/**/*.test.ts'] },
  };
});
