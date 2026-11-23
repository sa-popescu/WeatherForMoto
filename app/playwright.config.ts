import { defineConfig, devices } from '@playwright/test';

// Layout checks (e2e/layout.spec.ts): the built app in Chromium at the sizes
// it has to work at, run in CI before anything is published. They look for
// text running off the screen or over other text, and for rows cut off in the
// wide layout, the kind of break unit tests cannot see. The map check
// (e2e/map.spec.ts) makes sure the radar map still draws rain from radar
// tiles, through the reading worker and the WebGL layer.

const PORT = 4173;

const phone = { ...devices['Pixel 7'], viewport: { width: 390, height: 844 } };

export default defineConfig({
  testDir: 'e2e',
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    locale: 'ro-RO',
    timezoneId: 'Europe/Bucharest',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'phone', use: phone },
    // A phone turned sideways: an iPhone 15 and an iPhone SE.
    { name: 'phone-sideways', use: { ...phone, viewport: { width: 852, height: 393 } } },
    { name: 'small-phone-sideways', use: { ...phone, viewport: { width: 667, height: 375 } } },
    // The narrowest desktop window with the wide layout, and a laptop.
    { name: 'desktop-narrow', use: { ...devices['Desktop Chrome'], viewport: { width: 1024, height: 768 } } },
    { name: 'desktop', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } } },
  ],
  // The production build, served as the Worker would serve it (npm run build first).
  webServer: {
    command: `npx vite preview --port ${PORT} --strictPort --host 127.0.0.1`,
    url: `http://127.0.0.1:${PORT}`,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
