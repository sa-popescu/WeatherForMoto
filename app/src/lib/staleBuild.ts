// After a deploy, a page loaded before it can ask for one of its screens'
// files, which the new build no longer has (each build names its files by
// their content). That page cannot draw the screen, and a plain reload can
// bring back the same old page, because the old service worker keeps
// answering until the new one takes over. The way out is the new build: let
// the waiting service worker take over, then load the page again. Once a
// minute at most, so a file that is really broken shows the crash screen
// instead of reloading for ever.

const RELOADED_AT_KEY = 'motonorStaleReloadAt';
const GUARD_MS = 60_000;
/** How long to wait for the new service worker before reloading anyway. */
const SW_WAIT_MS = 8_000;

/** The messages browsers give for a script or style of the page that did not load. */
const STALE_FILE = [
  /Failed to fetch dynamically imported module/i,
  /error loading dynamically imported module/i,
  /Importing a module script failed/i,
  /is not a valid JavaScript MIME type/i,
  /Unable to preload CSS/i,
  /Loading (CSS )?chunk .* failed/i,
];

export function isStaleFileError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? '');
  return STALE_FILE.some((pattern) => pattern.test(message));
}

/** True, and noted, unless the page was already reloaded for this in the last minute. */
export function claimStaleReload(now: number = Date.now()): boolean {
  try {
    const last = Number(sessionStorage.getItem(RELOADED_AT_KEY) ?? 0);
    if (now - last < GUARD_MS) return false;
    sessionStorage.setItem(RELOADED_AT_KEY, String(now));
  } catch {
    // No session storage (some private modes): reload anyway, the browser's
    // own cache rules then decide; at worst the crash screen comes next time.
  }
  return true;
}

function waitFor(condition: (done: () => void) => void, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = window.setTimeout(resolve, ms);
    condition(() => {
      window.clearTimeout(timer);
      resolve();
    });
  });
}

/** Asks for the newest service worker and lets it take over, if there is one. */
async function takeNewServiceWorker(): Promise<void> {
  if (!('serviceWorker' in navigator)) return;
  const registration = await navigator.serviceWorker.getRegistration();
  if (!registration) return;
  await registration.update();
  const installing = registration.installing;
  if (installing) {
    await waitFor((done) => installing.addEventListener('statechange', () => installing.state !== 'installing' && done()), SW_WAIT_MS);
  }
  const waiting = registration.waiting;
  if (!waiting) return;
  await waitFor((done) => {
    navigator.serviceWorker.addEventListener('controllerchange', done, { once: true });
    waiting.postMessage({ type: 'SKIP_WAITING' });
  }, SW_WAIT_MS);
}

let reloading = false;

/** True once the page is on its way to the new build: errors until then need no crash screen. */
export function isReloadingToNewBuild(): boolean {
  return reloading;
}

/** Loads the newest build: the new service worker first, then the page. */
export async function reloadToNewBuild(): Promise<void> {
  reloading = true;
  try {
    await takeNewServiceWorker();
  } catch (err) {
    console.warn('[app] the new service worker could not take over, reloading anyway', err);
  }
  window.location.reload();
}

/**
 * Vite's signal that a screen's file did not load: off to the new build at
 * once. The error still goes on to React, where onUncaughtError (main.tsx)
 * sees the reload under way and leaves the screen as it is.
 */
export function watchStaleFiles(): void {
  window.addEventListener('vite:preloadError', (event) => {
    console.warn('[app] a file of this build did not load', (event as Event & { payload?: unknown }).payload);
    if (!reloading && claimStaleReload()) void reloadToNewBuild();
  });
}
