import { registerSW } from 'virtual:pwa-register';
import { reloadToNewBuild } from './staleBuild';
import { CHECK_EVERY_MS, checkDue, mayApplyUnasked } from './updateTiming';

// Service worker registration with an "update ready" signal the shell turns
// into a toast that stays until the user taps "Actualizează".
//
// The browser looks for a new sw.js only when a page loads (and once a day).
// An app added to the iPhone's home screen is almost never loaded again: iOS
// keeps it in memory for days and only brings it back to the front, so a new
// version went unnoticed until iOS happened to drop the app. Here the app asks
// itself: as soon as it comes back to the front, when it gets the network
// back, and every minute while it is on screen.

type Listener = () => void;

let updateServiceWorker: ((reloadPage?: boolean) => Promise<void>) | null = null;
let registration: ServiceWorkerRegistration | null = null;
let updateReady = false;
let lastCheckAt = 0;
let openedAt = 0;
let touched = false;
const listeners = new Set<Listener>();

function signalReady(): void {
  if (updateReady) return;
  updateReady = true;
  if (mayApplyUnasked(Date.now(), openedAt, touched)) {
    console.info('[pwa] new version found as the app opened, loading it');
    applyUpdate();
    return;
  }
  listeners.forEach((listener) => listener());
}

/** Asks the server for sw.js; a changed one installs and then waits (onNeedRefresh). */
async function checkForUpdate(force = false): Promise<void> {
  const reg = registration;
  if (!reg || updateReady || !navigator.onLine) return;
  const now = Date.now();
  if (!force && !checkDue(now, lastCheckAt)) return;
  lastCheckAt = now;
  try {
    await reg.update();
  } catch (err) {
    // Offline or the server away for a moment: the next check tries again.
    console.warn('[pwa] update check failed', err);
    return;
  }
  // Workbox normally reports the waiting worker; this covers a worker that
  // finished installing before anyone was listening.
  if (reg.waiting && navigator.serviceWorker.controller) signalReady();
}

function watchForUpdates(): void {
  const onFront = (): void => {
    // Back from the background the toast asks: the page may hold a ride in
    // progress, so only a fresh start loads the new version unasked.
    if (document.visibilityState === 'visible') void checkForUpdate();
  };
  document.addEventListener('visibilitychange', onFront);
  window.addEventListener('focus', onFront);
  window.addEventListener('pageshow', onFront);
  window.addEventListener('online', () => void checkForUpdate(true));
  const onTouch = (): void => {
    touched = true;
  };
  window.addEventListener('pointerdown', onTouch, { capture: true, passive: true });
  window.addEventListener('keydown', onTouch, { capture: true, passive: true });
  window.setInterval(() => {
    if (document.visibilityState === 'visible') void checkForUpdate();
  }, CHECK_EVERY_MS);
}

export function initServiceWorker(): void {
  if (!('serviceWorker' in navigator) || import.meta.env.DEV) return;
  openedAt = Date.now();
  updateServiceWorker = registerSW({
    immediate: true,
    onNeedRefresh: signalReady,
    onRegisteredSW(_url: string, reg: ServiceWorkerRegistration | undefined) {
      if (!reg) return;
      registration = reg;
      if (reg.waiting && navigator.serviceWorker.controller) signalReady();
    },
    onRegisterError(err: unknown) {
      console.warn('[pwa] service worker registration failed', err);
    },
  });
  watchForUpdates();
}

export function onUpdateReady(listener: Listener): () => void {
  listeners.add(listener);
  if (updateReady) listener();
  return () => {
    listeners.delete(listener);
  };
}

/**
 * The waiting worker takes over, then the page loads again. On iOS the
 * "controlling" event Workbox waits for sometimes never comes, which left the
 * button doing nothing; reloadToNewBuild reloads after a few seconds anyway.
 */
export function applyUpdate(): void {
  if (registration) {
    void reloadToNewBuild();
    return;
  }
  void updateServiceWorker?.(true);
}
