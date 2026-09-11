import { registerSW } from 'virtual:pwa-register';

// Service worker registration with an "update ready" signal the shell turns
// into a toast. The new worker waits until the user taps "Actualizează".

type Listener = () => void;

let updateServiceWorker: ((reloadPage?: boolean) => Promise<void>) | null = null;
let updateReady = false;
const listeners = new Set<Listener>();

export function initServiceWorker(): void {
  if (!('serviceWorker' in navigator) || import.meta.env.DEV) return;
  updateServiceWorker = registerSW({
    immediate: true,
    onNeedRefresh() {
      updateReady = true;
      listeners.forEach((listener) => listener());
    },
    onRegisterError(err: unknown) {
      console.warn('[pwa] service worker registration failed', err);
    },
  });
}

export function onUpdateReady(listener: Listener): () => void {
  listeners.add(listener);
  if (updateReady) listener();
  return () => {
    listeners.delete(listener);
  };
}

export function applyUpdate(): void {
  void updateServiceWorker?.(true);
}
