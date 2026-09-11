import { useSyncExternalStore } from 'react';
import { isStandalone } from './lib/platform';

// Captures Chrome/Edge/Android's beforeinstallprompt so "Instalează aplicația"
// in the Eu tab can show the native install dialog later. The event fires
// early, often before the lazy account chunk loads, so main.tsx imports this
// module for its side effect:  import './features/account/installPrompt';

interface BeforeInstallPromptEvent extends Event {
  prompt: () => Promise<void>;
  userChoice: Promise<{ outcome: 'accepted' | 'dismissed'; platform: string }>;
}

export type InstallState = 'available' | 'installed' | 'unavailable';

let deferred: BeforeInstallPromptEvent | null = null;
let installed = false;
const listeners = new Set<() => void>();

function emit(): void {
  listeners.forEach((listener) => listener());
}

if (typeof window !== 'undefined') {
  window.addEventListener('beforeinstallprompt', (event) => {
    // Keep the browser's own mini-infobar away; the app offers the button.
    event.preventDefault();
    deferred = event as BeforeInstallPromptEvent;
    emit();
  });
  window.addEventListener('appinstalled', () => {
    deferred = null;
    installed = true;
    emit();
  });
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function getInstallState(): InstallState {
  if (installed || isStandalone()) return 'installed';
  return deferred ? 'available' : 'unavailable';
}

export function useInstallState(): InstallState {
  return useSyncExternalStore(subscribe, getInstallState, () => 'unavailable');
}

/** Shows the native dialog. The event can be used once, so it is dropped afterwards. */
export async function promptInstall(): Promise<'accepted' | 'dismissed' | 'unavailable'> {
  const event = deferred;
  if (!event) return 'unavailable';
  deferred = null;
  emit();
  await event.prompt();
  const choice = await event.userChoice;
  return choice.outcome;
}
