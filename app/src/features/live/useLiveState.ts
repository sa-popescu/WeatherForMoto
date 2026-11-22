import { useEffect, useState } from 'react';
import type { LiveState } from '../../worker/live';
import { LIVE_PATH, SEND_EVERY_MS } from './constants';

// The shared ride's position for the viewer: asked for at the pace the rider
// sends it, and only while the page is on screen.

export type LiveViewState =
  | { status: 'loading' }
  | { status: 'gone' }
  | { status: 'ready'; data: LiveState; failed: boolean };

export function useLiveState(id: string): LiveViewState {
  const [state, setState] = useState<LiveViewState>({ status: 'loading' });

  useEffect(() => {
    let live = true;
    let timer: number | null = null;

    const load = async (): Promise<void> => {
      try {
        const response = await fetch(`${LIVE_PATH}/${id}`, { cache: 'no-store' });
        if (!live) return;
        if (response.status === 404) {
          setState({ status: 'gone' });
          return;
        }
        if (!response.ok) throw new Error(`live: HTTP ${response.status}`);
        const data = (await response.json()) as LiveState;
        if (live) setState({ status: 'ready', data, failed: false });
      } catch (err) {
        console.warn('[live] position not loaded', err);
        if (live) setState((current) => (current.status === 'ready' ? { ...current, failed: true } : current));
      }
    };

    const schedule = (): void => {
      if (timer !== null) window.clearInterval(timer);
      timer = null;
      if (document.visibilityState !== 'visible') return;
      void load();
      timer = window.setInterval(() => void load(), SEND_EVERY_MS);
    };

    schedule();
    document.addEventListener('visibilitychange', schedule);
    return () => {
      live = false;
      if (timer !== null) window.clearInterval(timer);
      document.removeEventListener('visibilitychange', schedule);
    };
  }, [id]);

  return state;
}
