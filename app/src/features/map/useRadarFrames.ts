import { useCallback, useEffect, useRef, useState } from 'react';
import { parseRadarIndex, RADAR_FETCH_TIMEOUT_MS, RADAR_INDEX_URL, RADAR_REFRESH_MS, type RadarIndex } from './radar';

// RainViewer frame list: loaded when the radar is first needed, refreshed
// every 10 minutes while the map tab is on screen, never while hidden.

export type RadarStatus = 'idle' | 'loading' | 'ready' | 'error';

export interface RadarFrames {
  status: RadarStatus;
  data: RadarIndex | null;
  reload: () => void;
}

export async function fetchRadarIndex(signal: AbortSignal): Promise<RadarIndex> {
  const res = await fetch(RADAR_INDEX_URL, { signal, headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`RainViewer HTTP ${res.status}`);
  const parsed = parseRadarIndex(await res.json());
  if (!parsed) throw new Error('RainViewer returned no usable frames');
  return parsed;
}

export function useRadarFrames(enabled: boolean): RadarFrames {
  const [data, setData] = useState<RadarIndex | null>(null);
  const [status, setStatus] = useState<RadarStatus>('idle');
  const [attempt, setAttempt] = useState(0);
  const fetchedAt = useRef(0);

  useEffect(() => {
    if (!enabled) return undefined;
    let disposed = false;
    let timer = 0;
    let request: AbortController | undefined;

    const load = async (): Promise<void> => {
      const current = new AbortController();
      request = current;
      const timeout = window.setTimeout(() => current.abort(), RADAR_FETCH_TIMEOUT_MS);
      setStatus((s) => (s === 'ready' ? s : 'loading'));
      try {
        const index = await fetchRadarIndex(current.signal);
        if (disposed) return;
        fetchedAt.current = Date.now();
        setData(index);
        setStatus('ready');
      } catch (err) {
        if (disposed) return;
        console.warn('[map] radar frame list failed', err);
        // A failed refresh keeps the frames already on screen.
        setStatus((s) => (s === 'ready' ? s : 'error'));
      } finally {
        window.clearTimeout(timeout);
      }
    };

    const schedule = (delay: number): void => {
      timer = window.setTimeout(() => {
        void load().then(() => {
          if (!disposed) schedule(RADAR_REFRESH_MS);
        });
      }, delay);
    };
    // Coming back within 10 minutes of the last fetch waits for the remainder.
    schedule(Math.max(0, RADAR_REFRESH_MS - (Date.now() - fetchedAt.current)));

    return () => {
      disposed = true;
      window.clearTimeout(timer);
      request?.abort();
    };
  }, [enabled, attempt]);

  const reload = useCallback(() => {
    fetchedAt.current = 0;
    setAttempt((n) => n + 1);
  }, []);

  return { status, data, reload };
}
