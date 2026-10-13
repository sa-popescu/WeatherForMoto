import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { api, ApiError, isAbort } from '../lib/api';
import { AUTO_REFRESH_MS, STALE_ON_FOCUS_MS } from '../lib/config';
import { fetchDirectWeather } from '../lib/directWeather';
import { lastGoodFor, rememberLastGood } from '../lib/lastGood';
import type { WeatherResponse } from '../lib/types';
import { usePlace } from './place';

// Weather for the current place, shared by every screen.
// One request in flight at a time; the previous data stays on screen while a
// refresh runs, so auto-refresh never collapses the page to a skeleton.

export type WeatherSource = 'backend' | 'direct' | 'offline' | 'stale';
export type WeatherErrorKind = 'network' | 'server';

interface WeatherState {
  data: WeatherResponse | null;
  status: 'loading' | 'ready' | 'error';
  error: WeatherErrorKind | null;
  source: WeatherSource | null;
  offlineAgeMin: number | null;
  /** Why the data on screen is old, when the server said so. */
  staleReason: string | null;
  fetchedAt: number | null;
  refreshing: boolean;
  refresh: () => void;
}

type Snapshot = Omit<WeatherState, 'refresh'>;

const INITIAL: Snapshot = { data: null, status: 'loading', error: null, source: null, offlineAgeMin: null, staleReason: null, fetchedAt: null, refreshing: false };

const WeatherContext = createContext<WeatherState | null>(null);

export function WeatherProvider({ children }: { children: ReactNode }) {
  const { place } = usePlace();
  const [snap, setSnap] = useState<Snapshot>(INITIAL);
  const inflight = useRef<AbortController | null>(null);
  const fetchedAtRef = useRef<number | null>(null);

  const load = useCallback(async () => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    setSnap((s) => ({ ...s, refreshing: true }));
    try {
      const result = await api.weather(place, controller.signal);
      if (controller.signal.aborted) return;
      fetchedAtRef.current = Date.now();
      const stale = result.data.stale ?? null;
      if (!result.offline && !stale) rememberLastGood(place, result.data);
      setSnap({
        data: result.data,
        status: 'ready',
        error: null,
        source: result.offline ? 'offline' : stale ? 'stale' : 'backend',
        offlineAgeMin: result.offline ? result.offlineAgeMin : (stale?.age_min ?? null),
        staleReason: stale?.reason ?? null,
        fetchedAt: fetchedAtRef.current,
        refreshing: false,
      });
    } catch (err) {
      if (isAbort(err) || controller.signal.aborted) return;
      const kind: WeatherErrorKind = err instanceof ApiError && !err.isNetwork ? 'server' : 'network';
      console.warn('[weather] backend failed, trying the direct fallback', err);
      try {
        const data = await fetchDirectWeather(place, controller.signal);
        if (controller.signal.aborted) return;
        fetchedAtRef.current = Date.now();
        setSnap({ data, status: 'ready', error: null, source: 'direct', offlineAgeMin: null, staleReason: null, fetchedAt: fetchedAtRef.current, refreshing: false });
      } catch (fallbackErr) {
        if (isAbort(fallbackErr) || controller.signal.aborted) return;
        console.warn('[weather] direct fallback failed', fallbackErr);
        // Every leg is down. An hour-old forecast is not an error card, and on
        // a bike the difference matters.
        const known = lastGoodFor(place);
        setSnap((s) => {
          if (s.data) return { ...s, status: 'ready', error: kind, refreshing: false };
          if (known) {
            return {
              data: known.data,
              status: 'ready',
              error: null,
              source: 'stale',
              offlineAgeMin: known.ageMin,
              staleReason: null,
              fetchedAt: Date.now(),
              refreshing: false,
            };
          }
          return { ...s, status: 'error', error: kind, refreshing: false };
        });
      }
    }
  }, [place]);

  // New place: drop the previous place's data and load.
  useEffect(() => {
    setSnap(INITIAL);
    fetchedAtRef.current = null;
    void load();
    return () => inflight.current?.abort();
  }, [load]);

  // Periodic refresh while visible, and on return if the data got stale.
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (document.visibilityState === 'visible') void load();
    }, AUTO_REFRESH_MS);
    const onVisible = (): void => {
      const last = fetchedAtRef.current;
      if (document.visibilityState === 'visible' && last !== null && Date.now() - last > STALE_ON_FOCUS_MS) void load();
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [load]);

  const refresh = useCallback(() => void load(), [load]);
  const value = useMemo<WeatherState>(() => ({ ...snap, refresh }), [snap, refresh]);
  return <WeatherContext.Provider value={value}>{children}</WeatherContext.Provider>;
}

export function useWeather(): WeatherState {
  const ctx = useContext(WeatherContext);
  if (!ctx) throw new Error('useWeather must be used inside WeatherProvider');
  return ctx;
}
