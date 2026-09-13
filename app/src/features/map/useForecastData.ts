import type { Map as LeafletMap } from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import { isAbort } from '../../lib/api';
import {
  boundsMovedEnough,
  forecastUrl,
  FORECAST_FETCH_TIMEOUT_MS,
  gridPoints,
  parseForecast,
  type ForecastData,
  type GridBounds,
} from './forecast';

// Fetches the forecast grid for what is on screen, once per meaningful move.
// Panning a little reuses what is already loaded; panning away, zooming or
// switching the layer on asks Open-Meteo again.

const DEBOUNCE_MS = 600;
/** A grid older than this is refetched even if the view did not move. */
const STALE_MS = 20 * 60_000;
/** The grid is fetched a little wider than the view, so a small pan stays covered. */
const PAD = 0.15;

export type ForecastStatus = 'idle' | 'loading' | 'ready' | 'error';

export interface ForecastGrid {
  status: ForecastStatus;
  data: ForecastData | null;
  /** The box the grid covers, which is what the overlay is drawn over. */
  bounds: GridBounds | null;
  reload: () => void;
}

function paddedBounds(map: LeafletMap): GridBounds {
  const b = map.getBounds().pad(PAD);
  return {
    south: Math.max(-85, b.getSouth()),
    west: Math.max(-180, b.getWest()),
    north: Math.min(85, b.getNorth()),
    east: Math.min(180, b.getEast()),
  };
}

export function useForecastData(map: LeafletMap | null, fetching: boolean): ForecastGrid {
  const [status, setStatus] = useState<ForecastStatus>('idle');
  const [data, setData] = useState<ForecastData | null>(null);
  const [bounds, setBounds] = useState<GridBounds | null>(null);
  const [attempt, setAttempt] = useState(0);
  const loadedFor = useRef<GridBounds | null>(null);
  const loadedAt = useRef(0);

  const reload = useCallback(() => {
    loadedFor.current = null;
    loadedAt.current = 0;
    setAttempt((n) => n + 1);
  }, []);

  useEffect(() => {
    if (!map || !fetching) return undefined;

    const controller = new AbortController();
    let timer = 0;

    const load = async (): Promise<void> => {
      const next = paddedBounds(map);
      const stale = Date.now() - loadedAt.current > STALE_MS;
      if (!stale && !boundsMovedEnough(loadedFor.current, next)) return;
      const points = gridPoints(next);
      setStatus('loading');
      const timeout = window.setTimeout(() => controller.abort(), FORECAST_FETCH_TIMEOUT_MS);
      try {
        const res = await fetch(forecastUrl(points), { signal: controller.signal });
        if (!res.ok) throw new Error(`Forecast grid HTTP ${res.status}`);
        const parsed = parseForecast(await res.json(), points.length, Date.now());
        if (controller.signal.aborted) return;
        if (!parsed) throw new Error('Forecast grid has no usable hours');
        loadedFor.current = next;
        loadedAt.current = Date.now();
        setBounds(next);
        setData(parsed);
        setStatus('ready');
      } catch (err) {
        if (isAbort(err) || controller.signal.aborted) return;
        console.warn('[map] forecast grid failed', err);
        setStatus('error');
      } finally {
        window.clearTimeout(timeout);
      }
    };

    const schedule = (): void => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => void load(), DEBOUNCE_MS);
    };

    void load();
    map.on('moveend zoomend', schedule);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
      map.off('moveend zoomend', schedule);
    };
  }, [map, fetching, attempt]);

  return { status, data, bounds, reload };
}
