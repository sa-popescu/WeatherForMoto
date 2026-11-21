import { useCallback, useEffect, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import { fetchDirectWeather } from '../../lib/directWeather';
import { reverseGeocode } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';
import type { WeatherResponse } from '../../lib/types';

// Weather and a name for the point picked on the desktop map. The forecast
// comes from the API like the app's own place, with the same direct
// Open-Meteo fallback; a point that came from the search already has a name,
// one clicked on the map gets it from reverse geocoding (Nominatim).

/** Two days cover the band of time and the rain line after it. */
const POINT_DAYS = 2;

export interface PickedPoint {
  lat: number;
  lon: number;
  /** Known when the point came from the search. */
  name: string | null;
}

export interface PointWeather {
  status: 'loading' | 'ready' | 'error';
  data: WeatherResponse | null;
  /** Null until reverse geocoding answers, and when it finds nothing. */
  name: string | null;
  retry: () => void;
}

export function usePointWeather(point: PickedPoint | null, lang: Lang): PointWeather {
  const [state, setState] = useState<Omit<PointWeather, 'retry'>>({ status: 'loading', data: null, name: null });
  const [attempt, setAttempt] = useState(0);
  const lat = point?.lat;
  const lon = point?.lon;
  const knownName = point?.name ?? null;

  useEffect(() => {
    if (lat === undefined || lon === undefined) return undefined;
    const controller = new AbortController();
    setState({ status: 'loading', data: null, name: knownName });

    const where = { lat, lon, name: knownName ?? `${lat.toFixed(3)}, ${lon.toFixed(3)}` };
    api
      .weather(where, controller.signal, POINT_DAYS)
      .then((result) => result.data)
      .catch((err: unknown) => {
        if (isAbort(err) || controller.signal.aborted) throw err;
        console.warn('[map] point weather from the API failed, trying direct', err);
        return fetchDirectWeather(where, controller.signal);
      })
      .then((data) => {
        if (!controller.signal.aborted) setState((s) => ({ ...s, status: 'ready', data }));
      })
      .catch((err: unknown) => {
        if (isAbort(err) || controller.signal.aborted) return;
        console.warn('[map] point weather failed', err);
        setState((s) => ({ ...s, status: 'error', data: null }));
      });

    if (!knownName) {
      reverseGeocode(lat, lon, lang, controller.signal)
        .then((name) => {
          if (name && !controller.signal.aborted) setState((s) => ({ ...s, name }));
        })
        .catch((err: unknown) => {
          if (!isAbort(err)) console.warn('[map] no name for the picked point', err);
        });
    }
    return () => controller.abort();
  }, [lat, lon, knownName, lang, attempt]);

  const retry = useCallback(() => setAttempt((n) => n + 1), []);
  return { ...state, retry };
}
