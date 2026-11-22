import { useEffect, useMemo, useRef, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import { distanceKm } from '../../lib/geo';
import { sunDays } from '../route/daylight';
import { runPool } from '../route/pool';
import type { PointWeather, SamplePoint } from '../route/types';
import { SAME_WEATHER_KM, type WeatherById } from './logic/driveWeather';

// Loads the forecasts of the weather points on screen (logic/driveWeather.ts),
// three at a time, filling in as they arrive. More routes come in over time
// (the later departures of "Plecarea perfectă", another departure picked), so
// forecasts are kept by place: a point within SAME_WEATHER_KM of one already
// loaded, or loading, uses it instead of asking again. A new trip (`tripKey`)
// starts from nothing.

const CONCURRENCY = 3;
/** Today and tomorrow, plus a day for a ride past midnight: what "Drum" needs. */
const DAYS = 3;

interface Entry {
  lat: number;
  lon: number;
  weather: PointWeather;
}

function nearest(entries: readonly Entry[], p: SamplePoint): number {
  return entries.findIndex((e) => distanceKm(e, p) < SAME_WEATHER_KM);
}

export function useDriveWeather(points: readonly SamplePoint[], tripKey: string): WeatherById {
  const [entries, setEntries] = useState<Entry[]>([]);
  const entriesRef = useRef<Entry[]>([]);
  const controllerRef = useRef<AbortController | null>(null);

  // A new trip drops the old forecasts and whatever is still loading for them.
  useEffect(() => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    entriesRef.current = [];
    setEntries([]);
    return () => controller.abort();
  }, [tripKey]);

  useEffect(() => {
    const controller = controllerRef.current;
    if (!controller || controller.signal.aborted) return;
    const missing: SamplePoint[] = [];
    for (const p of points) {
      const known = nearest(entriesRef.current, p) >= 0 || missing.some((m) => distanceKm(m, p) < SAME_WEATHER_KM);
      if (!known) missing.push(p);
    }
    if (missing.length === 0) return;
    entriesRef.current = [...entriesRef.current, ...missing.map((p) => ({ lat: p.lat, lon: p.lon, weather: LOADING }))];
    setEntries(entriesRef.current);
    const settle = (p: SamplePoint, weather: PointWeather): void => {
      if (controller.signal.aborted) return;
      entriesRef.current = entriesRef.current.map((e) => (e.lat === p.lat && e.lon === p.lon ? { ...e, weather } : e));
      setEntries(entriesRef.current);
    };
    void runPool(
      missing,
      CONCURRENCY,
      async (p) => {
        try {
          const res = await api.weather({ name: p.name, lat: p.lat, lon: p.lon }, controller.signal, DAYS);
          settle(p, { status: 'ready', forecast: { hourly: res.data.hourly, utcOffsetSeconds: res.data.utc_offset_seconds, sun: sunDays(res.data.daily ?? []) } });
        } catch (err) {
          if (isAbort(err) || controller.signal.aborted) return;
          console.warn('[drive] weather failed at', p.name, err);
          settle(p, { status: 'error', forecast: null });
        }
      },
      controller.signal,
    );
  }, [points, tripKey]);

  return useMemo(() => {
    const out: Record<string, PointWeather | undefined> = {};
    for (const p of points) {
      const i = nearest(entries, p);
      out[p.id] = i >= 0 ? entries[i].weather : undefined;
    }
    return out;
  }, [points, entries]);
}

const LOADING: PointWeather = { status: 'loading', forecast: null };
