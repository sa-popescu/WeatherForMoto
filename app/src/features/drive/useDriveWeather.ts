import { useEffect, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import { sunDays } from '../route/daylight';
import { runPool } from '../route/pool';
import type { PointWeather, SamplePoint } from '../route/types';
import type { WeatherById } from './logic/driveWeather';

// Loads the forecast of every shared weather point of the routes on screen
// (logic/driveWeather.ts), three at a time, filling in as they arrive. A new
// set of points cancels what is still loading for the old one.

const CONCURRENCY = 3;
/** Today and tomorrow, plus a day for a ride past midnight: what "Drum" needs. */
const DAYS = 3;

export function useDriveWeather(points: readonly SamplePoint[]): WeatherById {
  const [weather, setWeather] = useState<Record<string, PointWeather>>({});

  useEffect(() => {
    const controller = new AbortController();
    setWeather(Object.fromEntries(points.map((p) => [p.id, { status: 'loading', forecast: null }])));
    const settle = (id: string, next: PointWeather): void => {
      if (!controller.signal.aborted) setWeather((w) => ({ ...w, [id]: next }));
    };
    void runPool(
      points,
      CONCURRENCY,
      async (p) => {
        try {
          const res = await api.weather({ name: p.name, lat: p.lat, lon: p.lon }, controller.signal, DAYS);
          settle(p.id, {
            status: 'ready',
            forecast: { hourly: res.data.hourly, utcOffsetSeconds: res.data.utc_offset_seconds, sun: sunDays(res.data.daily ?? []) },
          });
        } catch (err) {
          if (isAbort(err) || controller.signal.aborted) return;
          console.warn('[drive] weather failed at', p.name, err);
          settle(p.id, { status: 'error', forecast: null });
        }
      },
      controller.signal,
    );
    return () => controller.abort();
  }, [points]);

  return weather;
}
