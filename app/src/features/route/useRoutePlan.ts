import { useCallback, useEffect, useRef, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import { searchPlaces } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { fetchRoute, RouteError, type RouteErrorKind } from './osrm';
import { nearestKnown } from './stops';
import { runPool } from './pool';
import { buildSamplePoints, type NameBetween } from './samples';
import type { PointWeather, RouteLine, SamplePoint, StopDraft } from './types';

// Orchestrates one calculation: resolve typed stops, route with OSRM, then
// load the weather of every sample point (3 at a time, filling in as they
// arrive). A new calculation or unmounting cancels everything in flight.

const WEATHER_CONCURRENCY = 3;

export type PlanStep = 'idle' | 'geocoding' | 'routing' | 'weather' | 'ready' | 'error';

export interface PlanState {
  step: PlanStep;
  error: { kind: RouteErrorKind; name?: string } | null;
  route: RouteLine | null;
  points: SamplePoint[];
  weather: Record<string, PointWeather>;
  /** Forecast length loaded for the points. */
  days: number;
}

const INITIAL: PlanState = { step: 'idle', error: null, route: null, points: [], weather: {}, days: 0 };

/** A stop as it was resolved, with the county that tells homonyms apart. */
export interface ResolvedStop extends Place {
  region: string | null;
}

export interface CalculateInput {
  stops: readonly StopDraft[];
  days: number;
  lang: Lang;
  nameBetween: NameBetween;
  /** Receives the stops as resolved, so the editor can show what was used. */
  onResolved: (places: ResolvedStop[]) => void;
}

async function resolveStops(stops: readonly StopDraft[], lang: Lang, signal: AbortSignal): Promise<ResolvedStop[]> {
  const out: ResolvedStop[] = [];
  for (const [index, stop] of stops.entries()) {
    if (stop.place) {
      out.push({ ...stop.place, region: stop.region });
      continue;
    }
    // A typed name is resolved against the stop before it, so "Cheia" next to
    // Vălenii de Munte is the one in Prahova and not its homonym in Brașov.
    const near = out[out.length - 1] ?? nearestKnown(stops, index);
    let found;
    try {
      found = (await searchPlaces(stop.text, lang, signal, near))[0];
    } catch (err) {
      if (isAbort(err)) throw err;
      throw new RouteError('network', err instanceof Error ? err.message : 'geocoding');
    }
    if (!found) throw new RouteError('geocode', stop.text.trim());
    out.push({ name: found.name, lat: found.lat, lon: found.lon, region: found.region });
  }
  return out;
}

export function useRoutePlan() {
  const [state, setState] = useState<PlanState>(INITIAL);
  const controllerRef = useRef<AbortController | null>(null);
  const stateRef = useRef(state);
  stateRef.current = state;

  useEffect(() => () => controllerRef.current?.abort(), []);

  const fresh = useCallback((): AbortController => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    return controller;
  }, []);

  const loadWeather = useCallback(async (points: readonly SamplePoint[], days: number, controller: AbortController) => {
    setState((s) => ({
      ...s,
      step: 'weather',
      days,
      // Merge: a partial reload (retry) must not blank the points already loaded.
      weather: {
        ...s.weather,
        ...Object.fromEntries(points.map((p) => [p.id, { status: 'loading', forecast: s.weather[p.id]?.forecast ?? null }])),
      },
    }));
    const settle = (id: string, next: PointWeather): void => setState((s) => ({ ...s, weather: { ...s.weather, [id]: next } }));
    await runPool(
      points,
      WEATHER_CONCURRENCY,
      async (p) => {
        try {
          const res = await api.weather({ name: p.name, lat: p.lat, lon: p.lon }, controller.signal, days);
          if (controller.signal.aborted) return;
          settle(p.id, { status: 'ready', forecast: { hourly: res.data.hourly, utcOffsetSeconds: res.data.utc_offset_seconds } });
        } catch (err) {
          if (isAbort(err) || controller.signal.aborted) return;
          console.warn('[route] weather failed at', p.name, err);
          settle(p.id, { status: 'error', forecast: stateRef.current.weather[p.id]?.forecast ?? null });
        }
      },
      controller.signal,
    );
    if (!controller.signal.aborted) setState((s) => ({ ...s, step: 'ready' }));
  }, []);

  const calculate = useCallback(
    async ({ stops, days, lang, nameBetween, onResolved }: CalculateInput) => {
      const controller = fresh();
      setState({ ...INITIAL, step: 'geocoding' });
      try {
        const places = await resolveStops(stops, lang, controller.signal);
        if (controller.signal.aborted) return;
        onResolved(places);
        setState((s) => ({ ...s, step: 'routing' }));
        const route = await fetchRoute(places, controller.signal);
        if (controller.signal.aborted) return;
        const points = buildSamplePoints(route, places, nameBetween);
        setState((s) => ({ ...s, route, points }));
        await loadWeather(points, days, controller);
      } catch (err) {
        if (isAbort(err) || controller.signal.aborted) return;
        const error = err instanceof RouteError ? { kind: err.kind, name: err.kind === 'geocode' ? err.message : undefined } : { kind: 'network' as const };
        if (!(err instanceof RouteError)) console.warn('[route] calculation failed', err);
        setState((s) => ({ ...s, step: 'error', error }));
      }
    },
    [fresh, loadWeather],
  );

  /** Reloads every point with a longer forecast when the departure moves further out. */
  const ensureDays = useCallback(
    (days: number) => {
      const s = stateRef.current;
      if (!s.route || s.days >= days || s.step === 'routing' || s.step === 'geocoding') return;
      void loadWeather(s.points, days, fresh());
    },
    [fresh, loadWeather],
  );

  /** Retries only the points that failed, once the batch has finished. */
  const retryFailed = useCallback(() => {
    const s = stateRef.current;
    if (s.step !== 'ready') return;
    const failed = s.points.filter((p) => s.weather[p.id]?.status === 'error');
    if (failed.length === 0) return;
    void loadWeather(failed, s.days, fresh());
  }, [fresh, loadWeather]);

  const reset = useCallback(() => {
    controllerRef.current?.abort();
    setState(INITIAL);
  }, []);

  return { state, calculate, ensureDays, retryFailed, reset };
}
