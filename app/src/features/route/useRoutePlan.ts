import { useCallback, useEffect, useRef, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import { searchPlaces } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { noteUse } from '../../lib/usage';
import { sunDays } from './daylight';
import { routeSummit } from './elevation';
import { RouteError, type RouteErrorKind } from './osrm';
import { fetchPlannedRoute, type PlannedRouteInfo } from './plannedRoute';
import { getRouteOptions } from './routeOptions';
import { nearestKnown } from './stops';
import { runPool } from './pool';
import { buildSamplePoints, withSummit, type NameBetween, type NameSummit } from './samples';
import { nameTrackPoints } from './trackNames';
import type { PointWeather, RouteLine, SamplePoint, StopDraft } from './types';

// Orchestrates one calculation: resolve typed stops, route them (TomTom with the
// rider's route options, OSRM when it cannot answer; plannedRoute.ts), look for
// a summit between the weather points, then load the weather of every point
// (3 at a time, filling in as they arrive). An imported GPX track skips the
// first two steps. A new calculation or unmounting cancels everything in flight.

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
  /**
   * Better names for points of an imported track, by point id, as they come in
   * (trackNames.ts). Kept apart from `points` so the hazards and the weather,
   * which depend on where the points are, do not reload on every new name.
   */
  placeNames: Record<string, string>;
  /** Who drew the road, for routed stops; null for an imported track, which is its own road. */
  routeInfo: PlannedRouteInfo | null;
}

const INITIAL: PlanState = { step: 'idle', error: null, route: null, points: [], weather: {}, days: 0, placeNames: {}, routeInfo: null };

/** A stop as it was resolved, with the county that tells homonyms apart. */
export interface ResolvedStop extends Place {
  region: string | null;
}

export interface CalculateInput {
  stops: readonly StopDraft[];
  days: number;
  lang: Lang;
  nameBetween: NameBetween;
  nameSummit: NameSummit;
  /** Receives the stops as resolved, so the editor can show what was used. */
  onResolved: (places: ResolvedStop[]) => void;
}

export interface TrackInput {
  /** The line of the imported file, with its stops already placed along it. */
  route: RouteLine;
  stops: readonly Place[];
  days: number;
  lang: Lang;
  nameBetween: NameBetween;
  nameSummit: NameSummit;
  /** The name of a point between stops, from the locality it is near. */
  nameNear: (locality: string) => string;
}

/** The weather points of a route, plus its summit when one stands well above them. */
async function pointsFor(
  route: RouteLine,
  stops: readonly Place[],
  names: { nameBetween: NameBetween; nameSummit: NameSummit },
  signal: AbortSignal,
): Promise<SamplePoint[]> {
  const points = buildSamplePoints(route, stops, names.nameBetween);
  const summit = await routeSummit(route, points.map((p) => p.km), signal);
  return withSummit(points, summit, names.nameSummit);
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
  // The naming of track points has its own controller: reloading the weather
  // for a longer forecast must not stop it, only a new calculation does.
  const namesRef = useRef<AbortController | null>(null);
  const stateRef = useRef(state);
  stateRef.current = state;

  useEffect(
    () => () => {
      controllerRef.current?.abort();
      namesRef.current?.abort();
    },
    [],
  );

  const fresh = useCallback((): AbortController => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    return controller;
  }, []);

  /** Cancels the naming of the previous route's points; a new one is returned for the caller to use, or ignore. */
  const freshNames = useCallback((): AbortController => {
    namesRef.current?.abort();
    const controller = new AbortController();
    namesRef.current = controller;
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
          const res = await api.weather({ name: p.name, lat: p.lat, lon: p.lon }, controller.signal, days, 'forecast');
          if (controller.signal.aborted) return;
          settle(p.id, {
            status: 'ready',
            forecast: { hourly: res.data.hourly, utcOffsetSeconds: res.data.utc_offset_seconds, sun: sunDays(res.data.daily ?? []) },
          });
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
    async ({ stops, days, lang, nameBetween, nameSummit, onResolved }: CalculateInput) => {
      const controller = fresh();
      // A routed road names its points itself: nothing to look up.
      freshNames();
      setState({ ...INITIAL, step: 'geocoding' });
      try {
        const places = await resolveStops(stops, lang, controller.signal);
        if (controller.signal.aborted) return;
        onResolved(places);
        setState((s) => ({ ...s, step: 'routing' }));
        const { route, info } = await fetchPlannedRoute(places, getRouteOptions(), controller.signal);
        if (controller.signal.aborted) return;
        const points = await pointsFor(route, places, { nameBetween, nameSummit }, controller.signal);
        if (controller.signal.aborted) return;
        setState((s) => ({ ...s, route, points, routeInfo: info }));
        noteUse('plan:route');
        await loadWeather(points, days, controller);
      } catch (err) {
        if (isAbort(err) || controller.signal.aborted) return;
        const error = err instanceof RouteError ? { kind: err.kind, name: err.kind === 'geocode' ? err.message : undefined } : { kind: 'network' as const };
        if (!(err instanceof RouteError)) console.warn('[route] calculation failed', err);
        setState((s) => ({ ...s, step: 'error', error }));
      }
    },
    [fresh, freshNames, loadWeather],
  );

  /** An imported track already is the road: no geocoding, no OSRM, only the summit lookup before the weather. */
  const calculateFromTrack = useCallback(
    async ({ route, stops, days, lang, nameBetween, nameSummit, nameNear }: TrackInput) => {
      const controller = fresh();
      const naming = freshNames();
      // The summit lookup takes a moment (5 s at most); it shows as finding the road.
      setState({ ...INITIAL, step: 'routing' });
      let points: SamplePoint[];
      try {
        points = await pointsFor(route, stops, { nameBetween, nameSummit }, controller.signal);
      } catch (err) {
        if (isAbort(err) || controller.signal.aborted) return;
        console.warn('[route] track points failed', err);
        setState((s) => ({ ...s, step: 'error', error: { kind: 'network' } }));
        return;
      }
      if (controller.signal.aborted) return;
      setState({ ...INITIAL, step: 'weather', route, points });
      // The points between stops get locality names one by one while the weather loads.
      void nameTrackPoints(points, lang, naming.signal, (id, locality) =>
        setState((s) => ({ ...s, placeNames: { ...s.placeNames, [id]: nameNear(locality) } })),
      );
      await loadWeather(points, days, controller);
    },
    [fresh, freshNames, loadWeather],
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
    namesRef.current?.abort();
    setState(INITIAL);
  }, []);

  return { state, calculate, calculateFromTrack, ensureDays, retryFailed, reset };
}
