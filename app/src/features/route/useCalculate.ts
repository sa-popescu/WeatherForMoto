import { useCallback, useState } from 'react';
import { distanceKm, searchPlaces } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { Place, SavedRoute } from '../../lib/types';
import { useToast } from '../../state/toast';
import { AS } from './actionStrings';
import type { NameBetween } from './samples';
import { MAX_STOPS, newStop, stopsKey } from './stops';
import { RS } from './strings';
import { SWEEP_LAST } from './sweep';
import { daysBetween, forecastDaysFor, minutesOf } from './timing';
import type { Departure, StopDraft } from './types';
import type { CalculateInput } from './useRoutePlan';
import type { StopsState } from './useStopsState';

// Starting a calculation from the editor, the example or a saved route, and
// noticing when the stops no longer match the route on screen.

const EXAMPLE: Place[] = [
  { name: 'București', lat: 44.4268, lon: 26.1025 },
  { name: 'Pitești', lat: 44.8565, lon: 24.8692 },
  { name: 'Sibiu', lat: 45.7983, lon: 24.1256 },
];

/** Road distance is roughly 1.3 times the straight line; enough to size the forecast. */
const ROAD_FACTOR = 1.3;

interface Options {
  stopsState: StopsState;
  calculate: (input: CalculateInput) => Promise<void>;
  hasRoute: boolean;
  departure: Departure;
  speed: number;
  today: string;
}

export function useCalculate({ stopsState, calculate, hasRoute, departure, speed, today }: Options) {
  const lang = useLang();
  const s = useStrings(RS);
  const as = useStrings(AS);
  const toast = useToast();
  const { stops, setStops } = stopsState;
  const [formError, setFormError] = useState<string | null>(null);
  const [calcKey, setCalcKey] = useState<string | null>(null);

  const nameBetween = useCallback<NameBetween>((road, previous) => road ?? fmt(s.afterStop, { stop: previous }), [s.afterStop]);

  const run = useCallback(
    (list: StopDraft[]) => {
      // Empty stops in the middle are dropped; start and destination are required.
      const filled = list.filter((st, i) => i === 0 || i === list.length - 1 || st.place || st.text.trim());
      if (!filled[0]?.text.trim() || !filled[filled.length - 1]?.text.trim()) {
        setFormError(s.needTwo);
        return;
      }
      setFormError(null);
      setStops(filled);
      const known = filled.map((st) => st.place).filter((p): p is Place => p !== null);
      let straight = 0;
      for (let i = 1; i < known.length; i += 1) straight += distanceKm(known[i - 1], known[i]);
      const latest = Math.max(minutesOf(departure.time), minutesOf(SWEEP_LAST));
      const days = forecastDaysFor(daysBetween(today, departure.date), latest, (straight * ROAD_FACTOR) / speed);
      void calculate({
        stops: filled,
        days,
        lang,
        nameBetween,
        onResolved: (places) => {
          const resolved = filled.map((st, i) => (st.place ? st : { ...st, text: places[i].name, place: places[i] }));
          setStops(resolved);
          setCalcKey(stopsKey(resolved));
        },
      });
    },
    [calculate, departure, lang, nameBetween, s.needTwo, setStops, speed, today],
  );

  const runExample = useCallback(() => run(EXAMPLE.map((p) => newStop(p))), [run]);

  const loadSaved = useCallback(
    async (route: SavedRoute) => {
      // Legacy routes can hold up to 20 stops: keep the first ones and the destination.
      const labels = route.stops.length > MAX_STOPS ? [...route.stops.slice(0, MAX_STOPS - 1), route.stops[route.stops.length - 1]] : route.stops;
      toast(as.savedLoading);
      const drafts: StopDraft[] = [];
      for (const label of labels) {
        let hit;
        try {
          hit = (await searchPlaces(label, lang))[0];
        } catch (err) {
          console.warn('[route] saved stop lookup failed', err);
        }
        if (!hit) {
          toast(fmt(as.savedLoadErr, { name: label }), { tone: 'error' });
          return;
        }
        drafts.push(newStop({ name: hit.name, lat: hit.lat, lon: hit.lon }, { region: hit.region }));
      }
      if (drafts.length >= 2) run(drafts);
    },
    [as.savedLoadErr, as.savedLoading, lang, run, toast],
  );

  const stale = hasRoute && calcKey !== null && stopsKey(stops) !== calcKey;
  return { run, runExample, loadSaved, formError, stale };
}
