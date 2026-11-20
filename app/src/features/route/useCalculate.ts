import { useCallback, useRef, useState } from 'react';
import { distanceKm, reverseGeocode, searchPlaces } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { Place, SavedRoute } from '../../lib/types';
import { useToast } from '../../state/toast';
import { AS } from './actionStrings';
import { GpxError, MAX_GPX_BYTES, parseGpx } from './gpx';
import type { NameBetween } from './samples';
import { MAX_STOPS, newStop, stopsKey } from './stops';
import { RS } from './strings';
import { SWEEP_LAST } from './sweep';
import { daysBetween, forecastDaysFor, minutesOf } from './timing';
import { trackFromLink, type RouteLink } from './routeLink';
import { trackFromSaved } from './savedTrack';
import { planImport, type GpxPlan } from './track';
import type { Departure, RouteLine, StopDraft } from './types';
import type { CalculateInput, TrackInput } from './useRoutePlan';
import type { StopsState } from './useStopsState';

// Starting a calculation from the editor, the example, a saved route, a GPX
// file or a shared link, and noticing when the stops no longer match the
// route on screen.

/** When and how fast: what the forecast length depends on. */
export interface RidePlan {
  departure: Departure;
  speed: number;
}

const EXAMPLE: Place[] = [
  { name: 'București', lat: 44.4268, lon: 26.1025 },
  { name: 'Pitești', lat: 44.8565, lon: 24.8692 },
  { name: 'Sibiu', lat: 45.7983, lon: 24.1256 },
];

/** Road distance is roughly 1.3 times the straight line; enough to size the forecast. */
const ROAD_FACTOR = 1.3;

/** Nominatim allows one request per second; the two ends of a track are looked up one after the other. */
const REVERSE_GAP_MS = 1100;

const pause = (ms: number): Promise<void> => new Promise((resolve) => window.setTimeout(resolve, ms));

/** The imported road on screen, kept so recalculating with the same stops does not swap it for an OSRM one. */
interface ImportedTrack {
  key: string;
  route: RouteLine;
  places: Place[];
}

interface Options {
  stopsState: StopsState;
  calculate: (input: CalculateInput) => Promise<void>;
  calculateFromTrack: (input: TrackInput) => Promise<void>;
  hasRoute: boolean;
  departure: Departure;
  speed: number;
  today: string;
}

export function useCalculate({ stopsState, calculate, calculateFromTrack, hasRoute, departure, speed, today }: Options) {
  const lang = useLang();
  const s = useStrings(RS);
  const as = useStrings(AS);
  const toast = useToast();
  const { stops, setStops } = stopsState;
  const [formError, setFormError] = useState<string | null>(null);
  const [calcKey, setCalcKey] = useState<string | null>(null);
  const imported = useRef<ImportedTrack | null>(null);
  // Bumped by every calculation and import, so a file still being read cannot overwrite a newer request.
  const requestSeq = useRef(0);

  const nameBetween = useCallback<NameBetween>((road, previous) => road ?? fmt(s.afterStop, { stop: previous }), [s.afterStop]);

  /**
   * Forecast length needed for a ride of this many km. The departure and pace
   * default to the ones on screen; a shared link passes its own, because the
   * screen has not re-rendered with them yet when the calculation starts.
   */
  const daysFor = useCallback(
    (roadKm: number, plan: RidePlan = { departure, speed }): number => {
      const latest = Math.max(minutesOf(plan.departure.time), minutesOf(SWEEP_LAST));
      return forecastDaysFor(daysBetween(today, plan.departure.date), latest, roadKm / plan.speed);
    },
    [departure, speed, today],
  );

  /** Puts a known road on screen with its stops, and remembers it so recalculating keeps it. */
  const startTrack = useCallback(
    (route: RouteLine, places: Place[], plan?: RidePlan) => {
      const drafts = places.map((p) => newStop(p));
      const key = stopsKey(drafts);
      imported.current = { key, route, places };
      setFormError(null);
      setStops(drafts);
      setCalcKey(key);
      void calculateFromTrack({ route, stops: places, days: daysFor(route.distanceKm, plan), nameBetween });
    },
    [calculateFromTrack, daysFor, nameBetween, setStops],
  );

  const run = useCallback(
    (list: StopDraft[], plan?: RidePlan) => {
      // Empty stops in the middle are dropped; start and destination are required.
      const filled = list.filter((st, i) => i === 0 || i === list.length - 1 || st.place || st.text.trim());
      if (!filled[0]?.text.trim() || !filled[filled.length - 1]?.text.trim()) {
        setFormError(s.needTwo);
        return;
      }
      setFormError(null);
      setStops(filled);
      requestSeq.current += 1;
      const track = imported.current;
      if (track && stopsKey(filled) === track.key) {
        // Untouched stops of an imported GPX: keep the road from the file.
        setCalcKey(track.key);
        void calculateFromTrack({ route: track.route, stops: track.places, days: daysFor(track.route.distanceKm, plan), nameBetween });
        return;
      }
      imported.current = null;
      const known = filled.map((st) => st.place).filter((p): p is Place => p !== null);
      let straight = 0;
      for (let i = 1; i < known.length; i += 1) straight += distanceKm(known[i - 1], known[i]);
      void calculate({
        stops: filled,
        days: daysFor(straight * ROAD_FACTOR, plan),
        lang,
        nameBetween,
        onResolved: (places) => {
          const resolved = filled.map((st, i) =>
            st.place ? st : { ...st, text: places[i].name, place: { name: places[i].name, lat: places[i].lat, lon: places[i].lon }, region: places[i].region },
          );
          setStops(resolved);
          setCalcKey(stopsKey(resolved));
        },
      });
    },
    [calculate, calculateFromTrack, daysFor, lang, nameBetween, s.needTwo, setStops],
  );

  const runExample = useCallback(() => run(EXAMPLE.map((p) => newStop(p))), [run]);

  const loadSaved = useCallback(
    async (route: SavedRoute) => {
      // A route imported from GPX comes back on its own road, with its stops where they were.
      const road = trackFromSaved(route);
      if (road) {
        requestSeq.current += 1;
        startTrack(road.route, road.places);
        return;
      }
      // Legacy routes can hold up to 20 stops: keep the first ones and the destination.
      const labels = route.stops.length > MAX_STOPS ? [...route.stops.slice(0, MAX_STOPS - 1), route.stops[route.stops.length - 1]] : route.stops;
      toast(as.savedLoading);
      const drafts: StopDraft[] = [];
      for (const label of labels) {
        let hit;
        try {
          // Same reasoning as a typed stop: the stop before it disambiguates the name.
          hit = (await searchPlaces(label, lang, undefined, drafts[drafts.length - 1]?.place))[0];
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
    [as.savedLoadErr, as.savedLoading, lang, run, startTrack, toast],
  );

  const importGpx = useCallback(
    async (file: File) => {
      requestSeq.current += 1;
      const seq = requestSeq.current;
      let plan: GpxPlan;
      try {
        if (file.size > MAX_GPX_BYTES) throw new GpxError('too-large');
        toast(as.gpxReading);
        plan = planImport(parseGpx(await file.text()));
      } catch (err) {
        const kind = err instanceof GpxError ? err.kind : null;
        if (kind === null) console.warn('[route] GPX import failed', err);
        toast(kind === 'too-large' ? as.gpxTooLarge : kind === 'empty' ? as.gpxEmpty : as.gpxReadErr, { tone: 'error' });
        return;
      }
      if (seq !== requestSeq.current) return;

      if (plan.kind === 'stops') {
        // A few points and no drawn road: route between them like typed stops.
        toast(as.gpxStopsOnly);
        run(plan.stops.map((p, i) => newStop({ name: p.name ?? fmt(as.gpxPoint, { n: i + 1 }), lat: p.lat, lon: p.lon })));
        return;
      }

      // Only the two ends of a track can lack a name; ask for the locality there.
      const places: Place[] = [];
      let lookedUp = false;
      for (const [index, stop] of plan.stops.entries()) {
        let name = stop.name;
        if (!name) {
          if (lookedUp) await pause(REVERSE_GAP_MS);
          name = await reverseGeocode(stop.lat, stop.lon, lang);
          lookedUp = true;
        }
        places.push({ name: name ?? (index === 0 ? s.fieldOrigin : s.fieldDestination), lat: stop.lat, lon: stop.lon });
      }
      if (seq !== requestSeq.current) return;

      toast(fmt(as.gpxImported, { km: Math.round(plan.route.distanceKm) }), { tone: 'success' });
      startTrack(plan.route, places);
    },
    [as, lang, run, s.fieldDestination, s.fieldOrigin, startTrack, toast],
  );

  /**
   * A plan from a shared link: the road it carries when it came from a GPX,
   * otherwise its stops, routed like typed ones. Returns false when the road
   * in the link has no length, so the caller can say the link is broken.
   */
  const openShared = useCallback(
    (link: RouteLink, plan: RidePlan): boolean => {
      requestSeq.current += 1;
      const places: Place[] = link.stops.map((st) => ({ name: st.name, lat: st.lat, lon: st.lon }));
      if (link.line) {
        const route = trackFromLink(link.line, link.stops);
        if (!route) return false;
        startTrack(route, places, plan);
        return true;
      }
      run(places.map((p) => newStop(p)), plan);
      return true;
    },
    [run, startTrack],
  );

  /** The imported road on screen, when that is what the current stops were calculated on. */
  const sharedTrack = useCallback((): RouteLine | null => {
    const track = imported.current;
    return track && track.key === calcKey ? track.route : null;
  }, [calcKey]);

  const stale = hasRoute && calcKey !== null && stopsKey(stops) !== calcKey;
  return { run, runExample, loadSaved, importGpx, openShared, sharedTrack, formError, stale };
}
