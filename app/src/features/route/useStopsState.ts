import { useCallback, useEffect, useRef, useState } from 'react';
import { geolocationPermission, type PlaceSuggestion } from '../../lib/geo';
import { useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { newStop } from './stops';
import { RS } from './strings';
import type { StopDraft } from './types';

// The stops being edited. The origin starts as the place the app is showing;
// "my location" reuses the app's GPS fix, or asks for one, and works on any
// row (a there-and-back ride starts and ends at the same fix).

/** Puts a GPS fix into one row, keeping the row's identity so the input is not remounted. */
function applyGps(list: readonly StopDraft[], id: string, place: Place): StopDraft[] {
  return list.map((st) => (st.id === id ? { ...st, text: place.name, place, region: null, gps: true } : st));
}

export function useStopsState() {
  const { place, source, locate, locating } = usePlace();
  const toast = useToast();
  const s = useStrings(RS);
  const [stops, setStops] = useState<StopDraft[]>(() => [newStop(place, { gps: source === 'gps' }), newStop()]);
  const [reorder, setReorder] = useState(false);
  // Id of the row waiting for a fix, or null when nothing is pending.
  const awaitingGps = useRef<string | null>(null);

  // locate() updates the shared place; pick it up once it lands.
  useEffect(() => {
    const id = awaitingGps.current;
    if (id === null || source !== 'gps') return;
    awaitingGps.current = null;
    setStops((list) => applyGps(list, id, place));
  }, [place, source]);

  const useMyLocation = useCallback(
    async (id: string) => {
      if (source === 'gps') {
        setStops((list) => applyGps(list, id, place));
        return;
      }
      awaitingGps.current = id;
      const ok = await locate();
      if (ok) return;
      awaitingGps.current = null;
      const permission = await geolocationPermission();
      toast(permission === 'denied' ? s.locateDenied : s.locateFailed, { tone: 'error' });
    },
    [source, place, locate, toast, s.locateDenied, s.locateFailed],
  );

  const updateText = useCallback((id: string, text: string) => {
    setStops((list) => list.map((st) => (st.id === id ? { ...st, text, place: null, region: null, gps: false } : st)));
  }, []);

  const pickSuggestion = useCallback((id: string, hit: PlaceSuggestion) => {
    setStops((list) =>
      list.map((st) =>
        st.id === id ? { ...st, text: hit.name, place: { name: hit.name, lat: hit.lat, lon: hit.lon }, region: hit.region, gps: false } : st,
      ),
    );
  }, []);

  // `appPlace` is the reference for an ambiguous name when no row is pinned yet.
  return { stops, setStops, reorder, setReorder, updateText, pickSuggestion, useMyLocation, locating, appPlace: place };
}

export type StopsState = ReturnType<typeof useStopsState>;
