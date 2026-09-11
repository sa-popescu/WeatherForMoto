import { useCallback, useEffect, useRef, useState } from 'react';
import { geolocationPermission, type PlaceSuggestion } from '../../lib/geo';
import { useStrings } from '../../lib/i18n';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { newStop } from './stops';
import { RS } from './strings';
import type { StopDraft } from './types';

// The stops being edited. The origin starts as the place the app is showing;
// "my location" reuses the app's GPS fix, or asks for one.

export function useStopsState() {
  const { place, source, locate, locating } = usePlace();
  const toast = useToast();
  const s = useStrings(RS);
  const [stops, setStops] = useState<StopDraft[]>(() => [newStop(place, { gps: source === 'gps' }), newStop()]);
  const [reorder, setReorder] = useState(false);
  const awaitingGps = useRef(false);

  // locate() updates the shared place; pick it up once it lands.
  useEffect(() => {
    if (!awaitingGps.current || source !== 'gps') return;
    awaitingGps.current = false;
    setStops((list) => [newStop(place, { gps: true }), ...list.slice(1)]);
  }, [place, source]);

  const useMyLocation = useCallback(async () => {
    if (source === 'gps') {
      setStops((list) => [newStop(place, { gps: true }), ...list.slice(1)]);
      return;
    }
    awaitingGps.current = true;
    const ok = await locate();
    if (ok) return;
    awaitingGps.current = false;
    const permission = await geolocationPermission();
    toast(permission === 'denied' ? s.locateDenied : s.locateFailed, { tone: 'error' });
  }, [source, place, locate, toast, s.locateDenied, s.locateFailed]);

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

  return { stops, setStops, reorder, setReorder, updateText, pickSuggestion, useMyLocation, locating };
}

export type StopsState = ReturnType<typeof useStopsState>;
