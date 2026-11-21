import { Marker, type MapLibreMap } from 'maplibre-gl';
import { useCallback, useEffect, useRef, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import type { Hazard } from '../../lib/types';
import { hazardMarkerHtml } from './hazardGlyphs';
import { hazardTypeOf, mergeHazards, radiusFromBounds } from './hazards';
import { markerElement } from './markerElement';

// Hazard reports around the visible map. Fetches once the map settles
// (debounced moveend), keeps every report seen this session keyed by id, and
// mirrors them as markers reachable with Tab and opened with Enter or Space.

const MOVE_DEBOUNCE_MS = 600;
const MARKER_PX = 44;

interface Options {
  fetching: boolean;
  shown: boolean;
  onSelect: (hazard: Hazard) => void;
  labelFor: (hazard: Hazard) => string;
}

export interface HazardLayerState {
  failed: boolean;
  refresh: () => void;
}

const round4 = (value: number): number => Math.round(value * 1e4) / 1e4;

function hazardMarker(hazard: Hazard, label: string, select: () => void): Marker {
  const el = markerElement('map-hazard', hazardMarkerHtml(hazardTypeOf(hazard.hazard_type), hazard.severity), MARKER_PX, MARKER_PX);
  el.tabIndex = 0;
  el.setAttribute('role', 'button');
  el.setAttribute('aria-label', label);
  el.addEventListener('click', (event) => {
    // The map would take the click as a pick of that point otherwise.
    event.stopPropagation();
    select();
  });
  el.addEventListener('keydown', (event: KeyboardEvent) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    select();
  });
  return new Marker({ element: el }).setLngLat([hazard.lon, hazard.lat]);
}

export function useHazardLayer(map: MapLibreMap | null, { fetching, shown, onSelect, labelFor }: Options): HazardLayerState {
  const [hazards, setHazards] = useState<ReadonlyMap<number, Hazard>>(() => new Map());
  const [failed, setFailed] = useState(false);
  const [refreshes, setRefreshes] = useState(0);
  const markers = useRef(new Map<number, Marker>());
  const latest = useRef({ hazards, onSelect, labelFor });
  latest.current = { hazards, onSelect, labelFor };

  // Fetch around the view now and after every pan or zoom.
  useEffect(() => {
    if (!map || !fetching) return undefined;
    let request: AbortController | undefined;
    let timer = 0;
    const load = async (): Promise<void> => {
      request?.abort();
      const current = new AbortController();
      request = current;
      const center = map.getCenter();
      const corner = map.getBounds().getNorthEast();
      const radius = radiusFromBounds({ lat: center.lat, lon: center.lng }, { lat: corner.lat, lon: corner.lng });
      const query = center.wrap();
      try {
        const list = await api.hazards(round4(query.lat), round4(query.lng), radius, current.signal);
        setHazards((known) => mergeHazards(known, list, Date.now()));
        setFailed(false);
      } catch (err) {
        if (isAbort(err) || current.signal.aborted) return;
        console.warn('[map] hazards could not be loaded', err);
        setFailed(true);
      }
    };
    const onMoveEnd = (): void => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => void load(), MOVE_DEBOUNCE_MS);
    };
    void load();
    map.on('moveend', onMoveEnd);
    return () => {
      map.off('moveend', onMoveEnd);
      window.clearTimeout(timer);
      request?.abort();
    };
  }, [map, fetching, refreshes]);

  // Mirror the known hazards as markers while the layer is on.
  useEffect(() => {
    if (!map) return;
    for (const [id, marker] of markers.current) {
      if (shown && hazards.has(id)) continue;
      marker.remove();
      markers.current.delete(id);
    }
    if (!shown) return;
    for (const hazard of hazards.values()) {
      if (markers.current.has(hazard.id)) continue;
      const id = hazard.id;
      const select = (): void => latest.current.onSelect(latest.current.hazards.get(id) ?? hazard);
      markers.current.set(id, hazardMarker(hazard, latest.current.labelFor(hazard), select).addTo(map));
    }
  }, [map, hazards, shown]);

  // A new map (theme switch) or leaving: the markers go with the old one.
  useEffect(() => {
    const own = markers.current;
    return () => {
      for (const marker of own.values()) marker.remove();
      own.clear();
    };
  }, [map]);

  // Language change: rename the markers.
  useEffect(() => {
    for (const [id, marker] of markers.current) {
      const hazard = latest.current.hazards.get(id);
      if (hazard) marker.getElement().setAttribute('aria-label', labelFor(hazard));
    }
  }, [labelFor]);

  const refresh = useCallback(() => setRefreshes((n) => n + 1), []);
  return { failed, refresh };
}
