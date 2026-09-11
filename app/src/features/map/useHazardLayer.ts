import * as L from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import type { Hazard } from '../../lib/types';
import { hazardMarkerHtml } from './hazardGlyphs';
import { hazardTypeOf, mergeHazards, radiusFromBounds } from './hazards';

// Hazard reports around the visible map. Fetches once the map settles
// (debounced moveend), keeps every report seen this session keyed by id, and
// mirrors them as markers reachable with Tab and opened with Enter.

const MOVE_DEBOUNCE_MS = 600;

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

function markerIcon(hazard: Hazard): L.DivIcon {
  return L.divIcon({
    className: 'map-hazard',
    html: hazardMarkerHtml(hazardTypeOf(hazard.hazard_type), hazard.severity),
    iconSize: [44, 44],
    iconAnchor: [22, 22],
  });
}

const round4 = (value: number): number => Math.round(value * 1e4) / 1e4;

export function useHazardLayer(map: L.Map | null, { fetching, shown, onSelect, labelFor }: Options): HazardLayerState {
  const [hazards, setHazards] = useState<ReadonlyMap<number, Hazard>>(() => new Map());
  const [failed, setFailed] = useState(false);
  const [refreshes, setRefreshes] = useState(0);
  const group = useRef<L.LayerGroup | null>(null);
  const markers = useRef(new Map<number, L.Marker>());
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

  useEffect(() => {
    if (!map) return undefined;
    const layerGroup = L.layerGroup();
    const own = markers.current;
    group.current = layerGroup;
    return () => {
      layerGroup.remove();
      group.current = null;
      own.clear();
    };
  }, [map]);

  useEffect(() => {
    if (!map || !group.current) return;
    if (shown) group.current.addTo(map);
    else group.current.remove();
  }, [map, shown]);

  // Mirror the known hazards as markers.
  useEffect(() => {
    const layerGroup = group.current;
    if (!layerGroup) return;
    for (const [id, marker] of markers.current) {
      if (hazards.has(id)) continue;
      layerGroup.removeLayer(marker);
      markers.current.delete(id);
    }
    for (const hazard of hazards.values()) {
      if (markers.current.has(hazard.id)) continue;
      const marker = L.marker([hazard.lat, hazard.lon], { icon: markerIcon(hazard), keyboard: true, riseOnHover: true });
      const id = hazard.id;
      const select = (): void => latest.current.onSelect(latest.current.hazards.get(id) ?? hazard);
      // Leaflet makes keyboard markers focusable (role=button) but only maps Enter
      // to popups, so Enter and Space are wired here. The element is rebuilt on
      // every add, so the listener never doubles up.
      marker.on('add', () => {
        const element = marker.getElement();
        if (!element) return;
        element.setAttribute('aria-label', latest.current.labelFor(hazard));
        element.addEventListener('keydown', (event: KeyboardEvent) => {
          if (event.key !== 'Enter' && event.key !== ' ') return;
          event.preventDefault();
          select();
        });
      });
      marker.on('click', select);
      markers.current.set(id, marker);
      layerGroup.addLayer(marker);
    }
  }, [map, hazards]);

  // Language change: rename the markers.
  useEffect(() => {
    for (const [id, marker] of markers.current) {
      const hazard = latest.current.hazards.get(id);
      if (hazard) marker.getElement()?.setAttribute('aria-label', labelFor(hazard));
    }
  }, [labelFor]);

  const refresh = useCallback(() => setRefreshes((n) => n + 1), []);
  return { failed, refresh };
}
