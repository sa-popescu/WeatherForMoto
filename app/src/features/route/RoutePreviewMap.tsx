import 'maplibre-gl/dist/maplibre-gl.css';
import { LngLatBounds, Marker, NavigationControl, type MapLibreMap } from 'maplibre-gl';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { createVectorMap, foldAttribution } from '../../lib/vectorMap';
import { useSettings } from '../../state/settings';
import { RS } from './strings';
import type { StopDraft } from './types';

// On a desktop, before the first calculation: the map of the area with the
// stops picked so far, so the middle column is not left empty. The start is
// a ring, the stops between are grey, the destination is filled, as in the
// stops list. Once calculated, RouteMap takes its place with the route.

/** Around the start, or the place on screen, before two stops can be framed. */
const AREA_ZOOM = 8;

type Role = 'start' | 'via' | 'end';

interface Picked {
  place: Place;
  role: Role;
}

function stopElement(role: Role): HTMLElement {
  const el = document.createElement('div');
  el.className = `route-preview__stop route-preview__stop--${role}`;
  el.setAttribute('aria-hidden', 'true');
  return el;
}

interface Props {
  stops: readonly StopDraft[];
  /** The place the app shows, for a first view before any stop is picked. */
  appPlace: Place;
  active: boolean;
}

export function RoutePreviewMap({ stops, appPlace, active }: Props) {
  const s = useStrings(RS);
  const { theme } = useSettings();
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const [ready, setReady] = useState(false);

  const picked = useMemo<Picked[]>(
    () =>
      stops.flatMap((stop, i): Picked[] => {
        if (!stop.place) return [];
        const role: Role = i === 0 ? 'start' : i === stops.length - 1 ? 'end' : 'via';
        return [{ place: stop.place, role }];
      }),
    [stops],
  );
  // The effects below follow the picked places, not every keystroke in the list.
  const pickedKey = picked.map((p) => `${p.role}:${p.place.lat},${p.place.lon}`).join('|');

  useEffect(() => {
    const el = elRef.current;
    if (!el) return undefined;
    let map: MapLibreMap;
    try {
      map = createVectorMap(el, theme, { scrollZoom: false, dragRotate: false, pitchWithRotate: false, touchPitch: false, maxPitch: 0 });
    } catch (err) {
      console.warn('[route] the area map cannot be drawn on this device', err);
      return undefined;
    }
    map.addControl(new NavigationControl({ showCompass: false }), 'top-left');
    mapRef.current = map;
    map.on('load', () => {
      foldAttribution(map);
      setReady(true);
    });
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => map.resize());
    observer?.observe(el);
    return () => {
      observer?.disconnect();
      map.remove();
      mapRef.current = null;
      setReady(false);
    };
  }, [theme]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return undefined;
    const markers = picked.map((p) => new Marker({ element: stopElement(p.role) }).setLngLat([p.place.lon, p.place.lat]).addTo(map));
    return () => {
      for (const marker of markers) marker.remove();
    };
    // pickedKey stands for picked: the same places draw the same markers.
  }, [pickedKey, ready]);

  // Frame the stops (or the area), once the tab is visible and has a size.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready || !active) return undefined;
    const frame = window.requestAnimationFrame(() => {
      map.resize();
      if (picked.length >= 2) {
        const bounds = new LngLatBounds();
        for (const p of picked) bounds.extend([p.place.lon, p.place.lat]);
        map.fitBounds(bounds, { padding: 56, maxZoom: 11, duration: 400 });
      } else {
        const center = picked[0]?.place ?? appPlace;
        map.jumpTo({ center: [center.lon, center.lat], zoom: AREA_ZOOM });
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [pickedKey, ready, active, appPlace.lat, appPlace.lon]);

  return (
    <figure className="route-map route-preview" aria-label={s.previewLabel}>
      <div ref={elRef} className="route-map__canvas" />
    </figure>
  );
}
