import { Marker, type MapLibreMap } from 'maplibre-gl';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { Place } from '../../lib/types';
import type { LatLon } from './hazards';
import { markerElement } from './markerElement';
import { REGIONAL_ZOOM } from './useMainMap';

// Keeps the map on the app's current place: a marker that follows it, a
// flight to it whenever the place changes, and a recenter action. Also the
// temporary pin shown while a hazard report is being written, and the ring on
// the point whose weather the desktop panel shows.

const PLACE_HTML = '<span class="map-place__halo"></span><span class="map-place__dot"></span>';
const PICK_HTML = '<span class="map-pick__pin"></span>';
const POINT_HTML = '<span class="map-point-mark__ring"></span><span class="map-point-mark__dot"></span>';

/** The pick pin's tip sits 4 px above the bottom of its 36 x 40 box. */
const PICK_TIP_OFFSET: [number, number] = [0, 4];

function sameSpot(a: LatLon, b: LatLon): boolean {
  return Math.abs(a.lat - b.lat) < 1e-4 && Math.abs(a.lon - b.lon) < 1e-4;
}

/**
 * Puts the map on a spot with that spot `offsetY` pixels above the map's
 * middle, at once (a flight takes the offset itself).
 */
function jumpWithOffset(map: MapLibreMap, at: LatLon, zoom: number, offsetY: number): void {
  map.jumpTo({ center: [at.lon, at.lat], zoom });
  if (offsetY !== 0) map.panBy([0, offsetY], { duration: 0 });
}

/**
 * Returns `recenter`, which flies back to the current place. The place is
 * shown `offsetY` pixels above the map's middle: in the middle of the part
 * the panels leave free (useFreeArea.ts).
 */
export function usePlaceView(map: MapLibreMap | null, place: Place, reducedMotion: boolean, offsetY = 0): () => void {
  const [recenterRequests, setRecenterRequests] = useState(0);
  // The map is created centred on the first place.
  const shown = useRef<LatLon>({ lat: place.lat, lon: place.lon });
  const handledRequests = useRef(0);
  const marker = useRef<Marker | null>(null);
  /** The rider moved the map since the place was last put in view: leave the view alone. */
  const movedByHand = useRef(false);
  const offset = useRef(offsetY);
  offset.current = offsetY;

  useEffect(() => {
    if (!map) return undefined;
    const onMoveStart = (event: { originalEvent?: unknown }): void => {
      // Moves the app makes have no input event behind them.
      if (event.originalEvent) movedByHand.current = true;
    };
    map.on('movestart', onMoveStart);
    return () => {
      map.off('movestart', onMoveStart);
    };
  }, [map]);

  // The free part changed (the band's panel filled in, the phone turned): the place keeps its spot in it.
  useEffect(() => {
    if (!map || movedByHand.current || map.getContainer().clientWidth === 0) return;
    jumpWithOffset(map, shown.current, map.getZoom(), offsetY);
  }, [map, offsetY]);

  useEffect(() => {
    if (!map) return undefined;
    const created = new Marker({ element: markerElement('map-place', PLACE_HTML, 28, 28) }).setLngLat([shown.current.lon, shown.current.lat]).addTo(map);
    marker.current = created;
    return () => {
      created.remove();
      marker.current = null;
    };
  }, [map]);

  useEffect(() => {
    if (!map) return;
    marker.current?.setLngLat([place.lon, place.lat]);
    const requested = recenterRequests !== handledRequests.current;
    handledRequests.current = recenterRequests;
    if (!requested && sameSpot(shown.current, place)) return;
    shown.current = { lat: place.lat, lon: place.lon };
    movedByHand.current = false;
    const zoom = Math.max(map.getZoom(), REGIONAL_ZOOM);
    // A flight needs a measured map; while the tab is hidden it would compute NaN.
    const hidden = map.getContainer().clientWidth === 0;
    if (reducedMotion || hidden) jumpWithOffset(map, shown.current, zoom, offset.current);
    else map.flyTo({ center: [place.lon, place.lat], zoom, duration: 800, offset: [0, -offset.current] });
  }, [map, place, recenterRequests, reducedMotion]);

  return useCallback(() => setRecenterRequests((n) => n + 1), []);
}

/** Ring on the point whose weather the desktop panel shows. */
export function usePointMarker(map: MapLibreMap | null, at: LatLon | null): void {
  useEffect(() => {
    if (!map || !at) return undefined;
    const ring = new Marker({ element: markerElement('map-point-mark', POINT_HTML, 32, 32) }).setLngLat([at.lon, at.lat]).addTo(map);
    return () => {
      ring.remove();
    };
  }, [map, at]);
}

/** Pin on the spot being reported, while the report sheet is open. */
export function usePickedMarker(map: MapLibreMap | null, at: LatLon | null): void {
  useEffect(() => {
    if (!map || !at) return undefined;
    const pin = new Marker({ element: markerElement('map-pick', PICK_HTML, 36, 40), anchor: 'bottom', offset: PICK_TIP_OFFSET })
      .setLngLat([at.lon, at.lat])
      .addTo(map);
    return () => {
      pin.remove();
    };
  }, [map, at]);
}
