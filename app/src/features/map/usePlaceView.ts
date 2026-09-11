import * as L from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import type { Place } from '../../lib/types';
import type { LatLon } from './hazards';
import { REGIONAL_ZOOM } from './useLeafletMap';

// Keeps the map on the app's current place: a marker that follows it, a
// flight to it whenever the place changes, and a recenter action. Also the
// temporary pin shown while a hazard report is being written.

const PLACE_ICON = L.divIcon({
  className: 'map-place',
  html: '<span class="map-place__halo"></span><span class="map-place__dot"></span>',
  iconSize: [28, 28],
  iconAnchor: [14, 14],
});

const PICK_ICON = L.divIcon({ className: 'map-pick', html: '<span class="map-pick__pin"></span>', iconSize: [36, 40], iconAnchor: [18, 36] });

function sameSpot(a: LatLon, b: LatLon): boolean {
  return Math.abs(a.lat - b.lat) < 1e-4 && Math.abs(a.lon - b.lon) < 1e-4;
}

/** Returns `recenter`, which flies back to the current place. */
export function usePlaceView(map: L.Map | null, place: Place, reducedMotion: boolean): () => void {
  const [recenterRequests, setRecenterRequests] = useState(0);
  // The map is created centred on the first place.
  const shown = useRef<LatLon>({ lat: place.lat, lon: place.lon });
  const handledRequests = useRef(0);
  const marker = useRef<L.Marker | null>(null);

  useEffect(() => {
    if (!map) return undefined;
    const created = L.marker([shown.current.lat, shown.current.lon], { icon: PLACE_ICON, interactive: false, keyboard: false }).addTo(map);
    marker.current = created;
    return () => {
      created.remove();
      marker.current = null;
    };
  }, [map]);

  useEffect(() => {
    if (!map) return;
    marker.current?.setLatLng([place.lat, place.lon]);
    const requested = recenterRequests !== handledRequests.current;
    handledRequests.current = recenterRequests;
    if (!requested && sameSpot(shown.current, place)) return;
    shown.current = { lat: place.lat, lon: place.lon };
    const zoom = Math.max(map.getZoom(), REGIONAL_ZOOM);
    // flyTo needs a measured map; while the tab is hidden it would compute NaN.
    const hidden = map.getSize().x === 0;
    if (reducedMotion || hidden) map.setView([place.lat, place.lon], zoom, { animate: false });
    else map.flyTo([place.lat, place.lon], zoom, { duration: 0.8 });
  }, [map, place, recenterRequests, reducedMotion]);

  return useCallback(() => setRecenterRequests((n) => n + 1), []);
}

/** Pin on the spot being reported, while the report sheet is open. */
export function usePickedMarker(map: L.Map | null, at: LatLon | null): void {
  useEffect(() => {
    if (!map || !at) return undefined;
    const pin = L.marker([at.lat, at.lon], { icon: PICK_ICON, interactive: false, keyboard: false, zIndexOffset: 1000 }).addTo(map);
    return () => {
      pin.remove();
    };
  }, [map, at]);
}
