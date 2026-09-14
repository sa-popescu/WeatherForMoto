import type { Map as LeafletMap } from 'leaflet';
import { useEffect, useState } from 'react';
import { fieldGeometry, geometryServes, type Box, type FieldGeometry } from './mercator';

// The canvas the rain ahead is painted on: the view with room around it, at
// the zoom of the radar tiles. It only changes when the view leaves it or
// needs another zoom, so panning a little never throws away painted frames.

const DEBOUNCE_MS = 400;

function viewOf(map: LeafletMap): Box | null {
  const bounds = map.getBounds();
  const view = { south: bounds.getSouth(), west: bounds.getWest(), north: bounds.getNorth(), east: bounds.getEast() };
  // A hidden map measures 0 x 0; there is nothing to size a canvas on.
  return view.north > view.south && view.east > view.west ? view : null;
}

export function useFieldGeometry(map: LeafletMap | null, enabled: boolean): FieldGeometry | null {
  const [geometry, setGeometry] = useState<FieldGeometry | null>(null);

  useEffect(() => {
    if (!map || !enabled) return undefined;
    let timer = 0;
    const update = (): void => {
      const view = viewOf(map);
      if (!view) return;
      const zoom = map.getZoom();
      setGeometry((current) => (current && geometryServes(current, view, zoom) ? current : fieldGeometry(view, zoom)));
    };
    const schedule = (): void => {
      window.clearTimeout(timer);
      timer = window.setTimeout(update, DEBOUNCE_MS);
    };
    update();
    map.on('moveend zoomend resize', schedule);
    return () => {
      window.clearTimeout(timer);
      map.off('moveend zoomend resize', schedule);
    };
  }, [map, enabled]);

  return geometry;
}
