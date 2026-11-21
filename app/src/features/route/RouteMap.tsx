import 'maplibre-gl/dist/maplibre-gl.css';
import { Marker, NavigationControl, Popup, type MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import { createVectorMap, foldAttribution } from '../../lib/vectorMap';
import { useSettings } from '../../state/settings';
import type { RouteHazard } from './hazards';
import type { TimelineRow } from './plan';
import { routeBounds } from './routeMapData';
import { addRouteLayers, hazardElement, setRouteData } from './routeMapLayers';
import { RS } from './strings';
import type { RouteLine } from './types';

// Map of the route on the app's vector base map (lib/vectorMap.ts): the line
// in stretches coloured by their worst score, the stops and sample points,
// and the hazards reported along it. It stays flat and north up. On a phone
// one finger scrolls the page past it; two fingers pinch it, and the + and -
// buttons zoom.

interface RouteMapProps {
  route: RouteLine;
  rows: readonly TimelineRow[];
  segments: ReadonlyArray<number | null>;
  hazards: readonly RouteHazard[];
  hazardText: (h: RouteHazard) => string;
  active: boolean;
  summary: string;
}

function touchScreen(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(pointer: coarse)').matches;
}

export function RouteMap({ route, rows, segments, hazards, hazardText, active, summary }: RouteMapProps) {
  const s = useStrings(RS);
  const { theme } = useSettings();
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const fittedRef = useRef<RouteLine | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const el = elRef.current;
    if (!el) return undefined;
    let map: MapLibreMap;
    try {
      map = createVectorMap(el, theme, {
        dragPan: !touchScreen(),
        scrollZoom: false,
        dragRotate: false,
        pitchWithRotate: false,
        touchPitch: false,
        maxPitch: 0,
      });
    } catch (err) {
      console.warn('[route] the map cannot be drawn on this device', err);
      return undefined;
    }
    map.touchZoomRotate.disableRotation();
    map.addControl(new NavigationControl({ showCompass: false }), 'top-left');
    mapRef.current = map;
    map.on('load', () => {
      addRouteLayers(map);
      foldAttribution(map);
      setReady(true);
    });
    // On a desktop the map fills what its column has left, so it follows the card above it.
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => map.resize());
    observer?.observe(el);
    return () => {
      observer?.disconnect();
      map.remove();
      mapRef.current = null;
      fittedRef.current = null;
      setReady(false);
    };
  }, [theme]);

  useEffect(() => {
    const map = mapRef.current;
    if (map && ready) setRouteData(map, route, rows, segments);
  }, [ready, route, rows, segments]);

  // Hazards are DOM markers, so they can be reached with the keyboard and name themselves.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return undefined;
    const markers = hazards.map((h) => {
      const text = hazardText(h);
      const element = hazardElement(h, text);
      // A tap on a phone has no hover: the same text opens above the sign.
      const tip = new Popup({ closeButton: false, offset: 24, className: 'route-map__tip' }).setText(text);
      return new Marker({ element, anchor: 'bottom' }).setLngLat([h.lon, h.lat]).setPopup(tip).addTo(map);
    });
    return () => {
      for (const marker of markers) marker.remove();
    };
  }, [hazards, hazardText, ready]);

  // Fit the new route; a hidden tab has no size, so wait until it is visible.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !active) return undefined;
    const frame = window.requestAnimationFrame(() => {
      map.resize();
      if (fittedRef.current !== route) {
        map.fitBounds(routeBounds(route), { padding: 28, duration: 0 });
        fittedRef.current = route;
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [active, route, ready]);

  return (
    <figure className="route-map" aria-label={s.mapLabel}>
      <div ref={elRef} className="route-map__canvas" />
      <figcaption className="route-map__summary num">{summary}</figcaption>
      <div className="route-map__legend" aria-hidden="true">
        <span className="route-map__legend-label num">{s.mapLegend}</span>
        <span className="route-map__swatch route-map__swatch--ideal" />
        <span className="route-map__swatch route-map__swatch--ok" />
        <span className="route-map__swatch route-map__swatch--atentie" />
        <span className="route-map__swatch route-map__swatch--evita" />
      </div>
    </figure>
  );
}
