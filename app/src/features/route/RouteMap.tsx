import 'leaflet/dist/leaflet.css';
import * as L from 'leaflet';
import { useEffect, useRef } from 'react';
import { useStrings } from '../../lib/i18n';
import { splitLineAtKm } from './geometry';
import type { RouteHazard } from './hazards';
import type { TimelineRow } from './plan';
import { tierClass } from './routeFormat';
import { RS } from './strings';
import type { LatLon, RouteLine } from './types';

// Leaflet map of the route: OpenStreetMap tiles (darkened by CSS in the dark
// theme; CARTO now watermarks keyless tiles), the line split into stretches
// coloured by their worst score (CSS classes, so colours come from the theme
// tokens), stop and sample markers, and hazard markers.

const TILE_URL = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
const ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>';
const ROMANIA: L.LatLngTuple = [45.9, 24.9];
// Static markup only; user text never goes through innerHTML.
const HAZARD_SVG =
  '<svg width="26" height="26" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 22 20H2Z" fill="currentColor" stroke="var(--bg)" stroke-width="1.6" stroke-linejoin="round"/><path d="M12 10v4M12 17v.01" stroke="var(--bg)" stroke-width="2.2" stroke-linecap="round"/></svg>';

interface RouteMapProps {
  route: RouteLine;
  rows: readonly TimelineRow[];
  segments: ReadonlyArray<number | null>;
  hazards: readonly RouteHazard[];
  hazardText: (h: RouteHazard) => string;
  active: boolean;
  summary: string;
}

const toLatLngs = (line: readonly LatLon[]): L.LatLngTuple[] => line.map((p) => [p.lat, p.lon]);

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

export function RouteMap({ route, rows, segments, hazards, hazardText, active, summary }: RouteMapProps) {
  const s = useStrings(RS);
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layersRef = useRef<L.LayerGroup | null>(null);
  const fittedRef = useRef<RouteLine | null>(null);

  useEffect(() => {
    if (!elRef.current) return undefined;
    const still = prefersReducedMotion();
    const map = L.map(elRef.current, {
      center: ROMANIA,
      zoom: 6,
      scrollWheelZoom: false,
      // On phones one finger scrolls the page; pinch still zooms the map.
      dragging: !L.Browser.mobile,
      zoomAnimation: !still,
      fadeAnimation: !still,
      markerZoomAnimation: !still,
    });
    mapRef.current = map;
    layersRef.current = L.layerGroup().addTo(map);
    return () => {
      map.remove();
      mapRef.current = null;
      layersRef.current = null;
      fittedRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return undefined;
    const tiles = L.tileLayer(TILE_URL, { maxZoom: 19, attribution: ATTRIBUTION, className: 'route-map__base' }).addTo(map);
    return () => {
      tiles.remove();
    };
  }, []);

  useEffect(() => {
    const group = layersRef.current;
    if (!group) return;
    group.clearLayers();
    L.polyline(toLatLngs(route.coords), { className: 'route-map__casing', weight: 10, interactive: false }).addTo(group);
    const pieces = rows.length >= 2 ? splitLineAtKm(route.coords, route.cumKm, rows.slice(1, -1).map((r) => r.point.km)) : [route.coords];
    pieces.forEach((piece, i) => {
      L.polyline(toLatLngs(piece), { className: `route-map__seg route-map__seg--${tierClass(segments[i])}`, weight: 5, interactive: false }).addTo(group);
    });
    rows.forEach((row, i) => {
      const isStop = row.point.stopIndex !== null;
      const kind = !isStop ? 'sample' : i === 0 ? 'origin' : i === rows.length - 1 ? 'destination' : 'via';
      L.circleMarker([row.point.lat, row.point.lon], {
        radius: kind === 'sample' ? 4 : kind === 'via' ? 6 : 8,
        weight: kind === 'sample' ? 2 : 3,
        className: `route-map__pt route-map__pt--${kind} route-map__pt--${tierClass(row.slot?.moto_score)}`,
        interactive: false,
      }).addTo(group);
    });
    for (const h of hazards) {
      const text = hazardText(h);
      const icon = L.divIcon({ className: `route-map__hz route-map__hz--sev${Math.min(5, Math.max(1, h.severity))}`, html: HAZARD_SVG, iconSize: [26, 26], iconAnchor: [13, 20] });
      const tip = document.createElement('span');
      tip.textContent = text;
      L.marker([h.lat, h.lon], { icon, title: text, alt: text, keyboard: true }).bindTooltip(tip, { direction: 'top', offset: [0, -16] }).addTo(group);
    }
  }, [route, rows, segments, hazards, hazardText]);

  // Fit the new route; a hidden tab has no size, so wait until it is visible.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !active) return undefined;
    const frame = window.requestAnimationFrame(() => {
      map.invalidateSize();
      if (fittedRef.current !== route) {
        map.fitBounds(L.latLngBounds(toLatLngs(route.coords)), { padding: [28, 28] });
        fittedRef.current = route;
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [active, route]);

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
