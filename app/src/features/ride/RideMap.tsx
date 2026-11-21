import 'leaflet/dist/leaflet.css';
import * as L from 'leaflet';
import { useEffect, useRef, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import { IconButton } from '../../ui/primitives';
import { rideRecorder, type RideSnapshot } from './recorder';
import { RIDE_S } from './strings';

// Map of the ride: OpenStreetMap tiles (darkened by CSS in the dark theme, as
// on the route map), the line ridden so far as the GPS gave it (one fix a
// second, so it follows the road by itself), a break at each pause, a dotted
// line along the most likely road across each gap, and the current position.
// It follows the rider until the map is dragged; a button brings the following
// back. Finished, it shows the whole ride.

const TILE_URL = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
const ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>';
const ROMANIA: L.LatLngTuple = [45.9, 24.9];
/** Street level, where the next junction is in view. */
const FOLLOW_ZOOM = 16;
/** Below this the map is too far out to follow; the first fix zooms in. */
const MIN_FOLLOW_ZOOM = 13;

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/** The dotted line across a gap: straight at first, along the road once its fill arrives. */
interface Connector {
  line: L.Polyline;
  from: L.LatLngTuple;
  to: L.LatLngTuple;
  filled: boolean;
}

/** The last point drawn, and the line it went on. */
interface DrawHead {
  seg: number;
  at: L.LatLngTuple;
  line: L.Polyline;
}

export function RideMap({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const headRef = useRef<DrawHead | null>(null);
  const connectorsRef = useRef(new Map<number, Connector>());
  const drawnRef = useRef(0);
  const meRef = useRef<L.CircleMarker | null>(null);
  const [following, setFollowing] = useState(true);
  const finished = ride.phase === 'finished';

  useEffect(() => {
    if (!elRef.current) return undefined;
    const still = prefersReducedMotion();
    const map = L.map(elRef.current, { zoomControl: false, zoomAnimation: !still, fadeAnimation: !still, markerZoomAnimation: !still });
    map.attributionControl.setPrefix(false);
    L.tileLayer(TILE_URL, { maxZoom: 19, attribution: ATTRIBUTION, className: 'ride-map__base' }).addTo(map);
    map.setView(ROMANIA, 6);
    // Dragging means "let me look around": stop following until asked.
    map.on('dragstart', () => setFollowing(false));
    mapRef.current = map;
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => map.invalidateSize());
    observer?.observe(elRef.current);
    return () => {
      observer?.disconnect();
      map.remove();
      mapRef.current = null;
      headRef.current = null;
      connectorsRef.current = new Map();
      drawnRef.current = 0;
      meRef.current = null;
    };
  }, []);

  // Draw only the points kept since the last time. A pause breaks the line; a
  // gap breaks it too and gets a dotted connector.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const points = rideRecorder.getPoints();
    for (let i = drawnRef.current; i < points.length; i += 1) {
      const p = points[i];
      // Heartbeats while standing would only scribble GPS jitter.
      if (p.still) continue;
      const at: L.LatLngTuple = [p.lat, p.lon];
      const head = headRef.current;
      const sameSeg = head !== null && head.seg === p.seg;
      let line = head?.line;
      if (!line || !sameSeg || p.gap) {
        if (p.gap && head && sameSeg) {
          const connector = L.polyline([head.at, at], { className: 'ride-map__line ride-map__line--gap', weight: 4, interactive: false }).addTo(map);
          connectorsRef.current.set(p.t, { line: connector, from: head.at, to: at, filled: false });
        }
        line = L.polyline([], { className: 'ride-map__line', weight: 5, interactive: false }).addTo(map);
      }
      line.addLatLng(at);
      headRef.current = { seg: p.seg, at, line };
    }
    drawnRef.current = points.length;
  }, [ride.pointCount]);

  // A gap's connector follows the road once its fill is in.
  useEffect(() => {
    for (const fill of ride.fills) {
      const connector = connectorsRef.current.get(fill.t);
      if (!connector || connector.filled) continue;
      connector.line.setLatLngs([connector.from, ...fill.coords.map((c): L.LatLngTuple => [c.lat, c.lon]), connector.to]);
      connector.filled = true;
    }
  }, [ride.fills, ride.pointCount]);

  // The current position, and following it.
  const lat = ride.position?.lat;
  const lon = ride.position?.lon;
  useEffect(() => {
    const map = mapRef.current;
    if (!map || lat === undefined || lon === undefined) return;
    const here: L.LatLngTuple = [lat, lon];
    if (meRef.current) meRef.current.setLatLng(here);
    else meRef.current = L.circleMarker(here, { radius: 9, weight: 3, className: 'ride-map__me', interactive: false }).addTo(map);
    if (!following || finished) return;
    const zoom = map.getZoom() < MIN_FOLLOW_ZOOM ? FOLLOW_ZOOM : map.getZoom();
    map.setView(here, zoom, { animate: !prefersReducedMotion() });
  }, [lat, lon, following, finished]);

  // Finished: the whole ride in view.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !finished) return;
    const points = rideRecorder.getPoints();
    if (points.length < 2) return;
    map.fitBounds(L.latLngBounds(points.map((p): L.LatLngTuple => [p.lat, p.lon])), { padding: [24, 24], animate: false });
  }, [finished]);

  return (
    <section className="ride-map" aria-label={s.mapLabel}>
      <div ref={elRef} className="ride-map__canvas" />
      {!following && !finished && (
        <IconButton icon="locate" label={s.recenter} tone="accent" className="ride-map__recenter" onClick={() => setFollowing(true)} />
      )}
    </section>
  );
}
