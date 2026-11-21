import 'leaflet/dist/leaflet.css';
import * as L from 'leaflet';
import { useEffect, useRef, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import { IconButton } from '../../ui/primitives';
import { rideRecorder, type RideSnapshot } from './recorder';
import { RIDE_S } from './strings';

// Map of the ride: OpenStreetMap tiles (darkened by CSS in the dark theme, as
// on the route map), the line ridden so far, one line per segment so a pause
// leaves a break, and the current position. It follows the rider until the map
// is dragged; a button brings the following back. Finished, it shows the whole ride.

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

interface SegmentLine {
  seg: number;
  line: L.Polyline;
}

export function RideMap({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const linesRef = useRef<SegmentLine[]>([]);
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
      linesRef.current = [];
      drawnRef.current = 0;
      meRef.current = null;
    };
  }, []);

  // Draw only the points kept since the last time.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const points = rideRecorder.getPoints();
    for (let i = drawnRef.current; i < points.length; i += 1) {
      const p = points[i];
      // Heartbeats while standing would only scribble GPS jitter.
      if (p.still) continue;
      let current = linesRef.current[linesRef.current.length - 1];
      if (!current || current.seg !== p.seg) {
        current = { seg: p.seg, line: L.polyline([], { className: 'ride-map__line', weight: 5, interactive: false }).addTo(map) };
        linesRef.current.push(current);
      }
      current.line.addLatLng([p.lat, p.lon]);
    }
    drawnRef.current = points.length;
  }, [ride.pointCount]);

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
