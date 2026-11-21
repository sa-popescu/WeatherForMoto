import 'maplibre-gl/dist/maplibre-gl.css';
import { Marker, type MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef, useState } from 'react';
import { distanceKm } from '../../lib/geo';
import { useStrings } from '../../lib/i18n';
import { useSettings } from '../../state/settings';
import { IconButton } from '../../ui/primitives';
import { rideRecorder, type RideSnapshot } from './recorder';
import { bearingDeg, boundsOf, routeParts, trackLines } from './rideMapData';
import { foldAttribution } from '../../lib/vectorMap';
import { addRideLayers, createRideMap, setLines } from './rideMapSetup';
import { RIDE_S } from './strings';

// The ride map, drawn like a navigation app: a vector map, the route ahead in
// blue and the part behind faded, the line ridden as the GPS gave it (one fix a
// second, so it follows the road), dots along the most likely road across each
// gap, and an arrow for the rider. While following, the map turns with the
// direction of travel and tilts, with the arrow low on the screen so more of
// the road ahead shows. A drag, a turn or a tilt by hand stops the following
// until the button brings it back. Finished, it shows the whole ride, north up.

/** Street level, where the next junction is in view. */
const FOLLOW_ZOOM = 16;
const FOLLOW_PITCH = 50;
/** The arrow sits this far down the map (share of its height, from the top padding). */
const FOLLOW_OFFSET = 0.4;
/** A move shorter than this keeps the last direction (GPS noise while slow). */
const HEADING_STEP_M = 8;

type MapProblem = 'none' | 'offline' | 'webgl';

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/** The rider: a dot until the direction is known, then an arrow. Static markup only. */
function riderElement(): HTMLElement {
  const el = document.createElement('div');
  el.className = 'ride-me';
  el.innerHTML =
    '<svg viewBox="0 0 40 40" aria-hidden="true"><circle class="ride-me__dot" cx="20" cy="20" r="9"/><path class="ride-me__arrow" d="M20 4 32 34 20 27 8 34Z"/></svg>';
  return el;
}

export function RideMap({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const { theme } = useSettings();
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const markerRef = useRef<Marker | null>(null);
  const headingRef = useRef<{ from: { lat: number; lon: number } | null; bearing: number | null }>({ from: null, bearing: null });
  const zoomRef = useRef(FOLLOW_ZOOM);
  const [ready, setReady] = useState(false);
  const [problem, setProblem] = useState<MapProblem>('none');
  const [following, setFollowing] = useState(true);
  const finished = ride.phase === 'finished';

  useEffect(() => {
    const el = elRef.current;
    if (!el) return undefined;
    let map: MapLibreMap;
    try {
      map = createRideMap(el, theme);
    } catch (err) {
      // No WebGL: the ride still records, only the map is missing.
      console.warn('[ride] the map cannot be drawn on this device', err);
      setProblem('webgl');
      return undefined;
    }
    mapRef.current = map;
    let loaded = false;
    map.on('load', () => {
      loaded = true;
      setProblem('none');
      addRideLayers(map, theme, rideRecorder.getRoute() !== null);
      foldAttribution(map);
      setReady(true);
    });
    map.on('error', (e) => {
      console.warn('[ride] map error', e.error);
      // Before the style is in, an error means no map at all (no network); later ones are single tiles.
      if (!loaded) setProblem('offline');
    });
    const byHand = (e: { originalEvent?: unknown }): void => {
      if (e.originalEvent) setFollowing(false);
    };
    map.on('dragstart', byHand);
    map.on('rotatestart', byHand);
    map.on('pitchstart', byHand);
    map.on('zoomend', (e) => {
      if (e.originalEvent) zoomRef.current = map.getZoom();
    });
    const route = rideRecorder.getRoute();
    if (route) map.fitBounds(boundsOf(route.coords), { padding: 32, duration: 0 });
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => map.resize());
    observer?.observe(el);
    return () => {
      observer?.disconnect();
      markerRef.current?.remove();
      markerRef.current = null;
      map.remove();
      mapRef.current = null;
      setReady(false);
    };
  }, [theme]);

  // The line ridden and the gaps, redrawn with every kept point or new fill.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const lines = trackLines(rideRecorder.getPoints(), ride.fills);
    setLines(map, 'ride-track', lines.solid);
    setLines(map, 'ride-gaps', lines.gaps);
  }, [ready, ride.pointCount, ride.fills]);

  // The route, split at the progress (to 10 m, so it is not redrawn for nothing).
  const progress = ride.follow?.km == null ? null : Math.round(ride.follow.km * 100) / 100;
  useEffect(() => {
    const map = mapRef.current;
    const route = rideRecorder.getRoute();
    if (!map || !ready || !route) return;
    const parts = routeParts(route, progress);
    setLines(map, 'ride-plan-done', [parts.done]);
    setLines(map, 'ride-plan-ahead', [parts.ahead]);
  }, [ready, progress]);

  // The rider, and the camera following them.
  const lat = ride.position?.lat;
  const lon = ride.position?.lon;
  useEffect(() => {
    const map = mapRef.current;
    if (!map || lat === undefined || lon === undefined) return;
    const here = { lat, lon };
    const heading = headingRef.current;
    if (!heading.from) heading.from = here;
    else if (distanceKm(heading.from, here) * 1000 >= HEADING_STEP_M) {
      heading.bearing = bearingDeg(heading.from, here);
      heading.from = here;
    }
    if (!markerRef.current) {
      markerRef.current = new Marker({ element: riderElement(), rotationAlignment: 'map', pitchAlignment: 'map' }).setLngLat([lon, lat]).addTo(map);
    } else {
      markerRef.current.setLngLat([lon, lat]);
    }
    markerRef.current.setRotation(heading.bearing ?? 0);
    markerRef.current.getElement().classList.toggle('ride-me--heading', heading.bearing !== null);
    if (!following || finished) return;
    map.easeTo({
      center: [lon, lat],
      zoom: zoomRef.current,
      bearing: heading.bearing ?? map.getBearing(),
      pitch: FOLLOW_PITCH,
      padding: { top: map.getContainer().clientHeight * FOLLOW_OFFSET, right: 0, bottom: 0, left: 0 },
      duration: prefersReducedMotion() ? 0 : 900,
      easing: (t) => t,
    });
  }, [lat, lon, following, finished]);

  // Finished: the whole ride in view, flat and north up.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !finished) return;
    const points = rideRecorder.getPoints();
    if (points.length < 2) return;
    map.setPadding({ top: 0, right: 0, bottom: 0, left: 0 });
    map.fitBounds(boundsOf(points), { padding: 32, bearing: 0, pitch: 0, duration: 0 });
  }, [finished]);

  return (
    <section className="ride-map" aria-label={s.mapLabel}>
      <div ref={elRef} className="ride-map__canvas" />
      {problem !== 'none' && <p className="ride-map__problem">{problem === 'webgl' ? s.mapNoWebgl : s.mapOffline}</p>}
      {!following && !finished && (
        <IconButton icon="locate" label={s.recenter} tone="accent" className="ride-map__recenter" onClick={() => setFollowing(true)} />
      )}
    </section>
  );
}
