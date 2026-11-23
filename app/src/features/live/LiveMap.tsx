import 'maplibre-gl/dist/maplibre-gl.css';
import { Marker, NavigationControl, type GeoJSONSource, type MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import { createVectorMap, cssVar, foldAttribution, labelsAboveRoads } from '../../lib/vectorMap';
import { useSettings } from '../../state/settings';
import { Icon } from '../../ui/icons';
import type { LiveState } from '../../worker/live';
import { LIVE_VIEW_S } from './strings';

// The viewer's map: the line the rider has left behind and a pulsing dot
// where the rider is. It follows the rider until the viewer moves the map;
// the button brings it back.

const RIDER_ZOOM = 13;
const EMPTY: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] };

function riderElement(): HTMLElement {
  const el = document.createElement('div');
  el.className = 'live-rider';
  el.setAttribute('aria-hidden', 'true');
  return el;
}

export function LiveMap({ data }: { data: LiveState }) {
  const s = useStrings(LIVE_VIEW_S);
  const { theme } = useSettings();
  const elRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const markerRef = useRef<Marker | null>(null);
  const [ready, setReady] = useState(false);
  const [following, setFollowing] = useState(true);

  useEffect(() => {
    const el = elRef.current;
    if (!el) return undefined;
    let map: MapLibreMap;
    try {
      map = createVectorMap(el, theme, { dragRotate: false, pitchWithRotate: false, touchPitch: false, maxPitch: 0 });
    } catch (err) {
      console.warn('[live] the map cannot be drawn on this device', err);
      return undefined;
    }
    map.touchZoomRotate.disableRotation();
    map.addControl(new NavigationControl({ showCompass: false }), 'top-left');
    mapRef.current = map;
    map.on('load', () => {
      map.addSource('live-trail', { type: 'geojson', data: EMPTY });
      map.addLayer(
        {
          id: 'live-trail',
          type: 'line',
          source: 'live-trail',
          layout: { 'line-cap': 'round', 'line-join': 'round' },
          paint: { 'line-color': cssVar('--accent', '#b4f03c'), 'line-width': 5, 'line-opacity': 0.85 },
        },
        labelsAboveRoads(map),
      );
      foldAttribution(map);
      setReady(true);
    });
    // A drag by the viewer stops the following; the button turns it back on.
    map.on('dragstart', () => setFollowing(false));
    return () => {
      markerRef.current?.remove();
      markerRef.current = null;
      map.remove();
      mapRef.current = null;
      setReady(false);
    };
  }, [theme]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const line: GeoJSON.Feature = {
      type: 'Feature',
      properties: {},
      geometry: { type: 'LineString', coordinates: data.trail.map(([lat, lon]) => [lon, lat]) },
    };
    map.getSource<GeoJSONSource>('live-trail')?.setData(data.trail.length > 1 ? line : EMPTY);
    if (!data.pos) return;
    const at: [number, number] = [data.pos.lon, data.pos.lat];
    markerRef.current ??= new Marker({ element: riderElement() }).setLngLat(at).addTo(map);
    markerRef.current.setLngLat(at);
    if (following) map.easeTo({ center: at, zoom: Math.max(map.getZoom(), RIDER_ZOOM), duration: 600 });
  }, [data, ready, following]);

  return (
    <div className="live-map">
      <div ref={elRef} className="live-map__canvas" aria-label={s.mapLabel} />
      {!following && data.pos && (
        <button type="button" className="icon-btn live-map__follow" aria-label={s.follow} title={s.follow} onClick={() => setFollowing(true)}>
          <Icon name="locate" size={22} />
        </button>
      )}
    </div>
  );
}
