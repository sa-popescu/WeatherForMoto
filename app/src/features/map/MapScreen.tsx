import type { LeafletMouseEvent } from 'leaflet';
import { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { PlaceSuggestion } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { useDesktop } from '../../lib/useMediaQuery';
import type { Hazard } from '../../lib/types';
import { useAuth } from '../../state/auth';
import { usePlace } from '../../state/place';
import { useSettings } from '../../state/settings';
import { Icon } from '../../ui/icons';
import { Button, cx, Spinner } from '../../ui/primitives';
import type { ScreenProps } from '../types';
import { HazardSheet } from './HazardSheet';
import { hazardLabelKey, severityKey, type LatLon } from './hazards';
import { useLayerPrefs } from './layerPrefs';
import { LayersPanel } from './LayersPanel';
import { MapControls } from './MapControls';
import { MapSearch } from './MapSearch';
import { PickBar } from './PickBar';
import { PointCard } from './PointCard';
import { ReportSheet } from './ReportSheet';
import { TimelinePanel } from './TimelinePanel';
import { MAP_STRINGS } from './strings';
import { usePageVisible, usePrefersReducedMotion } from './useEnvironment';
import { useHazardLayer } from './useHazardLayer';
import { REGIONAL_ZOOM, useLeafletMap } from './useLeafletMap';
import { useMapTimeline } from './useMapTimeline';
import { usePickedMarker, usePlaceView, usePointMarker } from './usePlaceView';
import { usePointWeather, type PickedPoint } from './usePointWeather';
import './map.css';
import './map-markers.css';
import './map-radar.css';
import './map-scrubber.css';
import './map-sheets.css';
import './map-desktop.css';

// "Hartă": full-height map with one band of time, from the oldest radar frame
// to the last forecast hour, plus rider-reported hazards. The shell keeps this
// tab mounted, so the map is created once and every timer and fetch stops
// while the tab or the page is hidden.

export default function MapScreen({ active }: ScreenProps) {
  const s = useStrings(MAP_STRINGS);
  const lang = useLang();
  const { place, setPlace } = usePlace();
  const { theme } = useSettings();
  const { token } = useAuth();
  const pageVisible = usePageVisible();
  const reducedMotion = usePrefersReducedMotion();
  const onScreen = active && pageVisible;

  const desktop = useDesktop();
  const layersId = useId();

  const containerRef = useRef<HTMLDivElement>(null);
  const { map, baseLoading } = useLeafletMap(containerRef, { center: place, active });
  const recenter = usePlaceView(map, place, reducedMotion);
  const [prefs, setPrefs] = useLayerPrefs();
  const timeline = useMapTimeline(map, {
    fetching: onScreen,
    radar: prefs.radar,
    lightning: prefs.lightning,
    kind: prefs.forecast,
    opacity: prefs.opacity,
    autoPlay: !reducedMotion,
  });

  const [selected, setSelected] = useState<Hazard | null>(null);
  const labelFor = useCallback(
    (h: Hazard) => fmt(s.hazardMarker, { type: s[hazardLabelKey(h.hazard_type)], severity: s[severityKey(h.severity)] }),
    [s],
  );
  const hazards = useHazardLayer(map, { fetching: onScreen && prefs.hazards, shown: prefs.hazards, onSelect: setSelected, labelFor });

  const [picking, setPicking] = useState(false);
  const [reportAt, setReportAt] = useState<LatLon | null>(null);
  usePickedMarker(map, token ? reportAt : null);

  // Long-press (touch) or right-click picks the report position directly.
  useEffect(() => {
    if (!map) return undefined;
    const onContextMenu = (event: LeafletMouseEvent): void => {
      const at = event.latlng.wrap();
      setPicking(false);
      setReportAt({ lat: at.lat, lon: at.lng });
    };
    map.on('contextmenu', onContextMenu);
    return () => {
      map.off('contextmenu', onContextMenu);
    };
  }, [map]);

  // Desktop: a click on the map shows the weather there in the side panel.
  // Not while a report spot is being picked, and never on a phone, where a
  // tap is for moving the map.
  const [point, setPoint] = useState<PickedPoint | null>(null);
  const pointWeather = usePointWeather(point, lang);
  usePointMarker(map, desktop ? point : null);
  useEffect(() => {
    if (!map || !desktop || picking) return undefined;
    const onClick = (event: LeafletMouseEvent): void => {
      const at = event.latlng.wrap();
      setPoint({ lat: at.lat, lon: at.lng, name: null });
    };
    map.on('click', onClick);
    return () => {
      map.off('click', onClick);
    };
  }, [map, desktop, picking]);

  const pickFound = (hit: PlaceSuggestion): void => {
    setPoint({ lat: hit.lat, lon: hit.lon, name: hit.name });
    if (!map) return;
    const zoom = Math.max(map.getZoom(), REGIONAL_ZOOM);
    if (reducedMotion) map.setView([hit.lat, hit.lon], zoom, { animate: false });
    else map.flyTo([hit.lat, hit.lon], zoom, { duration: 0.8 });
  };
  const openInNow = (name: string): void => {
    if (!point) return;
    setPlace({ name, lat: point.lat, lon: point.lon }, 'search');
    window.location.hash = '#/acum';
  };
  // While the band plays, the card stays on the current hour instead of
  // jumping along with the animation.
  const momentSec = timeline.playing || !timeline.current ? Date.now() / 1000 : timeline.current.timeSec;

  // Leaving the tab drops any half-finished interaction.
  useEffect(() => {
    if (active) return;
    setPicking(false);
    setSelected(null);
    setReportAt(null);
  }, [active]);

  // The point belongs to the desktop panel: a phone layout drops it.
  useEffect(() => {
    if (!desktop) setPoint(null);
  }, [desktop]);

  const mapCentre = (): LatLon | null => {
    if (!map) return null;
    const centre = map.getCenter().wrap();
    return { lat: centre.lat, lon: centre.lng };
  };
  // Signed out there is nothing to pick yet: the sheet explains the account first.
  const startReport = (): void => (token ? setPicking(true) : setReportAt(mapCentre()));
  const confirmPick = (): void => {
    setPicking(false);
    setReportAt(mapCentre());
  };
  const loading = baseLoading || timeline.tilesLoading;

  const placeChip = (
    <div className="map-chip" role="status">
      {loading ? <Spinner size={18} /> : <Icon name="pin" size={18} />}
      <span className="map-chip__name">{place.name}</span>
      {loading && <span className="visually-hidden">{s.loadingMap}</span>}
    </div>
  );
  const reportButton = (
    <Button icon="alert" className="map-report" disabled={!map} onClick={startReport}>
      {s.reportHere}
    </Button>
  );
  const pickBar = <PickBar onCancel={() => setPicking(false)} onConfirm={confirmPick} />;

  // The canvas stays the first child in both layouts, so switching between
  // them never remounts the element the Leaflet map lives in.
  return (
    <div className={cx('map-screen', desktop && 'map-screen--desk', theme === 'dark' && 'map-screen--dark')} hidden={!active}>
      <div ref={containerRef} className="map-canvas" aria-label={s.mapLabel} />

      <div className="map-top">
        {!desktop && placeChip}
        <MapControls map={map} prefs={prefs} onPrefs={setPrefs} onRecenter={recenter} hazardsFailed={hazards.failed} showLayers={!desktop} />
      </div>

      {picking && (
        <span className="map-crosshair" aria-hidden="true">
          <Icon name="locate" size={64} strokeWidth={1.6} />
        </span>
      )}

      {desktop ? (
        // Desktop: a fixed panel on the right with the search, the weather at
        // the picked point, the layers, the report button and the band of
        // time (map-desktop.css).
        <>
          {picking && <div className="map-bottom">{pickBar}</div>}
          <div className="map-side">
            <MapSearch near={place} loading={loading} onPick={pickFound} />
            {point ? (
              <PointCard weather={pointWeather} momentSec={momentSec} onOpen={openInNow} onClose={() => setPoint(null)} />
            ) : (
              <p className="map-point-hint">{s.pointHint}</p>
            )}
            <LayersPanel id={layersId} prefs={prefs} onPrefs={setPrefs} hazardsFailed={hazards.failed} />
            {reportButton}
            <TimelinePanel timeline={timeline} />
          </div>
        </>
      ) : (
        <div className="map-bottom">
          {picking ? (
            pickBar
          ) : (
            <>
              {reportButton}
              <TimelinePanel timeline={timeline} />
            </>
          )}
        </div>
      )}

      <HazardSheet hazard={selected} onClose={() => setSelected(null)} />
      <ReportSheet at={reportAt} onClose={() => setReportAt(null)} onReported={hazards.refresh} />
    </div>
  );
}
