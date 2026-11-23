import type { LngLat, MapMouseEvent } from 'maplibre-gl';
import { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { PlaceSuggestion } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { useWideLayout } from '../../lib/useMediaQuery';
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
import { onLongPress } from './longPress';
import { useHazardLayer } from './useHazardLayer';
import { REGIONAL_ZOOM, useMainMap } from './useMainMap';
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

  const wide = useWideLayout();
  const layersId = useId();

  const containerRef = useRef<HTMLDivElement>(null);
  const { map, baseLoading } = useMainMap(containerRef, { center: place, active, theme });
  const recenter = usePlaceView(map, place, reducedMotion);
  const [prefs, setPrefs] = useLayerPrefs();
  const timeline = useMapTimeline(map, {
    fetching: onScreen,
    radar: prefs.radar,
    lightning: prefs.lightning,
    kind: prefs.forecast,
    opacity: prefs.opacity,
    autoPlay: !reducedMotion,
    fade: !reducedMotion,
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
    const pickAt = (lngLat: LngLat): void => {
      const at = lngLat.wrap();
      setPicking(false);
      setReportAt({ lat: at.lat, lon: at.lng });
    };
    const onContextMenu = (event: MapMouseEvent): void => {
      event.preventDefault();
      pickAt(event.lngLat);
    };
    map.on('contextmenu', onContextMenu);
    const stopLongPress = onLongPress(map, pickAt);
    return () => {
      map.off('contextmenu', onContextMenu);
      stopLongPress();
    };
  }, [map]);

  // Wide layout (a desktop, or a phone turned sideways): a click or a tap on
  // the map shows the weather there in the side panel. Not while a report
  // spot is being picked, nor in the tall phone layout, which has no panel.
  const [point, setPoint] = useState<PickedPoint | null>(null);
  const pointWeather = usePointWeather(point, lang);
  usePointMarker(map, wide ? point : null);
  useEffect(() => {
    if (!map || !wide || picking) return undefined;
    const onClick = (event: MapMouseEvent): void => {
      // A click on a marker (a hazard) is the marker's, not a point to look at.
      const target = event.originalEvent.target;
      if (target instanceof Element && target.closest('.maplibregl-marker')) return;
      const at = event.lngLat.wrap();
      setPoint({ lat: at.lat, lon: at.lng, name: null });
    };
    map.on('click', onClick);
    return () => {
      map.off('click', onClick);
    };
  }, [map, wide, picking]);

  const pickFound = (hit: PlaceSuggestion): void => {
    setPoint({ lat: hit.lat, lon: hit.lon, name: hit.name });
    if (!map) return;
    const zoom = Math.max(map.getZoom(), REGIONAL_ZOOM);
    if (reducedMotion) map.jumpTo({ center: [hit.lon, hit.lat], zoom });
    else map.flyTo({ center: [hit.lon, hit.lat], zoom, duration: 800 });
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
    if (!wide) setPoint(null);
  }, [wide]);

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

  // The place's name is also the way back to it: one tap centres the map
  // there. "Locația mea" (the target in the controls) is the GPS.
  const recenterLabel = fmt(s.recenter, { place: place.name });
  const placeChip = (
    <button type="button" className="map-chip" onClick={recenter} aria-label={recenterLabel} title={recenterLabel}>
      {loading ? <Spinner size={18} /> : <Icon name="pin" size={18} />}
      <span className="map-chip__name">{place.name}</span>
    </button>
  );
  const reportButton = (
    <Button icon="alert" className="map-report" disabled={!map} onClick={startReport}>
      {s.reportHere}
    </Button>
  );
  const pickBar = <PickBar onCancel={() => setPicking(false)} onConfirm={confirmPick} />;

  // The canvas stays the first child in both layouts, so switching between
  // them never remounts the element the map lives in.
  return (
    <div className={cx('map-screen', wide && 'map-screen--desk', theme === 'dark' && 'map-screen--dark')} hidden={!active}>
      <div ref={containerRef} className="map-canvas" aria-label={s.mapLabel} />

      <div className="map-top">
        {placeChip}
        <span className="visually-hidden" role="status">
          {loading ? s.loadingMap : ''}
        </span>
        <MapControls map={map} prefs={prefs} onPrefs={setPrefs} onRecenter={recenter} hazardsFailed={hazards.failed} showLayers={!wide} />
      </div>

      {picking && (
        <span className="map-crosshair" aria-hidden="true">
          <Icon name="locate" size={64} strokeWidth={1.6} />
        </span>
      )}

      {wide ? (
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
