import type { LeafletMouseEvent } from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
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
import { MapControls } from './MapControls';
import { PickBar } from './PickBar';
import { ReportSheet } from './ReportSheet';
import { TimelinePanel } from './TimelinePanel';
import { MAP_STRINGS } from './strings';
import { usePageVisible, usePrefersReducedMotion } from './useEnvironment';
import { useHazardLayer } from './useHazardLayer';
import { useLeafletMap } from './useLeafletMap';
import { useMapTimeline } from './useMapTimeline';
import { usePickedMarker, usePlaceView } from './usePlaceView';
import './map.css';
import './map-markers.css';
import './map-radar.css';
import './map-scrubber.css';
import './map-sheets.css';

// "Hartă": full-height map with one band of time, from the oldest radar frame
// to the last forecast hour, plus rider-reported hazards. The shell keeps this
// tab mounted, so the map is created once and every timer and fetch stops
// while the tab or the page is hidden.

export default function MapScreen({ active }: ScreenProps) {
  const s = useStrings(MAP_STRINGS);
  const { place } = usePlace();
  const { theme } = useSettings();
  const { token } = useAuth();
  const pageVisible = usePageVisible();
  const reducedMotion = usePrefersReducedMotion();
  const onScreen = active && pageVisible;

  const containerRef = useRef<HTMLDivElement>(null);
  const { map, baseLoading } = useLeafletMap(containerRef, { center: place, active });
  const recenter = usePlaceView(map, place, reducedMotion);
  const [prefs, setPrefs] = useLayerPrefs();
  const timeline = useMapTimeline(map, {
    fetching: onScreen,
    radar: prefs.radar,
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

  // Leaving the tab drops any half-finished interaction.
  useEffect(() => {
    if (active) return;
    setPicking(false);
    setSelected(null);
    setReportAt(null);
  }, [active]);

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

  return (
    <div className={cx('map-screen', theme === 'dark' && 'map-screen--dark')} hidden={!active}>
      <div ref={containerRef} className="map-canvas" aria-label={s.mapLabel} />

      <div className="map-top">
        <div className="map-chip" role="status">
          {loading ? <Spinner size={18} /> : <Icon name="pin" size={18} />}
          <span className="map-chip__name">{place.name}</span>
          {loading && <span className="visually-hidden">{s.loadingMap}</span>}
        </div>
        <MapControls map={map} prefs={prefs} onPrefs={setPrefs} onRecenter={recenter} hazardsFailed={hazards.failed} />
      </div>

      {picking && (
        <span className="map-crosshair" aria-hidden="true">
          <Icon name="locate" size={64} strokeWidth={1.6} />
        </span>
      )}

      <div className="map-bottom">
        {picking ? (
          <PickBar onCancel={() => setPicking(false)} onConfirm={confirmPick} />
        ) : (
          <>
            <Button icon="alert" className="map-report" disabled={!map} onClick={startReport}>
              {s.reportHere}
            </Button>
            <TimelinePanel timeline={timeline} />
          </>
        )}
      </div>

      <HazardSheet hazard={selected} onClose={() => setSelected(null)} />
      <ReportSheet at={reportAt} onClose={() => setReportAt(null)} onReported={hazards.refresh} />
    </div>
  );
}
