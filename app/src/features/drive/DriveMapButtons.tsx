import type { MapLibreMap } from 'maplibre-gl';
import { useStrings } from '../../lib/i18n';
import { IconButton } from '../../ui/primitives';
import { DriveLocate } from './DriveLocate';
import { DRIVE_S } from './strings';

// The Drum map's buttons on a desktop, in the map's bottom right corner where
// map apps keep them: "Locația mea" and the zoom. On a phone "Locația mea"
// sits right above the card instead (DriveScreen.tsx), and fingers zoom.

interface Props {
  map: MapLibreMap | null;
  /** Off while routes are on the map: the map frames them then. */
  showLocate: boolean;
  onRecenter: () => void;
}

export function DriveMapButtons({ map, showLocate, onRecenter }: Props) {
  const s = useStrings(DRIVE_S);
  return (
    <div className="drive-corner">
      {showLocate && <DriveLocate onRecenter={onRecenter} />}
      <IconButton icon="plus" label={s.zoomIn} disabled={!map} onClick={() => map?.zoomIn()} />
      <IconButton icon="minus" label={s.zoomOut} disabled={!map} onClick={() => map?.zoomOut()} />
    </div>
  );
}
