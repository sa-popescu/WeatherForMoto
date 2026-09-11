import type { Map as LeafletMap } from 'leaflet';
import { useEffect, useId, useRef, useState } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { IconButton, Spinner } from '../../ui/primitives';
import type { LayerPrefs } from './layerPrefs';
import { LayersPanel } from './LayersPanel';
import { MAP_STRINGS } from './strings';

// Floating controls, top right: layers, my location, recenter and zoom. All
// 48 px buttons with labels, so they work with gloves and from the keyboard.

const LOCATE_ERRORS = {
  denied: 'locDenied',
  timeout: 'locTimeout',
  unsupported: 'locUnsupported',
  unavailable: 'locUnavailable',
} as const;

interface Props {
  map: LeafletMap | null;
  prefs: LayerPrefs;
  onPrefs: (change: Partial<LayerPrefs>) => void;
  onRecenter: () => void;
  hazardsFailed: boolean;
}

export function MapControls({ map, prefs, onPrefs, onRecenter, hazardsFailed }: Props) {
  const s = useStrings(MAP_STRINGS);
  const toast = useToast();
  const { place, locate, locating, locateError } = usePlace();
  const [layersOpen, setLayersOpen] = useState(false);
  const panelId = useId();
  const root = useRef<HTMLDivElement>(null);
  const askedToLocate = useRef(false);

  // The place hook only exposes the failure reason as state, so the toast waits for it.
  useEffect(() => {
    if (!askedToLocate.current || !locateError) return;
    askedToLocate.current = false;
    toast(s[LOCATE_ERRORS[locateError]], { tone: 'error' });
  }, [locateError, s, toast]);

  // Close the layers panel on Escape or a tap anywhere else.
  useEffect(() => {
    if (!layersOpen) return undefined;
    const onPointer = (event: PointerEvent): void => {
      if (event.target instanceof Node && !root.current?.contains(event.target)) setLayersOpen(false);
    };
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') setLayersOpen(false);
    };
    document.addEventListener('pointerdown', onPointer);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('pointerdown', onPointer);
      document.removeEventListener('keydown', onKey);
    };
  }, [layersOpen]);

  const onLocate = async (): Promise<void> => {
    askedToLocate.current = true;
    if (await locate()) {
      askedToLocate.current = false;
      onRecenter();
    }
  };

  return (
    <div className="map-controls" ref={root} role="group" aria-label={s.controls}>
      <IconButton
        icon="layers"
        label={s.layers}
        className="map-ctl"
        aria-expanded={layersOpen}
        aria-controls={panelId}
        onClick={() => setLayersOpen((open) => !open)}
      />
      {layersOpen && <LayersPanel id={panelId} prefs={prefs} onPrefs={onPrefs} hazardsFailed={hazardsFailed} />}
      <button
        type="button"
        className="icon-btn map-ctl"
        aria-label={s.locate}
        title={s.locate}
        aria-busy={locating || undefined}
        disabled={locating}
        onClick={() => void onLocate()}
      >
        {locating ? <Spinner size={20} /> : <Icon name="locate" size={22} />}
      </button>
      <IconButton icon="pin" label={fmt(s.recenter, { place: place.name })} className="map-ctl" onClick={onRecenter} />
      <div className="map-zoom">
        <IconButton icon="plus" label={s.zoomIn} disabled={!map} onClick={() => map?.zoomIn()} />
        <IconButton icon="minus" label={s.zoomOut} disabled={!map} onClick={() => map?.zoomOut()} />
      </div>
    </div>
  );
}
