import { useId } from 'react';
import { useStrings } from '../../lib/i18n';
import { Toggle } from '../../ui/primitives';
import type { LayerPrefs } from './layerPrefs';
import { MAP_STRINGS } from './strings';

// Layer switches: radar (with its opacity) and reported hazards.

interface Props {
  id: string;
  prefs: LayerPrefs;
  onPrefs: (change: Partial<LayerPrefs>) => void;
  hazardsFailed: boolean;
}

export function LayersPanel({ id, prefs, onPrefs, hazardsFailed }: Props) {
  const s = useStrings(MAP_STRINGS);
  const opacityId = useId();
  const percent = Math.round(prefs.opacity * 100);

  return (
    <div id={id} className="map-layers" role="group" aria-label={s.layers}>
      <p className="eyebrow">{s.layers}</p>
      <Toggle checked={prefs.radar} onChange={(radar) => onPrefs({ radar })} label={s.layerRadar} description={s.layerRadarDesc} />
      {prefs.radar && (
        <div className="map-layers__opacity">
          <label htmlFor={opacityId} className="map-layers__label">
            <span>{s.opacity}</span>
            <span className="num">{percent}%</span>
          </label>
          <input
            id={opacityId}
            type="range"
            className="map-range"
            min={20}
            max={100}
            step={10}
            value={percent}
            onChange={(event) => onPrefs({ opacity: Number(event.target.value) / 100 })}
          />
        </div>
      )}
      <Toggle
        checked={prefs.hazards}
        onChange={(hazards) => onPrefs({ hazards })}
        label={s.layerHazards}
        description={hazardsFailed ? s.hazardsFailed : s.layerHazardsDesc}
      />
    </div>
  );
}
