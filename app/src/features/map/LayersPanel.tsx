import { useId } from 'react';
import { useStrings } from '../../lib/i18n';
import { cx, Toggle } from '../../ui/primitives';
import type { ForecastLayer, LayerPrefs } from './layerPrefs';
import { MAP_STRINGS } from './strings';

// Layer switches: radar (with its opacity), the forecast field painted over
// the map, and reported hazards.

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
      <ForecastChoice value={prefs.forecast} onChange={(forecast) => onPrefs({ forecast })} />
      <Toggle
        checked={prefs.hazards}
        onChange={(hazards) => onPrefs({ hazards })}
        label={s.layerHazards}
        description={hazardsFailed ? s.hazardsFailed : s.layerHazardsDesc}
      />
    </div>
  );
}

const FORECAST_OPTIONS: ReadonlyArray<{ value: ForecastLayer; key: 'forecastRain' | 'forecastCloud' }> = [
  { value: 'rain', key: 'forecastRain' },
  { value: 'cloud', key: 'forecastCloud' },
];

function ForecastChoice({ value, onChange }: { value: ForecastLayer; onChange: (next: ForecastLayer) => void }) {
  const s = useStrings(MAP_STRINGS);
  return (
    <div className="map-layers__group" role="group" aria-label={s.layerForecast}>
      <p className="map-layers__title">{s.layerForecast}</p>
      <p className="map-layers__desc">{s.layerForecastDesc}</p>
      <div className="map-layers__choice">
        {FORECAST_OPTIONS.map((option) => (
          <button
            key={option.value}
            type="button"
            className={cx('map-layers__option', value === option.value && 'map-layers__option--on')}
            aria-pressed={value === option.value}
            onClick={() => onChange(option.value)}
          >
            {s[option.key]}
          </button>
        ))}
      </div>
    </div>
  );
}
