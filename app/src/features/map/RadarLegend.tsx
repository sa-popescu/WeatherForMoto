import { useStrings } from '../../lib/i18n';
import { useSettings } from '../../state/settings';
import { legendGradient, rainLegend, snowLegend } from './rainPalette';
import { MAP_STRINGS } from './strings';

// The colours rain is drawn in, from light to extreme, and snow below them.

export function RadarLegend() {
  const s = useStrings(MAP_STRINGS);
  const { theme } = useSettings();
  const bands = rainLegend(theme);
  const snow = snowLegend(theme);
  const summary = `${s.legendLabel}: ${bands.map((band) => s[band.key]).join(', ')}; ${s.legendSnow}`;
  return (
    <div role="img" aria-label={summary}>
      <div className="map-legend">
        {bands.map((band) => (
          <div key={band.key} className="map-legend__band">
            <span className="map-legend__bar" style={{ background: legendGradient(band.stops, band.fromDbz, band.toDbz) }} />
            <span className="map-legend__label">{s[band.key]}</span>
          </div>
        ))}
      </div>
      <p className="map-snow-key">
        <span
          className="map-snow-key__swatch"
          style={{ background: legendGradient(snow.stops, snow.fromDbz, snow.toDbz) }}
          aria-hidden="true"
        />
        {s.legendSnow}
      </p>
    </div>
  );
}
