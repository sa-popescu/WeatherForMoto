import { useStrings } from '../../lib/i18n';
import { legendGradient, RADAR_LEGEND } from './radar';
import { MAP_STRINGS } from './strings';

// Colour scale of the radar tiles, from light rain to extreme.

export function RadarLegend() {
  const s = useStrings(MAP_STRINGS);
  const summary = `${s.legendLabel}: ${RADAR_LEGEND.map((band) => s[band.key]).join(', ')}`;
  return (
    <div className="map-legend" role="img" aria-label={summary}>
      {RADAR_LEGEND.map((band) => (
        <div key={band.key} className="map-legend__band">
          <span className="map-legend__bar" style={{ background: legendGradient(band.colors) }} />
          <span className="map-legend__label">{s[band.key]}</span>
        </div>
      ))}
    </div>
  );
}
