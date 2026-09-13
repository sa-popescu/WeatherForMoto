import { useStrings } from '../../lib/i18n';
import { colorFor, type ForecastKind } from './forecast';
import { MAP_STRINGS } from './strings';

// Colour key for the forecast part of the band.

const CLOUD_STEPS = [20, 45, 70, 95];
const RAIN_STEPS = [0.2, 1, 4, 10];

export function ForecastLegend({ kind }: { kind: ForecastKind }) {
  const s = useStrings(MAP_STRINGS);
  const steps = kind === 'cloud' ? CLOUD_STEPS : RAIN_STEPS;
  return (
    <div className="map-forecast-legend" aria-hidden="true">
      <span className="map-forecast-legend__scale">
        {steps.map((value) => {
          const [r, g, b, a] = colorFor(kind, value);
          return (
            <span
              key={value}
              className="map-forecast-legend__swatch"
              style={{ background: `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(2)})` }}
            />
          );
        })}
      </span>
      <span className="map-forecast-legend__text">{kind === 'cloud' ? s.forecastLegendCloud : s.forecastLegendRain}</span>
    </div>
  );
}
