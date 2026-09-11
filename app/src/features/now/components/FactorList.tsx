import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor } from '../../../lib/scoring';
import type { CurrentWeather, HourlyWeather, ScoreFactor } from '../../../lib/types';
import { Icon, type IconName } from '../../../ui/icons';
import { causePhrase, isCauseFactor, reductionOf, type Cause, type CauseFactor } from '../logic/cause';
import { capitalize } from '../logic/formatting';
import { S_SHEETS } from '../sheetStrings';

/** The readings a factor sentence needs ("80% șanse, 3,2 mm/h", "rafale de 62 km/h"). */
export type FactorValues = Omit<Cause, 'factor' | 'from' | 'reduction'>;

export function valuesFromCurrent(c: CurrentWeather): FactorValues {
  return {
    probability: c.precipitation_probability,
    mm: c.precipitation_mm,
    gust: c.wind_gusts_kmh,
    feels: c.feels_like,
    visibilityM: c.visibility_km == null ? null : c.visibility_km * 1000,
    road: c.road_surface_temp,
  };
}

export function valuesFromHour(h: HourlyWeather): FactorValues {
  return {
    probability: h.precipitation_probability,
    mm: h.precipitation_mm,
    gust: h.wind_gusts_kmh,
    feels: h.feels_like,
    visibilityM: h.visibility,
    road: h.road_surface_temp,
  };
}

const NAME_KEY: Record<CauseFactor, keyof typeof S_SHEETS.ro> = {
  rain: 'factorRain',
  storm: 'factorStorm',
  snow: 'factorSnow',
  ice: 'factorIce',
  fog: 'factorFog',
  wind: 'factorWind',
  cold: 'factorCold',
  heat: 'factorHeat',
  cold_wet: 'factorColdWet',
  frost: 'factorFrost',
};

const ICON: Record<CauseFactor, IconName> = {
  rain: 'drop',
  storm: 'alert',
  snow: 'thermo',
  ice: 'alert',
  fog: 'eye',
  wind: 'wind',
  cold: 'thermo',
  heat: 'sun',
  cold_wet: 'drop',
  frost: 'road',
};

/** Score factors in plain language, the costliest first. */
export function FactorList({ factors, values }: { factors: ReadonlyArray<ScoreFactor>; values: FactorValues }) {
  const s = useStrings(S_SHEETS);
  const lang = useLang();
  if (!factors.length) return <p className="now-factors__empty">{s.scoreNoFactors}</p>;
  const sorted = [...factors].sort((a, b) => reductionOf(b) - reductionOf(a));

  return (
    <ul className="now-factors">
      {sorted.map((f) => {
        const known = isCauseFactor(f.factor);
        return (
          <li key={f.factor} className="now-factor">
            <span className="now-factor__icon" style={{ color: tierColor(100 - reductionOf(f)) }}>
              <Icon name={known ? ICON[f.factor as CauseFactor] : 'info'} size={22} />
            </span>
            <span className="now-factor__text">
              <span className="now-factor__name">{known ? s[NAME_KEY[f.factor as CauseFactor]] : s.factorOther}</span>
              <span className="now-factor__detail">
                {known ? capitalize(causePhrase({ factor: f.factor as CauseFactor, from: null, reduction: reductionOf(f), ...values }, lang)) : f.detail}
              </span>
            </span>
            <span className="now-factor__tags num">
              {f.penalty > 0 && <span className="now-factor__tag">{fmt(s.penalty, { n: f.penalty })}</span>}
              {f.cap !== null && <span className="now-factor__tag now-factor__tag--cap">{fmt(s.capTag, { n: f.cap })}</span>}
            </span>
          </li>
        );
      })}
    </ul>
  );
}
