import { fmtNumber, fmtTemp } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { TIER_COLOR, type Tier } from '../../../lib/scoring';
import type { CurrentWeather } from '../../../lib/types';
import { airTier, asphalt, gustText, gustTier } from '../logic/readouts';
import { S_NOW } from '../strings';
import '../now-lists.css';

interface Readout {
  key: string;
  label: string;
  value: string;
  unit: string;
  sub: string;
  tier: Tier | null;
}

/** Three instrument readouts: air, gusts, asphalt. */
export function Readouts({ current }: { current: CurrentWeather }) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const road = asphalt(current, lang);
  const whole = (v: number | null): string => fmtNumber(v == null ? null : Math.round(v), lang);
  const items: Readout[] = [
    { key: 'air', label: s.air, value: whole(current.temperature), unit: '°C', sub: fmt(s.feels, { t: fmtTemp(current.feels_like, lang) }), tier: airTier(current.feels_like) },
    { key: 'gust', label: s.gusts, value: whole(current.wind_gusts_kmh), unit: 'km/h', sub: gustText(current.wind_gusts_kmh, current.wind_direction_deg, lang), tier: gustTier(current.wind_gusts_kmh) },
    { key: 'road', label: s.asphalt, value: whole(current.road_surface_temp), unit: '°C', sub: road.word, tier: current.road_surface_temp == null ? null : road.tier },
  ];
  return (
    <div className="now-readouts">
      {items.map((r) => (
        <div key={r.key} className="now-readout">
          <div className="now-readout__head">
            <span className="now-readout__label num">{r.label}</span>
            <span className="now-readout__dot" style={{ background: r.tier ? TIER_COLOR[r.tier] : 'var(--dim)' }} aria-hidden="true" />
          </div>
          <div className="now-readout__value num">
            {r.value}
            <span className="now-readout__unit">{r.unit}</span>
          </div>
          <div className="now-readout__sub">{r.sub}</div>
        </div>
      ))}
    </div>
  );
}
