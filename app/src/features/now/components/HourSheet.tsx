import { useMemo } from 'react';
import { CORE } from '../../../i18n/core';
import { scoreHour } from '../../../lib/directWeather';
import { fmtMm, fmtNumber, fmtTemp } from '../../../lib/format';
import { fmt, pick, useLang, useStrings } from '../../../lib/i18n';
import { rainBandOf, tierColor, tierOf } from '../../../lib/scoring';
import type { HourlyWeather } from '../../../lib/types';
import { describeCode } from '../../../lib/weatherCodes';
import { Sheet } from '../../../ui/Sheet';
import { WeatherIcon } from '../../../ui/WeatherIcon';
import { cx } from '../../../ui/primitives';
import { bandWord, capitalize, fmtVisibility, timeLabel } from '../logic/formatting';
import { gustText, READOUT_TEXTS, uvWord } from '../logic/readouts';
import { S_SHEETS } from '../sheetStrings';
import { S_NOW } from '../strings';
import { FactorList, valuesFromHour } from './FactorList';
import { tierWord } from './tierWord';

interface Stat {
  label: string;
  value: string;
  sub?: string;
  wide?: boolean;
}

/** One hour in detail, opened from the timeline. */
export function HourSheet({ hour, nowIso, onClose }: { hour: HourlyWeather; nowIso: string; onClose: () => void }) {
  const s = useStrings(S_SHEETS);
  const now = useStrings(S_NOW);
  const core = useStrings(CORE);
  const lang = useLang();
  const factors = useMemo(() => scoreHour(hour).factors, [hour]);
  const score = hour.moto_score;
  const color = tierColor(score);
  const mm = Math.max(0, hour.precipitation_mm ?? 0);
  const band = rainBandOf(mm);
  const rain = `${hour.precipitation_probability == null ? '–' : Math.round(hour.precipitation_probability)}% · ${fmtMm(mm, lang)} mm/h`;

  const stats: Stat[] = [
    { label: s.rain, value: rain, sub: band === 'none' ? core.bandNone : bandWord(band, lang), wide: true },
    { label: s.temperature, value: fmtTemp(hour.temperature, lang), sub: fmt(now.feels, { t: fmtTemp(hour.feels_like, lang) }) },
    { label: s.wind, value: `${fmtNumber(hour.wind_gusts_kmh, lang)} km/h`, sub: gustText(hour.wind_gusts_kmh, hour.wind_direction_10m, lang) },
    { label: s.road, value: fmtTemp(hour.road_surface_temp, lang), sub: hour.frost_risk ? pick(READOUT_TEXTS, lang).asphaltFrost : undefined },
    { label: s.visibility, value: hour.visibility == null ? '–' : fmtVisibility(hour.visibility, lang) },
    { label: s.uv, value: fmtNumber(hour.uv_index, lang), sub: hour.uv_index == null ? undefined : uvWord(hour.uv_index, lang) },
    { label: s.humidity, value: hour.relative_humidity == null ? '–' : `${fmtNumber(hour.relative_humidity, lang)}%` },
  ];

  return (
    <Sheet open onClose={onClose} title={capitalize(timeLabel(hour.time, nowIso, lang))}>
      <div className="now-hour__top">
        <span className="now-hour__score num" style={{ color }}>
          {score ?? '–'}
        </span>
        <span className="now-hour__meta">
          <span className="now-hour__tier num" style={{ color }}>
            {tierWord(tierOf(score), core)}
          </span>
          <span className="now-hour__desc">
            <WeatherIcon code={hour.weather_code} isDay={hour.is_day} size={24} />
            {describeCode(hour.weather_code, lang)}
          </span>
        </span>
      </div>
      <dl className="now-stats">
        {stats.map((st) => (
          <div key={st.label} className={cx('now-stat', st.wide && 'now-stat--wide')}>
            <dt>{st.label}</dt>
            <dd className="num">{st.value}</dd>
            {st.sub && <dd className="now-stat__sub">{st.sub}</dd>}
          </div>
        ))}
      </dl>
      <section className="now-sheet__block">
        <h3 className="eyebrow">{fmt(s.whyScore, { score: score ?? '–' })}</h3>
        <FactorList factors={factors} values={valuesFromHour(hour)} />
      </section>
    </Sheet>
  );
}
