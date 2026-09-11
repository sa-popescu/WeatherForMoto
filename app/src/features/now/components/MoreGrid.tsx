import { useId, useState } from 'react';
import { fmtNumber, fmtTemp } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import type { CurrentWeather } from '../../../lib/types';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import { fmtVisibility } from '../logic/formatting';
import { aqiWord, pollenWord, uvWord } from '../logic/readouts';
import { S_NOW } from '../strings';

interface Extra {
  label: string;
  value: string;
  sub: string | null;
}

/** "Mai multe": the secondary readings, collapsed by default. */
export function MoreGrid({ current }: { current: CurrentWeather }) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const [open, setOpen] = useState(false);
  const gridId = useId();
  const n = (v: number | null, decimals = 0): string => fmtNumber(v, lang, decimals);

  const items: Extra[] = [
    { label: s.humidity, value: current.humidity == null ? '–' : `${n(current.humidity)}%`, sub: null },
    { label: s.pressure, value: current.pressure_hpa == null ? '–' : `${n(current.pressure_hpa)} hPa`, sub: null },
    { label: s.visibility, value: current.visibility_km == null ? '–' : fmtVisibility(current.visibility_km * 1000, lang), sub: null },
    { label: s.uv, value: n(current.uv_index), sub: current.uv_index == null ? null : uvWord(current.uv_index, lang) },
    { label: s.aqi, value: n(current.eu_aqi), sub: current.eu_aqi == null ? null : aqiWord(current.eu_aqi, lang) },
    { label: s.pollen, value: n(current.pollen_index), sub: current.pollen_index == null ? null : pollenWord(current.pollen_index, lang) },
    { label: s.dewPoint, value: fmtTemp(current.dew_point, lang), sub: null },
    { label: s.windAvg, value: current.wind_speed_kmh == null ? '–' : `${n(current.wind_speed_kmh)} km/h`, sub: current.beaufort == null ? null : fmt(s.beaufort, { b: current.beaufort }) },
  ];

  return (
    <div className="now-more">
      <button type="button" className="now-more__toggle" aria-expanded={open} aria-controls={gridId} onClick={() => setOpen((o) => !o)}>
        <span>{open ? s.less : s.more}</span>
        <Icon name="chevronDown" size={20} className={cx('now-more__chev', open && 'now-more__chev--open')} />
      </button>
      <dl id={gridId} className="now-more__grid" hidden={!open}>
        {items.map((it) => (
          <div key={it.label} className="now-more__item">
            <dt className="now-more__label">{it.label}</dt>
            <dd className="now-more__value num">{it.value}</dd>
            {it.sub && <dd className="now-more__sub">{it.sub}</dd>}
          </div>
        ))}
      </dl>
    </div>
  );
}
