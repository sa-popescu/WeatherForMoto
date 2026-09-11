import { useState } from 'react';
import { dayName, fmtTemp } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor } from '../../../lib/scoring';
import type { DailyWeather } from '../../../lib/types';
import { cx, SectionLabel } from '../../../ui/primitives';
import { dayRainLine } from '../logic/dayRain';
import { nextDate } from '../logic/daylight';
import { S_NOW } from '../strings';

const SHORT_LIST_DAYS = 7;

interface DayListProps {
  daily: DailyWeather[];
  /** Today's date at the location. */
  today: string;
  onOpen: (date: string) => void;
}

export function DayList({ daily, today, onOpen }: DayListProps) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const [expanded, setExpanded] = useState(false);
  const shown = expanded ? daily : daily.slice(0, SHORT_LIST_DAYS);
  const tomorrow = nextDate(today);
  const label = (date: string): string => (date === today ? s.today : date === tomorrow ? s.tomorrow : dayName(date, lang));

  return (
    <section className="now-section" aria-labelledby="now-days-title">
      <SectionLabel
        id="now-days-title"
        action={
          daily.length > SHORT_LIST_DAYS ? (
            <button type="button" className="now-link" aria-expanded={expanded} onClick={() => setExpanded((e) => !e)}>
              {expanded ? s.fewerDays : s.moreDays}
            </button>
          ) : undefined
        }
      >
        {fmt(s.daysTitle, { n: shown.length })}
      </SectionLabel>
      <ul className={cx('now-days', lang === 'en' && 'now-days--wide')}>
        {shown.map((d) => (
          <DayRow key={d.date} day={d} label={label(d.date)} onOpen={() => onOpen(d.date)} />
        ))}
      </ul>
    </section>
  );
}

function DayRow({ day, label, onOpen }: { day: DailyWeather; label: string; onOpen: () => void }) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const rain = dayRainLine(day, lang);
  const temps = `${fmtTemp(day.temp_max, lang)} / ${fmtTemp(day.temp_min, lang)}`;
  const score = day.moto_score;
  return (
    <li>
      <button type="button" className="now-day" onClick={onOpen} aria-haspopup="dialog" aria-label={fmt(s.dayAria, { day: label, score: score ?? '–', temps, rain: rain.text })}>
        <span className="now-day__name">{label}</span>
        <span className="now-day__mid">
          <span className="now-day__track">
            <span className="now-day__fill" style={{ width: `${score ?? 0}%`, background: tierColor(score) }} />
          </span>
          <span className={cx('now-day__rain', rain.wet && 'now-day__rain--wet')}>{rain.text}</span>
        </span>
        <span className="now-day__temps">{temps}</span>
        <span className="now-day__score num" style={{ color: tierColor(score) }}>
          {score ?? '–'}
        </span>
      </button>
    </li>
  );
}
