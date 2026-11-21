import { useState } from 'react';
import { dayMonth, dayName, fmtTemp } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor } from '../../../lib/scoring';
import type { DailyWeather } from '../../../lib/types';
import { cx, SectionLabel } from '../../../ui/primitives';
import { dayRainLine } from '../logic/dayRain';
import { nextDate } from '../logic/daylight';
import { S_NOW } from '../strings';
import '../now-lists.css';

const SHORT_LIST_DAYS = 7;

interface DayListProps {
  daily: DailyWeather[];
  /** Today's date at the location. */
  today: string;
  /**
   * Desktop only: the day shown in the hour table below. Its card is marked,
   * and a card then picks its day instead of opening a sheet.
   */
  picked?: string;
  onOpen: (date: string) => void;
}

export function DayList({ daily, today, picked, onOpen }: DayListProps) {
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
          <DayRow key={d.date} day={d} label={label(d.date)} picked={picked === undefined ? undefined : d.date === picked} onOpen={() => onOpen(d.date)} />
        ))}
      </ul>
    </section>
  );
}

interface DayRowProps {
  day: DailyWeather;
  label: string;
  /** Undefined on a phone, where the row opens the day's sheet. */
  picked: boolean | undefined;
  onOpen: () => void;
}

function DayRow({ day, label, picked, onOpen }: DayRowProps) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const rain = dayRainLine(day, lang);
  const temps = `${fmtTemp(day.temp_max, lang)} / ${fmtTemp(day.temp_min, lang)}`;
  const date = dayMonth(day.date, lang);
  const score = day.moto_score;
  return (
    <li>
      <button
        type="button"
        className={cx('now-day', picked && 'now-day--picked')}
        onClick={onOpen}
        aria-haspopup={picked === undefined ? 'dialog' : undefined}
        aria-pressed={picked}
        aria-label={fmt(s.dayAria, { day: `${label} ${date}`, score: score ?? '–', temps, rain: rain.text })}
      >
        {/* The date sits under the day name, level with the rain line. */}
        <span className="now-day__name">
          {label}
          <span className="now-day__date">{date}</span>
        </span>
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
