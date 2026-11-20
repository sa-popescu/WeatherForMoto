import { useEffect, useMemo, useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import { Icon } from '../../ui/icons';
import { cx } from '../../ui/primitives';
import { dayChipLabel } from './routeFormat';
import { RS } from './strings';
import { bestDay, dayBests, type DayBest, type SweepPointInput } from './sweep';
import type { Departure } from './types';

// "Compară zilele", under the departure chart: the best departure of every
// planner day and the worst score met along the route at that time. Closed
// by default; opening it asks for the week of forecast when only three days
// are loaded, since the other days cannot be compared without it.

interface DayCompareProps {
  dates: readonly string[];
  departure: Departure;
  speed: number;
  points: readonly SweepPointInput[];
  originOffsetSec: number;
  /** The weather is still loading, so a day without data may still get it. */
  loading: boolean;
  /** Called when the comparison opens; loads the whole week if it is not there yet. */
  onNeedWeek: () => void;
  onPick: (departure: Departure) => void;
}

export function DayCompare({ dates, departure, speed, points, originOffsetSec, loading, onNeedWeek, onPick }: DayCompareProps) {
  const s = useStrings(RS);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (open) onNeedWeek();
  }, [open, onNeedWeek]);

  const days = useMemo(
    () => (open ? dayBests({ points, dates, speedKmh: speed, originOffsetSec, nowMs: Date.now(), chosenTime: departure.time }) : []),
    [open, points, dates, speed, originOffsetSec, departure.time],
  );
  const top = bestDay(days);

  return (
    <div className="route-compare">
      <button type="button" className="route-compare__toggle" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
        {open ? s.compareClose : s.compareOpen}
        <Icon name="chevronDown" size={16} className={cx('route-compare__chevron', open && 'route-compare__chevron--open')} />
      </button>
      {open && (
        <div className="route-compare__days" role="group" aria-label={s.compareOpen}>
          {days.map((day, i) => (
            <DayButton
              key={day.date}
              day={day}
              index={i}
              chosen={day.date === departure.date}
              top={top?.date === day.date}
              loading={loading}
              onPick={onPick}
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface DayButtonProps {
  day: DayBest;
  index: number;
  chosen: boolean;
  top: boolean;
  loading: boolean;
  onPick: (departure: Departure) => void;
}

function DayButton({ day, index, chosen, top, loading, onPick }: DayButtonProps) {
  const s = useStrings(RS);
  const lang = useLang();
  const label = dayChipLabel(day.date, index, lang, s);
  const best = day.best;
  let time = '–';
  let aria = fmt(s.compareNone, { day: label });
  if (best) {
    time = best.time;
    aria = fmt(s.compareDay, { day: label, time: best.time, score: best.worst ?? '–' });
  } else if (day.state === 'missing') {
    time = loading ? '…' : '–';
    aria = fmt(loading ? s.compareLoading : s.compareMissing, { day: label });
  }
  return (
    <button
      type="button"
      className={cx('route-compare__day', chosen && 'route-compare__day--chosen', top && 'route-compare__day--top')}
      aria-pressed={chosen}
      aria-label={top ? `${aria} ${s.compareTop}` : aria}
      disabled={!best}
      onClick={() => best && onPick({ date: day.date, time: best.time })}
    >
      <span className="route-compare__name">{label}</span>
      <span className="route-compare__time num">{time}</span>
      <span className="route-compare__score num" style={{ color: tierColor(best?.worst ?? null) }}>
        {best?.worst ?? ''}
      </span>
    </button>
  );
}
