import { useId } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Card, Chip, IconButton, Segmented } from '../../ui/primitives';
import { dayChipLabel } from './routeFormat';
import { RS } from './strings';
import { minutesOf, SPEEDS, STEP_MIN, timeOf, timesBetween, type Speed } from './timing';
import type { Departure } from './types';

// Day (today .. +6), time in 30-minute steps (origin local time) and the
// average motorcycle pace used for every ETA.

const ALL_TIMES = timesBetween('00:00', '23:30');

interface DepartureControlsProps {
  dates: readonly string[];
  departure: Departure;
  onDeparture: (next: Departure) => void;
  speed: Speed;
  onSpeed: (next: Speed) => void;
}

export function DepartureControls({ dates, departure, onDeparture, speed, onSpeed }: DepartureControlsProps) {
  const s = useStrings(RS);
  const lang = useLang();
  const timeId = useId();
  const minutes = minutesOf(departure.time);

  const shift = (delta: number): void => {
    const next = Math.min(minutesOf('23:30'), Math.max(0, minutes + delta));
    onDeparture({ ...departure, time: timeOf(next) });
  };

  return (
    <Card className="route-dep" aria-labelledby="route-dep-title">
      <h2 className="eyebrow" id="route-dep-title">
        {s.departureTitle}
      </h2>
      <div className="route-dep__group">
        <span className="route-dep__label num" id={`${timeId}-day`}>
          {s.dayLabel}
        </span>
        <div className="route-days" role="group" aria-labelledby={`${timeId}-day`}>
          {dates.map((date, i) => (
            <Chip key={date} selected={date === departure.date} onClick={() => onDeparture({ ...departure, date })}>
              {dayChipLabel(date, i, lang, s)}
            </Chip>
          ))}
        </div>
      </div>
      <div className="route-dep__group">
        <label className="route-dep__label num" htmlFor={timeId}>
          {s.timeLabel} <span className="route-dep__hint">· {s.timeHint}</span>
        </label>
        <div className="route-time">
          <IconButton icon="minus" label={s.earlier} disabled={minutes <= 0} onClick={() => shift(-STEP_MIN)} />
          <select id={timeId} className="route-time__select num" value={departure.time} onChange={(e) => onDeparture({ ...departure, time: e.target.value })}>
            {ALL_TIMES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
          <IconButton icon="plus" label={s.later} disabled={minutes >= minutesOf('23:30')} onClick={() => shift(STEP_MIN)} />
        </div>
      </div>
      <div className="route-dep__group route-speed">
        <span className="route-dep__label num">
          {s.speedLabel} <span className="route-dep__hint">· km/h</span>
        </span>
        <Segmented
          label={s.speedAria}
          options={SPEEDS.map((v) => ({ value: String(v), label: String(v) }))}
          value={String(speed)}
          onChange={(v) => onSpeed(Number(v) as Speed)}
        />
        <span className="visually-hidden">{fmt(s.speedUnit, { v: speed })}</span>
      </div>
    </Card>
  );
}
