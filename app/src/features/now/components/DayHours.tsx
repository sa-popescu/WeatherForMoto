import type { ReactNode } from 'react';
import { describeHour } from '../../../lib/directWeather';
import { dayMonth, dayName, fmtNumber, fmtTemp, hourOf } from '../../../lib/format';
import { fmt, pick, useLang, useStrings } from '../../../lib/i18n';
import { tierColor } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather } from '../../../lib/types';
import { WeatherIcon } from '../../../ui/WeatherIcon';
import { cx, SectionLabel } from '../../../ui/primitives';
import { nextDate } from '../logic/daylight';
import { gustText, READOUT_TEXTS } from '../logic/readouts';
import { S_SHEETS } from '../sheetStrings';
import { S_NOW } from '../strings';
import { RainCell } from './DaySheet';

interface DayHoursProps {
  day: DailyWeather;
  /** That day's hours still ahead (hoursOfDay in logic/dayHours.ts). */
  hours: HourlyWeather[];
  /** Today's date at the location. */
  today: string;
  /** The current hour slot, marked in today's table. */
  nowIso: string;
  onOpenHour: (time: string) => void;
}

/**
 * Desktop only: the day picked on the cards above, hour by hour, with more
 * columns than the phone's day sheet has room for. A row opens that hour.
 */
export function DayHours({ day, hours, today, nowIso, onOpenHour }: DayHoursProps) {
  const s = useStrings(S_SHEETS);
  const now = useStrings(S_NOW);
  const lang = useLang();
  const name = day.date === today ? now.today : day.date === nextDate(today) ? now.tomorrow : dayName(day.date, lang, 'long');
  const sun = [day.sunrise && fmt(s.sunrise, { t: hourOf(day.sunrise) }), day.sunset && fmt(s.sunset, { t: hourOf(day.sunset) })].filter(Boolean).join(' · ');

  return (
    <section className="now-section now-dayhours" aria-labelledby="now-dayhours-title">
      <SectionLabel id="now-dayhours-title" action={sun ? <span className="now-section__meta">{sun}</span> : undefined}>
        {fmt(now.hoursTitle, { day: `${name} ${dayMonth(day.date, lang)}` })}
      </SectionLabel>
      {hours.length ? (
        <div className="now-dayhours__wrap">
          <table className="now-hours now-hours--wide">
            <thead>
              <tr>
                <th scope="col">{s.colHour}</th>
                <th scope="col">{s.colSky}</th>
                <th scope="col">{s.colTemp}</th>
                <th scope="col">{s.colRain}</th>
                <th scope="col">{s.colWind}</th>
                <th scope="col">{s.colRoad}</th>
                <th scope="col" className="now-hours__score">
                  {s.colScore}
                </th>
              </tr>
            </thead>
            <tbody>
              {hours.map((h) => (
                <HourRow key={h.time} hour={h} isNow={h.time === nowIso} onOpen={() => onOpenHour(h.time)} />
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">{s.noHours}</p>
      )}
    </section>
  );
}

function HourRow({ hour, isNow, onOpen }: { hour: HourlyWeather; isNow: boolean; onOpen: () => void }) {
  const now = useStrings(S_NOW);
  const lang = useLang();
  const time = hourOf(hour.time);
  return (
    // The whole row answers a mouse click. Keyboard users get the button in
    // the first cell; its click bubbles up to this same handler.
    <tr className={cx('now-dayhours__row', hour.is_day === false && 'now-hours__row--night', isNow && 'now-dayhours__row--now')} onClick={onOpen}>
      <th scope="row">
        <button type="button" className="now-dayhours__hour num" aria-haspopup="dialog" aria-label={fmt(now.hourDetails, { hour: time })}>
          {time}
          {isNow && <span className="now-dayhours__now">{now.nowLabel}</span>}
        </button>
      </th>
      <td>
        <span className="now-dayhours__sky">
          <WeatherIcon code={hour.weather_code} isDay={hour.is_day} size={24} />
          {describeHour(hour, lang)}
        </span>
      </td>
      <td>
        <Pair main={fmtTemp(hour.temperature, lang)} sub={fmt(now.feels, { t: fmtTemp(hour.feels_like, lang) })} />
      </td>
      <td>
        <RainCell hour={hour} />
      </td>
      <td>
        <Pair
          main={`${fmtNumber(hour.wind_speed_kmh, lang)} / ${fmtNumber(hour.wind_gusts_kmh, lang)} km/h`}
          sub={gustText(hour.wind_gusts_kmh, hour.wind_direction_10m, lang)}
        />
      </td>
      <td>
        <Pair main={fmtTemp(hour.road_surface_temp, lang)} sub={hour.frost_risk ? pick(READOUT_TEXTS, lang).asphaltFrost : null} />
      </td>
      <td className="num now-hours__score" style={{ color: tierColor(hour.moto_score) }}>
        {hour.moto_score ?? '–'}
      </td>
    </tr>
  );
}

/** A value with a quieter line under it. */
function Pair({ main, sub }: { main: string; sub: ReactNode }) {
  return (
    <span className="now-dayhours__pair">
      <span className="num">{main}</span>
      {sub && <span className="now-dayhours__sub">{sub}</span>}
    </span>
  );
}
