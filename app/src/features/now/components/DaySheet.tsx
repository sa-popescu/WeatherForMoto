import { dayMonth, dayName, fmtMm, fmtNumber, fmtTemp, hourOf } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { rainBandOf, tierColor } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather } from '../../../lib/types';
import { describeCode } from '../../../lib/weatherCodes';
import { Sheet } from '../../../ui/Sheet';
import { WeatherIcon } from '../../../ui/WeatherIcon';
import { cx } from '../../../ui/primitives';
import { dayRainLine } from '../logic/dayRain';
import { bandWord } from '../logic/formatting';
import { S_SHEETS } from '../sheetStrings';

const MEASURABLE_MM = 0.05;

/** A day's summary and its hourly rows (rain always as % plus mm/h). */
export function DaySheet({ day, hours, onClose }: { day: DailyWeather; hours: HourlyWeather[]; onClose: () => void }) {
  const s = useStrings(S_SHEETS);
  const lang = useLang();
  const rain = dayRainLine(day, lang);
  const peak = day.precipitation_max_mm_h;
  const sun = [day.sunrise && fmt(s.sunrise, { t: hourOf(day.sunrise) }), day.sunset && fmt(s.sunset, { t: hourOf(day.sunset) })].filter(Boolean).join(' · ');

  return (
    <Sheet open onClose={onClose} title={`${dayName(day.date, lang, 'long')} ${dayMonth(day.date, lang)}`}>
      <div className="now-daysum">
        <span className="now-daysum__score num" style={{ color: tierColor(day.moto_score) }}>
          {day.moto_score ?? '–'}
        </span>
        <span className="now-daysum__text">
          <span>
            {describeCode(day.weather_code, lang)} · {fmtTemp(day.temp_max, lang)} / {fmtTemp(day.temp_min, lang)}
          </span>
          <span className="muted">
            {rain.text}
            {peak != null && peak >= MEASURABLE_MM ? ` · ${fmtMm(peak, lang)} mm/h` : ''}
          </span>
          {sun && <span className="muted">{sun}</span>}
        </span>
      </div>
      {hours.length ? <HourTable hours={hours} /> : <p className="muted">{s.noHours}</p>}
    </Sheet>
  );
}

function HourTable({ hours }: { hours: HourlyWeather[] }) {
  const s = useStrings(S_SHEETS);
  const lang = useLang();
  return (
    <div className="now-table-wrap">
      <table className="now-hours">
        <caption className="visually-hidden">{s.hourlyTitle}</caption>
        <thead>
          <tr>
            <th scope="col">{s.colHour}</th>
            <th scope="col">{s.colSky}</th>
            <th scope="col">{s.colTemp}</th>
            <th scope="col">{s.colGust}</th>
            <th scope="col">{s.colRain}</th>
            <th scope="col" className="now-hours__score">
              {s.colScore}
            </th>
          </tr>
        </thead>
        <tbody>
          {hours.map((h) => {
            const mm = Math.max(0, h.precipitation_mm ?? 0);
            const p = h.precipitation_probability ?? 0;
            const dry = p === 0 && mm < MEASURABLE_MM;
            return (
              <tr key={h.time} className={cx(h.is_day === false && 'now-hours__row--night')}>
                <th scope="row" className="num">
                  {hourOf(h.time)}
                </th>
                <td>
                  <WeatherIcon code={h.weather_code} isDay={h.is_day} size={22} title={describeCode(h.weather_code, lang)} />
                </td>
                <td className="num">{fmtTemp(h.temperature, lang)}</td>
                <td className="num">{fmtNumber(h.wind_gusts_kmh, lang)}</td>
                <td>
                  {dry ? (
                    <span className="now-hours__dry">{s.dry}</span>
                  ) : (
                    <span className="now-hours__rain num">
                      {`${Math.round(p)}% · ${fmtMm(mm, lang)} mm/h`}
                      {mm >= MEASURABLE_MM && <span className="now-hours__band">{bandWord(rainBandOf(mm), lang)}</span>}
                    </span>
                  )}
                </td>
                <td className="num now-hours__score" style={{ color: tierColor(h.moto_score) }}>
                  {h.moto_score ?? '–'}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
