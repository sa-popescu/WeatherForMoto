import { fmt, useLang, useStrings } from '../../lib/i18n';
import { noteUse } from '../../lib/usage';
import { Icon } from '../../ui/icons';
import { cx } from '../../ui/primitives';
import { fmtClockTime, fmtTimeLeft } from '../ride/logic/rideFormat';
import type { DepartureOption } from './logic/departures';
import type { RouteOutlook } from './logic/driveWeather';
import { DRIVE_S } from './strings';

// "Plecarea perfectă" under the routes: now and five later departure times,
// each with its riding time in the traffic of that hour and the weather met
// on the way, the recommended one marked and said in one line. A tap on a
// time shows the routes for that departure.

type DriveText = { [K in keyof (typeof DRIVE_S)['ro']]: string };

/** One word for the chip: "uscat", "ploaie", "polei"… */
function shortWeather(outlook: RouteOutlook | null, s: DriveText): string {
  const w = outlook?.warning;
  if (!outlook?.settled) return '…';
  if (!w) return outlook.worstScore === null ? '–' : s.wxDry;
  if (w.kind === 'rain') return s.wxRain;
  if (w.kind === 'frost') return s.wxFrost;
  if (w.kind === 'gust') return s.wxGust;
  return s.wxBad;
}

/** A few words for the recommendation: "uscat tot drumul", "ploaie pe drum, pe întuneric". */
function longWeather(outlook: RouteOutlook, s: DriveText): string {
  const kind = outlook.warning?.kind;
  const what = !kind ? s.allDry : kind === 'rain' ? s.onWayRain : kind === 'frost' ? s.onWayFrost : kind === 'gust' ? s.onWayGust : s.onWayBad;
  return outlook.dark ? fmt(s.andDark, { what }) : what;
}

interface DepartureStripProps {
  options: readonly DepartureOption[];
  best: number | null;
  /** The departure on screen (null for now). */
  selected: number | null;
  noTraffic: boolean;
  onPick: (departMs: number | null) => void;
}

export function DepartureStrip({ options, best, selected, noTraffic, onPick }: DepartureStripProps) {
  const s = useStrings(DRIVE_S);
  const lang = useLang();
  if (options.length === 0) return null;
  const label = (o: DepartureOption): string => (o.departMs === null ? s.departNow : fmtClockTime(o.departMs, lang));
  const settled = options.every((o) => o.status !== 'loading' && (o.status === 'error' || o.outlook?.settled));
  const pick = best !== null ? options[best] : null;

  let line: string;
  if (pick?.outlook && pick.durationS !== null) {
    const parts = { what: longWeather(pick.outlook, s), time: fmtTimeLeft(pick.durationS * 1000) };
    line = pick.departMs === null ? fmt(s.perfectNow, parts) : fmt(s.perfectLater, { ...parts, at: label(pick) });
  } else {
    line = settled ? s.perfectFailed : s.perfectComparing;
  }

  return (
    <section className="drive-depart" aria-labelledby="drive-depart-title">
      <h3 id="drive-depart-title" className="drive-depart__title eyebrow">
        {s.perfectTitle}
      </h3>
      <p className="drive-depart__line" role="status">
        {line}
        {noTraffic && <span className="drive-depart__note"> {s.perfectNoTraffic}</span>}
      </p>
      <div className="drive-depart__chips" role="group" aria-label={s.perfectLabel}>
        {options.map((o, i) => {
          const time = o.durationS === null ? '–' : fmtTimeLeft(o.durationS * 1000);
          const what = o.status === 'loading' ? '…' : shortWeather(o.outlook, s);
          const warn = Boolean(o.outlook?.warning);
          return (
            <button
              key={o.departMs ?? 'now'}
              type="button"
              className={cx('drive-depart__chip', o.departMs === selected && 'drive-depart__chip--on', i === best && 'drive-depart__chip--best')}
              aria-pressed={o.departMs === selected}
              aria-label={fmt(s.departAria, { at: label(o), time, what }) + (i === best ? ` · ${s.best}` : '')}
              disabled={o.status === 'error'}
              onClick={() => {
                if (o.departMs !== null) noteUse('drive:departure');
                onPick(o.departMs);
              }}
            >
              <span className="drive-depart__at num">
                {label(o)}
                {i === best && <Icon name="star" size={12} />}
              </span>
              <span className="drive-depart__time num">{time}</span>
              <span className={cx('drive-depart__sky', warn && 'drive-depart__sky--warn')}>
                {what}
                {o.outlook?.dark && <Icon name="moon" size={12} />}
              </span>
            </button>
          );
        })}
      </div>
    </section>
  );
}
