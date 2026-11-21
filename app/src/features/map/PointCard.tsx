import { CORE } from '../../i18n/core';
import { fmtNumber, fmtTemp, hourOf } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import { Button, IconButton, Spinner } from '../../ui/primitives';
import { timeLabel } from '../now/logic/formatting';
import { inViewerClock, summarizePoint } from './pointWeather';
import { MAP_STRINGS } from './strings';
import type { PointWeather } from './usePointWeather';

// The card in the desktop map panel for the point picked on the map or found
// by the search: score, air and gusts at the moment on the band of time, the
// coming rain in a sentence, and a way to make it the app's place.

interface PointCardProps {
  weather: PointWeather;
  /** The moment on the band (UTC seconds); the card shows the hour holding it. */
  momentSec: number;
  onOpen: (name: string) => void;
  onClose: () => void;
}

export function PointCard({ weather, momentSec, onOpen, onClose }: PointCardProps) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const lang = useLang();
  const name = weather.name ?? s.pointUnnamed;
  // On the viewer's clock, like the band of time the moment comes from.
  const summary = weather.data ? summarizePoint(inViewerClock(weather.data), momentSec, lang) : null;
  const when = summary
    ? summary.isNow
      ? s.pointNow
      : summary.hourIso.slice(0, 10) === summary.nowIso.slice(0, 10)
        ? fmt(s.pointAt, { time: hourOf(summary.hourIso) })
        : timeLabel(summary.hourIso, summary.nowIso, lang)
    : null;

  return (
    <section className="map-point" aria-labelledby="map-point-title" aria-busy={weather.status === 'loading'}>
      <div className="map-point__head">
        <h2 id="map-point-title" className="map-point__name">
          {name}
        </h2>
        {when && <span className="map-point__when">{when}</span>}
        <IconButton icon="close" label={s.pointClose} className="map-point__close" onClick={onClose} />
      </div>

      {weather.status === 'loading' && (
        <p className="map-point__note" role="status">
          <Spinner size={18} /> {s.pointLoading}
        </p>
      )}

      {weather.status === 'error' && (
        <div className="map-point__note" role="alert">
          <span>{s.pointFailed}</span>
          <Button onClick={weather.retry}>{core.retry}</Button>
        </div>
      )}

      {weather.status === 'ready' && !summary && <p className="map-point__note">{s.pointBeyond}</p>}

      {summary && (
        <>
          <dl className="map-point__facts">
            <div>
              <dt>{s.pointScore}</dt>
              <dd style={{ color: tierColor(summary.score) }}>{summary.score == null ? '–' : Math.round(summary.score)}</dd>
            </div>
            <div>
              <dt>{s.pointAir}</dt>
              <dd>{fmtTemp(summary.temperature, lang)}</dd>
            </div>
            <div>
              <dt>{s.pointGusts}</dt>
              <dd>
                {fmtNumber(summary.gusts, lang)}
                <span className="map-point__unit"> km/h</span>
              </dd>
            </div>
          </dl>
          <p className="map-point__rain">{summary.rain}</p>
        </>
      )}

      {weather.status !== 'error' && (
        <Button icon="pin" className="map-point__open" disabled={!weather.data} onClick={() => onOpen(name)}>
          {s.pointOpen}
        </Button>
      )}
    </section>
  );
}
