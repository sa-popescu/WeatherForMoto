import { CORE } from '../../i18n/core';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button, cx, Spinner } from '../../ui/primitives';
import { ForecastLegend } from './ForecastLegend';
import { frameClock } from './radar';
import { RadarLegend } from './RadarLegend';
import { MAP_STRINGS } from './strings';
import { radarOffsetText } from './texts';
import type { MapTimeline } from './useMapTimeline';

// One panel for the whole band of time: play/pause, the moment on screen, the
// scrubber from the oldest radar frame to the last forecast hour, and the
// legend of whichever layer that moment belongs to.

/** Label every sixth entry, so the row stays readable on a phone. */
const LABEL_STEP = 6;

/** Minutes, up to an hour and a half out; hours beyond that. */
function relativeText(s: (typeof MAP_STRINGS)['ro'], timeSec: number, nowSec: number): string {
  const minutes = Math.round((timeSec - nowSec) / 60);
  if (Math.abs(minutes) <= 90) return radarOffsetText(s, minutes);
  return fmt(s.forecastAhead, { h: Math.round(minutes / 60) });
}

export function TimelinePanel({ timeline }: { timeline: MapTimeline }) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const lang = useLang();
  const { entries, index, current } = timeline;
  const bothFailed = timeline.radarStatus === 'error' && timeline.forecast.status === 'error';

  if (bothFailed || (entries.length === 0 && timeline.forecast.status === 'error')) {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Icon name="radar" size={22} />
        <span className="map-radar-msg__text">
          {s.timelineUnavailable}
          {timeline.forecast.error && <span className="map-radar-msg__why"> {timeline.forecast.error}</span>}
        </span>
        <Button onClick={timeline.retry}>{core.retry}</Button>
      </div>
    );
  }

  if (!current) {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Spinner size={20} />
        <span className="map-radar-msg__text">{s.timelineLoading}</span>
      </div>
    );
  }

  const nowSec = Date.now() / 1000;
  const last = entries.length - 1;
  const at = (i: number): string => `${last > 0 ? (i / last) * 100 : 50}%`;
  const firstForecast = entries.findIndex((entry) => entry.forecast);
  const forecastStart = firstForecast <= 0 || last === 0 ? 0 : ((firstForecast - 0.5) / last) * 100;
  const relative = relativeText(s, current.timeSec, nowSec);
  const busy = timeline.tilesLoading || timeline.forecast.status === 'loading';

  return (
    <section className="map-panel map-radar" aria-label={s.timelineLabel}>
      <div className="map-radar__head">
        <button
          type="button"
          className="map-radar__play"
          aria-label={timeline.playing ? s.radarPause : s.radarPlay}
          title={timeline.playing ? s.radarPause : s.radarPlay}
          onClick={timeline.togglePlay}
        >
          <Icon name={timeline.playing ? 'pause' : 'play'} size={22} strokeWidth={2.4} />
        </button>
        <div className="map-radar__readout">
          <span className="map-radar__time num">{frameClock(current.timeSec, lang)}</span>
          <span className="map-radar__rel">{relative}</span>
        </div>
        {busy && <Spinner size={18} />}
        <span className={cx('map-radar__tag', current.forecast && 'map-radar__tag--forecast')}>
          {current.forecast ? s.radarForecast : s.radarObserved}
        </span>
      </div>

      <div className="map-scrub">
        <div className="map-scrub__rail" aria-hidden="true">
          {firstForecast >= 0 && (
            <span className="map-scrub__forecast" style={{ left: `${forecastStart}%` }}>
              <span className="map-scrub__forecast-label">{s.radarForecast}</span>
            </span>
          )}
          {entries.map((entry, i) => (
            <span
              key={`${entry.source}-${entry.timeSec}`}
              className={cx('map-scrub__tick', entry.forecast && 'map-scrub__tick--forecast', i === index && 'map-scrub__tick--on')}
              style={{ left: at(i) }}
            />
          ))}
        </div>
        <input
          type="range"
          className="map-range"
          min={0}
          max={Math.max(0, last)}
          step={1}
          value={index}
          aria-label={s.timelineScrubber}
          aria-valuetext={`${frameClock(current.timeSec, lang)}, ${relative}${current.forecast ? `, ${s.radarForecastWord}` : ''}`}
          onChange={(event) => timeline.select(Number(event.target.value))}
        />
        <div className="map-scrub__labels" aria-hidden="true">
          {entries
            .map((_, i) => i)
            .filter((i) => i % LABEL_STEP === 0 || i === last)
            .map((i) => (
              <span
                key={entries[i].timeSec}
                className={cx('map-scrub__label', i === index && 'map-scrub__label--on')}
                style={{ left: at(i) }}
              >
                {frameClock(entries[i].timeSec, lang)}
              </span>
            ))}
        </div>
      </div>

      {/* Rain reads on the radar's scale in both halves of the band, so the key
          under the scrubber stays put; only the cloud layer needs its own. */}
      {current.source === 'forecast' && timeline.kind === 'cloud' ? <ForecastLegend kind="cloud" /> : <RadarLegend />}
      {(current.source === 'radar' || timeline.forecast.cellKm !== null) && (
        <p className="map-radar__note">
          {current.source === 'radar' ? (
            <>
              {s.radarNote} <a href="#/acum">{s.radarNoteLink}</a>.
            </>
          ) : (
            fmt(s.forecastNote, { km: timeline.forecast.cellKm ?? 0 })
          )}
        </p>
      )}
    </section>
  );
}
