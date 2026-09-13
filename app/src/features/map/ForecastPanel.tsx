import { CORE } from '../../i18n/core';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button, cx, Spinner } from '../../ui/primitives';
import { colorFor, frameOffsetHours, type ForecastKind } from './forecast';
import { frameClock } from './radar';
import { MAP_STRINGS } from './strings';
import type { ForecastGrid } from './useForecastData';
import type { ForecastPlayback } from './useForecastPlayback';

// Bottom panel for the forecast layer: play/pause, the hour on screen, a
// scrubber over the next 24 hours, the legend and what the grid can resolve.

const LABEL_STEP = 6;

interface Props {
  kind: ForecastKind;
  grid: ForecastGrid;
  playback: ForecastPlayback;
}

export function ForecastPanel({ kind, grid, playback }: Props) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const lang = useLang();
  const times = grid.data?.times ?? [];
  const time = times[playback.index] ?? null;

  if (grid.status === 'error') {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Icon name="radar" size={22} />
        <span className="map-radar-msg__text">{s.forecastUnavailable}</span>
        <Button onClick={grid.reload}>{core.retry}</Button>
      </div>
    );
  }

  if (!time) {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Spinner size={20} />
        <span className="map-radar-msg__text">{s.forecastLoading}</span>
      </div>
    );
  }

  const ahead = frameOffsetHours(time, Date.now());
  const relative = ahead <= 0 ? s.forecastNow : fmt(s.forecastAhead, { h: ahead });
  const last = times.length - 1;
  const at = (i: number): string => `${last > 0 ? (i / last) * 100 : 50}%`;
  const labels = times.map((_, i) => i).filter((i) => i % LABEL_STEP === 0);

  return (
    <section className="map-panel map-radar" aria-label={s.layerForecast}>
      <div className="map-radar__head">
        <button
          type="button"
          className="map-radar__play"
          aria-label={playback.playing ? s.forecastPause : s.forecastPlay}
          title={playback.playing ? s.forecastPause : s.forecastPlay}
          onClick={playback.toggle}
        >
          <Icon name={playback.playing ? 'pause' : 'play'} size={22} strokeWidth={2.4} />
        </button>
        <div className="map-radar__readout">
          <span className="map-radar__time num">{frameClock(localSeconds(time), lang)}</span>
          <span className="map-radar__rel">{relative}</span>
        </div>
        {grid.status === 'loading' && <Spinner size={18} />}
        <span className="map-radar__tag map-radar__tag--forecast">{s.forecastTag}</span>
      </div>

      <div className="map-scrub">
        <div className="map-scrub__rail" aria-hidden="true">
          {times.map((frameTime, i) => (
            <span
              key={frameTime}
              className={cx('map-scrub__tick', 'map-scrub__tick--forecast', i === playback.index && 'map-scrub__tick--on')}
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
          value={playback.index}
          aria-label={s.forecastScrubber}
          aria-valuetext={`${frameClock(localSeconds(time), lang)}, ${relative}`}
          onChange={(event) => playback.select(Number(event.target.value))}
        />
        <div className="map-scrub__labels" aria-hidden="true">
          {labels.map((i) => (
            <span
              key={times[i]}
              className={cx('map-scrub__label', i === playback.index && 'map-scrub__label--on')}
              style={{ left: at(i) }}
            >
              {frameClock(localSeconds(times[i]), lang)}
            </span>
          ))}
        </div>
      </div>

      <ForecastLegend kind={kind} />
      <p className="map-radar__note">{s.forecastNote}</p>
    </section>
  );
}

/** Frame times are UTC; the clock on the panel is the reader's own. */
function localSeconds(time: string): number {
  return Date.parse(`${time}Z`) / 1000;
}

const CLOUD_STEPS = [20, 45, 70, 95];
const RAIN_STEPS = [0.2, 1, 4, 10];

function ForecastLegend({ kind }: { kind: ForecastKind }) {
  const s = useStrings(MAP_STRINGS);
  const steps = kind === 'cloud' ? CLOUD_STEPS : RAIN_STEPS;
  return (
    <div className="map-forecast-legend" aria-hidden="true">
      <span className="map-forecast-legend__scale">
        {steps.map((value) => {
          const [r, g, b, a] = colorFor(kind, value);
          return (
            <span
              key={value}
              className="map-forecast-legend__swatch"
              style={{ background: `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(2)})` }}
            />
          );
        })}
      </span>
      <span className="map-forecast-legend__text">{kind === 'cloud' ? s.forecastLegendCloud : s.forecastLegendRain}</span>
    </div>
  );
}
