import { useLang, useStrings } from '../../lib/i18n';
import { cx } from '../../ui/primitives';
import { frameClock, frameOffsetMinutes, labelledTicks, type RadarFrame } from './radar';
import { MAP_STRINGS } from './strings';
import { radarOffsetText } from './texts';

// Time scrubber: a native range input (arrow keys, screen readers) over a rail
// with one tick per frame. Forecast frames sit in a tinted "prognoză" zone.

const LABEL_STEP = 3;

interface Props {
  frames: readonly RadarFrame[];
  index: number;
  onSelect: (index: number) => void;
}

export function RadarScrubber({ frames, index, onSelect }: Props) {
  const s = useStrings(MAP_STRINGS);
  const lang = useLang();
  const last = frames.length - 1;
  const at = (i: number): string => `${last > 0 ? (i / last) * 100 : 50}%`;
  const firstForecast = frames.findIndex((frame) => frame.nowcast);
  const forecastStart = firstForecast <= 0 || last === 0 ? 0 : ((firstForecast - 0.5) / last) * 100;

  const current = frames[index];
  const relative = radarOffsetText(s, frameOffsetMinutes(current, Date.now() / 1000));
  const valueText = `${frameClock(current.time, lang)}, ${relative}${current.nowcast ? `, ${s.radarForecastWord}` : ''}`;

  return (
    <div className="map-scrub">
      <div className="map-scrub__rail" aria-hidden="true">
        {firstForecast >= 0 && (
          <span className="map-scrub__forecast" style={{ left: `${forecastStart}%` }}>
            <span className="map-scrub__forecast-label">{s.radarForecast}</span>
          </span>
        )}
        {frames.map((frame, i) => (
          <span
            key={frame.time}
            className={cx('map-scrub__tick', frame.nowcast && 'map-scrub__tick--forecast', i === index && 'map-scrub__tick--on')}
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
        aria-label={s.radarScrubber}
        aria-valuetext={valueText}
        onChange={(event) => onSelect(Number(event.target.value))}
      />
      <div className="map-scrub__labels" aria-hidden="true">
        {labelledTicks(frames.length, LABEL_STEP).map((i) => (
          <span key={frames[i].time} className={cx('map-scrub__label', i === index && 'map-scrub__label--on')} style={{ left: at(i) }}>
            {frameClock(frames[i].time, lang)}
          </span>
        ))}
      </div>
    </div>
  );
}
