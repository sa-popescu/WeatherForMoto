import { useMemo } from 'react';
import { fmtTemp } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { RAIN_BAND_COLOR, tierColor } from '../../../lib/scoring';
import { Card, cx, IconButton } from '../../../ui/primitives';
import { timeLabel } from '../logic/formatting';
import { rainConsequence, rainPeakLine, rainPeriod, type RainOutlook } from '../logic/rainOutlook';
import { hourReadout, MEASURABLE_MM, nightRanges, peakRain, type TimelineBar } from '../logic/timeline';
import { S_NOW } from '../strings';
import { useScrub } from '../useScrub';
import { TimelineReadout } from './TimelineReadout';
import '../now-timeline.css';

// The next 24 hours in one card: the picked hour in full, the score bars,
// and the rain of the day under them. It used to be four sections (this
// card, a rain card, three readout tiles and "Mai multe"); the details that
// do not fit here live in the hour's sheet.

// Bar geometry from the design: 14 px + 0.9 px per score point; the rain lane
// is 4 px + 9 px per mm/h, capped at the lane height. Heights are mm/h,
// never probability.
const BAR_BASE_PX = 14;
const BAR_PX_PER_POINT = 0.9;
const LANE_BASE_PX = 4;
const LANE_PX_PER_MM = 9;
const LANE_MAX_PX = 40;
const NIGHT_LABEL_MIN_BARS = 3;

const laneHeight = (mm: number): number => (mm < MEASURABLE_MM ? 0 : Math.max(LANE_BASE_PX, Math.min(LANE_MAX_PX, Math.round(LANE_BASE_PX + mm * LANE_PX_PER_MM))));

interface TimelineProps {
  bars: TimelineBar[];
  outlook: RainOutlook;
  nowIso: string;
  selected: number;
  onSelect: (index: number) => void;
  onOpenHour: (index: number) => void;
  onRainInfo: () => void;
}

export function Timeline({ bars, outlook, nowIso, selected, onSelect, onOpenHour, onRainInfo }: TimelineProps) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const { ref, handlers } = useScrub(bars.length, selected, onSelect, onOpenHour);
  const nights = useMemo(() => nightRanges(bars), [bars]);
  // No measurable rain in the whole day: the lane would be an empty strip.
  const showLane = useMemo(() => peakRain(bars) !== null, [bars]);
  const bar = bars[selected];
  if (!bar) return null;

  const n = bars.length;
  const columns = { gridTemplateColumns: `repeat(${n}, minmax(0, 1fr))` };
  const pct = (i: number): string => `${(i / n) * 100}%`;
  const readout = hourReadout(bar, selected === 0, nowIso, lang);
  const period = rainPeriod(outlook, nowIso, lang);
  const peakLine = rainPeakLine(outlook, nowIso, lang);

  return (
    <Card className="now-tl" aria-labelledby="now-tl-title">
      <div className="now-card-head">
        <h2 id="now-tl-title" className="eyebrow">
          {s.timelineTitle}
        </h2>
        <span className="now-card-head__hint">{s.timelineHint}</span>
      </div>

      <TimelineReadout readout={readout} label={fmt(s.hourDetails, { hour: timeLabel(bar.time, nowIso, lang) })} onOpen={() => onOpenHour(selected)} />

      <div
        ref={ref}
        className="now-tl__plot"
        role="slider"
        tabIndex={0}
        aria-label={s.timelineAria}
        aria-valuemin={0}
        aria-valuemax={n - 1}
        aria-valuenow={selected}
        aria-valuetext={readout.sentence}
        {...handlers}
      >
        {nights.map((r) => (
          <div key={r.start} className="now-tl__night" style={{ left: pct(r.start), width: pct(r.end - r.start) }}>
            {r.end - r.start >= NIGHT_LABEL_MIN_BARS && <span className="now-tl__night-label num">{s.nightLabel}</span>}
          </div>
        ))}
        <div className="now-tl__temps" style={columns} aria-hidden="true">
          {bars.map((b, i) => (
            <span key={b.time} className={cx('now-tl__temp num', i === 0 && 'now-tl__temp--now')}>
              {i === 0 || b.label ? fmtTemp(b.hour.temperature, lang) : ''}
            </span>
          ))}
        </div>
        <div className="now-tl__bars" style={columns}>
          {bars.map((b, i) => (
            <span
              key={b.time}
              className={cx('now-tl__bar', b.night && 'now-tl__bar--night', i === selected && 'now-tl__bar--on')}
              style={{ height: BAR_BASE_PX + (b.score ?? 0) * BAR_PX_PER_POINT, background: tierColor(b.score), animationDelay: `${250 + i * 28}ms` }}
            />
          ))}
        </div>
        <div className="now-tl__cursor" style={{ left: pct(selected + 0.5) }} aria-hidden="true" />
      </div>

      <div className="now-tl__labels" style={columns} aria-hidden="true">
        {bars.map((b, i) => (
          <span key={b.time} className={cx('now-tl__label num', i === 0 && 'now-tl__label--now')}>
            {i === 0 ? s.nowLabel : b.label}
          </span>
        ))}
      </div>

      <div className="now-tl__rain">
        <div className="now-tl__lane-head">
          <h3 className="now-tl__lane-title num">{period ? fmt(s.rainHead, { period }) : s.rainTitle}</h3>
          <IconButton icon="info" label={s.rainInfo} onClick={onRainInfo} aria-haspopup="dialog" className="now-tl__rain-info" />
        </div>
        {showLane && (
          <div className="now-tl__lane" style={columns} aria-hidden="true">
            {bars.map((b, i) => (
              <span key={b.time} className={cx('now-tl__rain-cell', i === selected && 'now-tl__rain-cell--on')}>
                <span className="now-tl__rain-bar" style={{ height: laneHeight(b.mm), background: b.band === 'none' ? 'transparent' : RAIN_BAND_COLOR[b.band] }} />
              </span>
            ))}
          </div>
        )}
        {peakLine && <p className="now-tl__rain-peak">{peakLine}</p>}
        <p className="now-tl__rain-advice">{rainConsequence(outlook, lang)}</p>
      </div>
    </Card>
  );
}
