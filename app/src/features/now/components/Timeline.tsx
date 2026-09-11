import { useMemo } from 'react';
import { CORE } from '../../../i18n/core';
import { fmtMm } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { RAIN_BAND_COLOR, tierColor } from '../../../lib/scoring';
import { Icon } from '../../../ui/icons';
import { Card, cx } from '../../../ui/primitives';
import { timeLabel } from '../logic/formatting';
import { calloutText, maxRain, nightRanges, type TimelineBar } from '../logic/timeline';
import { S_NOW } from '../strings';
import { useScrub } from '../useScrub';
import '../now-timeline.css';

// Bar geometry from the design: 14 px + 0.9 px per score point; the rain lane
// is 2 px + 6.5 px per mm/h, capped at the lane height. Heights are mm/h,
// never probability.
const BAR_BASE_PX = 14;
const BAR_PX_PER_POINT = 0.9;
const LANE_BASE_PX = 2;
const LANE_PX_PER_MM = 6.5;
const LANE_MAX_PX = 28;
const MEASURABLE_MM = 0.05;
const NIGHT_LABEL_MIN_BARS = 3;

const laneHeight = (mm: number): number => (mm < MEASURABLE_MM ? 0 : Math.max(LANE_BASE_PX, Math.min(LANE_MAX_PX, Math.round(LANE_BASE_PX + mm * LANE_PX_PER_MM))));

interface TimelineProps {
  bars: TimelineBar[];
  nowIso: string;
  selected: number;
  onSelect: (index: number) => void;
  onOpenHour: (index: number) => void;
}

export function Timeline({ bars, nowIso, selected, onSelect, onOpenHour }: TimelineProps) {
  const s = useStrings(S_NOW);
  const core = useStrings(CORE);
  const lang = useLang();
  const { ref, handlers } = useScrub(bars.length, selected, onSelect, onOpenHour);
  const nights = useMemo(() => nightRanges(bars), [bars]);
  const peak = useMemo(() => maxRain(bars), [bars]);
  const bar = bars[selected];
  if (!bar) return null;

  const n = bars.length;
  const columns = { gridTemplateColumns: `repeat(${n}, minmax(0, 1fr))` };
  const pct = (i: number): string => `${(i / n) * 100}%`;
  const left = pct(selected + 0.5);
  const shift = selected > n * 0.7 ? '-88%' : selected < n * 0.17 ? '-12%' : '-50%';
  const callout = calloutText(bar, nowIso, lang);

  return (
    <Card className="now-tl" aria-labelledby="now-tl-title">
      <div className="now-card-head">
        <h2 id="now-tl-title" className="eyebrow">
          {s.timelineTitle}
        </h2>
        <span className="now-card-head__hint">{s.timelineHint}</span>
      </div>

      <div
        ref={ref}
        className="now-tl__plot"
        role="slider"
        tabIndex={0}
        aria-label={s.timelineAria}
        aria-valuemin={0}
        aria-valuemax={n - 1}
        aria-valuenow={selected}
        aria-valuetext={callout}
        {...handlers}
      >
        {nights.map((r) => (
          <div key={r.start} className="now-tl__night" style={{ left: pct(r.start), width: pct(r.end - r.start) }}>
            {r.end - r.start >= NIGHT_LABEL_MIN_BARS && <span className="now-tl__night-label num">{s.nightLabel}</span>}
          </div>
        ))}
        <div className="now-tl__bars" style={columns}>
          {bars.map((b, i) => (
            <span
              key={b.time}
              className={cx('now-tl__bar', b.night && 'now-tl__bar--night', i === selected && 'now-tl__bar--on')}
              style={{ height: BAR_BASE_PX + (b.score ?? 0) * BAR_PX_PER_POINT, background: tierColor(b.score), animationDelay: `${250 + i * 28}ms` }}
            />
          ))}
        </div>
        <div className="now-tl__cursor" style={{ left }} aria-hidden="true" />
        <div className="now-tl__callout num" style={{ left, transform: `translateX(${shift})`, borderColor: tierColor(bar.score) }} aria-hidden="true">
          {callout}
        </div>
      </div>

      <div className="now-tl__labels" style={columns} aria-hidden="true">
        {bars.map((b, i) => (
          <span key={b.time} className={cx('now-tl__label num', i === 0 && 'now-tl__label--now')}>
            {i === 0 ? s.nowLabel : b.label}
          </span>
        ))}
      </div>

      <div className="now-tl__lane-head">
        <span className="now-tl__lane-title num">
          {s.rainLane}
          <span className="now-tl__unit"> · mm/h</span>
        </span>
        <span className="now-tl__lane-max">{peak >= MEASURABLE_MM ? fmt(s.laneMax, { mm: fmtMm(peak, lang) }) : core.bandNone}</span>
      </div>
      <div className="now-tl__lane" style={columns} aria-hidden="true">
        {bars.map((b) => (
          <span key={b.time} className="now-tl__rain" style={{ height: laneHeight(b.mm), background: b.band === 'none' ? 'transparent' : RAIN_BAND_COLOR[b.band] }} />
        ))}
      </div>

      <button type="button" className="now-tl__detail" onClick={() => onOpenHour(selected)} aria-haspopup="dialog">
        <span>{fmt(s.hourDetails, { hour: timeLabel(bar.time, nowIso, lang) })}</span>
        <Icon name="chevronRight" size={20} />
      </button>
    </Card>
  );
}
