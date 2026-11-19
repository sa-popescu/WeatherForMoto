import { CORE } from '../../../i18n/core';
import { useStrings } from '../../../lib/i18n';
import { tierColor, tierOf } from '../../../lib/scoring';
import { cx } from '../../../ui/primitives';
import type { HourReadout } from '../logic/timeline';
import { S_NOW } from '../strings';
import { tierWord } from './tierWord';

/**
 * The picked hour in full, fixed above the bars: when, score, air, rain, gusts.
 * It replaces the bubble that floated over the bars and showed either the rain
 * or the temperature, never both.
 */
export function TimelineReadout({ readout }: { readout: HourReadout }) {
  const s = useStrings(S_NOW);
  const core = useStrings(CORE);
  const color = tierColor(readout.score);

  return (
    <div className="now-tl__readout">
      <div className="now-tl__readout-top">
        <span className="now-tl__when num">{readout.when}</span>
        <span className="now-tl__score num" style={{ borderColor: color }}>
          <span className="now-tl__score-dot" style={{ background: color }} aria-hidden="true" />
          {readout.score ?? '–'}
          <span className="now-tl__score-tier">{tierWord(tierOf(readout.score), core)}</span>
        </span>
      </div>
      <dl className="now-tl__stats">
        <div className="now-tl__stat">
          <dt className="now-tl__stat-label num">{s.air}</dt>
          <dd className="now-tl__stat-value num">{readout.temp}</dd>
          <dd className="now-tl__stat-sub">{readout.feels}</dd>
        </div>
        <div className="now-tl__stat">
          <dt className="now-tl__stat-label now-tl__stat-label--rain num">{s.rainTitle}</dt>
          <dd className={cx('now-tl__stat-value num', readout.rain.state !== 'dry' && 'now-tl__stat-value--rain')}>{readout.rain.value}</dd>
          <dd className="now-tl__stat-sub">{readout.rain.sub}</dd>
        </div>
        <div className="now-tl__stat">
          <dt className="now-tl__stat-label num">{s.gusts}</dt>
          <dd className="now-tl__stat-value num">
            {readout.gust}
            <span className="now-tl__stat-unit">km/h</span>
          </dd>
          <dd className="now-tl__stat-sub">{readout.gustWord}</dd>
        </div>
      </dl>
    </div>
  );
}
