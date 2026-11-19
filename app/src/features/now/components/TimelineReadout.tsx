import { CORE } from '../../../i18n/core';
import { useStrings } from '../../../lib/i18n';
import { tierColor, tierOf } from '../../../lib/scoring';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import type { HourReadout } from '../logic/timeline';
import { S_NOW } from '../strings';
import { tierWord } from './tierWord';

interface TimelineReadoutProps {
  readout: HourReadout;
  /** "Detalii pentru 22:00": the panel is also the way into the hour's details. */
  label: string;
  onOpen: () => void;
}

/**
 * The picked hour in full, fixed above the bars: when and the score, then
 * air, gusts and asphalt, then rain on a line of its own because its text is
 * the longest. It replaces the readout tiles, the rain card's numbers and the
 * "Detalii" button that used to sit under the card.
 */
export function TimelineReadout({ readout, label, onOpen }: TimelineReadoutProps) {
  const s = useStrings(S_NOW);
  const core = useStrings(CORE);
  const color = tierColor(readout.score);
  const { rain, road } = readout;

  return (
    <button type="button" className="now-tl__readout" onClick={onOpen} aria-label={label} aria-haspopup="dialog">
      <span className="now-tl__readout-top">
        <span className="now-tl__when num">
          {readout.when}
          <Icon name="chevronRight" size={18} />
        </span>
        <span className="now-tl__score num" style={{ borderColor: color }}>
          <span className="now-tl__score-dot" style={{ background: color }} aria-hidden="true" />
          {readout.score ?? '–'}
          <span className="now-tl__score-tier">{tierWord(tierOf(readout.score), core)}</span>
        </span>
      </span>
      <span className="now-tl__stats">
        <span className="now-tl__stat">
          <span className="now-tl__stat-label num">{s.air}</span>
          <span className="now-tl__stat-value num">{readout.temp}</span>
          <span className="now-tl__stat-sub">{readout.feels}</span>
        </span>
        <span className="now-tl__stat">
          <span className="now-tl__stat-label num">{s.gusts}</span>
          <span className="now-tl__stat-value num">
            {readout.gust}
            <span className="now-tl__stat-unit">km/h</span>
          </span>
          <span className="now-tl__stat-sub">{readout.gustWord}</span>
        </span>
        <span className="now-tl__stat">
          <span className="now-tl__stat-label num">{s.asphalt}</span>
          <span className="now-tl__stat-value num">{road.temp}</span>
          <span className={cx('now-tl__stat-sub', `now-tl__road--${road.state}`)}>{road.word}</span>
        </span>
      </span>
      <span className="now-tl__rain-row">
        <span className="now-tl__stat-label now-tl__stat-label--rain num">{s.rainTitle}</span>
        <span className={cx('now-tl__rain-value num', `now-tl__rain-value--${rain.state}`)}>{rain.value}</span>
        {rain.sub && <span className="now-tl__rain-sub">{rain.sub}</span>}
      </span>
    </button>
  );
}
