import { fmt, useStrings } from '../../../lib/i18n';
import type { DustOutlook } from '../../../lib/types';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import { S_NOW } from '../strings';

// Saharan dust on the way (CAMS forecast): haze, and rain that leaves mud on
// the visor and the bike. A forecast, not a warning anyone issued, so it says
// where it comes from.

/** "14:00" from a local "YYYY-MM-DDTHH:MM", with "mâine" / "tomorrow" when it is not today's date. */
function hourLabel(slot: string, today: string, tomorrowWord: string): string {
  const hhmm = slot.slice(11, 16);
  return slot.slice(0, 10) === today ? hhmm : `${tomorrowWord} ${hhmm}`;
}

export function DustBanner({ dust, nowLocal }: { dust: DustOutlook | null | undefined; nowLocal: string | null }) {
  const s = useStrings(S_NOW);
  if (!dust) return null;
  const today = (nowLocal ?? dust.peak_time).slice(0, 10);
  const peakAt = hourLabel(dust.peak_time, today, s.dustTomorrow);
  const lines = [dust.level === 'strong' ? s.dustStrongBody : s.dustNotableBody];
  if (dust.muddy_rain_at) lines.push(fmt(s.dustMuddyRain, { time: hourLabel(dust.muddy_rain_at, today, s.dustTomorrow) }));
  return (
    <article className={cx('now-alert', dust.level === 'strong' ? 'now-alert--orange' : 'now-alert--yellow')} role="status">
      <div className="now-alert__head">
        <Icon name="eye" size={20} />
        <h2 className="now-alert__title">{s.dustTitle}</h2>
      </div>
      <p className="now-alert__meta">{fmt(s.dustPeak, { ug: dust.peak_ug, time: peakAt })}</p>
      <p className="now-alert__body">{lines.join(' ')}</p>
      <p className="now-alert__source">{s.dustSource}</p>
    </article>
  );
}
