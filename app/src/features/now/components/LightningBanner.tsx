import { fmt, useStrings } from '../../../lib/i18n';
import type { Lightning } from '../../../lib/types';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import { compassPoint, lightningTone } from '../logic/lightning';
import { S_NOW } from '../strings';

// Lightning the satellite saw around here lately. Measured, not a warning
// someone issued, so it sits apart from the official ones and says where it
// comes from and how old it is.

export function LightningBanner({ lightning }: { lightning: Lightning | null | undefined }) {
  const s = useStrings(S_NOW);
  const tone = lightningTone(lightning);
  if (!lightning || !tone) return null;
  const where =
    lightning.nearest_km != null && lightning.bearing_deg != null
      ? fmt(s.lightningNearest, {
          km: Math.max(1, Math.round(lightning.nearest_km)),
          dir: s[`compass${compassPoint(lightning.bearing_deg)}`],
        })
      : null;
  const ago = lightning.last_minutes_ago != null ? fmt(s.lightningAgo, { min: lightning.last_minutes_ago }) : null;
  return (
    <article className={cx('now-alert', tone === 'near' ? 'now-alert--red' : 'now-alert--orange')} role="status">
      <div className="now-alert__head">
        <Icon name="bolt" size={20} />
        <h2 className="now-alert__title">{tone === 'near' ? s.lightningNearTitle : s.lightningAroundTitle}</h2>
      </div>
      <p className="now-alert__meta">
        {[
          fmt(s.lightningCount, { n: lightning.count, km: lightning.radius_km, min: lightning.window_min }),
          where,
          ago,
        ]
          .filter(Boolean)
          .join(' · ')}
      </p>
      {tone === 'near' && <p className="now-alert__body">{s.lightningAdvice}</p>}
      <p className="now-alert__source">{fmt(s.lightningSource, { credit: lightning.attribution })}</p>
    </article>
  );
}
