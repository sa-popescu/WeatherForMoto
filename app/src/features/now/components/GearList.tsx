import { fmt, useStrings } from '../../../lib/i18n';
import { Icon } from '../../../ui/icons';
import { cx, SectionLabel } from '../../../ui/primitives';
import type { GearRec, GearUrgency } from '../logic/gear';
import { GEAR_TEXTS } from '../logic/gearStrings';
import { S_NOW } from '../strings';
import { useGearChecks } from '../useGearChecks';

const TAG_KEY: Record<GearUrgency, 'tagRequired' | 'tagRecommended' | 'tagAdvice'> = {
  required: 'tagRequired',
  recommended: 'tagRecommended',
  advice: 'tagAdvice',
};

interface GearListProps {
  recs: GearRec[];
  /** Ride date at the location; ticks are stored per date. */
  date: string;
  tomorrow: boolean;
}

export function GearList({ recs, date, tomorrow }: GearListProps) {
  const s = useStrings(S_NOW);
  const g = useStrings(GEAR_TEXTS);
  const { checked, toggle } = useGearChecks(date);
  if (!recs.length) return null;
  const done = recs.filter((r) => checked.has(r.id)).length;

  return (
    <section className="now-section" aria-labelledby="now-gear-title">
      <SectionLabel id="now-gear-title" action={<span className="now-section__meta num">{fmt(s.gearCount, { done, total: recs.length })}</span>}>
        {tomorrow ? s.gearTomorrow : s.gearToday}
      </SectionLabel>
      <ul className="now-gear">
        {recs.map((r) => {
          const on = checked.has(r.id);
          return (
            <li key={r.id}>
              <button
                type="button"
                role="checkbox"
                aria-checked={on}
                className={cx('now-gear__row', `now-gear__row--${r.urgency}`, on && 'now-gear__row--done')}
                onClick={() => toggle(r.id)}
              >
                <span className="now-gear__box" aria-hidden="true">
                  {on && <Icon name="check" size={18} strokeWidth={2.6} />}
                </span>
                <span className="now-gear__text">
                  <span className="now-gear__item">{r.item}</span>
                  <span className="now-gear__why">{r.reason}</span>
                </span>
                <span className="now-gear__tag num">{g[TAG_KEY[r.urgency]]}</span>
              </button>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
