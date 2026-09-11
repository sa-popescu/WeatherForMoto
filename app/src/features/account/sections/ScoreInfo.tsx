import { CORE } from '../../../i18n/core';
import { fmt, useStrings } from '../../../lib/i18n';
import { getScoringMeta, TIER_COLOR, tierOf, type Tier } from '../../../lib/scoring';
import { Icon } from '../../../ui/icons';
import { Card } from '../../../ui/primitives';
import { APP } from '../strings/app';
import { Group } from '../ui/controls';

type TierName = 'tierIdeal' | 'tierOk' | 'tierAtentie' | 'tierEvita';
type TierDesc = 'tierIdealDesc' | 'tierOkDesc' | 'tierAtentieDesc' | 'tierEvitaDesc';
const TIER_TEXT: Record<Tier, { name: TierName; desc: TierDesc }> = {
  ideal: { name: 'tierIdeal', desc: 'tierIdealDesc' },
  ok: { name: 'tierOk', desc: 'tierOkDesc' },
  atentie: { name: 'tierAtentie', desc: 'tierAtentieDesc' },
  evita: { name: 'tierEvita', desc: 'tierEvitaDesc' },
};

/** Plain-language explainer. Thresholds come from the live scoring constants (85 / 60 / 40). */
export function ScoreInfo() {
  const s = useStrings(APP);
  const core = useStrings(CORE);
  const labels = [...getScoringMeta().labels].sort((a, b) => b.min_score - a.min_score);

  return (
    <Group title={s.scoreSection}>
      <Card className="acct-details-card">
        <details className="acct-details">
          <summary>
            <span>{s.scoreSummary}</span>
            <Icon name="chevronDown" size={20} />
          </summary>
          <div className="acct-details__body">
            <p className="acct-lead">{s.scoreLead}</p>
            <ul className="acct-tiers">
              {labels.map((label, i) => {
                const tier = tierOf(label.min_score) ?? 'evita';
                const lowest = i === labels.length - 1;
                const range = lowest ? fmt(s.tierBelow, { max: labels[i - 1]?.min_score ?? 0 }) : fmt(s.tierFrom, { min: label.min_score });
                return (
                  <li key={label.label} className="acct-tier">
                    <span className="acct-tier__dot" style={{ background: TIER_COLOR[tier] }} aria-hidden="true" />
                    <span className="acct-tier__name num" style={{ color: TIER_COLOR[tier] }}>
                      {core[TIER_TEXT[tier].name]}
                    </span>
                    <span className="acct-tier__text">
                      <span className="acct-tier__range num">{range}</span> {s[TIER_TEXT[tier].desc]}
                    </span>
                  </li>
                );
              })}
            </ul>
            <p className="acct-note">
              <Icon name="drop" size={18} />
              <span>{s.scoreRain}</span>
            </p>
          </div>
        </details>
      </Card>
    </Group>
  );
}
