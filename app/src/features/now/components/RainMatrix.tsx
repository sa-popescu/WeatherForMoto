import { useLang, useStrings } from '../../../lib/i18n';
import { getScoringMeta, IMPACT_TIER, TIER_COLOR, type RainImpact } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import { cx } from '../../../ui/primitives';
import { bandWord } from '../logic/formatting';
import { S_SHEETS } from '../sheetStrings';

// Probability x intensity matrix, straight from the published scoring
// constants, with the current situation ringed.

const ROW_KEYS = ['rowLow', 'rowMid', 'rowHigh'] as const;
const LEGEND: ReadonlyArray<{ impact: RainImpact; key: 'legendNone' | 'legendLow' | 'legendMedium' | 'legendHigh' }> = [
  { impact: 'none', key: 'legendNone' },
  { impact: 'low', key: 'legendLow' },
  { impact: 'medium', key: 'legendMedium' },
  { impact: 'high', key: 'legendHigh' },
];

/** Same row split as the backend: below 30 %, 30-60 %, above 60 %. */
function probabilityRow(probability: number): number {
  if (probability < 30) return 0;
  return probability <= 60 ? 1 : 2;
}

export function RainMatrix({ probability, band }: { probability: number; band: RainBand }) {
  const s = useStrings(S_SHEETS);
  const lang = useLang();
  const { columns, rows } = getScoringMeta().rain.impact_matrix;
  const nowRow = probabilityRow(probability);
  // A likely hour with no amount is scored as traces, so it is ringed there.
  const effective = band === 'none' && nowRow === 2 ? 'urme' : band;
  const nowCol = columns.indexOf(effective);
  const legendText = (impact: RainImpact): string => s[LEGEND.find((l) => l.impact === impact)?.key ?? 'legendNone'];

  return (
    <section className="now-sheet__block">
      <h3 className="eyebrow">{s.matrixTitle}</h3>
      <p className="now-sheet__caption">{s.matrixCaption}</p>
      <table className="now-matrix">
        <thead>
          <tr>
            <td />
            {columns.map((c) => (
              <th key={c} scope="col">
                {bandWord(c as RainBand, lang)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ROW_KEYS[ri] ?? ri}>
              <th scope="row" className="num">
                {s[ROW_KEYS[ri] ?? 'rowHigh']}
              </th>
              {row.map((cell, ci) => {
                const impact = cell as RainImpact;
                const here = ri === nowRow && ci === nowCol;
                return (
                  <td key={columns[ci]}>
                    <span className={cx('now-matrix__cell num', here && 'now-matrix__cell--now')} style={{ background: TIER_COLOR[IMPACT_TIER[impact]] }}>
                      {here ? s.matrixNow : ''}
                      <span className="visually-hidden">{legendText(impact)}</span>
                    </span>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <ul className="now-legend">
        {LEGEND.map((l) => (
          <li key={l.impact}>
            <span className="now-legend__swatch" style={{ background: TIER_COLOR[IMPACT_TIER[l.impact]] }} aria-hidden="true" />
            {s[l.key]}
          </li>
        ))}
      </ul>
    </section>
  );
}
