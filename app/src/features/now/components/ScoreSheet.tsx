import { fmt, useStrings } from '../../../lib/i18n';
import type { CurrentWeather } from '../../../lib/types';
import { Sheet } from '../../../ui/Sheet';
import { S_SHEETS } from '../sheetStrings';
import { FactorList, valuesFromCurrent } from './FactorList';

/** Why the current score is what it is: the backend's score_breakdown, explained. */
export function ScoreSheet({ current, onClose }: { current: CurrentWeather; onClose: () => void }) {
  const s = useStrings(S_SHEETS);
  const factors = current.score_breakdown ?? [];
  const penalty = factors.reduce((sum, f) => sum + f.penalty, 0);
  const caps = factors.map((f) => f.cap).filter((c): c is number => c !== null);
  const cap = caps.length ? Math.min(...caps) : null;
  const raw = Math.max(0, 100 - penalty);

  return (
    <Sheet open onClose={onClose} title={fmt(s.scoreSheetTitle, { score: current.moto_score ?? '–' })}>
      <p className="now-sheet__intro">{s.scoreIntro}</p>
      <FactorList factors={factors} values={valuesFromCurrent(current)} />
      {factors.length > 0 && (
        <p className="now-sheet__math num">{cap !== null && cap < raw ? fmt(s.scoreMathCap, { penalty, raw, cap }) : fmt(s.scoreMath, { penalty, raw })}</p>
      )}
    </Sheet>
  );
}
