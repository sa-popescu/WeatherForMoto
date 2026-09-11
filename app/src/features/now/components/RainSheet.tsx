import { fmtMm } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { RAIN_BAND_COLOR, RAIN_BANDS } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import { Sheet } from '../../../ui/Sheet';
import { Button, cx } from '../../../ui/primitives';
import { bandWord, capitalize } from '../logic/formatting';
import type { RainOutlook } from '../logic/rainOutlook';
import { S_SHEETS } from '../sheetStrings';
import { RainMatrix } from './RainMatrix';
import { bandRangeLabel } from './RainScale';

// The rain explainer: probability and amount defined side by side, the
// intensity bands with what they mean on a bike, and the scoring matrix.

// When there is no rain to talk about, the definitions use the design's example.
const EXAMPLE_PROBABILITY = 70;
const EXAMPLE_MM = 0.2;

export function RainSheet({ outlook, onClose }: { outlook: RainOutlook; onClose: () => void }) {
  const s = useStrings(S_SHEETS);
  const lang = useLang();
  const p = outlook.peakProbability >= 10 ? outlook.peakProbability : EXAMPLE_PROBABILITY;
  const mm = outlook.peakMm >= 0.05 ? outlook.peakMm : EXAMPLE_MM;
  const effects: Record<Exclude<RainBand, 'none'>, string> = {
    urme: s.effectUrme,
    slaba: s.effectSlaba,
    moderata: s.effectModerata,
    puternica: s.effectPuternica,
  };

  return (
    <Sheet
      open
      onClose={onClose}
      title={s.rainSheetTitle}
      footer={
        <Button variant="primary" size="lg" full className="now-gotit" onClick={onClose}>
          {s.gotIt}
        </Button>
      }
    >
      <p className="now-sheet__intro">{s.rainSheetIntro}</p>
      <div className="now-defs">
        <div className="now-def">
          <span className="now-def__title num">
            <span className="now-def__key">{s.chanceKey}</span> · {p}%
          </span>
          <span className="now-def__body">{fmt(s.chanceDef, { n: Math.round(p / 10) })}</span>
          <span className="now-def__note">{s.chanceDefNote}</span>
        </div>
        <div className="now-def">
          <span className="now-def__title now-def__title--rain num">
            <span className="now-def__key">{s.amountKey}</span> · {fmtMm(mm, lang)} mm/h
          </span>
          <span className="now-def__body">{s.amountDef}</span>
          <span className="now-def__note">{s.amountDefNote}</span>
        </div>
      </div>

      <section className="now-sheet__block">
        <h3 className="eyebrow">{s.bandsTitle}</h3>
        <ul className="now-bands">
          {RAIN_BANDS.map((b) => (
            <li key={b} className={cx('now-band', b === outlook.band && 'now-band--on')}>
              <span className="now-band__swatch" style={{ background: RAIN_BAND_COLOR[b] }} aria-hidden="true" />
              <span className="now-band__name">
                <strong>{capitalize(bandWord(b, lang))}</strong>
                <span className="num">{bandRangeLabel(b, lang, true)}</span>
              </span>
              <span className="now-band__effect">{effects[b]}</span>
            </li>
          ))}
        </ul>
      </section>

      <RainMatrix probability={outlook.peakProbability} band={outlook.band} />
      <p className="now-sheet__note">{s.thresholdsNote}</p>
    </Sheet>
  );
}
