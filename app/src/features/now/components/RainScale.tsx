import { fmtMm } from '../../../lib/format';
import { fmt, pick, useLang, type Lang } from '../../../lib/i18n';
import { bandRange, RAIN_BAND_COLOR, RAIN_BANDS } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import { cx } from '../../../ui/primitives';
import { bandWord, capitalize } from '../logic/formatting';
import { S_NOW } from '../strings';

type BandName = Exclude<RainBand, 'none'>;

/** "sub 0,5", "0,5–2,5", "peste 7,5" (optionally with the unit). */
export function bandRangeLabel(band: BandName, lang: Lang, withUnit = false): string {
  const s = pick(S_NOW, lang);
  const { min, max } = bandRange(band);
  const unit = withUnit ? ' mm/h' : '';
  if (band === 'urme' && max != null) return `${fmt(s.scaleBelow, { v: fmtMm(max, lang) })}${unit}`;
  if (max == null) return `${fmt(s.scaleAbove, { v: fmtMm(min, lang) })}${unit}`;
  return `${fmtMm(min, lang)}–${fmtMm(max, lang)}${unit}`;
}

/** Four intensity segments with a marker where the peak amount falls. */
export function RainScale({ peakMm, band }: { peakMm: number; band: RainBand }) {
  const lang = useLang();
  const index = band === 'none' ? -1 : RAIN_BANDS.indexOf(band);
  let marker: number | null = null;
  if (index >= 0) {
    const { min, max } = bandRange(RAIN_BANDS[index]);
    // The open-ended heavy band is drawn as twice its lower bound.
    const top = max ?? min * 2;
    const within = Math.min(1, Math.max(0, (peakMm - min) / (top - min)));
    marker = ((index + within) / RAIN_BANDS.length) * 100;
  }

  return (
    <div className="now-scale" role="img" aria-label={`${bandWord(band, lang)}, ${fmtMm(peakMm, lang)} mm/h`}>
      <div className="now-scale__track">
        {RAIN_BANDS.map((b, i) => (
          <span key={b} className={cx('now-scale__seg', i === index && 'now-scale__seg--on')} style={{ background: RAIN_BAND_COLOR[b] }} />
        ))}
        {marker !== null && <span className="now-scale__marker" style={{ left: `${marker}%` }} />}
      </div>
      <div className="now-scale__labels" aria-hidden="true">
        {RAIN_BANDS.map((b, i) => (
          <span key={b} className={cx('now-scale__name', i === index && 'now-scale__name--on')}>
            {capitalize(bandWord(b, lang))}
            <span className="now-scale__range num">{bandRangeLabel(b, lang)}</span>
          </span>
        ))}
      </div>
    </div>
  );
}
