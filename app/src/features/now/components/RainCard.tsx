import { fmtMm } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { Card, IconButton } from '../../../ui/primitives';
import { bandWord, timeLabel } from '../logic/formatting';
import { rainConsequence, type RainOutlook } from '../logic/rainOutlook';
import { S_NOW } from '../strings';
import { RainScale } from './RainScale';
import '../now-rain.css';

// "Șanse să plouă" and "Cât plouă" as two equal numbers: a percentage is
// never shown without the amount.

const MEASURABLE_MM = 0.05;

interface RainCardProps {
  outlook: RainOutlook;
  nowIso: string;
  onInfo: () => void;
}

export function RainCard({ outlook, nowIso, onInfo }: RainCardProps) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const at = (iso: string | null): string => (iso ? timeLabel(iso, nowIso, lang) : '');
  const period =
    outlook.kind === 'dry'
      ? s.rainNext24
      : outlook.startsNow
        ? fmt(s.rainNowUntil, { to: at(outlook.end) })
        : fmt(s.rainSpan, { from: at(outlook.start), to: at(outlook.end) });
  const measurable = outlook.peakMm >= MEASURABLE_MM;

  return (
    <Card className="now-rain" aria-labelledby="now-rain-title">
      <div className="now-rain__head">
        <div className="now-rain__title">
          <h2 id="now-rain-title" className="eyebrow">
            {s.rainTitle}
          </h2>
          <span className="now-rain__period num">{period}</span>
        </div>
        <IconButton icon="info" label={s.rainInfo} onClick={onInfo} aria-haspopup="dialog" />
      </div>

      <div className="now-rain__tiles">
        <div className="now-rain__tile">
          <span className="now-rain__label num">{s.chance}</span>
          <span className="now-rain__value num">
            {outlook.peakProbability}
            <span className="now-rain__unit">%</span>
          </span>
          <span className="now-rain__sub">{outlook.peakProbability > 0 && outlook.peakProbabilityTime ? fmt(s.peakAt, { hour: at(outlook.peakProbabilityTime) }) : s.noRain}</span>
        </div>
        <div className="now-rain__tile">
          <span className="now-rain__label num">{s.amount}</span>
          <span className="now-rain__value now-rain__value--mm num">
            {measurable ? fmtMm(outlook.peakMm, lang) : '0'}
            <span className="now-rain__unit">mm/h</span>
          </span>
          <span className="now-rain__sub">
            {measurable && outlook.peakMmTime ? fmt(s.bandPeakAt, { band: bandWord(outlook.band, lang), hour: at(outlook.peakMmTime) }) : s.noRain}
          </span>
        </div>
      </div>

      <RainScale peakMm={outlook.peakMm} band={outlook.band} />
      <p className="now-rain__consequence">{rainConsequence(outlook, lang)}</p>
    </Card>
  );
}
