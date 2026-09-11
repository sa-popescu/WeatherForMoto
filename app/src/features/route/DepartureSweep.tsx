import { useState, type CSSProperties } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import { Button, Card, cx, Skeleton } from '../../ui/primitives';
import { RS } from './strings';
import type { SweepBar } from './sweep';

// "Când pleci": one bar per 30-minute departure, the height and colour being
// the worst score met along the route. Best and chosen departures are ringed.

interface DepartureSweepProps {
  bars: readonly SweepBar[];
  best: SweepBar | null;
  chosenTime: string;
  chosenWorst: number | null;
  chosenWorstName: string | null;
  ready: boolean;
  onAdopt: (time: string) => void;
}

const CHART_MAX_PX = 92;

function barHeight(score: number | null): number {
  return Math.round(8 + ((score ?? 0) / 100) * (CHART_MAX_PX - 8));
}

export function DepartureSweep({ bars, best, chosenTime, chosenWorst, chosenWorstName, ready, onAdopt }: DepartureSweepProps) {
  const s = useStrings(RS);
  const [keptKey, setKeptKey] = useState<string | null>(null);
  const key = `${chosenTime}|${best?.time}|${best?.worst}`;
  const improves = best !== null && best.time !== chosenTime && (best.worst ?? 0) > (chosenWorst ?? -1);
  const kept = keptKey === key;
  const place = chosenWorstName ?? '–';

  let title: string;
  let body: string;
  if (!ready) {
    title = s.sweepTitle;
    body = s.sweepWaiting;
  } else if (!best) {
    title = s.sweepTitle;
    body = s.sweepNone;
  } else if (improves && !kept) {
    title = fmt(s.sweepGo, { time: best.time });
    body = fmt(s.sweepGain, { from: chosenWorst ?? '–', to: best.worst ?? '–', place });
  } else if (improves) {
    title = fmt(s.keep, { time: chosenTime });
    body = fmt(s.sweepGain, { from: chosenWorst ?? '–', to: best.worst ?? '–', place });
  } else {
    title = s.sweepSame;
    body = fmt(s.sweepSameBody, { score: chosenWorst ?? '–', place });
  }

  const chartLabel = fmt(s.sweepChart, {
    best: best?.time ?? '–',
    bestScore: best?.worst ?? '–',
    chosen: chosenTime,
    chosenScore: chosenWorst ?? '–',
  });

  return (
    <Card className="route-sweep" aria-labelledby="route-sweep-title">
      <h2 className="eyebrow" id="route-sweep-title">
        {s.sweepTitle}
      </h2>
      <p className={cx('route-sweep__title num', (!ready || !best || !improves || kept) && 'route-sweep__title--calm')} aria-live="polite">
        {title}
      </p>
      <p className="route-sweep__body">{body}</p>
      {!ready ? (
        <div className="route-sweep__skeleton">
          <Skeleton height={96} radius={10} />
        </div>
      ) : (
        <>
          <div className="route-sweep__chart" role="img" aria-label={chartLabel} style={{ '--bars': bars.length } as CSSProperties}>
            {bars.map((bar) => (
              <span
                key={bar.time}
                className={cx(
                  'route-sweep__bar',
                  bar.past && 'route-sweep__bar--past',
                  !bar.complete && 'route-sweep__bar--partial',
                  best?.time === bar.time && 'route-sweep__bar--best',
                  bar.time === chosenTime && 'route-sweep__bar--chosen',
                )}
                style={{ height: barHeight(bar.worst), background: tierColor(bar.worst) }}
              />
            ))}
          </div>
          <div className="route-sweep__axis" aria-hidden="true" style={{ '--bars': bars.length } as CSSProperties}>
            {bars.map((bar, i) =>
              i % 4 === 0 ? (
                <span key={bar.time} className="route-sweep__tick num" style={{ gridColumn: `${i + 1} / span 4` }}>
                  {bar.time}
                </span>
              ) : null,
            )}
          </div>
          <div className="route-sweep__legend" aria-hidden="true">
            <span className="route-sweep__key route-sweep__key--best">{s.sweepBest}</span>
            <span className="route-sweep__key route-sweep__key--chosen">{s.sweepChosen}</span>
          </div>
        </>
      )}
      {ready && best && improves && !kept && (
        <div className="route-sweep__actions">
          <Button variant="primary" onClick={() => onAdopt(best.time)}>
            {fmt(s.adopt, { time: best.time })}
          </Button>
          <Button onClick={() => setKeptKey(key)}>{fmt(s.keep, { time: chosenTime })}</Button>
        </div>
      )}
    </Card>
  );
}
