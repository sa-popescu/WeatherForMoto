import { useMemo } from 'react';
import { tierColor } from '../lib/scoring';

// The Cockpit gauge: a 270 degree arc (r=120) plus a tick ring that lights up
// to the score. Geometry is in a 300x252 viewBox and scales with its box.

const ARC_LENGTH = 565.49; // 270 degrees of a circle with r = 120
const CIRCUMFERENCE = 753.98;

function tickPaths(score: number): { lit: string; dim: string } {
  let lit = '';
  let dim = '';
  for (let k = 0; k <= 25; k += 1) {
    const fraction = k / 25;
    const theta = ((135 + 270 * fraction) * Math.PI) / 180;
    const inner = k % 5 === 0 ? 86 : 94;
    const outer = 102;
    const segment =
      `M${(150 + inner * Math.cos(theta)).toFixed(1)} ${(150 + inner * Math.sin(theta)).toFixed(1)}` +
      `L${(150 + outer * Math.cos(theta)).toFixed(1)} ${(150 + outer * Math.sin(theta)).toFixed(1)}`;
    if (fraction * 100 <= score) lit += segment;
    else dim += segment;
  }
  return { lit, dim };
}

interface GaugeProps {
  score: number | null;
  /** Tier word shown under the number, e.g. "OK". */
  label: string;
  /** Spoken description, e.g. "Scor moto 72 din 100, OK". */
  ariaLabel: string;
}

export function Gauge({ score, label, ariaLabel }: GaugeProps) {
  const value = score == null ? 0 : Math.max(0, Math.min(100, score));
  const color = tierColor(score);
  const { lit, dim } = useMemo(() => tickPaths(value), [value]);
  const offset = ARC_LENGTH * (1 - value / 100);

  return (
    <div className="gauge" role="img" aria-label={ariaLabel} style={{ color }}>
      <svg className="gauge__svg" viewBox="0 0 300 252" aria-hidden="true" focusable="false">
        <g transform="rotate(135 150 150)">
          <circle className="gauge__track" cx="150" cy="150" r="120" strokeDasharray={`${ARC_LENGTH} ${CIRCUMFERENCE}`} />
          <circle
            key={value}
            className="gauge__arc"
            cx="150"
            cy="150"
            r="120"
            strokeDasharray={`${ARC_LENGTH} ${CIRCUMFERENCE}`}
            strokeDashoffset={offset}
          />
        </g>
        <path className="gauge__ticks" d={dim} />
        <path className="gauge__ticks gauge__ticks--lit" d={lit} />
        <text className="gauge__end" x="92" y="212" textAnchor="middle">
          0
        </text>
        <text className="gauge__end" x="208" y="212" textAnchor="middle">
          100
        </text>
      </svg>
      <div className="gauge__center">
        <div className="gauge__score num">{score == null ? '–' : score}</div>
        <div className="gauge__label num">{label}</div>
      </div>
    </div>
  );
}
