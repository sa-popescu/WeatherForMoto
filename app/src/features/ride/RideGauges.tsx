import { useLang, useStrings } from '../../lib/i18n';
import { averageKmh, elapsedMs } from './logic/clock';
import { hasLean, type LeanState } from './logic/lean';
import { fmtDuration, fmtKm, fmtKmh, msToKmh } from './logic/rideFormat';
import type { RideSnapshot } from './recorder';
import { RIDE_S } from './strings';

// The numbers of a finished ride, in one grid. While the ride is recorded
// they are laid over the map instead (RideLive.tsx), which reuses the grid.

type RideText = { [K in keyof (typeof RIDE_S)['ro']]: string };

/** A lean under this is riding straight, give or take, so it is shown without a side. */
const LEAN_SIDE_MIN_DEG = 5;

export interface Stat {
  key: string;
  label: string;
  value: string;
  unit?: string;
  /** A second value in the same cell, after a dot. */
  then?: { value: string; unit?: string };
  /** Takes the whole row. */
  span?: boolean;
}

export function StatGrid({ stats, wide }: { stats: readonly Stat[]; wide?: boolean }) {
  return (
    <dl className={wide ? 'ride-stats ride-stats--wide' : 'ride-stats'}>
      {stats.map((st) => (
        <div key={st.key} className={st.span ? 'ride-stats__item ride-stats__item--span' : 'ride-stats__item'}>
          <dt className="ride-stats__label">{st.label}</dt>
          <dd className="ride-stats__value num">
            {st.value}
            {st.unit && <span className="ride-stats__unit"> {st.unit}</span>}
            {st.then && (
              <>
                <span className="ride-stats__sep"> · </span>
                {st.then.value}
                {st.then.unit && <span className="ride-stats__unit"> {st.then.unit}</span>}
              </>
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}

/** The most on each side, in one cell across the grid. */
function leanStat(lean: LeanState, s: RideText): Stat {
  return {
    key: 'lean',
    label: s.leanMax,
    value: `${Math.round(lean.maxLeftDeg)}°`,
    unit: s.leanLeft,
    then: { value: `${Math.round(lean.maxRightDeg)}°`, unit: s.leanRight },
    span: true,
  };
}

export function leanText(deg: number, s: RideText): string {
  const n = Math.round(Math.abs(deg));
  if (n < LEAN_SIDE_MIN_DEG) return `${n}°`;
  return `${n}° ${deg > 0 ? s.leanRight : s.leanLeft}`;
}

export function RideGauges({ ride, now }: { ride: RideSnapshot; now: number }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const { totals, clock } = ride;
  const elapsed = elapsedMs(clock, now);
  const avgMoving = averageKmh(totals.movingDistanceM, totals.movingMs);
  const avgTotal = averageKmh(totals.distanceM, elapsed);
  const top = totals.topSpeedMs > 0 ? msToKmh(totals.topSpeedMs) : null;
  const stats: Stat[] = [
    { key: 'distance', label: s.distance, value: fmtKm(totals.distanceM, lang), unit: s.km },
    { key: 'total', label: s.totalTime, value: fmtDuration(elapsed) },
    { key: 'moving', label: s.movingTime, value: fmtDuration(totals.movingMs) },
    { key: 'avg', label: s.avgMoving, value: fmtKmh(avgMoving, lang), unit: s.kmh },
    { key: 'avgTotal', label: s.avgTotal, value: fmtKmh(avgTotal, lang), unit: s.kmh },
    { key: 'top', label: s.topSpeed, value: fmtKmh(top, lang), unit: s.kmh },
  ];
  // Rides recorded without a course from the GPS have no lean to show.
  const lean = hasLean(ride.lean);
  if (lean) stats.push(leanStat(ride.lean, s));
  return (
    <section className="ride-gauges ride-gauges--done" aria-labelledby="ride-done-title">
      <h2 id="ride-done-title" className="ride-gauges__title num">
        {s.finishedTitle}
      </h2>
      <StatGrid stats={stats} wide />
      <p className="ride-gauges__note">{lean ? `${s.finishedSaved} ${s.leanEstimate}` : s.finishedSaved}</p>
    </section>
  );
}
