import { useLang, useStrings, type Lang } from '../../lib/i18n';
import { averageKmh, elapsedMs } from './logic/clock';
import { arrivalAt } from './logic/follow';
import { fmtClockTime, fmtDuration, fmtKm, fmtKmh, fmtTimeLeft, msToKmh } from './logic/rideFormat';
import type { RideSnapshot } from './recorder';
import { RIDE_S } from './strings';

// The numbers of the ride. While riding: the speed as large as the screen
// allows, the averages under it, then distance and time (or, on a route, what
// is left of it). Once finished: the whole summary in one grid.

type RideText = { [K in keyof (typeof RIDE_S)['ro']]: string };

/** A speed older than this is not "now" any more. */
const LIVE_SPEED_STALE_MS = 5_000;

interface Stat {
  key: string;
  label: string;
  value: string;
  unit?: string;
}

function StatGrid({ stats, wide }: { stats: readonly Stat[]; wide?: boolean }) {
  return (
    <dl className={wide ? 'ride-stats ride-stats--wide' : 'ride-stats'}>
      {stats.map((st) => (
        <div key={st.key} className="ride-stats__item">
          <dt className="ride-stats__label">{st.label}</dt>
          <dd className="ride-stats__value num">
            {st.value}
            {st.unit && <span className="ride-stats__unit"> {st.unit}</span>}
          </dd>
        </div>
      ))}
    </dl>
  );
}

export function RideGauges({ ride, now }: { ride: RideSnapshot; now: number }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const { totals, clock } = ride;
  const elapsed = elapsedMs(clock, now);
  const avgMoving = averageKmh(totals.movingDistanceM, totals.movingMs);
  const avgTotal = averageKmh(totals.distanceM, elapsed);
  const top = totals.topSpeedMs > 0 ? msToKmh(totals.topSpeedMs) : null;

  if (ride.phase === 'finished') {
    const stats: Stat[] = [
      { key: 'distance', label: s.distance, value: fmtKm(totals.distanceM, lang), unit: s.km },
      { key: 'total', label: s.totalTime, value: fmtDuration(elapsed) },
      { key: 'moving', label: s.movingTime, value: fmtDuration(totals.movingMs) },
      { key: 'avg', label: s.avgMoving, value: fmtKmh(avgMoving, lang), unit: s.kmh },
      { key: 'avgTotal', label: s.avgTotal, value: fmtKmh(avgTotal, lang), unit: s.kmh },
      { key: 'top', label: s.topSpeed, value: fmtKmh(top, lang), unit: s.kmh },
    ];
    return (
      <section className="ride-gauges ride-gauges--done" aria-labelledby="ride-done-title">
        <h2 id="ride-done-title" className="ride-gauges__title num">
          {s.finishedTitle}
        </h2>
        <StatGrid stats={stats} wide />
        <p className="ride-gauges__note">{s.finishedSaved}</p>
      </section>
    );
  }

  // A weak position still comes with a usable speed: the GPS measures it apart.
  const fresh = ride.lastFixAt !== null && now - ride.lastFixAt <= LIVE_SPEED_STALE_MS && (ride.gps === 'ok' || ride.gps === 'weak');
  const speed = fresh ? ride.speedKmh : null;
  const onRoute = ride.route !== null && ride.follow !== null;
  const stats: Stat[] = onRoute ? routeStats(ride, avgMoving, now, lang, s) : [
    { key: 'distance', label: s.distance, value: fmtKm(totals.distanceM, lang), unit: s.km },
    { key: 'moving', label: s.movingTime, value: fmtDuration(totals.movingMs) },
    { key: 'total', label: s.totalTime, value: fmtDuration(elapsed) },
  ];
  return (
    <section className="ride-gauges" aria-label={s.dialogLabel}>
      <div className="ride-speed">
        <span className="ride-speed__value num">{fmtKmh(speed, lang)}</span>
        <span className="ride-speed__unit num">{s.kmh}</span>
      </div>
      <p className="ride-gauges__avg num">
        {/* On a route the grid is about what is left, so what was ridden moves up here. */}
        {onRoute && `${fmtKm(totals.distanceM, lang)} ${s.km} · `}
        {s.avgMoving} {fmtKmh(avgMoving, lang)} · {s.topSpeed} {fmtKmh(top, lang)}
      </p>
      <StatGrid stats={stats} />
    </section>
  );
}

/** What is left of the route: distance, time and the arrival on this phone's clock. */
function routeStats(ride: RideSnapshot, avgMoving: number | null, now: number, lang: Lang, s: RideText): Stat[] {
  const route = ride.route;
  const km = ride.follow?.km ?? 0;
  const remainingKm = route ? Math.max(0, route.distanceKm - km) : 0;
  const arrival = route ? arrivalAt(remainingKm, avgMoving, ride.totals.movingMs, route.plannedKmh, now) : now;
  const known = ride.follow?.km !== null && ride.follow?.km !== undefined;
  return [
    { key: 'remaining', label: s.remaining, value: fmtKm(remainingKm * 1000, lang), unit: s.km },
    { key: 'left', label: s.timeLeft, value: known ? fmtTimeLeft(arrival - now) : '–' },
    { key: 'arrival', label: s.arrival, value: known ? fmtClockTime(arrival, lang) : '–' },
  ];
}
