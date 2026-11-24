import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { averageKmh, elapsedMs } from './logic/clock';
import { arrivalAt } from './logic/follow';
import { hasLean, MIN_LEAN_SPEED_MS } from './logic/lean';
import { fmtClockTime, fmtDuration, fmtKm, fmtKmh, fmtTimeLeft, msToKmh } from './logic/rideFormat';
import { LiveShareControl } from './LiveShareControl';
import type { RideSnapshot } from './recorder';
import { leanText, StatGrid, type Stat } from './RideGauges';
import { RIDE_S } from './strings';

// The numbers of a ride while it is recorded, laid over the map: the speed in
// a bubble, what matters most in one line (what is left of the route, or what
// was ridden), and the rest when that line is tapped. RideGauges.tsx keeps
// the summary of a finished ride.

/** A speed older than this is not "now" any more. */
const LIVE_SPEED_STALE_MS = 5_000;

/** The speed now, or null when the GPS has gone quiet. */
function liveSpeed(ride: RideSnapshot, now: number): number | null {
  // A weak position still comes with a usable speed: the GPS measures it apart.
  const fresh = ride.lastFixAt !== null && now - ride.lastFixAt <= LIVE_SPEED_STALE_MS && (ride.gps === 'ok' || ride.gps === 'weak');
  return fresh ? ride.speedKmh : null;
}

export function RideSpeed({ ride, now }: { ride: RideSnapshot; now: number }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  return (
    <div className="ride-speed">
      <span className="ride-speed__value num">{fmtKmh(liveSpeed(ride, now), lang)}</span>
      <span className="ride-speed__unit num">{s.kmh}</span>
    </div>
  );
}

/** What matters most, in one line: on a route what is left of it, otherwise what was ridden. */
type RideText = { [K in keyof (typeof RIDE_S)['ro']]: string };

function summaryParts(ride: RideSnapshot, now: number, lang: Lang, s: RideText): string[] {
  const { totals, clock } = ride;
  const avgMoving = averageKmh(totals.movingDistanceM, totals.movingMs);
  const route = ride.route;
  if (route && ride.follow) {
    const km = ride.follow.km;
    if (km === null) return [`${fmtKm(route.distanceKm * 1000, lang)} ${s.km}`];
    const remainingKm = Math.max(0, route.distanceKm - km);
    const arrival = arrivalAt(remainingKm, avgMoving, totals.movingMs, route.plannedKmh, now);
    return [`${fmtKm(remainingKm * 1000, lang)} ${s.km}`, fmtTimeLeft(arrival - now), fmt(s.arrivalAt, { time: fmtClockTime(arrival, lang) })];
  }
  return [
    `${fmtKm(totals.distanceM, lang)} ${s.km}`,
    fmtDuration(elapsedMs(clock, now)),
    `${s.avgMoving} ${fmtKmh(avgMoving, lang)}`,
  ];
}

export function RideSummary({ ride, now, open, onToggle, detailsId }: {
  ride: RideSnapshot;
  now: number;
  open: boolean;
  onToggle: () => void;
  detailsId: string;
}) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  return (
    <button type="button" className="ride-summary" aria-expanded={open} aria-controls={detailsId} aria-label={open ? s.detailsHide : s.detailsShow} onClick={onToggle}>
      <span className="ride-summary__line num">
        {summaryParts(ride, now, lang, s).map((part, i) => (
          <span key={i} className="ride-summary__part">
            {part}
          </span>
        ))}
      </span>
      <Icon name="chevronDown" size={20} className={open ? 'ride-summary__chevron ride-summary__chevron--open' : 'ride-summary__chevron'} />
    </button>
  );
}

/** Everything else, when the summary is tapped: the ride so far, the lean, the live location. */
export function RideDetails({ ride, now, id }: { ride: RideSnapshot; now: number; id: string }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const { totals, clock } = ride;
  const elapsed = elapsedMs(clock, now);
  const top = totals.topSpeedMs > 0 ? msToKmh(totals.topSpeedMs) : null;
  const stats: Stat[] = [
    { key: 'distance', label: s.distance, value: fmtKm(totals.distanceM, lang), unit: s.km },
    { key: 'moving', label: s.movingTime, value: fmtDuration(totals.movingMs) },
    { key: 'total', label: s.totalTime, value: fmtDuration(elapsed) },
    { key: 'avg', label: s.avgMoving, value: fmtKmh(averageKmh(totals.movingDistanceM, totals.movingMs), lang), unit: s.kmh },
    { key: 'top', label: s.topSpeed, value: fmtKmh(top, lang), unit: s.kmh },
  ];
  const speed = liveSpeed(ride, now);
  if (hasLean(ride.lean) || ride.lean.nowDeg !== null) {
    const fast = speed !== null && speed >= msToKmh(MIN_LEAN_SPEED_MS);
    stats.push({
      key: 'lean',
      label: s.leanNow,
      value: fast && ride.lean.nowDeg !== null ? leanText(ride.lean.nowDeg, s) : '–',
      then: { value: `${Math.round(Math.max(ride.lean.maxLeftDeg, ride.lean.maxRightDeg))}°`, unit: s.leanMaxShort },
    });
  }
  return (
    <div id={id} className="ride-details">
      <StatGrid stats={stats} />
      <LiveShareControl now={now} />
    </div>
  );
}
