import type { CSSProperties } from 'react';
import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import type { Place } from '../../lib/types';
import { Icon } from '../../ui/icons';
import { Button, cx, Spinner } from '../../ui/primitives';
import { aheadLine } from '../ride/aheadText';
import { fmtKm, fmtTimeLeft } from '../ride/logic/rideFormat';
import { localDay, seasonRisk, type SeasonRisk } from '../route/seasonalClosures';
import { DepartureStrip } from './DepartureStrip';
import type { DepartureOption } from './logic/departures';
import type { RouteOutlook } from './logic/driveWeather';
import { mainRoad, type DriveRoute } from './logic/driveRoute';
import { RouteNotes } from './RouteNotes';
import type { RoutesState } from './useDriveRoutes';
import { DRIVE_S } from './strings';

// The card under the map once a destination is picked: the routes there, each
// with its time in today's traffic and the weather the rider meets on it,
// then the start. Picking a route here or on the map makes it the one drawn
// in colour, and the one the start follows.

type DriveText = { [K in keyof (typeof DRIVE_S)['ro']]: string };

function trafficLine(route: DriveRoute, s: DriveText): string {
  if (route.source !== 'tomtom') return s.noTraffic;
  const min = Math.round(route.delayS / 60);
  return min >= 1 ? fmt(s.trafficDelay, { min }) : s.trafficClear;
}

/** The weather of a route in a few words, and the warning to spell out under it, if any. */
function weatherWords(outlook: RouteOutlook | undefined, s: DriveText, lang: Lang): { short: string; warning: string | null } {
  if (!outlook || !outlook.settled) return { short: s.weatherPending, warning: null };
  if (outlook.warning) return { short: '', warning: aheadLine(outlook.warning, lang) };
  return { short: outlook.worstScore === null ? s.weatherFailed : s.weatherClear, warning: null };
}

interface RouteOptionProps {
  route: DriveRoute;
  outlook: RouteOutlook | undefined;
  season: SeasonRisk;
  fastest: boolean;
  selected: boolean;
  onSelect: () => void;
}

function RouteOption({ route, outlook, season, fastest, selected, onSelect }: RouteOptionProps) {
  const s = useStrings(DRIVE_S);
  const lang = useLang();
  const road = mainRoad(route);
  const weather = weatherWords(outlook, s, lang);
  const score = outlook?.settled ? outlook.worstScore : null;
  const delayed = route.source === 'tomtom' && route.delayS >= 60;
  return (
    <li>
      <button type="button" className={cx('drive-option', selected && 'drive-option--on')} aria-pressed={selected} onClick={onSelect}>
        <span className="drive-option__row">
          <span className="drive-option__time num">{fmtTimeLeft(route.durationS * 1000)}</span>
          <span className="drive-option__km num">{fmt(s.km, { km: fmtKm(route.line.distanceKm * 1000, lang) })}</span>
          <span className={cx('drive-option__traffic', delayed && 'drive-option__traffic--slow')}>{trafficLine(route, s)}</span>
        </span>
        <span className="drive-option__row">
          <span className="drive-option__via">{[road && fmt(s.via, { road }), fastest && s.fastest].filter(Boolean).join(' · ')}</span>
          <span className="drive-option__sky">
            {weather.short}
            {score !== null && (
              <span className="drive-option__score num" style={{ '--drive-tier': tierColor(score) } as CSSProperties} title={fmt(s.worstScore, { score })}>
                {score}
              </span>
            )}
          </span>
        </span>
        {weather.warning && (
          <span className="drive-option__warning">
            <Icon name="drop" size={14} />
            {weather.warning}
          </span>
        )}
        <RouteNotes route={route} season={season} />
      </button>
    </li>
  );
}

interface DriveRoutesProps {
  destination: Place;
  state: RoutesState;
  outlooks: Readonly<Record<string, RouteOutlook>>;
  selectedId: string | null;
  fromLabel: string;
  locating: boolean;
  /** A ride is recorded by a phone on the handlebar: a computer gets no start button. */
  canRide: boolean;
  departures: readonly DepartureOption[];
  bestDeparture: number | null;
  /** The departure on screen, or null for now. */
  departMs: number | null;
  onDepart: (departMs: number | null) => void;
  onSelect: (id: string) => void;
  onClose: () => void;
  onRetry: () => void;
  onStart: () => void;
  onDetailed: () => void;
}

export function DriveRoutes(props: DriveRoutesProps) {
  const { destination, state, outlooks, selectedId, fromLabel, locating, canRide, departures, bestDeparture, departMs, onDepart } = props;
  const { onSelect, onClose, onRetry, onStart, onDetailed } = props;
  const s = useStrings(DRIVE_S);
  const ready = state.status === 'ready' ? state : null;
  const fastestId = ready ? [...ready.routes].sort((a, b) => a.durationS - b.durationS)[0]?.id : null;
  const season = seasonRisk(localDay(departMs ?? Date.now()));

  return (
    <section className="drive-sheet" aria-labelledby="drive-sheet-title">
      <header className="drive-sheet__head">
        <div className="drive-sheet__titles">
          <h2 id="drive-sheet-title" className="drive-sheet__title num">
            {fmt(s.towards, { name: destination.name })}
          </h2>
          <p className="drive-sheet__from">
            {locating ? s.locating : fromLabel}
            {ready?.attribution && <span className="drive-sheet__credit"> · {ready.attribution}</span>}
          </p>
        </div>
        <button type="button" className="icon-btn drive-sheet__close" aria-label={s.closeRoutes} onClick={onClose}>
          <Icon name="close" size={20} />
        </button>
      </header>

      {(state.status === 'loading' || locating) && (
        <p className="drive-sheet__status" role="status">
          <Spinner size={18} /> {locating ? s.locating : s.routing}
        </p>
      )}
      {state.status === 'error' && !locating && (
        <div className="drive-sheet__status drive-sheet__status--error" role="alert">
          <p>{state.kind === 'no-route' ? s.noRoute : s.routeFailed}</p>
          <Button icon="refresh" onClick={onRetry}>
            {s.retry}
          </Button>
        </div>
      )}
      {ready && !locating && (
        <>
          <ul className="drive-options" aria-label={s.routesLabel}>
            {ready.routes.map((route) => (
              <RouteOption
                key={route.id}
                route={route}
                outlook={outlooks[route.id]}
                season={season}
                fastest={ready.routes.length > 1 && route.id === fastestId}
                selected={route.id === selectedId}
                onSelect={() => onSelect(route.id)}
              />
            ))}
          </ul>
          {ready.source === 'osrm' && <p className="drive-sheet__note">{s.fallback}</p>}
          <DepartureStrip options={departures} best={bestDeparture} selected={departMs} noTraffic={ready.source === 'osrm'} onPick={onDepart} />
          {canRide && (
            <>
              <Button variant="primary" size="lg" full icon="play" onClick={onStart} disabled={!selectedId} aria-describedby="drive-start-hint">
                {departMs === null ? s.start : s.startNow}
              </Button>
              <p className="visually-hidden" id="drive-start-hint">
                {s.startHint}
              </p>
            </>
          )}
          <button type="button" className="drive-sheet__detailed" onClick={onDetailed}>
            <Icon name="route" size={16} />
            {s.detailed}
          </button>
        </>
      )}
    </section>
  );
}
