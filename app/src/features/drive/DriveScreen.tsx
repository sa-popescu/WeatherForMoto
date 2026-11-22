import { useCallback, useEffect, useRef, useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { useDesktop } from '../../lib/useMediaQuery';
import { usePlace } from '../../state/place';
import { useSettings } from '../../state/settings';
import { useToast } from '../../state/toast';
import { aheadPoints } from '../ride/logic/ahead';
import { turnsFromInstructions } from '../ride/logic/turns';
import { rideRecorder } from '../ride/recorder';
import { buildRouteLink } from '../route/routeLink';
import type { NameBetween } from '../route/samples';
import { DEFAULT_SPEED, defaultDeparture, deviceOffsetSec } from '../route/timing';
import type { ScreenProps } from '../types';
import { boundsOf, setDriveData } from './driveMapLayers';
import { DriveRoutes } from './DriveRoutes';
import { DriveSearch, type SearchPurpose } from './DriveSearch';
import { DriveWeatherCard } from './DriveWeatherCard';
import { routeKmh } from './logic/driveRoute';
import { useDrivePlaces } from './places';
import { DRIVE_S } from './strings';
import { useDriveMap } from './useDriveMap';
import { useDrivePlan } from './useDrivePlan';
import './drive.css';

// "Drum", the home screen: the map around you, "Unde mergi?" on top and the
// weather here under it. Pick a destination and the card turns into the
// routes there, timed in today's traffic, with the weather met on each, and
// "Plecarea perfectă": the same trip at later hours, the best one marked.
// "Pornește" starts the ride on the chosen route.

/** The map at street level, before there is a route to frame. */
const HOME_ZOOM = 13;

export default function DriveScreen({ active }: ScreenProps) {
  const s = useStrings(DRIVE_S);
  const lang = useLang();
  const toast = useToast();
  const { theme } = useSettings();
  const desktop = useDesktop();
  const { place, source, locate, locating } = usePlace();
  const places = useDrivePlaces();
  const [destination, setDestination] = useState<Place | null>(null);
  const [purpose, setPurpose] = useState<SearchPurpose>('go');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [departMs, setDepartMs] = useState<number | null>(null);
  const [attempt, setAttempt] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const topRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const fromGps = source === 'gps';

  const nameBetween = useCallback<NameBetween>((road, previous) => road ?? fmt(s.afterStop, { stop: previous }), [s.afterStop]);
  // Routes start where the rider is, so they wait for the position asked for at the pick.
  const trip = useDrivePlan({ origin: destination && !locating ? place : null, destination, departMs, attempt, originName: s.myPosition, nameBetween });
  const { routes, outlooks } = trip;
  const ready = routes.status === 'ready' ? routes : null;
  const { map, ready: mapReady } = useDriveMap(containerRef, theme, active, setSelectedId);

  useEffect(() => setSelectedId(ready?.routes[0]?.id ?? null), [ready]);

  // The map: the routes and both ends, framed when new routes arrive, or the place on screen before that.
  useEffect(() => {
    if (map && mapReady) setDriveData(map, ready?.routes ?? [], selectedId, place, destination);
  }, [map, mapReady, ready, selectedId, place, destination]);
  useEffect(() => {
    if (!map || !mapReady || !active) return;
    if (!destination) {
      map.jumpTo({ center: [place.lon, place.lat], zoom: Math.max(map.getZoom(), HOME_ZOOM) });
      return;
    }
    const bounds = ready ? boundsOf(ready.routes, [destination]) : null;
    if (!bounds) return;
    const height = map.getContainer().clientHeight;
    const top = (topRef.current?.offsetHeight ?? 60) + 28;
    const bottom = Math.min((bottomRef.current?.offsetHeight ?? 200) + 28, height * 0.6);
    map.fitBounds(bounds, { padding: { top, bottom, left: 36, right: 36 }, duration: 600, maxZoom: 16 });
  }, [map, mapReady, active, ready, destination, place.lat, place.lon]);

  const pick = (picked: Place, why: SearchPurpose): void => {
    if (why !== 'go') {
      places.setShortcut(why, picked);
      toast(fmt(why === 'home' ? s.homeSaved : s.workSaved, { name: picked.name }), { tone: 'success' });
      setPurpose('go');
      return;
    }
    places.remember(picked);
    setDestination(picked);
    setSelectedId(null);
    setDepartMs(null);
    // A route starts from a fresh position, unless the place on screen already is one.
    if (!fromGps) void locate();
  };

  const close = (): void => {
    setDestination(null);
    setDepartMs(null);
  };

  const start = (): void => {
    const route = ready?.routes.find((r) => r.id === selectedId);
    const rp = selectedId ? trip.pointsOf(selectedId) : null;
    if (!route || !destination) return;
    rideRecorder.start({
      lang,
      route: {
        name: destination.name,
        coords: route.line.coords,
        cumKm: route.line.cumKm,
        distanceKm: route.line.distanceKm,
        plannedKmh: routeKmh(route),
        // The forecasts go along as they are now, so the warnings work without a connection.
        ahead: rp ? aheadPoints(rp.points, rp.weather, Date.now()) : [],
        turns: turnsFromInstructions(route.instructions),
        // Leaving the route asks for a new one to here.
        destination: { lat: destination.lat, lon: destination.lon },
      },
    });
  };

  // The planner, with the same two ends, for stops, another day or a GPX.
  const openDetailed = (): void => {
    if (!destination) return;
    const link = buildRouteLink({
      stops: [
        { name: fromGps ? s.myPosition : place.name, lat: place.lat, lon: place.lon, gps: fromGps },
        { ...destination, gps: false },
      ],
      departure: defaultDeparture(Date.now(), deviceOffsetSec()),
      speed: DEFAULT_SPEED,
      line: null,
    });
    window.location.hash = new URL(link).hash;
  };

  return (
    <div className="drive" hidden={!active}>
      <div ref={containerRef} className="drive-map" aria-label={s.mapLabel} />
      <div className="drive-top" ref={topRef}>
        <DriveSearch
          destination={destination}
          purpose={purpose}
          places={places}
          near={place}
          onPick={pick}
          onClear={close}
          onPurpose={setPurpose}
        />
      </div>
      <div className="drive-bottom" ref={bottomRef}>
        {destination ? (
          <DriveRoutes
            destination={destination}
            state={routes}
            outlooks={outlooks}
            selectedId={selectedId}
            fromLabel={fromGps ? s.fromHere : fmt(s.fromPlace, { place: place.name })}
            locating={locating}
            canRide={!desktop}
            departures={trip.departures}
            bestDeparture={trip.best}
            departMs={departMs}
            onDepart={setDepartMs}
            onSelect={setSelectedId}
            onClose={close}
            onRetry={() => setAttempt((a) => a + 1)}
            onStart={start}
            onDetailed={openDetailed}
          />
        ) : (
          <DriveWeatherCard active={active} />
        )}
      </div>
    </div>
  );
}
