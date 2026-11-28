import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { InstallInvite } from './features/install/InstallInvite';
import { MoveBanner } from './features/move/MoveBanner';
import { onOldAddress } from './features/move/move';
import { liveIdFromHash } from './features/live/constants';
import { rideRecorder } from './features/ride/recorder';
import { useRide } from './features/ride/useRide';
import type { ScreenProps } from './features/types';
import { CORE } from './i18n/core';
import { useLang, useStrings } from './lib/i18n';
import { applyUpdate, onUpdateReady } from './lib/pwa';
import { noteUse, noteVisit } from './lib/usage';
import { useAuth } from './state/auth';
import { useToast } from './state/toast';
import { Icon, type IconName } from './ui/icons';
import { ViewSwitch } from './ui/ViewSwitch';

// App shell: four tabs (Vreme, Drum, Ture, Eu) over six views driven by the
// URL hash (#/drum, #/traseu, #/acum, #/harta, #/ture, #/eu), so every view
// works as a deep link and older links keep working. "Vreme" comes first and
// is where the app opens: the forecast and the radar map, with a switch
// between them. "Drum" holds the route screen and the detailed planner. A view stays mounted after its
// first visit, so scroll position and map state survive switching.

const DriveScreen = lazy(() => import('./features/drive/DriveScreen'));
const NowScreen = lazy(() => import('./features/now/NowScreen'));
const RouteScreen = lazy(() => import('./features/route/RouteScreen'));
const MapScreen = lazy(() => import('./features/map/MapScreen'));
const RidesScreen = lazy(() => import('./features/ride/RidesScreen'));
const AccountScreen = lazy(() => import('./features/account/AccountScreen'));
const ResetPasswordView = lazy(() => import('./features/account/ResetPasswordView'));
const RideScreen = lazy(() => import('./features/ride/RideScreen'));
const LiveView = lazy(() => import('./features/live/LiveView'));

type View = 'drum' | 'traseu' | 'acum' | 'harta' | 'ture' | 'eu';
type Tab = 'drum' | 'vreme' | 'ture' | 'eu';
type WeatherView = Extract<View, 'acum' | 'harta'>;

const VIEWS: readonly View[] = ['drum', 'traseu', 'acum', 'harta', 'ture', 'eu'];
/** Where the app opens without a link to somewhere else. */
const HOME: View = 'acum';
const TAB_OF: Record<View, Tab> = { drum: 'drum', traseu: 'drum', acum: 'vreme', harta: 'vreme', ture: 'ture', eu: 'eu' };

function readView(): View {
  const key = window.location.hash.replace(/^#\/?/, '').split(/[/?]/)[0];
  if (key === 'vreme') return 'acum';
  return (VIEWS as readonly string[]).includes(key) ? (key as View) : HOME;
}

function isWeatherView(view: View): view is WeatherView {
  return view === 'acum' || view === 'harta';
}

function useOnline(): boolean {
  const [online, setOnline] = useState(() => navigator.onLine);
  useEffect(() => {
    const on = (): void => setOnline(true);
    const off = (): void => setOnline(false);
    window.addEventListener('online', on);
    window.addEventListener('offline', off);
    return () => {
      window.removeEventListener('online', on);
      window.removeEventListener('offline', off);
    };
  }, []);
  return online;
}

const SCREENS: Record<View, React.ComponentType<ScreenProps>> = {
  drum: DriveScreen,
  traseu: RouteScreen,
  acum: NowScreen,
  harta: MapScreen,
  ture: RidesScreen,
  eu: AccountScreen,
};

export default function App() {
  const s = useStrings(CORE);
  const toast = useToast();
  const online = useOnline();
  const [view, setView] = useState<View>(readView);
  const [visited, setVisited] = useState<ReadonlySet<View>>(() => new Set([readView()]));
  // "Vreme" in the nav goes back to whichever of its views was open last.
  const [weatherView, setWeatherView] = useState<WeatherView>(() => {
    const first = readView();
    return isWeatherView(first) ? first : 'acum';
  });
  const resetToken = useMemo(() => new URLSearchParams(window.location.search).get('reset_token'), []);
  const [showReset, setShowReset] = useState(Boolean(resetToken));
  // The content area scrolls, not the page (see .shell in base.css).
  const mainRef = useRef<HTMLElement>(null);
  // A ride covers the whole app, tabs included, until it is closed.
  const riding = useRide().phase !== 'idle';
  // The app moved to motonor.bluemouse.cc: the old address offers to move there.
  const oldAddress = useMemo(onOldAddress, []);
  // A live location link (#/live/<id>) shows that ride instead of the app.
  const [liveId, setLiveId] = useState(() => liveIdFromHash(window.location.hash));

  // A ride the app was closed in the middle of is offered again at start-up.
  useEffect(() => {
    void rideRecorder.restoreUnfinished();
  }, []);

  // Anonymous usage counts (lib/usage.ts): the visit, and the views it opened.
  const lang = useLang();
  const { status: authStatus } = useAuth();
  useEffect(noteVisit, []);
  useEffect(() => {
    if (lang === 'en') noteUse('visit:en');
  }, [lang]);
  useEffect(() => {
    if (authStatus === 'signed-in') noteUse('visit:signed-in');
  }, [authStatus]);
  useEffect(() => {
    noteUse(liveId ? 'view:live' : `view:${view}`);
  }, [view, liveId]);

  useEffect(() => {
    const onHash = (): void => {
      setLiveId(liveIdFromHash(window.location.hash));
      const next = readView();
      setView(next);
      if (isWeatherView(next)) setWeatherView(next);
      setVisited((prev) => (prev.has(next) ? prev : new Set([...prev, next])));
      mainRef.current?.scrollTo({ top: 0 });
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(
    () => onUpdateReady(() => toast(s.updateReady, { actionLabel: s.updateNow, onAction: applyUpdate, durationMs: Infinity })),
    [toast, s.updateReady, s.updateNow],
  );

  const tab = TAB_OF[view];
  const navItems: { tab: Tab; href: string; icon: IconName; label: string }[] = [
    { tab: 'vreme', href: `#/${weatherView}`, icon: 'sun', label: s.tabWeather },
    { tab: 'drum', href: '#/drum', icon: 'route', label: s.tabDrive },
    { tab: 'ture', href: '#/ture', icon: 'bike', label: s.tabRides },
    { tab: 'eu', href: '#/eu', icon: 'user', label: s.tabMe },
  ];

  if (liveId) {
    return (
      <Suspense fallback={<div className="screen-fallback">{s.loading}</div>}>
        <LiveView id={liveId} />
      </Suspense>
    );
  }

  if (showReset && resetToken) {
    return (
      <div className="shell">
        <main className="shell__main">
          <Suspense fallback={<div className="screen-fallback">{s.loading}</div>}>
            <ResetPasswordView
              token={resetToken}
              onDone={() => {
                // Drop the token from the URL so it is not kept in history.
                window.history.replaceState(null, '', `${window.location.pathname}#/eu`);
                setShowReset(false);
                setView('eu');
                setVisited((prev) => new Set([...prev, 'eu']));
              }}
            />
          </Suspense>
        </main>
      </div>
    );
  }

  return (
    <>
      {/* data-tab holds the view: the desktop layout gives Traseu the whole
          height of the window, and the radar map starts under the switch. */}
      <div className="shell" data-tab={view} inert={riding}>
        {!online && (
          <div className="offline-strip" role="status">
            {s.offline}
          </div>
        )}
        <main className="shell__main" id="main" ref={mainRef}>
          {oldAddress && <MoveBanner />}
          {/* Not on the old address (it asks to move first) nor over a ride. */}
          {!oldAddress && !riding && <InstallInvite />}
          {isWeatherView(view) && (
            <ViewSwitch
              label={s.weatherViews}
              items={[
                { href: '#/acum', label: s.weatherForecast, current: view === 'acum' },
                { href: '#/harta', label: s.weatherRadar, current: view === 'harta' },
              ]}
            />
          )}
          {/* One Suspense boundary per view: loading a new one must not hide (and
              disconnect the effects of) the views already mounted. */}
          {VIEWS.filter((v) => visited.has(v)).map((v) => {
            const Screen = SCREENS[v];
            return (
              <Suspense key={v} fallback={v === view ? <div className="screen-fallback">{s.loading}</div> : null}>
                <Screen active={v === view} />
              </Suspense>
            );
          })}
        </main>
        <nav className="bottom-nav" aria-label={s.navLabel}>
          <span className="bottom-nav__mark" aria-hidden="true">
            <Icon name="gauge" size={30} strokeWidth={2} />
          </span>
          {navItems.map((item) => (
            <a key={item.tab} href={item.href} className="bottom-nav__item" aria-current={item.tab === tab ? 'page' : undefined}>
              <Icon name={item.icon} />
              {item.label}
            </a>
          ))}
        </nav>
      </div>
      {riding && (
        <Suspense fallback={null}>
          <RideScreen />
        </Suspense>
      )}
    </>
  );
}
