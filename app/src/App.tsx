import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import type { ScreenProps } from './features/types';
import { CORE } from './i18n/core';
import { useStrings } from './lib/i18n';
import { applyUpdate, onUpdateReady } from './lib/pwa';
import { useToast } from './state/toast';
import { Icon, type IconName } from './ui/icons';

// App shell: four tabs driven by the URL hash (#/acum, #/traseu, #/harta,
// #/eu) so they work as deep links and PWA shortcuts. A tab stays mounted
// after its first visit, so scroll position and map state survive switching.

const NowScreen = lazy(() => import('./features/now/NowScreen'));
const RouteScreen = lazy(() => import('./features/route/RouteScreen'));
const MapScreen = lazy(() => import('./features/map/MapScreen'));
const AccountScreen = lazy(() => import('./features/account/AccountScreen'));
const ResetPasswordView = lazy(() => import('./features/account/ResetPasswordView'));

type Tab = 'acum' | 'traseu' | 'harta' | 'eu';
const TABS: readonly Tab[] = ['acum', 'traseu', 'harta', 'eu'];

function readTab(): Tab {
  const key = window.location.hash.replace(/^#\/?/, '').split(/[/?]/)[0];
  return (TABS as readonly string[]).includes(key) ? (key as Tab) : 'acum';
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

const SCREENS: Record<Tab, React.ComponentType<ScreenProps>> = {
  acum: NowScreen,
  traseu: RouteScreen,
  harta: MapScreen,
  eu: AccountScreen,
};

export default function App() {
  const s = useStrings(CORE);
  const toast = useToast();
  const online = useOnline();
  const [tab, setTab] = useState<Tab>(readTab);
  const [visited, setVisited] = useState<ReadonlySet<Tab>>(() => new Set([readTab()]));
  const resetToken = useMemo(() => new URLSearchParams(window.location.search).get('reset_token'), []);
  const [showReset, setShowReset] = useState(Boolean(resetToken));

  useEffect(() => {
    const onHash = (): void => {
      const next = readTab();
      setTab(next);
      setVisited((prev) => (prev.has(next) ? prev : new Set([...prev, next])));
      window.scrollTo({ top: 0 });
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(
    () => onUpdateReady(() => toast(s.updateReady, { actionLabel: s.updateNow, onAction: applyUpdate, durationMs: 60_000 })),
    [toast, s.updateReady, s.updateNow],
  );

  const navItems: { tab: Tab; icon: IconName; label: string }[] = [
    { tab: 'acum', icon: 'gauge', label: s.tabNow },
    { tab: 'traseu', icon: 'route', label: s.tabRoute },
    { tab: 'harta', icon: 'map', label: s.tabMap },
    { tab: 'eu', icon: 'user', label: s.tabMe },
  ];

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
                setTab('eu');
                setVisited((prev) => new Set([...prev, 'eu']));
              }}
            />
          </Suspense>
        </main>
      </div>
    );
  }

  return (
    <div className="shell">
      {!online && (
        <div className="offline-strip" role="status">
          {s.offline}
        </div>
      )}
      <main className="shell__main" id="main">
        <Suspense fallback={<div className="screen-fallback">{s.loading}</div>}>
          {TABS.filter((t) => visited.has(t)).map((t) => {
            const Screen = SCREENS[t];
            return <Screen key={t} active={t === tab} />;
          })}
        </Suspense>
      </main>
      <nav className="bottom-nav" aria-label={s.navLabel}>
        {navItems.map((item) => (
          <a key={item.tab} href={`#/${item.tab}`} className="bottom-nav__item" aria-current={item.tab === tab ? 'page' : undefined}>
            <Icon name={item.icon} />
            {item.label}
          </a>
        ))}
      </nav>
    </div>
  );
}
