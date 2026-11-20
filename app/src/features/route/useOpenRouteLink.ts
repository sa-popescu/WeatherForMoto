import { useEffect, useRef } from 'react';
import { useStrings } from '../../lib/i18n';
import { useToast } from '../../state/toast';
import { AS } from './actionStrings';
import { departureUsable, parseRouteLink, ROUTE_TAB_HASH, type RouteLink } from './routeLink';
import { defaultDeparture, deviceOffsetSec, localIsoAt, type Speed } from './timing';
import type { Departure } from './types';
import type { RidePlan } from './useCalculate';

// Opens a shared route link (routeLink.ts): when the app starts on one, and
// when one is opened while the app is already running. The link is taken out
// of the address once read, so a reload does not undo what the rider changed
// since.

interface Options {
  /** The days the planner offers, today first. */
  dates: readonly string[];
  speed: Speed;
  setDeparture: (next: Departure) => void;
  setSpeed: (next: Speed) => void;
  openShared: (link: RouteLink, plan: RidePlan) => boolean;
}

export function useOpenRouteLink(options: Options): void {
  const toast = useToast();
  const as = useStrings(AS);
  // The listener lives as long as the screen; it reads the newest values from here.
  const latest = useRef({ ...options, toast, as });
  latest.current = { ...options, toast, as };

  useEffect(() => {
    const open = (): void => {
      const parsed = parseRouteLink(window.location.hash);
      if (parsed.kind === 'none') return;
      window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}${ROUTE_TAB_HASH}`);
      const { dates, speed, setDeparture, setSpeed, openShared, toast: show, as: text } = latest.current;
      if (parsed.kind === 'invalid' || parsed.kind === 'gone') {
        show(parsed.kind === 'gone' ? text.linkGone : text.linkInvalid, { tone: 'error' });
        return;
      }

      const link = parsed.link;
      const nowMs = Date.now();
      const offset = deviceOffsetSec(nowMs);
      const usable = link.departure !== null && departureUsable(link.departure, dates, localIsoAt(nowMs, offset));
      const departure = usable && link.departure ? link.departure : defaultDeparture(nowMs, offset);
      const pace = link.speed ?? speed;
      setDeparture(departure);
      setSpeed(pace);
      if (!openShared(link, { departure, speed: pace })) {
        show(text.linkInvalid, { tone: 'error' });
        return;
      }
      if (link.departure !== null && !usable) show(text.linkPastDeparture);
    };
    // Deferred by a tick: React's development mode mounts every screen twice
    // and the first pass would cancel the calculation started here. The timer
    // of that first pass is cleared before it fires, so the link opens once.
    const timer = window.setTimeout(open, 0);
    window.addEventListener('hashchange', open);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener('hashchange', open);
    };
  }, []);
}
