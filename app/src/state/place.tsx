import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { api } from '../lib/api';
import { DEFAULT_PLACE, MAX_FAVORITES } from '../lib/config';
import { GeoError, geolocationPermission, getCurrentPosition, reverseGeocode } from '../lib/geo';
import { useLang } from '../lib/i18n';
import { useSettings } from './settings';
import { parsePlaceLink } from '../lib/placeLink';
import { shouldLocateOnBoot, shouldRefreshOnResume } from '../lib/followLocation';
import { KEYS, readJson, writeJson } from '../lib/storage';
import type { Place } from '../lib/types';

// The place the whole app is looking at, plus favourites.
// Start-up order: shared link (?q=Name, with ?ll=lat,lon from newer links),
// then the last place, then the default. GPS runs at launch when the browser
// has already granted it (silent), or when you asked the app to open on your
// location (see lib/followLocation.ts); otherwise you tap "my location", and
// doing so turns that setting on.

export type PlaceSource = 'default' | 'last' | 'gps' | 'search' | 'favorite' | 'link';

interface PlaceState {
  place: Place;
  source: PlaceSource;
  setPlace: (place: Place, source: PlaceSource) => void;
  favorites: Place[];
  isFavorite: (place: Place) => boolean;
  toggleFavorite: (place: Place) => void;
  removeFavorite: (place: Place) => void;
  /** Resolves true when a GPS fix was obtained. */
  locate: () => Promise<boolean>;
  locating: boolean;
  locateError: GeoError['reason'] | null;
}

const PlaceContext = createContext<PlaceState | null>(null);

function isPlace(value: unknown): value is Place {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  return typeof v.name === 'string' && typeof v.lat === 'number' && typeof v.lon === 'number';
}

function isPlaceArray(value: unknown): value is Place[] {
  return Array.isArray(value) && value.every(isPlace);
}

export function samePlace(a: Place, b: Place): boolean {
  return a.name === b.name || (Math.abs(a.lat - b.lat) < 0.001 && Math.abs(a.lon - b.lon) < 0.001);
}

function initialPlace(): { place: Place; source: PlaceSource } {
  const last = readJson<Place | null>(KEYS.lastPlace, null, (v): v is Place | null => v === null || isPlace(v));
  if (last) return { place: { name: last.name, lat: last.lat, lon: last.lon }, source: 'last' };
  return { place: { ...DEFAULT_PLACE }, source: 'default' };
}

export function PlaceProvider({ children }: { children: ReactNode }) {
  const lang = useLang();
  const { followMe, setFollowMe } = useSettings();
  const [{ place, source }, setState] = useState(initialPlace);
  const [favorites, setFavorites] = useState<Place[]>(() => readJson(KEYS.favorites, [], isPlaceArray));
  const [locating, setLocating] = useState(false);
  const [locateError, setLocateError] = useState<GeoError['reason'] | null>(null);
  const booted = useRef(false);
  /** When the last GPS fix landed, so a return to the app can refresh a stale one. */
  const fixAt = useRef<number | null>(null);

  const setPlace = useCallback((next: Place, nextSource: PlaceSource) => {
    const clean = { name: next.name, lat: next.lat, lon: next.lon };
    writeJson(KEYS.lastPlace, clean);
    setState({ place: clean, source: nextSource });
  }, []);

  const locate = useCallback(async (): Promise<boolean> => {
    setLocating(true);
    setLocateError(null);
    try {
      const pos = await getCurrentPosition();
      const name = (await reverseGeocode(pos.lat, pos.lon, lang)) ?? `${pos.lat.toFixed(3)}, ${pos.lon.toFixed(3)}`;
      setPlace({ name, lat: pos.lat, lon: pos.lon }, 'gps');
      fixAt.current = Date.now();
      // It worked once, so from now on the app opens here instead of on
      // yesterday's city. Turned off again from the "Eu" screen.
      setFollowMe(true);
      return true;
    } catch (err) {
      setLocateError(err instanceof GeoError ? err.reason : 'unavailable');
      return false;
    } finally {
      setLocating(false);
    }
  }, [lang, setPlace, setFollowMe]);

  useEffect(() => {
    if (booted.current) return;
    booted.current = true;
    const params = new URLSearchParams(window.location.search);
    const shared = parsePlaceLink(params);
    if (shared) {
      params.delete('q');
      params.delete('ll');
      const query = params.toString();
      window.history.replaceState(null, '', `${window.location.pathname}${query ? `?${query}` : ''}${window.location.hash}`);
      if (shared.kind === 'place') {
        setPlace(shared.place, 'link');
        return;
      }
      api
        .geocode(shared.name)
        .then((g) => setPlace({ name: g.name, lat: g.lat, lon: g.lon }, 'link'))
        .catch((err: unknown) => console.warn('[place] shared link could not be resolved', err));
      return;
    }
    void geolocationPermission().then((state) => {
      if (shouldLocateOnBoot(followMe, state)) void locate();
    });
  }, [followMe, locate, setPlace]);

  // Back in the app after a while: the place on screen should be where you are
  // now, not where you were when you last closed it.
  useEffect(() => {
    const onVisible = (): void => {
      if (document.visibilityState !== 'visible') return;
      void geolocationPermission().then((state) => {
        if (shouldRefreshOnResume(followMe, state, fixAt.current, Date.now())) void locate();
      });
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => document.removeEventListener('visibilitychange', onVisible);
  }, [followMe, locate]);

  const persistFavorites = useCallback((list: Place[]) => {
    writeJson(KEYS.favorites, list);
    setFavorites(list);
  }, []);

  const isFavorite = useCallback((p: Place) => favorites.some((f) => samePlace(f, p)), [favorites]);

  const toggleFavorite = useCallback(
    (p: Place) => {
      if (favorites.some((f) => samePlace(f, p))) persistFavorites(favorites.filter((f) => !samePlace(f, p)));
      else persistFavorites([{ name: p.name, lat: p.lat, lon: p.lon }, ...favorites].slice(0, MAX_FAVORITES));
    },
    [favorites, persistFavorites],
  );

  const removeFavorite = useCallback((p: Place) => persistFavorites(favorites.filter((f) => !samePlace(f, p))), [favorites, persistFavorites]);

  const value = useMemo<PlaceState>(
    () => ({ place, source, setPlace, favorites, isFavorite, toggleFavorite, removeFavorite, locate, locating, locateError }),
    [place, source, setPlace, favorites, isFavorite, toggleFavorite, removeFavorite, locate, locating, locateError],
  );

  return <PlaceContext.Provider value={value}>{children}</PlaceContext.Provider>;
}

export function usePlace(): PlaceState {
  const ctx = useContext(PlaceContext);
  if (!ctx) throw new Error('usePlace must be used inside PlaceProvider');
  return ctx;
}
