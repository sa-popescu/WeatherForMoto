import { useCallback, useState } from 'react';
import { KEYS, readJson, writeJson } from '../../lib/storage';
import type { Place } from '../../lib/types';

// The quick destinations of "Drum", kept on this phone: Acasă, Serviciu and
// the last few places the rider went to.

const MAX_RECENT = 6;

function isPlace(value: unknown): value is Place {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  return typeof v.name === 'string' && typeof v.lat === 'number' && typeof v.lon === 'number';
}

function isPlaceOrNull(value: unknown): value is Place | null {
  return value === null || isPlace(value);
}

function isPlaceList(value: unknown): value is Place[] {
  return Array.isArray(value) && value.every(isPlace);
}

const clean = (p: Place): Place => ({ name: p.name, lat: p.lat, lon: p.lon });

/** The same spot, give or take 50 m. By position only: street names repeat from town to town. */
function sameSpot(a: Place, b: Place): boolean {
  return Math.abs(a.lat - b.lat) < 0.0005 && Math.abs(a.lon - b.lon) < 0.0005;
}

export type Shortcut = 'home' | 'work';

export interface DrivePlaces {
  home: Place | null;
  work: Place | null;
  recent: Place[];
  setShortcut: (which: Shortcut, place: Place) => void;
  /** Puts a destination first among the recent ones. */
  remember: (place: Place) => void;
}

export function useDrivePlaces(): DrivePlaces {
  const [home, setHome] = useState<Place | null>(() => readJson(KEYS.driveHome, null, isPlaceOrNull));
  const [work, setWork] = useState<Place | null>(() => readJson(KEYS.driveWork, null, isPlaceOrNull));
  const [recent, setRecent] = useState<Place[]>(() => readJson(KEYS.driveRecent, [], isPlaceList));

  const setShortcut = useCallback((which: Shortcut, place: Place) => {
    const value = clean(place);
    writeJson(which === 'home' ? KEYS.driveHome : KEYS.driveWork, value);
    (which === 'home' ? setHome : setWork)(value);
  }, []);

  const remember = useCallback((place: Place) => {
    setRecent((list) => {
      const next = [clean(place), ...list.filter((p) => !sameSpot(p, place))].slice(0, MAX_RECENT);
      writeJson(KEYS.driveRecent, next);
      return next;
    });
  }, []);

  return { home, work, recent, setShortcut, remember };
}
