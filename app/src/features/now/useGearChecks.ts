import { useCallback, useEffect, useMemo, useState } from 'react';
import { KEYS, readJson, writeJson } from '../../lib/storage';

// Ticked gear items, kept per ride day (location date) in localStorage, so a
// new day starts with an empty checklist.

interface StoredChecks {
  date: string;
  ids: string[];
}

function isStoredChecks(value: unknown): value is StoredChecks | null {
  if (value === null) return true;
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  return typeof v.date === 'string' && Array.isArray(v.ids) && v.ids.every((id) => typeof id === 'string');
}

function load(date: string): StoredChecks {
  const stored = readJson<StoredChecks | null>(KEYS.gearChecked, null, isStoredChecks);
  return stored && stored.date === date ? stored : { date, ids: [] };
}

export function useGearChecks(date: string): { checked: ReadonlySet<string>; toggle: (id: string) => void } {
  const [state, setState] = useState<StoredChecks>(() => load(date));

  useEffect(() => {
    setState(load(date));
  }, [date]);

  const toggle = useCallback(
    (id: string) => {
      const base = state.date === date ? state.ids : [];
      const next: StoredChecks = { date, ids: base.includes(id) ? base.filter((x) => x !== id) : [...base, id] };
      writeJson(KEYS.gearChecked, next);
      setState(next);
    },
    [state, date],
  );

  const checked = useMemo(() => new Set(state.date === date ? state.ids : []), [state, date]);
  return { checked, toggle };
}
