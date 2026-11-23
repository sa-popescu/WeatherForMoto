import { useEffect, useState } from 'react';

// Whether the browser says data costs here: the user's data saver (Save-Data),
// a very slow connection, or the reduced-data preference. The map then reads
// only the moments it shows instead of the whole band ahead (a band is about
// 1.5 MB of radar on a phone, 2.5 MB on a desktop), and does not play by
// itself. Browsers without these hints (Safari) read the band as usual.

interface NetworkHints extends EventTarget {
  saveData?: boolean;
  effectiveType?: string;
}

function connection(): NetworkHints | undefined {
  return typeof navigator === 'undefined' ? undefined : (navigator as Navigator & { connection?: NetworkHints }).connection;
}

export function prefersLessData(): boolean {
  const hints = connection();
  if (hints?.saveData) return true;
  if (hints?.effectiveType === 'slow-2g' || hints?.effectiveType === '2g') return true;
  return typeof matchMedia === 'function' && matchMedia('(prefers-reduced-data: reduce)').matches;
}

export function useLessData(): boolean {
  const [less, setLess] = useState(prefersLessData);
  useEffect(() => {
    const hints = connection();
    if (!hints) return undefined;
    const update = (): void => setLess(prefersLessData());
    hints.addEventListener('change', update);
    return () => hints.removeEventListener('change', update);
  }, []);
  return less;
}
