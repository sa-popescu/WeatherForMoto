import { useEffect, useState } from 'react';

/**
 * From this width the app uses its desktop layout: the nav becomes a rail of
 * icons on the left and the tabs spread over several columns. The CSS repeats
 * the same value in its `@media (min-width: 1024px)` blocks (CSS cannot read
 * it from here), so change both together.
 */
export const DESKTOP_QUERY = '(min-width: 1024px)';

function matches(query: string): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia(query).matches;
}

/** Whether a media query matches now, updated when it starts or stops matching. */
export function useMediaQuery(query: string): boolean {
  const [matched, setMatched] = useState(() => matches(query));
  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return undefined;
    const list = window.matchMedia(query);
    const onChange = (): void => setMatched(list.matches);
    // The query may have changed between the first render and this effect.
    onChange();
    list.addEventListener('change', onChange);
    return () => list.removeEventListener('change', onChange);
  }, [query]);
  return matched;
}

/** True on a screen wide enough for the desktop layout. */
export function useDesktop(): boolean {
  return useMediaQuery(DESKTOP_QUERY);
}
