import { useEffect, useState } from 'react';

/**
 * The wide layout: the nav becomes a rail of icons on the left and the tabs
 * spread over several columns. A desktop window from 1024 px, and a phone
 * turned sideways, which is wide but short: in the tall phone layout its bars
 * would leave almost no room for the content. The CSS repeats this query in
 * its wide blocks (CSS cannot read it from here), so change them together.
 */
export const WIDE_QUERY = '(min-width: 1024px), (orientation: landscape) and (max-height: 500px) and (min-width: 600px)';

/** A phone turned sideways: the wide layout, tightened for the short screen (base.css). */
export const SHORT_LANDSCAPE_QUERY = '(orientation: landscape) and (max-height: 500px)';

/**
 * A computer: the main pointer is a mouse or a trackpad, not a finger. Rides,
 * offline route maps and what only a phone on the handlebar can do follow
 * this, not the width, so a phone or a tablet turned sideways keeps them.
 */
export const COMPUTER_QUERY = '(hover: hover) and (pointer: fine)';

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

/** True in the wide layout (WIDE_QUERY): a desktop window, or a phone turned sideways. */
export function useWideLayout(): boolean {
  return useMediaQuery(WIDE_QUERY);
}

/** True on a computer (COMPUTER_QUERY), whatever the window's size. */
export function useComputer(): boolean {
  return useMediaQuery(COMPUTER_QUERY);
}
