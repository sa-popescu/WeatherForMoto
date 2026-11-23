import { useEffect, useState, type RefObject } from 'react';

// In the phone layout the band of time and the report button cover the lower
// part of the map and the place chip its top: the map's own middle sits low,
// behind the panel. This measures how far above the map's middle the middle
// of the part left free is, so the place can be shown there (usePlaceView.ts).
// In the wide layout the panel sits beside the map and the offset is 0.

/**
 * Pixels the middle of the free part sits above the map's middle; follows the
 * panels as they change size, except while `paused` (a report spot is being
 * aimed at, and the map must not move under the crosshair).
 */
export function useFreeAreaOffset(
  canvas: RefObject<HTMLElement | null>,
  top: RefObject<HTMLElement | null>,
  bottom: RefObject<HTMLElement | null>,
  enabled: boolean,
  paused: boolean,
): number {
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    if (!enabled) {
      setOffset(0);
      return undefined;
    }
    const measure = (): void => {
      if (paused) return;
      const map = canvas.current?.getBoundingClientRect();
      const panel = bottom.current?.getBoundingClientRect();
      // A hidden tab measures 0: keep the last offset until it shows again.
      if (!map || !panel || map.height === 0 || panel.height === 0) return;
      const chip = top.current?.getBoundingClientRect();
      const topCover = chip ? Math.max(0, chip.bottom - map.top) : 0;
      const bottomCover = Math.max(0, map.bottom - panel.top);
      setOffset(Math.max(0, Math.round((bottomCover - topCover) / 2)));
    };
    measure();
    if (typeof ResizeObserver === 'undefined') return undefined;
    const observer = new ResizeObserver(measure);
    for (const element of [canvas.current, top.current, bottom.current]) if (element) observer.observe(element);
    return () => observer.disconnect();
  }, [canvas, top, bottom, enabled, paused]);

  return offset;
}
