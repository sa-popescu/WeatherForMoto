import { useCallback, useRef, type KeyboardEvent, type PointerEvent } from 'react';

// Scrubbing across the timeline bars: tap or drag with a finger / mouse
// (touch-action: pan-y keeps vertical page scrolling), arrows when focused.

const PAGE_STEP = 3;

export function useScrub(count: number, selected: number, onSelect: (index: number) => void, onActivate: (index: number) => void) {
  const ref = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);

  const indexAt = useCallback(
    (clientX: number): number => {
      const box = ref.current?.getBoundingClientRect();
      if (!box || box.width === 0) return selected;
      const raw = Math.floor(((clientX - box.left) / box.width) * count);
      return Math.max(0, Math.min(count - 1, raw));
    },
    [count, selected],
  );

  const onPointerDown = (e: PointerEvent<HTMLDivElement>): void => {
    dragging.current = true;
    e.currentTarget.setPointerCapture?.(e.pointerId);
    onSelect(indexAt(e.clientX));
  };

  const onPointerMove = (e: PointerEvent<HTMLDivElement>): void => {
    if (dragging.current) onSelect(indexAt(e.clientX));
  };

  const stop = (): void => {
    dragging.current = false;
  };

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>): void => {
    const steps: Record<string, number> = { ArrowLeft: -1, ArrowDown: -1, ArrowRight: 1, ArrowUp: 1, PageDown: -PAGE_STEP, PageUp: PAGE_STEP };
    let next: number | null = null;
    if (e.key in steps) next = selected + steps[e.key];
    else if (e.key === 'Home') next = 0;
    else if (e.key === 'End') next = count - 1;
    else if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onActivate(selected);
      return;
    }
    if (next === null) return;
    e.preventDefault();
    onSelect(Math.max(0, Math.min(count - 1, next)));
  };

  return { ref, handlers: { onPointerDown, onPointerMove, onPointerUp: stop, onPointerCancel: stop, onKeyDown } };
}
