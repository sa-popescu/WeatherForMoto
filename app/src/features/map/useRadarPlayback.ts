import { useCallback, useEffect, useRef, useState } from 'react';
import { closestFrameIndex, FRAME_STEP_MS, frameDelayMs, nextFrameIndex, startFrameIndex, type RadarFrame } from './radar';

// Which radar frame is on screen and whether it animates. Playback waits for
// the next frame's tiles (preloaded by useRadarLayer) so it never flashes an
// empty map, and only runs while the map is on screen.

/** Checks to wait for a slow frame before moving on anyway (about 3 s). */
const MAX_WAITS = 6;

export interface RadarPlayback {
  index: number;
  playing: boolean;
  select: (index: number) => void;
  toggle: () => void;
}

export function useRadarPlayback(
  frames: readonly RadarFrame[],
  canPlay: boolean,
  autoPlay: boolean,
  isReady: (index: number) => boolean,
): RadarPlayback {
  const [index, setIndex] = useState(-1);
  const [playing, setPlaying] = useState(false);
  const [waitTick, setWaitTick] = useState(0);
  const shownTime = useRef<number | null>(null);
  const atNow = useRef(true);
  const holdIndex = useRef(-1);
  const waits = useRef(0);
  const autoStarted = useRef(false);

  // New frame list: stay on the same moment if the user left it there, otherwise show "now".
  useEffect(() => {
    if (frames.length === 0) {
      setIndex(-1);
      return;
    }
    const start = startFrameIndex(frames, Date.now() / 1000);
    holdIndex.current = start;
    setIndex(shownTime.current === null || atNow.current ? start : closestFrameIndex(frames, shownTime.current));
    if (autoPlay && !autoStarted.current) {
      autoStarted.current = true;
      setPlaying(true);
    }
  }, [frames, autoPlay]);

  useEffect(() => {
    shownTime.current = index >= 0 && index < frames.length ? frames[index].time : null;
    atNow.current = index === holdIndex.current;
  }, [frames, index]);

  useEffect(() => {
    if (!playing || !canPlay || frames.length < 2 || index < 0) return undefined;
    const delay = waits.current > 0 ? FRAME_STEP_MS : frameDelayMs(index, frames.length, holdIndex.current);
    const timer = window.setTimeout(() => {
      const next = nextFrameIndex(index, frames.length);
      if (isReady(next) || waits.current >= MAX_WAITS) {
        waits.current = 0;
        setIndex(next);
      } else {
        waits.current += 1;
        setWaitTick((n) => n + 1);
      }
    }, delay);
    return () => window.clearTimeout(timer);
  }, [playing, canPlay, frames, index, isReady, waitTick]);

  const select = useCallback(
    (next: number) => {
      setPlaying(false);
      setIndex(Math.max(0, Math.min(frames.length - 1, next)));
    },
    [frames.length],
  );

  const toggle = useCallback(() => setPlaying((p) => !p), []);

  return { index, playing, select, toggle };
}
