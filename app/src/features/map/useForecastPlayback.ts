import { useCallback, useEffect, useState } from 'react';
import { FORECAST_STEP_MS } from './forecast';

// Which forecast hour is on screen. Simpler than the radar's: the frames are
// painted locally, so there is nothing to wait for.

export interface ForecastPlayback {
  index: number;
  playing: boolean;
  select: (index: number) => void;
  toggle: () => void;
}

export function useForecastPlayback(frameCount: number, canPlay: boolean): ForecastPlayback {
  const [index, setIndex] = useState(0);
  const [playing, setPlaying] = useState(false);

  // A new grid starts again from the current hour.
  useEffect(() => {
    setIndex(0);
  }, [frameCount]);

  useEffect(() => {
    if (!playing || !canPlay || frameCount < 2) return undefined;
    const timer = window.setInterval(() => setIndex((i) => (i + 1) % frameCount), FORECAST_STEP_MS);
    return () => window.clearInterval(timer);
  }, [playing, canPlay, frameCount]);

  // Nothing to play while the map is off screen.
  useEffect(() => {
    if (!canPlay) setPlaying(false);
  }, [canPlay]);

  const select = useCallback((next: number) => {
    setPlaying(false);
    setIndex(next);
  }, []);

  const toggle = useCallback(() => setPlaying((on) => !on), []);

  return { index, playing, select, toggle };
}
