import { useCallback, useState } from 'react';

// Layer choices for this session only. Kept in module memory, so they survive
// tab switches and remounts; deliberately not persisted, so a fresh start
// always shows both layers.

/** Which forecast field is painted over the map, if any. */
export type ForecastLayer = 'off' | 'cloud' | 'rain';

export interface LayerPrefs {
  radar: boolean;
  hazards: boolean;
  forecast: ForecastLayer;
  /** Radar and forecast opacity, 0.2 to 1. */
  opacity: number;
}

let sessionPrefs: LayerPrefs = { radar: true, hazards: true, forecast: 'off', opacity: 0.7 };

export function useLayerPrefs(): [LayerPrefs, (change: Partial<LayerPrefs>) => void] {
  const [prefs, setPrefs] = useState<LayerPrefs>(sessionPrefs);
  const update = useCallback((change: Partial<LayerPrefs>) => {
    setPrefs((current) => {
      sessionPrefs = { ...current, ...change };
      return sessionPrefs;
    });
  }, []);
  return [prefs, update];
}
