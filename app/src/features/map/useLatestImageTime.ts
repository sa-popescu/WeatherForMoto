import { useEffect, useState } from 'react';
import { capabilitiesUrl, latestFromCapabilities, type SatelliteProduct } from './eumetsat';

// The newest image a satellite product has, read from its small capabilities
// document and checked again every few minutes while the map is on screen.

const REFRESH_MS = 5 * 60_000;
const TIMEOUT_MS = 12_000;

export function useLatestImageTime(product: SatelliteProduct, enabled: boolean): number | null {
  const [latest, setLatest] = useState<number | null>(null);

  useEffect(() => {
    if (!enabled) return undefined;
    let disposed = false;
    let controller: AbortController | undefined;

    const load = async (): Promise<void> => {
      controller = new AbortController();
      const timer = window.setTimeout(() => controller?.abort(), TIMEOUT_MS);
      try {
        const res = await fetch(capabilitiesUrl(product), { signal: controller.signal });
        if (!res.ok) throw new Error(`EUMETSAT HTTP ${res.status}`);
        const time = latestFromCapabilities(await res.text());
        if (time === null) throw new Error('no time in the capabilities');
        if (!disposed) setLatest(time);
      } catch (err) {
        // A failed refresh keeps the time already known.
        if (!disposed) console.warn(`[map] newest ${product.layer} image unknown`, err);
      } finally {
        window.clearTimeout(timer);
      }
    };

    void load();
    const interval = window.setInterval(() => void load(), REFRESH_MS);
    return () => {
      disposed = true;
      window.clearInterval(interval);
      controller?.abort();
    };
  }, [product, enabled]);

  return enabled ? latest : null;
}
