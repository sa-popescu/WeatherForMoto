import { useCallback, useEffect, useState } from 'react';
import { listFinished, type RideListItem } from './store';
import { useRide } from './useRide';

export interface RideHistory {
  items: readonly RideListItem[];
  status: 'loading' | 'ready' | 'error';
  reload: () => void;
}

/** The finished rides on this phone, read again whenever the ride screen closes. */
export function useRideHistory(): RideHistory {
  const [items, setItems] = useState<readonly RideListItem[]>([]);
  const [status, setStatus] = useState<RideHistory['status']>('loading');
  const [version, setVersion] = useState(0);
  const idle = useRide().phase === 'idle';

  useEffect(() => {
    if (!idle) return undefined;
    let current = true;
    listFinished()
      .then((list) => {
        if (!current) return;
        setItems(list);
        setStatus('ready');
      })
      .catch((err: unknown) => {
        if (!current) return;
        console.warn('[ride] could not list the rides on this phone', err);
        setStatus('error');
      });
    return () => {
      current = false;
    };
  }, [idle, version]);

  const reload = useCallback(() => setVersion((v) => v + 1), []);
  return { items, status, reload };
}
