import { useCallback, useEffect, useState } from 'react';
import { currentPushSubscription, disablePush, enablePush, pushSupported, type EnablePushResult } from '../../../lib/push';
import { useAuth } from '../../../state/auth';
import { currentIsIos, isStandalone, notificationPermission, pushAvailability, withTimeout, type PushAvailability } from '../lib/platform';

// Push state of THIS device. Without an active service worker
// navigator.serviceWorker.ready never settles, hence the timeout.
const ENABLE_TIMEOUT_MS = 20_000;

export interface PushState {
  availability: PushAvailability;
  on: boolean;
  busy: boolean;
  enable: () => Promise<EnablePushResult>;
  disable: () => Promise<void>;
}

export function usePushState(active: boolean): PushState {
  const { token, refreshMe } = useAuth();
  const [on, setOn] = useState(false);
  const [busy, setBusy] = useState(false);
  const [permission, setPermission] = useState(notificationPermission);

  const availability = pushAvailability({
    supported: pushSupported(),
    ios: currentIsIos(),
    standalone: isStandalone(),
    permission,
  });

  // Re-read on every visit: the user may have changed browser settings meanwhile.
  useEffect(() => {
    if (!active) return undefined;
    let cancelled = false;
    setPermission(notificationPermission());
    currentPushSubscription()
      .then((sub) => {
        if (!cancelled) setOn(Boolean(sub));
      })
      .catch((err: unknown) => console.warn('[account] could not read the push subscription', err));
    return () => {
      cancelled = true;
    };
  }, [active]);

  const enable = useCallback(async (): Promise<EnablePushResult> => {
    if (!token) return 'unsupported';
    setBusy(true);
    try {
      const result = await withTimeout(enablePush(token), ENABLE_TIMEOUT_MS);
      setPermission(notificationPermission());
      if (result === 'enabled') {
        setOn(true);
        await refreshMe();
      }
      return result;
    } finally {
      setBusy(false);
    }
  }, [token, refreshMe]);

  const disable = useCallback(async (): Promise<void> => {
    setBusy(true);
    try {
      await disablePush(token);
      setOn(false);
      await refreshMe();
    } finally {
      setBusy(false);
    }
  }, [token, refreshMe]);

  return { availability, on, busy, enable, disable };
}
