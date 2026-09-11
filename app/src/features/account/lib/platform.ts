// Platform checks for push and install hints.

export function isIosDevice(userAgent: string, platform: string, maxTouchPoints: number): boolean {
  // iPadOS 13+ reports itself as a Mac; the touch points give it away.
  return /iphone|ipad|ipod/i.test(userAgent) || (platform === 'MacIntel' && maxTouchPoints > 1);
}

export function currentIsIos(): boolean {
  return isIosDevice(navigator.userAgent, navigator.platform, navigator.maxTouchPoints ?? 0);
}

export function isStandalone(): boolean {
  const nav = navigator as Navigator & { standalone?: boolean };
  return nav.standalone === true || window.matchMedia?.('(display-mode: standalone)').matches === true;
}

export type NotificationState = NotificationPermission | 'unsupported';

export function notificationPermission(): NotificationState {
  return 'Notification' in window ? Notification.permission : 'unsupported';
}

export type PushAvailability = 'ok' | 'ios-install' | 'unsupported' | 'denied';

export interface PushEnvironment {
  supported: boolean;
  ios: boolean;
  standalone: boolean;
  permission: NotificationState;
}

/** iOS only delivers web push to an installed (home screen) app, so that case gets its own hint. */
export function pushAvailability(env: PushEnvironment): PushAvailability {
  if (env.ios && !env.standalone) return 'ios-install';
  if (!env.supported) return 'unsupported';
  if (env.permission === 'denied') return 'denied';
  return 'ok';
}

/** Rejects after ms, so a promise that can never settle (no service worker) does not spin forever. */
export function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => reject(new Error('timeout')), ms);
    promise.then(
      (value) => {
        window.clearTimeout(timer);
        resolve(value);
      },
      (err: unknown) => {
        window.clearTimeout(timer);
        reject(err);
      },
    );
  });
}
