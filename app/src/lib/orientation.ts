/* The manifest no longer pins the installed app to portrait, so a tablet can
   turn to landscape and get the desktop layout. A phone keeps portrait where
   the browser lets an installed app lock it (Chrome on Android). Elsewhere the
   lock is refused or missing and the screen simply follows the device; iOS
   never honoured the manifest lock in the first place. */

/** Below this shorter side (CSS px) the device is a phone, as Android's sw600dp. */
export const TABLET_MIN_SIDE = 600;

type Lockable = { lock?: (orientation: 'portrait') => Promise<void> };

export function keepPhonesUpright(): void {
  if (typeof window === 'undefined' || typeof screen === 'undefined') return;
  const orientation = screen.orientation as (ScreenOrientation & Lockable) | undefined;
  if (typeof orientation?.lock !== 'function') return;
  if (!window.matchMedia?.('(display-mode: standalone)').matches) return;
  if (Math.min(screen.width, screen.height) >= TABLET_MIN_SIDE) return;
  orientation.lock('portrait').catch(() => {
    // Not allowed in this browser or mode: turning the screen is fine too.
  });
}
