// PermissionState is the browser's own type; 'unsupported' is ours, for a
// browser with no Geolocation at all.

/**
 * When to ask the device where you are.
 *
 * The app used to decide this from navigator.permissions alone: locate at
 * launch only when the Permissions API said "granted". Safari does not answer
 * that query for geolocation, so the answer was always "prompt", the app never
 * located itself, and it opened on yesterday's city until you tapped the icon
 * (and were asked for permission all over again).
 *
 * So the decision follows YOUR choice, kept in settings, and the Permissions
 * API is only used to avoid pointless prompts where it does answer.
 */

/** A fix older than this is not "where you are" any more. */
export const FIX_STALE_MS = 15 * 60_000;

export function shouldLocateOnBoot(followMe: boolean, permission: PermissionState | 'unsupported'): boolean {
  if (permission === 'denied' || permission === 'unsupported') return false;
  // Already granted: locating is silent, so it is worth doing either way.
  if (permission === 'granted') return true;
  // The browser would ask. Only someone who chose to start on their own
  // location gets that prompt, and only because they asked for it.
  return followMe;
}

/**
 * Whether coming back to the app should refresh the fix.
 *
 * Coming back must never put the permission prompt on screen: the rider was
 * asked at launch, or tapped the pin, and that is the only asking there
 * should be. So this refreshes only when it is known to be silent: the
 * browser says "granted", or a fix already landed in this run of the app,
 * which means the answer was yes and browsers keep it while the page lives
 * (the only memory an iPhone web app has).
 */
export function shouldRefreshOnResume(
  followMe: boolean,
  permission: PermissionState | 'unsupported',
  fixAtMs: number | null,
  nowMs: number,
): boolean {
  if (!followMe || permission === 'denied' || permission === 'unsupported') return false;
  // No fix yet in this run: the prompt was refused, ignored or never shown.
  if (fixAtMs === null) return permission === 'granted';
  return nowMs - fixAtMs > FIX_STALE_MS;
}

/** How long a rider may take to answer the permission prompt before the app stops waiting. */
export const PROMPT_WAIT_MS = 60_000;

/**
 * The app's own limit for one position request.
 *
 * The browser's timeout only starts once the permission is settled, so it
 * covers a slow GPS but not an unanswered prompt. The app's limit covers the
 * prompt, and it has to be patient there: with the short limit, a rider who
 * read the prompt and tapped "Allow" a few seconds later had the fix thrown
 * away, saw nothing happen, and was asked again.
 */
export function locateWaitMs(permission: PermissionState | 'unsupported', gpsTimeoutMs: number): number {
  return permission === 'granted' ? gpsTimeoutMs : Math.max(gpsTimeoutMs, PROMPT_WAIT_MS);
}
