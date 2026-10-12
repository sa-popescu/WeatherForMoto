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

/** Whether coming back to the app should refresh the fix. */
export function shouldRefreshOnResume(
  followMe: boolean,
  permission: PermissionState | 'unsupported',
  fixAtMs: number | null,
  nowMs: number,
): boolean {
  if (!followMe || permission === 'denied' || permission === 'unsupported') return false;
  if (fixAtMs === null) return true;
  return nowMs - fixAtMs > FIX_STALE_MS;
}
