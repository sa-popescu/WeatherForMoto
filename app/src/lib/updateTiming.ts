// When the app looks for a new version of itself, and when it may load one
// without asking (lib/pwa.ts).

/** Checks while the app is on screen. */
export const CHECK_EVERY_MS = 60_000;
/** Two checks closer than this are one: coming back fires visibility, focus and pageshow together. */
export const MIN_GAP_MS = 15_000;
/** A new version found this soon after the app started, before any tap, is put on straight away. */
export const AUTO_APPLY_WINDOW_MS = 12_000;

/** True when a check is due: none yet, or the last one at least MIN_GAP_MS ago. */
export function checkDue(now: number, last: number, minGap: number = MIN_GAP_MS): boolean {
  return last === 0 || now - last >= minGap;
}

/** True when a new version may replace the page with no question: just opened, nothing touched yet. */
export function mayApplyUnasked(now: number, opened: number, userTouched: boolean, windowMs: number = AUTO_APPLY_WINDOW_MS): boolean {
  return !userTouched && opened > 0 && now - opened <= windowMs;
}
