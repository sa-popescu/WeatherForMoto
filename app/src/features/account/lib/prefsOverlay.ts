import type { AlertPrefs } from '../../../lib/types';

// Local edits waiting to be (or being) saved are kept as an overlay on top of
// the server copy, so the UI answers instantly and a second quick change is
// never lost while the first PUT is still in flight.

export type PrefsChanges = Partial<AlertPrefs>;

export function mergePrefs(server: AlertPrefs | null | undefined, overlay: PrefsChanges): AlertPrefs | null {
  return server ? { ...server, ...overlay } : null;
}

/**
 * Removes the keys that were just sent, unless the user changed them again
 * meanwhile (then the newer value must survive for the next save).
 * Used after success (the server copy now has them) and after failure (the
 * display falls back to the server value). Returns the same object when
 * nothing changed, so React can skip a render.
 */
export function dropSent(overlay: PrefsChanges, sent: PrefsChanges): PrefsChanges {
  const next: Record<string, unknown> = { ...overlay };
  let changed = false;
  for (const key of Object.keys(sent) as Array<keyof AlertPrefs>) {
    if (key in next && next[key] === sent[key]) {
      delete next[key];
      changed = true;
    }
  }
  return changed ? (next as PrefsChanges) : overlay;
}

export function hasChanges(overlay: PrefsChanges): boolean {
  return Object.keys(overlay).length > 0;
}
