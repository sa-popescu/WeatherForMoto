import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useStrings } from '../../../lib/i18n';
import type { AlertPrefs, MeResponse } from '../../../lib/types';
import { PREF_DEFAULTS, useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { isSessionError } from '../lib/errors';
import { dropSent, hasChanges, mergePrefs, type PrefsChanges } from '../lib/prefsOverlay';
import { COMMON } from '../strings/common';
import { useErrorText } from './useErrorText';

// One editor for every preference control on the screen. Changes show at once
// (overlay), are saved one PUT at a time in order, and sliders can debounce.
// Each PUT carries the full set, built from the last known server copy plus
// all pending edits, so parallel edits in different sections never undo each
// other.

export interface UpdateOptions {
  /** Wait this long for more changes before saving (sliders). */
  debounceMs?: number;
  /** No success toast (background sync such as the moto profile). */
  quiet?: boolean;
  /** Success toast text instead of the generic "Saved". */
  successMessage?: string;
}

export interface PrefsEditor {
  /** Server prefs with pending edits applied; null while signed out or not loaded. */
  prefs: AlertPrefs | null;
  update: (changes: PrefsChanges, options?: UpdateOptions) => void;
  saving: boolean;
}

const PrefsEditorContext = createContext<PrefsEditor>({ prefs: null, update: () => undefined, saving: false });

/** An account without a prefs row gets the backend defaults (the first PUT creates the row). */
function serverPrefs(me: MeResponse | null): AlertPrefs | null {
  return me ? (me.prefs ?? PREF_DEFAULTS) : null;
}

export function PrefsEditorProvider({ children }: { children: ReactNode }) {
  const { me, token, savePrefs, handleAuthError } = useAuth();
  const toast = useToast();
  const s = useStrings(COMMON);
  const errorText = useErrorText();
  const [overlay, setOverlay] = useState<PrefsChanges>({});
  const [saving, setSaving] = useState(false);
  const overlayRef = useRef<PrefsChanges>({});
  const baseRef = useRef<AlertPrefs | null>(serverPrefs(me));
  const savePrefsRef = useRef(savePrefs);
  savePrefsRef.current = savePrefs;
  const timerRef = useRef<number | null>(null);
  const chainRef = useRef<Promise<void>>(Promise.resolve());
  const pendingToast = useRef<{ loud: boolean; message: string | null }>({ loud: false, message: null });

  useEffect(() => {
    baseRef.current = serverPrefs(me);
  }, [me]);

  const replaceOverlay = useCallback((next: PrefsChanges) => {
    overlayRef.current = next;
    setOverlay(next);
  }, []);

  // A different session (sign-out, other account) must not inherit edits.
  useEffect(() => {
    replaceOverlay({});
  }, [token, replaceOverlay]);

  const saveNow = useCallback(
    async (loud: boolean, message: string | null): Promise<void> => {
      const sent = { ...overlayRef.current };
      const base = baseRef.current;
      if (!base || !hasChanges(sent)) return;
      const full: AlertPrefs = { ...base, ...sent };
      setSaving(true);
      try {
        await savePrefsRef.current(full);
        baseRef.current = full;
        if (loud) toast(message ?? s.saved, { tone: 'success' });
      } catch (err) {
        console.warn('[account] saving preferences failed', err);
        toast(errorText(err, 'other'), { tone: 'error' });
        if (isSessionError(err, 'other')) handleAuthError(err);
      } finally {
        // Success: the server copy has them. Failure: show the server value again.
        replaceOverlay(dropSent(overlayRef.current, sent));
        setSaving(false);
      }
    },
    [toast, s.saved, errorText, handleAuthError, replaceOverlay],
  );

  const flush = useCallback(() => {
    if (timerRef.current !== null) window.clearTimeout(timerRef.current);
    timerRef.current = null;
    const { loud, message } = pendingToast.current;
    pendingToast.current = { loud: false, message: null };
    chainRef.current = chainRef.current.then(() => saveNow(loud, message));
  }, [saveNow]);

  const update = useCallback(
    (changes: PrefsChanges, options: UpdateOptions = {}) => {
      if (!baseRef.current) return;
      replaceOverlay({ ...overlayRef.current, ...changes });
      const pending = pendingToast.current;
      pendingToast.current = { loud: pending.loud || !options.quiet, message: options.successMessage ?? pending.message };
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
      if (options.debounceMs && options.debounceMs > 0) timerRef.current = window.setTimeout(flush, options.debounceMs);
      else flush();
    },
    [flush, replaceOverlay],
  );

  // Save a pending slider change when the app goes to the background.
  useEffect(() => {
    const onHide = (): void => {
      if (document.visibilityState === 'hidden' && timerRef.current !== null) flush();
    };
    document.addEventListener('visibilitychange', onHide);
    return () => {
      document.removeEventListener('visibilitychange', onHide);
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
    };
  }, [flush]);

  const prefs = useMemo(() => mergePrefs(serverPrefs(me), overlay), [me, overlay]);
  const value = useMemo<PrefsEditor>(() => ({ prefs, update, saving }), [prefs, update, saving]);
  return <PrefsEditorContext.Provider value={value}>{children}</PrefsEditorContext.Provider>;
}

export function usePrefsEditor(): PrefsEditor {
  return useContext(PrefsEditorContext);
}
