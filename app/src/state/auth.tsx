import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { api, ApiError } from '../lib/api';
import { disablePush } from '../lib/push';
import { KEYS, readString, writeString } from '../lib/storage';
import type { AlertPrefs, AuthResponse, MeResponse } from '../lib/types';

// Session and account state. The token is a bearer token kept in
// localStorage under the legacy key, so existing users stay signed in.

export const PREF_DEFAULTS: AlertPrefs = {
  enabled: true,
  email_alerts_enabled: true,
  email_alert_wind: true,
  email_alert_rain: true,
  email_alert_score: true,
  email_alert_temp_low: true,
  email_alert_temp_high: true,
  email_alert_frost: true,
  min_score: 45,
  max_wind_gust: 50,
  min_temp: null,
  max_temp: null,
  frost_risk_enabled: true,
  quiet_hours_enabled: false,
  quiet_start_hour: 22,
  quiet_end_hour: 7,
  severity: 'medium',
  home_lat: null,
  home_lon: null,
  city: null,
  alert_states: null,
  moto_type: 'naked',
  comfort_temp: 20,
  wind_tolerance: 'medium',
  rain_tolerance: 'medium',
};

/** The database returns 0/1 for booleans and may carry retired fields; keep only known ones. */
export function normalizePrefs(raw: unknown): AlertPrefs {
  const source = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const out: Record<string, unknown> = { ...PREF_DEFAULTS };
  for (const key of Object.keys(PREF_DEFAULTS)) {
    const value = source[key];
    if (value === undefined) continue;
    const fallback = (PREF_DEFAULTS as unknown as Record<string, unknown>)[key];
    out[key] = typeof fallback === 'boolean' ? Boolean(value) : value;
  }
  return out as unknown as AlertPrefs;
}

export type AuthStatus = 'anonymous' | 'loading' | 'signed-in';

interface AuthState {
  token: string | null;
  me: MeResponse | null;
  status: AuthStatus;
  /** Set when the account could not be loaded (offline, server error). */
  loadError: boolean;
  signup: (email: string, password: string, displayName?: string) => Promise<AuthResponse>;
  login: (email: string, password: string) => Promise<void>;
  requestCode: (email: string) => Promise<void>;
  verifyCode: (email: string, code: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshMe: () => Promise<MeResponse | null>;
  /** Merges changes into the stored preferences and saves the full set (PUT replaces all). */
  savePrefs: (changes: Partial<AlertPrefs>) => Promise<void>;
  deleteAccount: () => Promise<void>;
  /** Call from any feature that gets an ApiError: signs out on 401/403 only. */
  handleAuthError: (err: unknown) => void;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => readString(KEYS.token));
  const [me, setMe] = useState<MeResponse | null>(null);
  const [status, setStatus] = useState<AuthStatus>(() => (readString(KEYS.token) ? 'loading' : 'anonymous'));
  const [loadError, setLoadError] = useState(false);
  const tokenRef = useRef(token);
  tokenRef.current = token;

  const applySession = useCallback((nextToken: string | null, email?: string | null) => {
    writeString(KEYS.token, nextToken);
    writeString(KEYS.userEmail, nextToken ? email ?? null : null);
    setToken(nextToken);
    if (!nextToken) {
      setMe(null);
      setStatus('anonymous');
      setLoadError(false);
    } else {
      setStatus('loading');
    }
  }, []);

  const refreshMe = useCallback(async (): Promise<MeResponse | null> => {
    const current = tokenRef.current;
    if (!current) return null;
    try {
      const data = await api.me(current);
      const normalized: MeResponse = { ...data, prefs: data.prefs ? normalizePrefs(data.prefs) : null };
      setMe(normalized);
      setStatus('signed-in');
      setLoadError(false);
      return normalized;
    } catch (err) {
      if (err instanceof ApiError && err.isAuth) {
        applySession(null);
      } else {
        // Offline or server trouble: keep the session, show a retry.
        console.warn('[auth] could not load the account', err);
        setStatus('signed-in');
        setLoadError(true);
      }
      return null;
    }
  }, [applySession]);

  useEffect(() => {
    if (token) void refreshMe();
  }, [token, refreshMe]);

  const signup = useCallback(
    async (email: string, password: string, displayName?: string) => {
      const res = await api.signup(email, password, displayName);
      applySession(res.token, res.user?.email ?? email);
      return res;
    },
    [applySession],
  );

  const login = useCallback(
    async (email: string, password: string) => {
      const res = await api.login(email, password);
      applySession(res.token, res.user?.email ?? email);
    },
    [applySession],
  );

  const requestCode = useCallback(async (email: string) => {
    await api.requestCode(email);
  }, []);

  const verifyCode = useCallback(
    async (email: string, code: string) => {
      const res = await api.verifyCode(email, code);
      applySession(res.token, res.user?.email ?? email);
    },
    [applySession],
  );

  const logout = useCallback(async () => {
    const current = tokenRef.current;
    // Stop this device's push alerts first, so a shared phone does not keep
    // receiving the previous user's notifications.
    await disablePush(current).catch((err: unknown) => console.warn('[auth] push cleanup failed', err));
    if (current) {
      try {
        await api.logout(current);
      } catch (err) {
        console.warn('[auth] server logout failed; clearing the local session anyway', err);
      }
    }
    applySession(null);
  }, [applySession]);

  const savePrefs = useCallback(
    async (changes: Partial<AlertPrefs>) => {
      const current = tokenRef.current;
      if (!current) throw new ApiError(401, 'not signed in');
      const payload: AlertPrefs = { ...(me?.prefs ?? PREF_DEFAULTS), ...changes };
      await api.updatePrefs(current, payload);
      await refreshMe();
    },
    [me, refreshMe],
  );

  const deleteAccount = useCallback(async () => {
    const current = tokenRef.current;
    if (!current) return;
    await disablePush(current).catch((err: unknown) => console.warn('[auth] push cleanup failed', err));
    await api.deleteAccount(current);
    applySession(null);
  }, [applySession]);

  const handleAuthError = useCallback(
    (err: unknown) => {
      if (err instanceof ApiError && err.isAuth) applySession(null);
    },
    [applySession],
  );

  const value = useMemo<AuthState>(
    () => ({ token, me, status, loadError, signup, login, requestCode, verifyCode, logout, refreshMe, savePrefs, deleteAccount, handleAuthError }),
    [token, me, status, loadError, signup, login, requestCode, verifyCode, logout, refreshMe, savePrefs, deleteAccount, handleAuthError],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside AuthProvider');
  return ctx;
}
