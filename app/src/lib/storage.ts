// Failure-tolerant localStorage access. Private mode, some WebViews and full
// quotas throw; the app must keep working without persistence.
// The key names are the legacy app's keys, so existing users keep their
// session, favourites and settings when they get the new interface.

export const KEYS = {
  token: 'mmAuthTokenV1',
  userEmail: 'mmUserEmailV1',
  userName: 'mmUserNameV1',
  theme: 'mmThemeV1',
  lang: 'moto_lang',
  profile: 'motoProfile',
  alertStates: 'motoAlerts',
  favorites: 'motoFavs',
  lastPlace: 'motoLastLoc',
  installDismissed: 'pwaInstallDismissed',
  gearChecked: 'mmGearCheckedV1',
} as const;

export type StorageKey = (typeof KEYS)[keyof typeof KEYS];

export function readString(key: StorageKey): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

export function writeString(key: StorageKey, value: string | null): void {
  try {
    if (value === null) localStorage.removeItem(key);
    else localStorage.setItem(key, value);
  } catch (err) {
    console.warn(`[storage] could not write ${key}`, err);
  }
}

export function readJson<T>(key: StorageKey, fallback: T, isValid?: (value: unknown) => value is T): T {
  const raw = readString(key);
  if (raw === null) return fallback;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (isValid && !isValid(parsed)) return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
}

export function writeJson(key: StorageKey, value: unknown): void {
  writeString(key, JSON.stringify(value));
}
