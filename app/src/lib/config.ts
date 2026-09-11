// Runtime configuration. Everything tunable lives here so screens never hardcode it.

const LIVE_API = 'https://weatherformoto-1056457771445.europe-west1.run.app';

/** Base URL of the API, without a trailing slash. */
export const API_BASE: string = (import.meta.env.VITE_API_BASE ?? LIVE_API).replace(/\/$/, '');

/** The backend answers within ~8 s by design; give the network a little on top. */
export const WEATHER_TIMEOUT_MS = 12_000;
export const DEFAULT_TIMEOUT_MS = 10_000;

export const FORECAST_DAYS = 14;
export const AUTO_REFRESH_MS = 30 * 60_000;
/** On returning to the app, refresh only if the data is older than this. */
export const STALE_ON_FOCUS_MS = 5 * 60_000;

export const GPS_TIMEOUT_MS = 6_000;
export const MAX_FAVORITES = 8;

export const DEFAULT_PLACE = { name: 'București', lat: 44.4268, lon: 26.1025 } as const;

export const PRIVACY_POLICY_URL = '/privacy-policy.html';
