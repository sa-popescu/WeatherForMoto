import { API_BASE, DEFAULT_TIMEOUT_MS, FORECAST_DAYS, WEATHER_TIMEOUT_MS } from './config';
import type { ScoringMeta } from './scoring';
import type {
  AlertPrefs,
  AuthResponse,
  CheckNowResponse,
  GeocodeResult,
  Hazard,
  MeResponse,
  Place,
  RideStats,
  SavedRoute,
  WeatherResponse,
} from './types';

/** status 0 means the request never got an HTTP answer (offline, DNS, timeout). */
export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
  }

  get isAuth(): boolean {
    return this.status === 401 || this.status === 403;
  }

  get isNetwork(): boolean {
    return this.status === 0;
  }
}

export function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === 'AbortError';
}

type Query = Record<string, string | number | boolean | null | undefined>;

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
  body?: unknown;
  token?: string | null;
  query?: Query;
  timeoutMs?: number;
  signal?: AbortSignal;
}

function buildUrl(path: string, query?: Query): string {
  const url = new URL(API_BASE + path, window.location.origin);
  for (const [key, value] of Object.entries(query ?? {})) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  }
  return url.toString();
}

/** FastAPI errors are {detail: string} or {detail: [{msg}]} for validation. */
function detailFrom(body: unknown, status: number): string {
  if (body && typeof body === 'object' && 'detail' in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      const msgs = detail.map((d) => (d && typeof d === 'object' && 'msg' in d ? String((d as { msg: unknown }).msg) : '')).filter(Boolean);
      if (msgs.length) return msgs.join(' ');
    }
  }
  return `HTTP ${status}`;
}

async function send(path: string, opts: RequestOptions = {}): Promise<Response> {
  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  let timedOut = false;
  const timer = window.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  const forwardAbort = (): void => controller.abort();
  opts.signal?.addEventListener('abort', forwardAbort, { once: true });

  const headers: Record<string, string> = { Accept: 'application/json' };
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  if (opts.token) headers.Authorization = `Bearer ${opts.token}`;

  let res: Response;
  try {
    res = await fetch(buildUrl(path, opts.query), {
      method: opts.method ?? 'GET',
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      signal: controller.signal,
    });
  } catch (err) {
    if (opts.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    throw new ApiError(0, timedOut ? 'timeout' : err instanceof Error ? err.message : 'network');
  } finally {
    window.clearTimeout(timer);
    opts.signal?.removeEventListener('abort', forwardAbort);
  }

  if (!res.ok) {
    let body: unknown = null;
    try {
      body = await res.json();
    } catch {
      // Non-JSON error body (proxy, HTML error page): the status is enough.
    }
    throw new ApiError(res.status, detailFrom(body, res.status));
  }
  return res;
}

async function json<T>(path: string, opts?: RequestOptions): Promise<T> {
  const res = await send(path, opts);
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export interface WeatherResult {
  data: WeatherResponse;
  /** true when the service worker answered from its offline cache. */
  offline: boolean;
  offlineAgeMin: number | null;
}

export interface PushSubscriptionJson {
  endpoint: string;
  keys: { p256dh: string; auth: string };
}

export interface HazardInput {
  lat: number;
  lon: number;
  hazard_type: string;
  severity: number;
  description: string;
  ttl_hours?: number;
}

export interface RideInput {
  route_name?: string | null;
  start_city: string;
  end_city: string;
  distance_km: number;
  duration_min: number;
  avg_moto_score?: number | null;
  max_wind_gust?: number | null;
  max_precip?: number | null;
}

export const api = {
  async weather(place: Place, signal?: AbortSignal, days: number = FORECAST_DAYS): Promise<WeatherResult> {
    const res = await send('/weather', {
      query: { lat: place.lat.toFixed(4), lon: place.lon.toFixed(4), city: place.name, days },
      timeoutMs: WEATHER_TIMEOUT_MS,
      signal,
    });
    const offline = res.headers.get('x-sw-offline') === '1';
    const age = Number.parseInt(res.headers.get('x-sw-age-min') ?? '', 10);
    return { data: (await res.json()) as WeatherResponse, offline, offlineAgeMin: Number.isFinite(age) ? age : null };
  },

  metaScoring: (): Promise<ScoringMeta> => json('/meta/scoring'),
  geocode: (city: string, signal?: AbortSignal): Promise<GeocodeResult> => json('/geocode', { query: { city }, signal }),

  hazards: async (lat: number, lon: number, radiusKm = 80, signal?: AbortSignal): Promise<Hazard[]> =>
    (await json<{ hazards: Hazard[] }>('/hazards', { query: { lat, lon, radius_km: radiusKm }, signal })).hazards,
  reportHazard: (token: string, payload: HazardInput): Promise<{ ok: boolean; hazard_id: number }> =>
    json('/hazards', { method: 'POST', body: payload, token }),

  signup: (email: string, password: string, displayName?: string): Promise<AuthResponse> =>
    json('/auth/signup', { method: 'POST', body: { email, password, display_name: displayName || null } }),
  login: (email: string, password: string): Promise<AuthResponse> =>
    json('/auth/login', { method: 'POST', body: { email, password } }),
  requestCode: (email: string): Promise<unknown> => json('/auth/request-code', { method: 'POST', body: { email } }),
  verifyCode: (email: string, code: string): Promise<AuthResponse> =>
    json('/auth/verify-code', { method: 'POST', body: { email, code } }),
  logout: (token: string): Promise<unknown> => json('/auth/logout', { method: 'POST', token }),
  requestReset: (email: string): Promise<unknown> => json('/auth/request-reset', { method: 'POST', body: { email } }),
  resetPassword: (resetToken: string, newPassword: string): Promise<unknown> =>
    json('/auth/reset-password', { method: 'POST', body: { token: resetToken, new_password: newPassword } }),
  changePassword: (token: string, currentPassword: string, newPassword: string): Promise<unknown> =>
    json('/auth/change-password', { method: 'POST', token, body: { current_password: currentPassword, new_password: newPassword } }),

  me: (token: string): Promise<MeResponse> => json('/me', { token }),
  updatePrefs: (token: string, prefs: AlertPrefs): Promise<unknown> => json('/me/prefs', { method: 'PUT', token, body: prefs }),
  updateProfile: (token: string, displayName: string): Promise<unknown> =>
    json('/me/profile', { method: 'PUT', token, body: { display_name: displayName } }),
  changeEmail: (token: string, newEmail: string, password: string): Promise<unknown> =>
    json('/me/email', { method: 'PUT', token, body: { new_email: newEmail, password } }),
  resendVerification: (token: string): Promise<unknown> => json('/me/resend-verification', { method: 'POST', token }),
  deleteAccount: (token: string): Promise<unknown> => json('/me', { method: 'DELETE', token }),

  pushPublicKey: (): Promise<Record<string, string>> => json('/push/public-key'),
  subscribePush: (token: string, sub: PushSubscriptionJson): Promise<unknown> =>
    json('/me/push-subscriptions', { method: 'POST', token, body: sub }),
  unsubscribePush: (token: string, endpoint: string): Promise<unknown> =>
    json('/me/push-subscriptions', { method: 'DELETE', token, query: { endpoint } }),
  checkNow: (token: string): Promise<CheckNowResponse> =>
    json('/alerts/check-now', { method: 'POST', token, timeoutMs: WEATHER_TIMEOUT_MS }),

  routes: async (token: string): Promise<SavedRoute[]> => (await json<{ routes: SavedRoute[] }>('/me/routes', { token })).routes,
  saveRoute: (token: string, route: { name: string; stops: string[]; total_distance_km?: number | null }): Promise<{ ok: boolean; route_id: number }> =>
    json('/me/routes', { method: 'POST', token, body: route }),
  deleteRoute: (token: string, id: number): Promise<unknown> => json(`/me/routes/${id}`, { method: 'DELETE', token }),
  logRide: (token: string, ride: RideInput): Promise<{ ok: boolean; ride_id: number }> =>
    json('/me/rides/log', { method: 'POST', token, body: ride }),
  rideStats: (token: string): Promise<RideStats> => json('/me/rides/stats', { token }),
};
