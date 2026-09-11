// Shapes of the MotoMeteo API (backend/main.py + backend/auth_alerts.py).
// Numeric fields are nullable because providers can be missing.

export type RainBand = 'none' | 'urme' | 'slaba' | 'moderata' | 'puternica';
export type MotoLabel = 'IDEAL' | 'OK' | 'ATENȚIE' | 'EVITĂ';
export type Level = 'low' | 'medium' | 'high';
export type MotoType = 'naked' | 'touring' | 'enduro' | 'sport' | 'scooter';

export interface Place {
  name: string;
  lat: number;
  lon: number;
}

export interface ScoreFactor {
  factor: string;
  penalty: number;
  cap: number | null;
  detail: string;
}

export interface GearItem {
  category: string;
  item: string;
  reason: string;
  urgency: string;
  icon: string;
}

export interface CurrentWeather {
  temperature: number | null;
  feels_like: number | null;
  humidity: number | null;
  wind_speed_kmh: number | null;
  wind_gusts_kmh: number | null;
  wind_direction_deg: number | null;
  wind_direction: string | null;
  beaufort: number | null;
  precipitation_mm: number | null;
  precipitation_probability: number | null;
  rain_intensity: RainBand | null;
  weather_code: number | null;
  description: string | null;
  icon: string | null;
  pressure_hpa: number | null;
  visibility_km: number | null;
  aqi: number | null;
  pm10: number | null;
  pm2_5: number | null;
  ozone: number | null;
  eu_aqi: number | null;
  us_aqi: number | null;
  pollen_index: number | null;
  uv_index: number | null;
  is_day: boolean | null;
  dew_point: number | null;
  frost_risk: boolean | null;
  moto_score: number | null;
  moto_label: MotoLabel | null;
  score_breakdown: ScoreFactor[] | null;
  gear_recommendation: GearItem[] | null;
  road_surface_temp: number | null;
  sources: string[] | null;
}

export interface HourlyWeather {
  /** Local time of the location, "YYYY-MM-DDTHH:MM". */
  time: string;
  temperature: number | null;
  feels_like: number | null;
  precipitation_mm: number | null;
  precipitation_probability: number | null;
  rain_intensity: RainBand | null;
  wind_speed_kmh: number | null;
  wind_gusts_kmh: number | null;
  weather_code: number | null;
  icon: string | null;
  description: string | null;
  uv_index: number | null;
  relative_humidity: number | null;
  surface_pressure: number | null;
  pressure_msl: number | null;
  dew_point_2m: number | null;
  cloud_cover: number | null;
  /** Metres. */
  visibility: number | null;
  wind_direction_10m: number | null;
  is_day: boolean | null;
  road_surface_temp: number | null;
  frost_risk: boolean | null;
  moto_score: number | null;
  moto_label: MotoLabel | null;
}

export interface DailyWeather {
  /** "YYYY-MM-DD" */
  date: string;
  weather_code: number | null;
  icon: string | null;
  description: string | null;
  temp_max: number | null;
  temp_min: number | null;
  feels_max: number | null;
  feels_min: number | null;
  precipitation_mm: number | null;
  precipitation_probability: number | null;
  precipitation_max_mm_h: number | null;
  rain_intensity_max: RainBand | null;
  wind_max_kmh: number | null;
  wind_gusts_kmh: number | null;
  moto_score: number | null;
  moto_label: MotoLabel | null;
  /** Local "YYYY-MM-DDTHH:MM" */
  sunrise: string | null;
  sunset: string | null;
}

export interface WeatherResponse {
  city: string;
  latitude: number;
  longitude: number;
  timezone: string;
  utc_offset_seconds: number;
  current: CurrentWeather;
  hourly: HourlyWeather[];
  daily: DailyWeather[];
}

export interface GeocodeResult {
  lat: number;
  lon: number;
  name: string;
  country?: string;
  timezone?: string;
}

export interface AuthUser {
  email: string;
  display_name?: string | null;
  email_verified?: boolean;
}

export interface AuthResponse {
  token: string;
  user?: AuthUser;
  verification_email_sent?: boolean;
}

/** Alert preferences as the UI uses them (booleans normalized). */
export interface AlertPrefs {
  enabled: boolean;
  email_alerts_enabled: boolean;
  email_alert_wind: boolean;
  email_alert_rain: boolean;
  email_alert_score: boolean;
  email_alert_temp_low: boolean;
  email_alert_temp_high: boolean;
  email_alert_frost: boolean;
  min_score: number;
  max_wind_gust: number;
  min_temp: number | null;
  max_temp: number | null;
  frost_risk_enabled: boolean;
  quiet_hours_enabled: boolean;
  quiet_start_hour: number;
  quiet_end_hour: number;
  severity: Level;
  home_lat: number | null;
  home_lon: number | null;
  city: string | null;
  /** JSON object string of per-alert toggles, e.g. {"wind_high":true}. */
  alert_states: string | null;
  moto_type: MotoType;
  comfort_temp: number;
  wind_tolerance: Level;
  rain_tolerance: Level;
}

export interface MeResponse {
  email: string;
  created_at: string | null;
  display_name: string;
  email_verified: boolean;
  prefs: AlertPrefs | null;
  pushSubscriptions: number;
}

export interface SavedRoute {
  id: number;
  name: string;
  stops: string[];
  total_distance_km: number | null;
  created_at: string;
}

export interface RideLog {
  id: number;
  route_name: string | null;
  start_city: string;
  end_city: string;
  distance_km: number;
  duration_min: number;
  avg_moto_score: number | null;
  created_at: string;
}

export interface RideStats {
  rides: number;
  total_distance_km: number;
  total_duration_min: number;
  avg_score: number | null;
  peak_wind: number | null;
  peak_precip: number | null;
  recent: RideLog[];
}

export type HazardType = 'gravel' | 'ice' | 'flood' | 'accident' | 'animals' | 'roadworks' | 'other';

export interface Hazard {
  id: number;
  lat: number;
  lon: number;
  hazard_type: HazardType | string;
  severity: number;
  description: string;
  distance_km: number;
  created_at: string;
  expires_at: string;
}

/** POST /alerts/check-now: the backend reports counts, not booleans. */
export interface CheckNowResponse {
  delivered?: number;
  sent?: number;
  email_sent?: number;
  events?: unknown[];
  reason?: string;
  [key: string]: unknown;
}
