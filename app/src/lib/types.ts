// Shapes of the Motonor API (backend/main.py + backend/auth_alerts.py).
// Numeric fields are nullable because providers can be missing.

export type RainBand = 'none' | 'urme' | 'slaba' | 'moderata' | 'puternica';
export type MotoLabel = 'IDEAL' | 'OK' | 'ATENȚIE' | 'EVITĂ';

/** The road after rain (backend/wet_road.py): still wet, or the first rain after dry days. */
export type WetRoadState = 'wet' | 'first_rain';
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

/** How much the national models agree on an hour. */
export type Confidence = 'high' | 'medium' | 'low';

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
  /** "convective" (showers on part of the area) or "frontal" (rain on all of it). */
  rain_character?: 'convective' | 'frontal' | null;
  /** Share of the ring around the point that sees rain that hour, 0 to 1. */
  rain_coverage?: number | null;
  /** That share, named: isolated, scattered or widespread. */
  rain_extent?: 'isolated' | 'scattered' | 'widespread' | null;
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
  /** Saharan dust over the next 24 h (CAMS); null or absent when the air stays clean. */
  dust?: DustOutlook | null;
  uv_index: number | null;
  is_day: boolean | null;
  dew_point: number | null;
  frost_risk: boolean | null;
  /** Missing from older servers and from the browser-direct fallback. */
  wet_road?: WetRoadState | null;
  moto_score: number | null;
  moto_label: MotoLabel | null;
  score_breakdown: ScoreFactor[] | null;
  road_surface_temp: number | null;
  /** Likely [low, high] of the asphalt now (absent from older servers and the direct fallback). */
  road_surface_range?: [number, number] | null;
  /** The asphalt's heat balance: range, confidence and why (absent from older servers and the direct fallback). */
  road_surface?: RoadSurface | null;
  forecast_confidence: Confidence | null;
  /** How many models answered for this hour. */
  model_count: number | null;
  sources: string[] | null;
  /** Every source and what it did for this answer (absent from older servers and the direct fallback). */
  source_status?: SourceStatus[] | null;
  /** The arbiter's verdict on now, from every witness at once (absent from older servers and the direct fallback). */
  verdict?: NowVerdict | null;
}

/** One of the arbiter's verdicts: what it decided, how sure, and why in one sentence. */
export interface VerdictPart {
  state: string;
  /** Chance the phenomenon is happening now, 0-100. */
  probability: number;
  /** How sure the verdict is, 50-100. */
  confidence: number;
  explanation: string;
  explanation_en: string;
  /** The verdict in a few words, for one line (rain only; absent from older servers). */
  short?: string;
  short_en?: string;
  /** For rain: rain, drizzle, snow or ice, when something falls. */
  kind?: 'rain' | 'drizzle' | 'snow' | 'ice' | null;
}

/** The asphalt's temperature from its heat balance (backend/road_surface.py). */
export interface RoadSurface {
  temp: number;
  low: number;
  high: number;
  confidence: Confidence;
  explanation: string;
  explanation_en: string;
  method: 'heat-balance';
  /** The airport whose observed sky was used around now. */
  sky: { station: string | null; name: string | null; distance_km: number | null; reported: string | null; cloud: number | null } | null;
  station_sun_wm2: number | null;
  /** The national models' ground surface (grass, soil) at this hour: [min, max]. */
  model_ground: [number, number] | null;
}

export interface NowVerdict {
  precipitation: VerdictPart & { state: 'wet' | 'dry' | 'uncertain' };
  /** Present only when someone mentions fog or a low visibility. */
  fog: (VerdictPart & { state: 'fog' | 'clear' }) | null;
  /** Present only when there are flashes or a storm code. */
  storm: (VerdictPart & { state: 'storm' | 'none' }) | null;
  summary: string;
  summary_en: string;
  /** The numbers, each weighed from every witness (absent from older servers). */
  values?: Partial<Record<'temperature' | 'humidity' | 'wind' | 'gusts', ValueVerdict>>;
}

/** How a number of "now" was decided (backend/value_arbiter.py). */
export interface ValueVerdict {
  value: number;
  /** One standard deviation of the decided value, in its units. */
  sigma: number;
  confidence: 'high' | 'medium' | 'low';
  /** What the providers alone said. */
  models_value: number | null;
  /** True when stations moved the value or a station was set aside: worth a sentence. */
  notable: boolean;
  explanation: string;
  explanation_en: string;
}

export type SourceState = 'used' | 'no-data' | 'none-nearby' | 'off' | 'rate-limited' | 'paused' | 'stale' | 'key-rejected';

export interface SourceStatus {
  id: string;
  status: SourceState;
  /** Station or airport that answered. */
  station?: string | null;
  distance_km?: number | null;
  /** When the station measured, ISO 8601. */
  observed_at?: string | null;
  /** Models that answered (the ensemble). */
  models?: number | null;
  /** Ensemble runs that answered (the rain ensemble). */
  members?: number | null;
  /** When a rate-limited source comes back, ISO 8601. */
  retry_at?: string | null;
  /** What the source itself said about the refusal. */
  detail?: string | null;
  /** Why a source gave nothing (Netatmo): 'unavailable', 'token' or 'slow'. */
  reason?: string | null;
  /** How many sensors agreed (Sensor.Community). */
  sensors?: number | null;
  /** Hours of measured rain, and their sum in mm (the OPERA radar). */
  hours?: number | null;
  rain_mm?: number | null;
  /** Warnings that apply to this place (warning feeds). */
  count?: number | null;
}

export interface HourlyWeather {
  /** Local time of the location, "YYYY-MM-DDTHH:MM". */
  time: string;
  temperature: number | null;
  feels_like: number | null;
  precipitation_mm: number | null;
  precipitation_probability: number | null;
  rain_intensity: RainBand | null;
  /** "convective" (showers on part of the area) or "frontal" (rain on all of it). */
  rain_character?: 'convective' | 'frontal' | null;
  /** Share of the ring around the point that sees rain that hour, 0 to 1. */
  rain_coverage?: number | null;
  /** That share, named: isolated, scattered or widespread. */
  rain_extent?: 'isolated' | 'scattered' | 'widespread' | null;
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
  /** Likely [low, high] of the asphalt for the hours near now (absent further ahead, from older servers and the direct fallback). */
  road_surface_range?: [number, number] | null;
  frost_risk: boolean | null;
  /** Missing from older servers and from the browser-direct fallback. */
  wet_road?: WetRoadState | null;
  moto_score: number | null;
  moto_label: MotoLabel | null;
  forecast_confidence: Confidence | null;
  model_count: number | null;
  /** How many rain sources give rain for this hour, out of how many answered. */
  rain_sources?: { wet: number; total: number } | null;
  /** The ensembles' likely amount if it rains (25th to 90th percentile), mm/h. */
  precipitation_range_mm?: [number, number] | null;
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

/** Meteoalarm's colour, worst first. */
export type AlertLevel = 'red' | 'orange' | 'yellow' | 'green';

/** An official warning from the national weather service, relayed by Meteoalarm. */
export interface OfficialAlert {
  id: string;
  event: string | null;
  headline: string | null;
  description: string | null;
  instruction: string | null;
  level: AlertLevel;
  /** "wind", "rain", "thunderstorm"… when the feed says so. */
  awareness_type: string | null;
  sender: string | null;
  /** ISO 8601 with an offset, as the issuing service wrote it. */
  onset: string | null;
  expires: string | null;
  areas: string[];
  /** How it reached us: inside the drawn area, or by the name of the place. */
  match: 'geometry' | 'name';
}

/** Set when the server could not reach its sources and served what it had. */
export interface StaleMark {
  age_min: number;
  /** What went wrong upstream, in the source's own words. */
  reason?: string | null;
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
  /** Official warnings covering this point; empty when there are none. */
  alerts?: OfficialAlert[];
  /** Flashes the MTG satellite saw around here lately; null when the source is off or unreadable. */
  lightning?: Lightning | null;
  /** Present when the server served an older answer because its sources failed. */
  stale?: StaleMark | null;
}

/** Saharan dust near the ground over the next 24 hours, from CAMS. */
export interface DustOutlook {
  /** "notable" from 50 µg/m³ (haze, muddy rain), "strong" from 150 (poor visibility). */
  level: 'notable' | 'strong';
  now_ug: number | null;
  peak_ug: number;
  /** Local "YYYY-MM-DDTHH:MM" of the worst hour. */
  peak_time: string;
  /** First hour with rain while the dust is in the air, local time; null when none. */
  muddy_rain_at: string | null;
  source: string;
}

/** Lightning seen from space (EUMETSAT MTG Lightning Imager) around the point. */
export interface Lightning {
  /** Flashes within radius_km in the last window_min minutes. */
  count: number;
  /** Of those, the ones within 10 km. */
  near_count: number;
  nearest_km: number | null;
  /** Direction of the nearest flash from the point, degrees, 0 = north. */
  bearing_deg: number | null;
  last_minutes_ago: number | null;
  radius_km: number;
  window_min: number;
  /** End of the newest satellite file read, ISO 8601. */
  observed_until: string | null;
  attribution: string;
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
  /** The daily forecast push, at this local hour in the device's time zone. */
  daily_forecast_enabled: boolean;
  daily_forecast_hour: number;
  /** IANA zone of the device that chose the hour, e.g. "Europe/Bucharest". */
  daily_forecast_tz: string | null;
}

export interface MeResponse {
  email: string;
  created_at: string | null;
  display_name: string;
  email_verified: boolean;
  prefs: AlertPrefs | null;
  pushSubscriptions: number;
  /** False until the server's database has the daily forecast columns. */
  dailyForecastAvailable?: boolean;
}

/** The road of a route imported from GPX, as the server keeps it (see features/route/savedTrack.ts). */
export interface SavedTrack {
  /** Points in `line`. */
  count: number;
  /** Encoded like the road in a route link (features/route/routeLink.ts). */
  line: string;
  /** [lat, lon] of each stop, in the order of SavedRoute.stops. */
  stops: [number, number][];
}

export interface SavedRoute {
  id: number;
  name: string;
  stops: string[];
  total_distance_km: number | null;
  created_at: string;
  /** Only routes imported from GPX have one; absent from older servers. */
  track?: SavedTrack | null;
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
export interface DailyForecastNowResponse {
  status?: string;
  push?: number;
  title?: string;
  body?: string;
}

export interface CheckNowResponse {
  delivered?: number;
  sent?: number;
  events?: unknown[];
  reason?: string;
  [key: string]: unknown;
}

/** A stretch of a route with traffic, by point index along `coords`. */
export interface UrbanTraffic {
  from: number | null;
  to: number | null;
  /** JAM, ROAD_WORK, ROAD_CLOSURE and the like. */
  category: string;
  /** 0 unknown, 1 minor, 2 moderate, 3 major, 4 undefined (closures). */
  magnitude: number;
  delay_s: number | null;
  speed_kmh: number | null;
}

/** One coded turn instruction (TomTom maneuver codes such as TURN_LEFT). */
export interface UrbanInstruction {
  offset_m: number;
  index: number | null;
  time_s: number | null;
  maneuver: string;
  street: string | null;
  road: string | null;
  exit: string | null;
  roundabout_exit: number | null;
  signpost: string | null;
}

export interface UrbanRoute {
  distance_m: number;
  /** With live traffic. */
  duration_s: number;
  no_traffic_s: number | null;
  delay_s: number;
  /** The length of each leg, metres: one leg from each point to the next. Missing from older servers. */
  legs_m?: number[];
  /** [lat, lon] pairs. */
  coords: Array<[number, number]>;
  traffic: UrbanTraffic[];
  instructions: UrbanInstruction[];
}

/** GET /route/urban (backend/urban_route.py). */
export interface UrbanRouteResponse {
  source: 'tomtom';
  attribution: string;
  /** The departure the times are for, UTC, or null for now. */
  depart_at?: string | null;
  routes: UrbanRoute[];
}
