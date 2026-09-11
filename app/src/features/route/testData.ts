import type { HourlyWeather } from '../../lib/types';
import type { PointForecast } from './types';

// Builders for unit tests only (not imported by the app).

export function hour(time: string, score: number | null, extra: Partial<HourlyWeather> = {}): HourlyWeather {
  return {
    time,
    temperature: 18,
    feels_like: 18,
    precipitation_mm: 0,
    precipitation_probability: 0,
    rain_intensity: 'none',
    wind_speed_kmh: 10,
    wind_gusts_kmh: 20,
    weather_code: 1,
    icon: null,
    description: null,
    uv_index: null,
    relative_humidity: null,
    surface_pressure: null,
    pressure_msl: null,
    dew_point_2m: null,
    cloud_cover: null,
    visibility: null,
    wind_direction_10m: null,
    is_day: true,
    road_surface_temp: null,
    frost_risk: false,
    moto_score: score,
    moto_label: null,
    ...extra,
  };
}

/** A day of hourly slots whose score is given by `scoreAt(hour)`. */
export function dayForecast(date: string, offsetSec: number, scoreAt: (h: number) => number | null): PointForecast {
  const hourly = Array.from({ length: 24 }, (_, h) => hour(`${date}T${String(h).padStart(2, '0')}:00`, scoreAt(h)));
  return { hourly, utcOffsetSeconds: offsetSec };
}
