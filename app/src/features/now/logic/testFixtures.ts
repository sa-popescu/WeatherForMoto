import { addMinutesLocal, hourNumber } from '../../../lib/format';
import type { CurrentWeather, DailyWeather, HourlyWeather } from '../../../lib/types';

// Builders for unit tests: calm, dry, warm hours unless overridden.

export function hour(time: string, over: Partial<HourlyWeather> = {}): HourlyWeather {
  const hh = hourNumber(time);
  return {
    time,
    temperature: 22,
    feels_like: 22,
    precipitation_mm: 0,
    precipitation_probability: 0,
    rain_intensity: 'none',
    wind_speed_kmh: 8,
    wind_gusts_kmh: 15,
    weather_code: 1,
    icon: null,
    description: null,
    uv_index: 3,
    relative_humidity: 55,
    surface_pressure: 1008,
    pressure_msl: 1016,
    dew_point_2m: 12,
    cloud_cover: 20,
    visibility: 20_000,
    wind_direction_10m: 90,
    is_day: hh >= 7 && hh < 20,
    road_surface_temp: 28,
    frost_risk: false,
    moto_score: 95,
    moto_label: 'IDEAL',
    ...over,
  };
}

/** Consecutive hours from `start`, with per-hour overrides by index. */
export function hours(start: string, count: number, over: (i: number, time: string) => Partial<HourlyWeather> = () => ({})): HourlyWeather[] {
  return Array.from({ length: count }, (_, i) => {
    const time = addMinutesLocal(start, i * 60);
    return hour(time, over(i, time));
  });
}

export function day(date: string, over: Partial<DailyWeather> = {}): DailyWeather {
  return {
    date,
    weather_code: 1,
    icon: null,
    description: null,
    temp_max: 24,
    temp_min: 13,
    feels_max: 24,
    feels_min: 13,
    precipitation_mm: 0,
    precipitation_probability: 0,
    precipitation_max_mm_h: 0,
    rain_intensity_max: 'none',
    wind_max_kmh: 12,
    wind_gusts_kmh: 25,
    moto_score: 92,
    moto_label: 'IDEAL',
    sunrise: `${date}T06:49`,
    sunset: `${date}T19:33`,
    ...over,
  };
}

export function current(over: Partial<CurrentWeather> = {}): CurrentWeather {
  return {
    temperature: 22, feels_like: 22, humidity: 55, wind_speed_kmh: 8, wind_gusts_kmh: 15, wind_direction_deg: 90,
    wind_direction: 'E', beaufort: 2, precipitation_mm: 0, precipitation_probability: 0, rain_intensity: 'none',
    weather_code: 1, description: null, icon: null, pressure_hpa: 1016, visibility_km: 20, aqi: null, pm10: null,
    pm2_5: null, ozone: null, eu_aqi: 30, us_aqi: null, pollen_index: null, uv_index: 3, is_day: true, dew_point: 12,
    frost_risk: false, moto_score: 95, moto_label: 'IDEAL', score_breakdown: [],
    road_surface_temp: 28, sources: ['open-meteo'],
    ...over,
  };
}
