import type { Lang } from './i18n';

// WMO weather codes as used by the backend (Open-Meteo convention).

export type WeatherIconKey =
  | 'sun'
  | 'moon'
  | 'cloudsun'
  | 'cloudmoon'
  | 'cloud'
  | 'fog'
  | 'drizzle'
  | 'rain'
  | 'heavyrain'
  | 'sleet'
  | 'snow'
  | 'storm';

export function iconKeyFor(code: number | null | undefined, isDay: boolean | null | undefined = true): WeatherIconKey {
  const day = isDay !== false;
  if (code == null) return day ? 'cloudsun' : 'cloudmoon';
  if (code === 0) return day ? 'sun' : 'moon';
  if (code === 1 || code === 2) return day ? 'cloudsun' : 'cloudmoon';
  if (code === 3) return 'cloud';
  if (code === 45 || code === 48) return 'fog';
  if (code >= 51 && code <= 55) return 'drizzle';
  if (code === 56 || code === 57 || code === 66 || code === 67) return 'sleet';
  if (code === 61 || code === 80) return 'rain';
  if (code === 63 || code === 65 || code === 81 || code === 82) return 'heavyrain';
  if ((code >= 71 && code <= 77) || code === 85 || code === 86) return 'snow';
  if (code >= 95) return 'storm';
  return 'cloud';
}

const DESCRIPTIONS: Record<number, [ro: string, en: string]> = {
  0: ['Senin', 'Clear sky'],
  1: ['Predominant senin', 'Mainly clear'],
  2: ['Parțial noros', 'Partly cloudy'],
  3: ['Înnorat', 'Overcast'],
  45: ['Ceață', 'Fog'],
  48: ['Ceață cu chiciură', 'Rime fog'],
  51: ['Burniță slabă', 'Light drizzle'],
  53: ['Burniță', 'Drizzle'],
  55: ['Burniță densă', 'Dense drizzle'],
  56: ['Burniță care îngheață', 'Freezing drizzle'],
  57: ['Burniță densă care îngheață', 'Dense freezing drizzle'],
  61: ['Ploaie slabă', 'Light rain'],
  63: ['Ploaie', 'Rain'],
  65: ['Ploaie puternică', 'Heavy rain'],
  66: ['Ploaie care îngheață', 'Freezing rain'],
  67: ['Ploaie puternică care îngheață', 'Heavy freezing rain'],
  71: ['Ninsoare slabă', 'Light snow'],
  73: ['Ninsoare', 'Snow'],
  75: ['Ninsoare abundentă', 'Heavy snow'],
  77: ['Grăunțe de zăpadă', 'Snow grains'],
  80: ['Averse slabe', 'Light showers'],
  81: ['Averse', 'Showers'],
  82: ['Averse puternice', 'Violent showers'],
  85: ['Averse de ninsoare', 'Snow showers'],
  86: ['Averse puternice de ninsoare', 'Heavy snow showers'],
  95: ['Furtună', 'Thunderstorm'],
  96: ['Furtună cu grindină', 'Thunderstorm with hail'],
  99: ['Furtună puternică cu grindină', 'Severe thunderstorm with hail'],
};

export function describeCode(code: number | null | undefined, lang: Lang): string {
  if (code == null) return '–';
  const entry = DESCRIPTIONS[code];
  if (!entry) return lang === 'en' ? 'Unknown' : 'Necunoscut';
  return lang === 'en' ? entry[1] : entry[0];
}
