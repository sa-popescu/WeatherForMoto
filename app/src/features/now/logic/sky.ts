import type { CurrentWeather } from '../../../lib/types';

// The scene drawn behind the forecast (components/WeatherSky.tsx): the
// weather right now, from its WMO code, with the rain the sources measure
// overriding a code that says dry.

export type SkyKind = 'clear' | 'partly' | 'overcast' | 'fog' | 'drizzle' | 'rain' | 'storm' | 'snow';

export interface Sky {
  kind: SkyKind;
  night: boolean;
}

type SkyInput = Pick<CurrentWeather, 'weather_code' | 'is_day' | 'precipitation_mm' | 'rain_intensity'>;

function fromCode(code: number | null): SkyKind | null {
  if (code == null) return null;
  if (code <= 1) return 'clear';
  if (code === 2) return 'partly';
  if (code === 3) return 'overcast';
  if (code === 45 || code === 48) return 'fog';
  if (code >= 51 && code <= 57) return 'drizzle';
  if ((code >= 61 && code <= 67) || (code >= 80 && code <= 82)) return 'rain';
  if ((code >= 71 && code <= 77) || code === 85 || code === 86) return 'snow';
  if (code >= 95 && code <= 99) return 'storm';
  return null;
}

const WET: ReadonlySet<SkyKind> = new Set(['drizzle', 'rain', 'storm', 'snow']);

export function skyOf(current: SkyInput): Sky {
  const night = current.is_day === false;
  let kind = fromCode(current.weather_code) ?? 'partly';
  // Rain the sources see now, under a code that says dry: show it falling.
  const band = current.rain_intensity;
  if (!WET.has(kind) && (band === 'slaba' || band === 'moderata' || band === 'puternica')) {
    kind = band === 'slaba' ? 'drizzle' : 'rain';
  } else if (!WET.has(kind) && (current.precipitation_mm ?? 0) >= 0.5) {
    kind = 'rain';
  }
  return { kind, night };
}
