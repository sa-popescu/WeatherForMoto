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

/** How the rain or snow falls in the scene: how much, and how the wind carries it. */
export interface Precip {
  /** 0 to 1: a few drops or flakes to a downpour or a thick fall. */
  amount: number;
  /** Sideways push on screen, -1 (to the left) to 1 (to the right). */
  wind: number;
  /** 0 to 1: how much the gusts come and go above the mean wind. */
  gust: number;
}

type PrecipInput = Pick<CurrentWeather, 'weather_code' | 'precipitation_mm' | 'wind_speed_kmh' | 'wind_gusts_kmh' | 'wind_direction_deg'>;

const clamp01 = (v: number): number => Math.min(1, Math.max(0, v));

/** How hard it falls by the WMO code: light, moderate or heavy. */
function amountOfCode(code: number | null): number {
  switch (code) {
    case 51: case 56: case 61: case 66: case 71: case 80: case 85:
      return 0.35;
    case 53: case 63: case 73: case 81: case 77:
      return 0.6;
    case 55: case 57: case 65: case 67: case 75: case 82: case 86: case 95:
      return 0.85;
    case 96: case 99:
      return 1;
    default:
      return 0.45;
  }
}

export function precipOf(current: PrecipInput, kind: SkyKind): Precip {
  // Water per hour: 1 mm/h is steady rain (or a steady snowfall), 4 mm/h and
  // over a downpour (or a heavy fall).
  const mm = current.precipitation_mm ?? 0;
  const byAmount = mm > 0 ? clamp01((kind === 'snow' ? 0.3 : 0.15) + mm / (kind === 'snow' ? 4 : 6)) : 0;
  const amount = clamp01(Math.max(amountOfCode(current.weather_code), byAmount));
  // The wind blows from wind_direction_deg: from the east (90°) the rain goes
  // to the left of a screen held facing north. 40 km/h is a full push; a
  // breeze along the screen still tilts the fall a little.
  const speed = current.wind_speed_kmh ?? 0;
  const dir = current.wind_direction_deg;
  const across = dir == null ? 0.5 : -Math.sin((dir * Math.PI) / 180);
  const tilt = Math.abs(across) < 0.2 ? 0.2 * Math.sign(across || 1) : across;
  const wind = Math.max(-1, Math.min(1, tilt * clamp01(speed / 40)));
  const gusts = current.wind_gusts_kmh ?? speed;
  const gust = clamp01((gusts - speed) / 30);
  return { amount, wind, gust };
}
