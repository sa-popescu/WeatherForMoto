import type { SampleWeather, StorySample } from './logic/story';

// The weather at the places and hours of a finished ride, for its story
// (logic/story.ts): one request to Open-Meteo for every sample at once (it
// takes lists of coordinates), the hours in UTC so they match the ride's
// clock. For hours already past these are the models' values after the
// fact, close to what happened; Open-Meteo keeps them for about three months.

const OPEN_METEO = 'https://api.open-meteo.com/v1/forecast';
const TIMEOUT_MS = 15_000;

interface Hourly {
  time?: string[];
  temperature_2m?: Array<number | null>;
  precipitation?: Array<number | null>;
  wind_gusts_10m?: Array<number | null>;
  is_day?: Array<number | null>;
}

interface Location {
  hourly?: Hourly;
}

const utcDay = (ms: number): string => new Date(ms).toISOString().slice(0, 10);
/** "2026-11-22T09:00", the hour a moment falls in, as Open-Meteo writes it in GMT. */
const utcHour = (ms: number): string => `${new Date(ms).toISOString().slice(0, 13)}:00`;

export function storyUrl(samples: readonly StorySample[]): string {
  const params = new URLSearchParams({
    latitude: samples.map((s) => s.lat.toFixed(3)).join(','),
    longitude: samples.map((s) => s.lon.toFixed(3)).join(','),
    hourly: 'temperature_2m,precipitation,wind_gusts_10m,is_day',
    timezone: 'GMT',
    wind_speed_unit: 'kmh',
    start_date: utcDay(samples[0].t),
    end_date: utcDay(samples[samples.length - 1].t),
  });
  return `${OPEN_METEO}?${params.toString()}`;
}

/** Each sample's weather, in order; null where its hour is missing. */
export function readStoryWeather(body: unknown, samples: readonly StorySample[]): Array<SampleWeather | null> {
  // One location comes back as an object, several as a list.
  const list: Location[] = Array.isArray(body) ? (body as Location[]) : [body as Location];
  return samples.map((sample, i) => {
    const hourly = list[i]?.hourly;
    const index = hourly?.time?.indexOf(utcHour(sample.t)) ?? -1;
    if (!hourly || index < 0) return null;
    const at = (values: Array<number | null> | undefined): number | null => {
      const v = values?.[index];
      return typeof v === 'number' && Number.isFinite(v) ? v : null;
    };
    const day = at(hourly.is_day);
    return { tempC: at(hourly.temperature_2m), precipMm: at(hourly.precipitation), gustKmh: at(hourly.wind_gusts_10m), isDay: day === null ? null : day === 1 };
  });
}

export async function fetchStoryWeather(samples: readonly StorySample[], signal: AbortSignal): Promise<Array<SampleWeather | null>> {
  if (samples.length === 0) return [];
  const controller = new AbortController();
  const forward = (): void => controller.abort();
  signal.addEventListener('abort', forward, { once: true });
  const timer = window.setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(storyUrl(samples), { signal: controller.signal });
    if (!res.ok) throw new Error(`Open-Meteo HTTP ${res.status}`);
    return readStoryWeather(await res.json(), samples);
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', forward);
  }
}
