import { api, type ObservedSample } from '../../lib/api';
import type { SampleWeather, StorySample } from './logic/story';

// The weather at the places and hours of a finished ride, for its story
// (logic/story.ts): one request to Open-Meteo for every sample at once (it
// takes lists of coordinates), the hours in UTC so they match the ride's
// clock. For hours already past these are the models' values after the
// fact, close to what happened; Open-Meteo keeps them for about three months.
// Where a station or an airport measured the place and hour (the server's
// verification log, /ride/observed), the measurement wins (withObserved).

const OPEN_METEO = 'https://api.open-meteo.com/v1/forecast';
const TIMEOUT_MS = 15_000;

interface Hourly {
  time?: string[];
  temperature_2m?: Array<number | null>;
  apparent_temperature?: Array<number | null>;
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
    hourly: 'temperature_2m,apparent_temperature,precipitation,wind_gusts_10m,is_day',
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
    return {
      tempC: at(hourly.temperature_2m),
      feelsC: at(hourly.apparent_temperature),
      precipMm: at(hourly.precipitation),
      gustKmh: at(hourly.wind_gusts_10m),
      isDay: day === null ? null : day === 1,
    };
  });
}

/** A measured rate standing in for "it rained" when the model had nothing (light rain). */
const OBSERVED_RAIN_MM = 0.5;

/**
 * The models' weather corrected by what was measured there and then: the
 * station's temperature (the real feel moves with it), its gust, and whether
 * a gauge or an airport saw rain. A sample nobody measured keeps the models'.
 */
export function withObserved(model: ReadonlyArray<SampleWeather | null>, observed: ReadonlyArray<ObservedSample | null>): Array<SampleWeather | null> {
  return model.map((w, i) => {
    const o = observed[i];
    if (!w || !o) return w;
    const out: SampleWeather = { ...w };
    if (o.temp != null) {
      if (out.feelsC != null && out.tempC != null) out.feelsC = out.feelsC + (o.temp - out.tempC);
      out.tempC = o.temp;
    }
    if (o.gusts != null) out.gustKmh = o.gusts;
    if (o.raining === 0) out.precipMm = 0;
    else if (o.raining === 1 && (out.precipMm ?? 0) < OBSERVED_RAIN_MM) out.precipMm = OBSERVED_RAIN_MM;
    return out;
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
    const model = readStoryWeather(await res.json(), samples);
    // The measurements are a correction, never a condition: without them the models' story stands.
    const observed = await api.rideObserved(samples, controller.signal).catch(() => []);
    return withObserved(model, observed);
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', forward);
  }
}
