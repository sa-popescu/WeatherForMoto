import type { Place, WeatherResponse } from './types';

/**
 * Fallback used only when the backend does not answer: fetch Open-Meteo
 * directly and estimate scores in the browser from the published scoring
 * constants. Implemented by the "now" feature work; until then the app shows
 * an error state instead.
 */
export async function fetchDirectWeather(place: Place, signal: AbortSignal): Promise<WeatherResponse> {
  void place;
  void signal;
  throw new Error('Direct weather fallback is not implemented yet');
}
