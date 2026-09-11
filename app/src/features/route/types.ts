import type { HourlyWeather, Place } from '../../lib/types';

// Shapes used only by the "Traseu" feature.

/** A coordinate pair on the route. */
export interface LatLon {
  lat: number;
  lon: number;
}

/** A stretch of road with a number or a name, taken from the OSRM steps. */
export interface RoadSpan {
  fromKm: number;
  toKm: number;
  label: string;
}

/** The driving route as the screen uses it. */
export interface RouteLine {
  coords: LatLon[];
  /** Distance along the route at each vertex, km, scaled so the last one equals distanceKm. */
  cumKm: number[];
  distanceKm: number;
  /** Distance along the route of every stop, km (the first is 0). */
  stopKm: number[];
  roads: RoadSpan[];
}

/** A place on the route where the weather is sampled. */
export interface SamplePoint {
  id: string;
  lat: number;
  lon: number;
  km: number;
  name: string;
  /** Index in the stop list, or null for a point between stops. */
  stopIndex: number | null;
}

export interface PointForecast {
  hourly: HourlyWeather[];
  /** Offset of the point's own timezone; hourly times are local to it. */
  utcOffsetSeconds: number;
}

export type PointStatus = 'loading' | 'ready' | 'error';

export interface PointWeather {
  status: PointStatus;
  forecast: PointForecast | null;
}

/** Departure, always in the ORIGIN's local time. */
export interface Departure {
  /** "YYYY-MM-DD" */
  date: string;
  /** "HH:MM", 30-minute steps */
  time: string;
}

/** One row of the stops editor. */
export interface StopDraft {
  id: string;
  text: string;
  place: Place | null;
  /** County or region of the chosen suggestion, kept for saving ("Sâmbăta, Brașov"). */
  region: string | null;
  gps: boolean;
}
