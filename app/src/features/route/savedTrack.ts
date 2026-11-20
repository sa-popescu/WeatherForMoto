import type { Place, SavedRoute, SavedTrack } from '../../lib/types';
import { decodeLine, encodeLine, lineForLink, trackFromLink } from './routeLink';
import { MAX_STOPS, MIN_STOPS } from './stops';
import type { RouteLine, StopDraft } from './types';

// A route imported from GPX keeps its road when it is saved to the account,
// simplified and encoded the way a route link carries it (routeLink.ts).
// Routes the router drew are saved by their stop names only, because routing
// those names again gives the same road; no router would draw a GPX road again.

/** Stops are kept to about a metre, like the points of the road. */
const STOP_DECIMALS = 5;

const round = (value: number): number => Number(value.toFixed(STOP_DECIMALS));

/** The road to save with these stops, or null when a stop has no place on the map. */
export function savedTrackOf(road: RouteLine, stops: readonly StopDraft[]): SavedTrack | null {
  const coords: [number, number][] = [];
  for (const stop of stops) {
    if (!stop.place) return null;
    coords.push([round(stop.place.lat), round(stop.place.lon)]);
  }
  const line = lineForLink(road.coords);
  if (line.length < 2) return null;
  return { count: line.length, line: encodeLine(line), stops: coords };
}

/** A saved road ready for the planner, with its stops named as saved. */
export interface SavedRoad {
  route: RouteLine;
  places: Place[];
}

/**
 * The road of a saved route, or null when it has none or the road does not
 * hold together. A route without one is loaded by its stop names instead.
 */
export function trackFromSaved(saved: SavedRoute): SavedRoad | null {
  const track = saved.track;
  if (!track) return null;
  const count = saved.stops.length;
  if (count < MIN_STOPS || count > MAX_STOPS || track.stops.length !== count) return null;
  const line = decodeLine(track.line, track.count);
  if (!line) return null;
  const places = saved.stops.map((name, i) => ({ name, lat: track.stops[i][0], lon: track.stops[i][1] }));
  const route = trackFromLink(line, places);
  return route ? { route, places } : null;
}
