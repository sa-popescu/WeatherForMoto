import type { Place } from '../../lib/types';
import type { StopDraft } from './types';

// The stop list: 2 to 5 rows, the first is the origin, the last the destination.

export const MIN_STOPS = 2;
export const MAX_STOPS = 5;

let seq = 0;

export function newStop(place: Place | null = null, options: { region?: string | null; gps?: boolean } = {}): StopDraft {
  seq += 1;
  return { id: `stop-${seq}`, text: place?.name ?? '', place, region: options.region ?? null, gps: options.gps ?? false };
}

export function moveItem<T>(list: readonly T[], from: number, to: number): T[] {
  if (from === to || from < 0 || to < 0 || from >= list.length || to >= list.length) return [...list];
  const copy = [...list];
  const [item] = copy.splice(from, 1);
  copy.splice(to, 0, item);
  return copy;
}

/** A new empty stop goes just before the destination. */
export function addStop(list: readonly StopDraft[]): StopDraft[] {
  if (list.length >= MAX_STOPS) return [...list];
  const copy = [...list];
  copy.splice(Math.max(1, copy.length - 1), 0, newStop());
  return copy;
}

export function removeStop(list: readonly StopDraft[], id: string): StopDraft[] {
  if (list.length <= MIN_STOPS) return [...list];
  return list.filter((s) => s.id !== id);
}

export type StopRole = 'origin' | 'via' | 'destination';

export function roleOf(index: number, count: number): StopRole {
  if (index === 0) return 'origin';
  return index === count - 1 ? 'destination' : 'via';
}

/** Text stored in a saved route; "sat, județ" keeps villages unambiguous when reloaded. */
export function savedLabel(stop: StopDraft): string {
  const name = stop.place?.name ?? stop.text.trim();
  return stop.region && !name.includes(',') ? `${name}, ${stop.region}` : name;
}

/** Identity of the stops that shaped a calculated route, to notice later edits. */
export function stopsKey(list: readonly StopDraft[]): string {
  return list.map((s) => (s.place ? `${s.place.lat.toFixed(4)},${s.place.lon.toFixed(4)}` : `?${s.text.trim()}`)).join('|');
}
