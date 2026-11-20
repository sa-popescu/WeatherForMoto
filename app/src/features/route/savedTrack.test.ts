import { describe, expect, it } from 'vitest';
import type { SavedRoute } from '../../lib/types';
import { trackFromLink } from './routeLink';
import { savedTrackOf, trackFromSaved } from './savedTrack';
import { newStop } from './stops';
import type { LatLon } from './types';

/** A wiggly road of `count` points from Novaci over the mountains towards Șugag. */
function road(count: number): LatLon[] {
  return Array.from({ length: count }, (_, i) => {
    const t = i / (count - 1);
    return { lat: 45.18 + 0.57 * t + 0.01 * Math.sin(t * 60), lon: 23.67 - 0.07 * t + 0.01 * Math.cos(t * 80) };
  });
}

const PLACES = [
  { name: 'Novaci', lat: 45.18, lon: 23.68 },
  { name: 'Rânca', lat: 45.29, lon: 23.69 },
  { name: 'Șugag', lat: 45.75, lon: 23.6 },
];

function savedRoute(track: SavedRoute['track'], stops = PLACES.map((p) => p.name)): SavedRoute {
  return { id: 7, name: 'Transalpina', stops, total_distance_km: 120, created_at: '2026-11-20T08:00:00Z', track };
}

describe('saved GPX road', () => {
  const line = road(3000);
  const route = trackFromLink(line, PLACES);
  if (!route) throw new Error('test road has no length');

  it('comes back with its stops and nearly its length', () => {
    const track = savedTrackOf(route, PLACES.map((p) => newStop(p)));
    expect(track).not.toBeNull();
    expect(track?.count).toBeLessThanOrEqual(1000);
    expect(track?.stops).toEqual(PLACES.map((p) => [p.lat, p.lon]));

    const back = trackFromSaved(savedRoute(track));
    expect(back?.places).toEqual(PLACES);
    expect(back?.route.distanceKm).toBeCloseTo(route.distanceKm, -1);
    expect(back?.route.stopKm).toHaveLength(PLACES.length);
  });

  it('is not saved when a stop was typed and never placed', () => {
    const stops = [newStop(PLACES[0]), { ...newStop(), text: 'Rânca' }, newStop(PLACES[2])];
    expect(savedTrackOf(route, stops)).toBeNull();
  });

  it('is ignored when missing, mismatched or damaged', () => {
    const track = savedTrackOf(route, PLACES.map((p) => newStop(p)));
    if (!track) throw new Error('no track');
    expect(trackFromSaved(savedRoute(undefined))).toBeNull();
    expect(trackFromSaved(savedRoute(null))).toBeNull();
    expect(trackFromSaved(savedRoute(track, ['Novaci', 'Șugag']))).toBeNull();
    expect(trackFromSaved(savedRoute({ ...track, line: track.line.slice(0, -4) }))).toBeNull();
    expect(trackFromSaved(savedRoute({ ...track, count: track.count + 1 }))).toBeNull();
  });
});
