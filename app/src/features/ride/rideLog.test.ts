import { afterAll, describe, expect, it, vi } from 'vitest';
import type { RidePoint } from './logic/track';

const logRide = vi.fn(async () => ({ ok: true, ride_id: 42 }));
vi.mock('../../lib/api', () => ({ api: { logRide: (...args: unknown[]) => logRide(...(args as [])) } }));
vi.mock('../../lib/geo', () => ({
  NOMINATIM_GAP_MS: 0,
  reverseGeocode: vi.fn(async (lat: number) => (lat < 45 ? 'București' : null)),
}));
vi.stubGlobal('window', { setTimeout: (fn: () => void) => setTimeout(fn, 0) });
afterAll(() => vi.unstubAllGlobals());

const { logRideToAccount } = await import('./rideLog');

function pt(lat: number): RidePoint {
  return { t: 0, lat, lon: 26.1, acc: 5, speed: 10, alt: null, seg: 0 };
}

describe('logRideToAccount', () => {
  it('sends the measured distance and time between the named ends', async () => {
    const id = await logRideToAccount('tok', [pt(44.4), pt(45.6)], { distanceM: 123_456, movingDistanceM: 120_000, movingMs: 5_000_000, elapsedMs: 5_430_000, topSpeedMs: 30 }, 'Sibiu', 'ro');
    expect(id).toBe(42);
    expect(logRide).toHaveBeenCalledWith('tok', {
      route_name: 'Sibiu',
      start_city: 'București',
      // Nothing names the end: its coordinates stand in, since the log wants text.
      end_city: '45.600, 26.100',
      distance_km: 123.5,
      duration_min: 91,
      avg_moto_score: null,
      max_wind_gust: null,
      max_precip: null,
    });
  });

  it('never sends zero, which the log refuses', async () => {
    await logRideToAccount('tok', [pt(44.4), pt(44.4)], { distanceM: 10, movingDistanceM: 0, movingMs: 0, elapsedMs: 5_000, topSpeedMs: 0 }, null, 'ro');
    expect(logRide).toHaveBeenLastCalledWith('tok', expect.objectContaining({ distance_km: 0.1, duration_min: 1, route_name: null }));
  });
});
