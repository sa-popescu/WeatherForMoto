import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError } from '../../lib/api';
import type { UrbanRouteResponse } from '../../lib/types';
import { DEFAULT_ROUTE_OPTIONS, type RouteOptions } from '../route/routeOptions';
import { fetchTrafficRoutes, worthRelaxing } from './routeService';

const ENDS = [
  { lat: 44.4, lon: 26.1 },
  { lat: 45.6, lon: 25.6 },
];

const ANSWER: UrbanRouteResponse = {
  source: 'tomtom',
  attribution: '© TomTom',
  routes: [
    {
      distance_m: 2000,
      duration_s: 180,
      no_traffic_s: 170,
      delay_s: 10,
      coords: [
        [44.4, 26.1],
        [44.41, 26.1],
      ],
      traffic: [],
      instructions: [],
    },
  ],
};

const curvy: RouteOptions = { ...DEFAULT_ROUTE_OPTIONS, type: 'thrilling', avoid: ['motorways'] };

describe('route service', () => {
  afterEach(() => vi.restoreAllMocks());

  it('tries the default route again only when the options may be the reason', () => {
    expect(worthRelaxing(new ApiError(404, 'no-route'))).toBe(true);
    expect(worthRelaxing(new ApiError(503, 'upstream'))).toBe(true);
    expect(worthRelaxing(new ApiError(429, 'busy'))).toBe(false);
    expect(worthRelaxing(new ApiError(503, 'no-key'))).toBe(false);
    expect(worthRelaxing(new ApiError(0, 'network'))).toBe(false);
    expect(worthRelaxing(new Error('boom'))).toBe(false);
  });

  it('sends the options, and says so when it had to fall back to the default route', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ask = vi.spyOn(api, 'urbanRoute').mockRejectedValueOnce(new ApiError(404, 'no-route')).mockResolvedValueOnce(ANSWER);
    const found = await fetchTrafficRoutes(ENDS, { alternatives: 2, departMs: null, options: curvy }, new AbortController().signal);
    expect(ask).toHaveBeenCalledTimes(2);
    expect(ask.mock.calls[0][1]?.route).toMatchObject({ route_type: 'thrilling', avoid: 'motorways' });
    expect(ask.mock.calls[1][1]?.route).toMatchObject({ route_type: 'fastest', avoid: 'unpavedRoads' });
    expect(found?.relaxed).toBe(true);
    expect(found?.options).toEqual(DEFAULT_ROUTE_OPTIONS);
    expect(found?.routes).toHaveLength(1);
  });

  it('asks once with the default options, and gives up on a spent budget', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const ask = vi.spyOn(api, 'urbanRoute').mockRejectedValue(new ApiError(429, 'busy'));
    expect(await fetchTrafficRoutes(ENDS, { alternatives: 0, departMs: null, options: curvy }, new AbortController().signal)).toBeNull();
    expect(ask).toHaveBeenCalledTimes(1);
    ask.mockReset().mockResolvedValue(ANSWER);
    const found = await fetchTrafficRoutes(ENDS, { alternatives: 0, departMs: null, traffic: false }, new AbortController().signal);
    expect(found?.relaxed).toBe(false);
    expect(ask.mock.calls[0][1]?.traffic).toBe(false);
  });
});
