import { describe, expect, it } from 'vitest';
import { DEFAULT_ROUTE_OPTIONS, isDefaultOptions, normalizeOptions, optionsQuery, queryKey, type RouteOptions } from './routeOptions';
import { pick } from '../../lib/i18n';
import { RO_S } from './routeOptionsStrings';
import { optionsSummary, typeDescription } from './routeOptionsText';

const curvy: RouteOptions = { type: 'thrilling', avoid: ['motorways', 'tollRoads'], windingness: 'high', hilliness: 'low', topSpeedKmh: 45 };

describe('route options', () => {
  it('asks by default for the route the app always made', () => {
    expect(optionsQuery(DEFAULT_ROUTE_OPTIONS)).toEqual({ route_type: 'fastest', avoid: 'unpavedRoads' });
    expect(isDefaultOptions(DEFAULT_ROUTE_OPTIONS)).toBe(true);
  });

  it('sends what was chosen, curves only with a curvy route, and "none" for nothing to avoid', () => {
    expect(optionsQuery(curvy)).toEqual({ route_type: 'thrilling', avoid: 'motorways,tollRoads', windingness: 'high', hilliness: 'low', top_speed: 45 });
    expect(optionsQuery({ ...DEFAULT_ROUTE_OPTIONS, type: 'shortest', avoid: [], windingness: 'high' })).toEqual({ route_type: 'shortest', avoid: 'none' });
  });

  it('counts as default when only settings that do not apply differ', () => {
    expect(isDefaultOptions({ ...DEFAULT_ROUTE_OPTIONS, windingness: 'high' })).toBe(true);
    expect(isDefaultOptions({ ...DEFAULT_ROUTE_OPTIONS, avoid: [] })).toBe(false);
    expect(queryKey(curvy)).not.toBe(queryKey(DEFAULT_ROUTE_OPTIONS));
  });

  it('reads saved options safely, keeping the known parts in a stable order', () => {
    expect(normalizeOptions(null)).toEqual(DEFAULT_ROUTE_OPTIONS);
    expect(normalizeOptions({ type: 'scenic', avoid: ['tollRoads', 'nope', 'motorways'], topSpeedKmh: 33 })).toEqual({
      ...DEFAULT_ROUTE_OPTIONS,
      avoid: ['motorways', 'tollRoads'],
    });
    expect(normalizeOptions(curvy)).toEqual(curvy);
  });

  it('sums the options up in one line, in both languages', () => {
    expect(optionsSummary(DEFAULT_ROUTE_OPTIONS, pick(RO_S, 'ro'))).toBe('Cel mai rapid · fără neasfaltate');
    expect(optionsSummary(curvy, pick(RO_S, 'ro'))).toBe('Cu curbe · curbe: multe · fără autostrăzi, taxe · max 45 km/h');
    expect(optionsSummary({ ...DEFAULT_ROUTE_OPTIONS, type: 'eco', avoid: [] }, pick(RO_S, 'en'))).toBe('Economical');
    expect(typeDescription('shortest', pick(RO_S, 'en'))).toMatch(/kilometres/);
  });
});
