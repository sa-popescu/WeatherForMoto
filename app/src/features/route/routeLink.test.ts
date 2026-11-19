import { describe, expect, it } from 'vitest';
import { distanceKm } from '../../lib/geo';
import { SHARE_BASE_URL } from '../../lib/placeLink';
import {
  buildRouteLink,
  decodeLine,
  departureUsable,
  encodeLine,
  lineForLink,
  MAX_LINK_POINTS,
  parseRouteLink,
  simplifyLine,
  trackFromLink,
  type ShareStop,
} from './routeLink';
import type { LatLon } from './types';

const STOPS: ShareStop[] = [
  { name: 'Floreasca', lat: 44.465123, lon: 26.101987, gps: true },
  { name: 'Sâmbăta de Sus, Brașov', lat: 45.7656, lon: 24.8143, gps: false },
  { name: 'Sibiu', lat: 45.7983, lon: 24.1256, gps: false },
];
const DEPARTURE = { date: '2026-11-22', time: '08:30' };

/** The part after the "#", as the app reads it from window.location.hash. */
const hashOf = (url: string): string => url.slice(url.indexOf('#'));

/** A wiggly road of `count` points from Bucharest towards Sibiu, about 290 km. */
function road(count: number): LatLon[] {
  return Array.from({ length: count }, (_, i) => {
    const t = i / (count - 1);
    return { lat: 44.43 + 1.37 * t + 0.02 * Math.sin(t * 90), lon: 26.1 - 1.98 * t + 0.02 * Math.cos(t * 130) };
  });
}

describe('route link', () => {
  it('carries the stops, the departure and the pace there and back', () => {
    const url = buildRouteLink({ stops: STOPS, departure: DEPARTURE, speed: 90, line: null });
    expect(url.startsWith(`${SHARE_BASE_URL}#/traseu?r=1&`)).toBe(true);
    const parsed = parseRouteLink(hashOf(url));
    expect(parsed.kind).toBe('route');
    if (parsed.kind !== 'route') return;
    expect(parsed.link.departure).toEqual(DEPARTURE);
    expect(parsed.link.speed).toBe(90);
    expect(parsed.link.line).toBeNull();
    expect(parsed.link.stops.map((s) => s.name)).toEqual(['Floreasca', 'Sâmbăta de Sus, Brașov', 'Sibiu']);
    expect(parsed.link.stops[2]).toEqual({ name: 'Sibiu', lat: 45.7983, lon: 24.1256 });
  });

  it('shares a GPS position only to about a kilometre', () => {
    const parsed = parseRouteLink(hashOf(buildRouteLink({ stops: STOPS, departure: DEPARTURE, speed: 75, line: null })));
    if (parsed.kind !== 'route') throw new Error('expected a route');
    expect(parsed.link.stops[0]).toMatchObject({ lat: 44.47, lon: 26.1 });
    expect(distanceKm(parsed.link.stops[0], STOPS[0])).toBeLessThan(1);
    expect(parsed.link.stops[1]).toMatchObject({ lat: 45.7656, lon: 24.8143 });
  });

  it('keeps names that look like separators', () => {
    const odd = 'A~B & C+D (vechi) 100%';
    const url = buildRouteLink({ stops: [{ ...STOPS[2], name: odd }, STOPS[1]], departure: DEPARTURE, speed: 75, line: null });
    // Nothing a chat app might cut the link at.
    expect(hashOf(url)).not.toMatch(/[ ()!'*]/);
    const parsed = parseRouteLink(hashOf(url));
    expect(parsed.kind === 'route' && parsed.link.stops[0].name).toBe(odd);
  });

  it('ignores an address that is not a route link', () => {
    expect(parseRouteLink('').kind).toBe('none');
    expect(parseRouteLink('#/traseu').kind).toBe('none');
    expect(parseRouteLink('#/acum?r=1&p=1~2~A&p=3~4~B').kind).toBe('none');
    expect(parseRouteLink('#/traseu?foo=1').kind).toBe('none');
  });

  it('refuses a link it cannot trust', () => {
    const stop = (name: string): string => `p=45~25~${name}`;
    for (const hash of [
      `#/traseu?r=2&${stop('A')}&${stop('B')}`, // a newer app's link
      `#/traseu?r=1&${stop('A')}`, // one stop
      `#/traseu?r=1&${Array.from({ length: 11 }, (_, i) => stop(`S${i}`)).join('&')}`, // eleven
      '#/traseu?r=1&p=95~25~A&p=45~25~B', // off the planet
      '#/traseu?r=1&p=45~25~&p=45~25~B', // no name
      '#/traseu?r=1&p=4e1~25~A&p=45~25~B', // not a plain number
      `#/traseu?r=1&${stop('A')}&${stop('B')}&n=2&l=***`, // damaged road
    ]) {
      expect(parseRouteLink(hash).kind, hash).toBe('invalid');
    }
  });

  it('drops a departure or a pace the planner cannot show, and keeps the rest', () => {
    for (const t of ['2026-02-31T08:00', '2026-11-22T08:15', '2026-11-22T24:00', 'mâine']) {
      const parsed = parseRouteLink(`#/traseu?r=1&t=${t}&k=200&p=45~25~A&p=46~24~B`);
      expect(parsed.kind === 'route' && parsed.link.departure, t).toBeNull();
      expect(parsed.kind === 'route' && parsed.link.speed).toBeNull();
    }
  });
});

describe('the road in a link', () => {
  it('survives the trip to about a metre', () => {
    const line = road(300);
    const back = decodeLine(encodeLine(line), line.length);
    expect(back).toHaveLength(line.length);
    back?.forEach((p, i) => expect(distanceKm(p, line[i])).toBeLessThan(0.002));
  });

  it('notices a link cut short, even between two points', () => {
    const text = encodeLine(road(50));
    expect(decodeLine(text, 51)).toBeNull();
    expect(decodeLine(text.slice(0, -1), 50)).toBeNull();
    expect(decodeLine(text, 1)).toBeNull();
  });

  it('drops what a straight stretch does not need and keeps the bends', () => {
    const straight = Array.from({ length: 100 }, (_, i) => ({ lat: 45 + i * 0.001, lon: 25 }));
    expect(simplifyLine(straight, 50)).toHaveLength(2);
    const bend = [{ lat: 45, lon: 25 }, { lat: 45.01, lon: 25 }, { lat: 45.01, lon: 25.01 }];
    expect(simplifyLine(bend, 50)).toHaveLength(3);
  });

  it('fits a long recording into a link a chat app will carry', () => {
    const recording = road(20_000);
    const line = lineForLink(recording);
    expect(line.length).toBeLessThanOrEqual(MAX_LINK_POINTS);
    const url = buildRouteLink({ stops: STOPS, departure: DEPARTURE, speed: 75, line: recording });
    expect(url.length).toBeLessThan(6000);
    const parsed = parseRouteLink(hashOf(url));
    expect(parsed.kind === 'route' && parsed.link.line?.length).toBe(line.length);
  });

  it('becomes a route with every stop placed in order along it', () => {
    const line = road(400);
    const stops = [
      { name: 'A', ...line[0] },
      { name: 'B', ...line[250] },
      { name: 'C', ...line[100] }, // listed after B but lies before it
      { name: 'D', ...line[399] },
    ];
    const route = trackFromLink(line, stops);
    expect(route).not.toBeNull();
    if (!route) return;
    expect(route.stopKm[0]).toBe(0);
    expect(route.stopKm[3]).toBe(route.distanceKm);
    expect(route.stopKm[2]).toBe(route.stopKm[1]);
    expect(route.roads).toEqual([]);
    expect(trackFromLink([line[0], line[0]], stops)).toBeNull();
  });
});

describe('departureUsable', () => {
  const dates = ['2026-11-19', '2026-11-20', '2026-11-21'];

  it('takes a departure still ahead on one of the planner days', () => {
    expect(departureUsable({ date: '2026-11-19', time: '18:00' }, dates, '2026-11-19T17:40')).toBe(true);
    expect(departureUsable({ date: '2026-11-21', time: '06:00' }, dates, '2026-11-19T17:40')).toBe(true);
  });

  it('refuses one that has gone, or lies past the forecast', () => {
    expect(departureUsable({ date: '2026-11-19', time: '17:30' }, dates, '2026-11-19T17:40')).toBe(false);
    expect(departureUsable({ date: '2026-11-18', time: '09:00' }, dates, '2026-11-19T17:40')).toBe(false);
    expect(departureUsable({ date: '2026-11-25', time: '09:00' }, dates, '2026-11-19T17:40')).toBe(false);
  });
});
