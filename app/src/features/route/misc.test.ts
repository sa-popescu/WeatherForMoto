import { describe, expect, it } from 'vitest';
import { buildGpx, escapeXml, GpxError, gpxFileName, parseGpx, unescapeXml } from './gpx';
import { placeHazards, serverTimeMs } from './hazards';
import { maneuverCode, osrmInstructions, osrmUrl, parseOsrm, roadAt, RouteError } from './osrm';
import { runPool } from './pool';
import { rainLine } from './rainText';
import { addStop, MAX_STOPS, moveItem, nearestKnown, newStop, removeStop, roleOf, savedLabel } from './stops';
import { pickEvenly, planImport, thinLine } from './track';
import type { Hazard } from '../../lib/types';

describe('gpx', () => {
  const gpx = buildGpx({
    name: 'București → Sibiu & <back>',
    line: [
      { lat: 44.4268, lon: 26.1025 },
      { lat: 45.7983, lon: 24.1256 },
    ],
    waypoints: [{ lat: 44.4268, lon: 26.1025, name: 'București' }],
    createdIso: '2026-09-11T10:00:00.000Z',
  });

  it('writes a GPX 1.1 track with waypoints first', () => {
    expect(gpx.startsWith('<?xml version="1.0" encoding="UTF-8"?>')).toBe(true);
    expect(gpx).toContain('xmlns="http://www.topografix.com/GPX/1/1"');
    expect(gpx).toContain('<trkpt lat="45.798300" lon="24.125600"/>');
    expect(gpx.indexOf('<wpt')).toBeLessThan(gpx.indexOf('<trk>'));
  });

  it('escapes names', () => {
    expect(gpx).toContain('București → Sibiu &amp; &lt;back&gt;');
    expect(escapeXml(`"a'`)).toBe('&quot;a&apos;');
  });

  it('builds a plain file name', () => {
    expect(gpxFileName('București → Râmnicu Vâlcea')).toBe('bucuresti-ramnicu-valcea.gpx');
    expect(gpxFileName('→')).toBe('traseu.gpx');
  });
});

describe('gpx import', () => {
  it('reads tracks, routes and waypoints however the points are written', () => {
    const parsed = parseGpx(`<?xml version="1.0"?>
      <gpx version="1.1" xmlns:gpxx="http://www.garmin.com/xmlschemas/GpxExtensions/v3">
        <wpt lat="44.4268" lon="26.1025"><name>Bucure&#537;ti &amp; Co</name></wpt>
        <wpt lon='24.1256' lat='45.7983'><ele>415</ele><name><![CDATA[Sibiu <centru>]]></name></wpt>
        <!-- <wpt lat="1" lon="1"><name>commented out</name></wpt> -->
        <rte><rtept lat="44.5" lon="26.0"/></rte>
        <trk><trkseg>
          <trkpt lat="44.4268" lon="26.1025"/>
          <trkpt lat="44.9" lon="25.5"><ele>300</ele><time>2026-09-11T10:00:00Z</time></trkpt>
          <trkpt lat="95" lon="25"/>
          <trkpt lat="abc" lon="25"/>
        </trkseg><trkseg>
          <trkpt lon="24.1256" lat="45.7983"></trkpt>
        </trkseg></trk>
      </gpx>`);
    expect(parsed.waypoints).toEqual([
      { lat: 44.4268, lon: 26.1025, name: 'București & Co' },
      { lat: 45.7983, lon: 24.1256, name: 'Sibiu <centru>' },
    ]);
    expect(parsed.routePoints).toEqual([{ lat: 44.5, lon: 26, name: '' }]);
    expect(parsed.track).toEqual([
      { lat: 44.4268, lon: 26.1025 },
      { lat: 44.9, lon: 25.5 },
      { lat: 45.7983, lon: 24.1256 },
    ]);
    expect(unescapeXml('&lt;a&gt; &#x41;&quot;&unknown;')).toBe('<a> A"&unknown;');
  });

  it('gets back the road and the stops of its own export', () => {
    const line = [
      { lat: 44.4268, lon: 26.1025 },
      { lat: 44.8565, lon: 24.8692 },
      { lat: 45.3, lon: 24.5 },
      { lat: 45.7983, lon: 24.1256 },
    ];
    const waypoints = [
      { ...line[0], name: 'București' },
      { ...line[1], name: 'Pitești' },
      { ...line[3], name: 'Sibiu' },
    ];
    const plan = planImport(parseGpx(buildGpx({ name: 'București → Sibiu', line, waypoints, createdIso: '2026-09-11T10:00:00.000Z' })));
    if (plan.kind !== 'track') throw new Error('expected the track to be kept');
    expect(plan.route.coords).toEqual(line);
    expect(plan.route.roads).toEqual([]);
    expect(plan.stops.map((s) => s.name)).toEqual(['București', 'Pitești', 'Sibiu']);
    expect(plan.route.stopKm).toHaveLength(3);
    expect(plan.route.stopKm[0]).toBe(0);
    expect(plan.route.stopKm[1]).toBeCloseTo(plan.route.cumKm[1], 6);
    expect(plan.route.stopKm[2]).toBe(plan.route.distanceKm);
    expect(plan.route.distanceKm).toBeGreaterThan(200);
  });

  it('leaves the ends unnamed when no waypoint sits on them and ignores far waypoints', () => {
    const plan = planImport({
      track: [{ lat: 45, lon: 25 }, { lat: 45, lon: 25 }, { lat: 45.5, lon: 25 }, { lat: 46, lon: 25 }],
      routePoints: [],
      waypoints: [{ lat: 45.5, lon: 27, name: 'Far away' }, { lat: 45.5, lon: 25.001, name: 'Popas' }, { lat: 45.5, lon: 25, name: '' }],
    });
    if (plan.kind !== 'track') throw new Error('expected the track to be kept');
    expect(plan.route.coords).toHaveLength(3);
    expect(plan.stops.map((s) => s.name)).toEqual([null, 'Popas', null]);
  });

  it('keeps a route drawn point by point, but routes between a few via points', () => {
    const dense = Array.from({ length: 50 }, (_, i) => ({ lat: 45 + i * 0.005, lon: 25, name: '' }));
    expect(planImport({ track: [], routePoints: dense, waypoints: [] }).kind).toBe('track');

    const sparse = Array.from({ length: 14 }, (_, i) => ({ lat: 44 + i * 0.2, lon: 25, name: i === 0 ? 'Start' : '' }));
    const plan = planImport({ track: [], routePoints: sparse, waypoints: [] });
    if (plan.kind !== 'stops') throw new Error('expected stops for the router');
    expect(plan.stops).toHaveLength(MAX_STOPS);
    expect(plan.stops[0]).toEqual({ lat: 44, lon: 25, name: 'Start' });
    expect(plan.stops[MAX_STOPS - 1]).toEqual({ lat: sparse[13].lat, lon: 25, name: null });
  });

  it('uses waypoints as stops when the file has nothing else', () => {
    const plan = planImport(parseGpx('<gpx><wpt lat="44.4" lon="26.1"><name>A</name></wpt><wpt lat="45.8" lon="24.1"><name>B</name></wpt></gpx>'));
    expect(plan).toEqual({ kind: 'stops', stops: [{ lat: 44.4, lon: 26.1, name: 'A' }, { lat: 45.8, lon: 24.1, name: 'B' }] });
  });

  it('rejects files without a route', () => {
    const empty = (xml: string): void => {
      expect(() => planImport(parseGpx(xml))).toThrow(GpxError);
    };
    empty('');
    empty('not xml at all <<<');
    empty('<kml><Placemark><coordinates>26.1,44.4</coordinates></Placemark></kml>');
    empty('<gpx><trk><trkseg><trkpt lat="45" lon="25"/></trkseg></trk></gpx>');
    empty('<gpx><trk><trkseg><trkpt lat="45" lon="25"/><trkpt lat="45" lon="25"/></trkseg></trk></gpx>');
  });

  it('thins very dense lines evenly and keeps both ends', () => {
    const line = Array.from({ length: 1001 }, (_, i) => ({ lat: 45 + i * 0.0001, lon: 25 }));
    const thin = thinLine(line, 101);
    expect(thin).toHaveLength(101);
    expect(thin[0]).toEqual(line[0]);
    expect(thin[50]).toEqual(line[500]);
    expect(thin[100]).toEqual(line[1000]);
    expect(pickEvenly([1, 2, 3], 5)).toEqual([1, 2, 3]);
  });
});

describe('rainLine', () => {
  it('always pairs chance, amount and intensity', () => {
    const slot = { precipitation_probability: 45, precipitation_mm: 1.2, rain_intensity: 'slaba' as const };
    expect(rainLine(slot, 'ro')).toEqual({ text: 'ploaie: 45% șanse · 1,2 mm/h, slabă', wet: true });
    expect(rainLine(slot, 'en').text).toBe('rain: 45% chance · 1.2 mm/h, light');
  });

  it('says there is no rain when nothing is measurable', () => {
    expect(rainLine({ precipitation_probability: 0, precipitation_mm: 0, rain_intensity: 'none' }, 'ro').text).toBe('fără ploaie');
    expect(rainLine({ precipitation_probability: 5, precipitation_mm: 0, rain_intensity: 'none' }, 'ro').text).toBe('fără ploaie (5% șanse, 0 mm)');
  });

  it('falls back to the amount when the chance is missing', () => {
    expect(rainLine({ precipitation_probability: null, precipitation_mm: 3.4, rain_intensity: null }, 'ro').text).toBe('ploaie: 3,4 mm/h, moderată');
  });
});

describe('osrm', () => {
  const body = {
    code: 'Ok',
    routes: [
      {
        distance: 222_400,
        geometry: { coordinates: [[26, 44], [27, 44], [28, 44]] },
        legs: [
          { distance: 111_200, steps: [{ distance: 60_000, ref: 'DN7;E81', name: 'Calea' }, { distance: 51_200, ref: 'DN7' }] },
          { distance: 111_200, steps: [{ distance: 111_200, name: 'Transalpina' }] },
        ],
      },
    ],
  };

  it('parses geometry, scaled distances, stops and road names', () => {
    const route = parseOsrm(body);
    expect(route.coords[1]).toEqual({ lat: 44, lon: 27 });
    expect(route.distanceKm).toBeCloseTo(222.4, 6);
    expect(route.cumKm[route.cumKm.length - 1]).toBeCloseTo(222.4, 6);
    expect(route.stopKm).toEqual([0, 111.2, 222.4]);
    expect(route.roads).toEqual([
      { fromKm: 0, toKm: 111.2, label: 'DN7' },
      { fromKm: 111.2, toKm: 222.4, label: 'Transalpina' },
    ]);
    expect(roadAt(route.roads, 150)).toBe('Transalpina');
  });

  it('rejects answers without a route', () => {
    expect(() => parseOsrm({ code: 'NoRoute' })).toThrow(RouteError);
    expect(() => parseOsrm(null)).toThrow(RouteError);
  });

  it('writes the turns in TomTom codes, with their distance along the route', () => {
    const step = (distance: number, type: string, modifier: string | undefined, over: Record<string, unknown> = {}) => ({ distance, maneuver: { type, modifier }, ...over });
    const legs = [
      { distance: 3000, steps: [step(1000, 'depart', undefined, { name: 'Calea Victoriei' }), step(2000, 'turn', 'right', { name: 'Strada Lipscani' }), step(0, 'arrive', 'left')] },
      {
        distance: 5000,
        steps: [
          step(500, 'depart', undefined),
          step(300, 'roundabout', 'right', { name: 'Bulevardul Unirii', maneuver: { type: 'roundabout', modifier: 'right', exit: 2 } }),
          step(700, 'on ramp', 'slight right'),
          step(3000, 'continue', 'straight', { ref: 'A1;E81' }),
          step(500, 'off ramp', 'slight right', { exits: '12', destinations: 'Pitești' }),
          step(0, 'arrive', 'right'),
        ],
      },
    ];
    const ins = osrmInstructions(legs);
    expect(ins.map((i) => i.maneuver)).toEqual([
      'DEPART',
      'TURN_RIGHT',
      'WAYPOINT_REACHED',
      'DEPART',
      'ROUNDABOUT_RIGHT',
      'ENTRANCE_RAMP',
      'STRAIGHT',
      'MOTORWAY_EXIT_RIGHT',
      'ARRIVE_RIGHT',
    ]);
    expect(ins[1]).toMatchObject({ offset_m: 1000, street: 'Strada Lipscani' });
    expect(ins[4]).toMatchObject({ offset_m: 3500, roundabout_exit: 2 });
    // The ramp names the motorway it leads to.
    expect(ins[5].road).toBe('A1');
    expect(ins[7]).toMatchObject({ exit: '12', signpost: 'Pitești', offset_m: 7500 });
    expect(maneuverCode('fork', 'slight left', true)).toBe('KEEP_LEFT');
    expect(maneuverCode('end of road', 'left', true)).toBe('TURN_LEFT');
    expect(maneuverCode('rotary', 'uturn', true)).toBe('ROUNDABOUT_BACK');
  });

  it('builds the request with lon,lat pairs', () => {
    expect(osrmUrl([{ lat: 44.4268, lon: 26.1025 }, { lat: 45.7983, lon: 24.1256 }])).toBe(
      'https://router.project-osrm.org/route/v1/driving/26.10250,44.42680;24.12560,45.79830?overview=full&geometries=geojson&steps=true',
    );
  });
});

describe('runPool', () => {
  it('never runs more than the limit at once and finishes everything', async () => {
    let running = 0;
    let peak = 0;
    const done: number[] = [];
    await runPool([1, 2, 3, 4, 5, 6, 7], 3, async (n) => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((r) => setTimeout(r, 5));
      done.push(n);
      running -= 1;
    });
    expect(peak).toBe(3);
    expect(done.sort()).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it('stops launching work after an abort', async () => {
    const controller = new AbortController();
    const started: number[] = [];
    await runPool([1, 2, 3, 4], 1, async (n) => {
      started.push(n);
      if (n === 2) controller.abort();
    }, controller.signal);
    expect(started).toEqual([1, 2]);
  });
});

describe('hazards', () => {
  const route = parseOsrm({ code: 'Ok', routes: [{ distance: 222_400, geometry: { coordinates: [[26, 44], [27, 44], [28, 44]] }, legs: [] }] });
  const hz = (id: number, lat: number, lon: number): Hazard => ({
    id, lat, lon, hazard_type: 'gravel', severity: 3, description: 'x', distance_km: 0, created_at: '2026-09-11T08:00:00', expires_at: '',
  });

  it('dedupes, drops far ones and sorts along the route', () => {
    const placed = placeHazards([hz(2, 44.05, 28), hz(1, 44, 26.1), hz(2, 44.05, 28), hz(3, 46, 27)], route);
    expect(placed.map((h) => h.id)).toEqual([1, 2]);
    expect(placed[1].routeKm).toBeCloseTo(222.4, 1);
  });

  it('treats timestamps without a zone as UTC', () => {
    expect(serverTimeMs('2026-09-11T08:00:00')).toBe(Date.UTC(2026, 8, 11, 8));
    expect(serverTimeMs('2026-09-11T08:00:00+00:00')).toBe(Date.UTC(2026, 8, 11, 8));
  });
});

describe('stops', () => {
  it('moves, adds before the destination and keeps 2 to 10 rows', () => {
    expect(moveItem(['a', 'b', 'c'], 2, 0)).toEqual(['c', 'a', 'b']);
    expect(moveItem(['a', 'b'], 0, 5)).toEqual(['a', 'b']);
    const two = [newStop({ name: 'A', lat: 1, lon: 1 }), newStop({ name: 'B', lat: 2, lon: 2 })];
    const three = addStop(two);
    expect(three.map((s) => s.text)).toEqual(['A', '', 'B']);
    expect(removeStop(two, two[0].id)).toHaveLength(2);
    let many = two;
    for (let i = 0; i < 20; i += 1) many = addStop(many);
    expect(many).toHaveLength(MAX_STOPS);
    expect(roleOf(0, 3)).toBe('origin');
    expect(roleOf(1, 3)).toBe('via');
    expect(roleOf(2, 3)).toBe('destination');
  });

  it('finds the nearest pinned stop, looking back before looking ahead', () => {
    const pinned = (name: string) => newStop({ name, lat: 45, lon: 25 });
    const list = [pinned('A'), newStop(), newStop(), pinned('D')];
    expect(nearestKnown(list, 1)?.name).toBe('A');
    expect(nearestKnown(list, 2)?.name).toBe('D');
    expect(nearestKnown([newStop(), newStop()], 0)).toBeNull();
  });

  it('saves villages with their county', () => {
    expect(savedLabel(newStop({ name: 'Sâmbăta', lat: 45.7, lon: 24.8 }, { region: 'Brașov' }))).toBe('Sâmbăta, Brașov');
    expect(savedLabel(newStop({ name: 'Sibiu', lat: 45.8, lon: 24.1 }))).toBe('Sibiu');
  });
});
