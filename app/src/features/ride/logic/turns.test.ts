import { describe, expect, it } from 'vitest';
import type { UrbanInstruction } from '../../../lib/types';
import { capitalize, turnDistanceText, turnPhrase, turnSentence } from '../turnText';
import type { RideRoute } from './follow';
import { MAX_REROUTES, mayReroute, REROUTE_GAP_MS, remapAhead, reroutedRoute } from './reroute';
import { nextTurn, turnsFromInstructions, turnStage, turnToSay, type RouteTurn, type TurnStage } from './turns';

function ins(offsetM: number, maneuver: string, over: Partial<UrbanInstruction> = {}): UrbanInstruction {
  return { offset_m: offsetM, index: null, time_s: null, maneuver, street: null, road: null, exit: null, roundabout_exit: null, signpost: null, ...over };
}

function turn(km: number, maneuver: string, over: Partial<RouteTurn> = {}): RouteTurn {
  return { km, maneuver, street: null, road: null, exit: null, roundaboutExit: null, signpost: null, ...over };
}

const KMH50 = 50 / 3.6;
const KMH110 = 110 / 3.6;

describe('turnsFromInstructions', () => {
  it('keeps the turns and leaves out the start, name changes and the arrival', () => {
    const turns = turnsFromInstructions([
      ins(0, 'DEPART', { street: 'Bulevardul Unirii' }),
      ins(600, 'STRAIGHT'),
      ins(1200, 'TURN_RIGHT', { street: 'Calea Victoriei' }),
      ins(2000, 'ROUNDABOUT_RIGHT', { roundabout_exit: 2 }),
      ins(3000, 'ARRIVE_RIGHT'),
      ins(3000, 'ARRIVE'),
    ]);
    expect(turns.map((t) => t.maneuver)).toEqual(['TURN_RIGHT', 'ROUNDABOUT_RIGHT', 'ARRIVE_RIGHT']);
    expect(turns[0]).toMatchObject({ km: 1.2, street: 'Calea Victoriei' });
    expect(turns[1].roundaboutExit).toBe(2);
  });
});

describe('when a turn is said', () => {
  it('says it to prepare about half a minute ahead, then at the turn', () => {
    // In town, 50 km/h: prepare from about 400 m, at the turn from about 80 m.
    expect(turnStage(800, KMH50, 1000)).toBeNull();
    expect(turnStage(400, KMH50, 1000)).toBe('prepare');
    expect(turnStage(80, KMH50, 1000)).toBe('now');
    // Slow, it still prepares at 150 m and says it at 35 m.
    expect(turnStage(150, 2, 1000)).toBe('prepare');
    expect(turnStage(35, 2, 1000)).toBe('now');
  });

  it('adds an early word on a fast road after a long way with no turn', () => {
    expect(turnStage(1900, KMH110, 20_000)).toBe('early');
    expect(turnStage(1900, KMH110, 1500)).toBeNull();
    expect(turnStage(1900, KMH50, 20_000)).toBeNull();
    expect(turnStage(900, KMH110, 20_000)).toBe('prepare');
  });

  it('says each stage once, and never an earlier one after a later one', () => {
    const turns = [turn(1, 'TURN_RIGHT')];
    const said = new Map<number, TurnStage>();
    const at = (progressKm: number): TurnStage | null => {
      const { call } = turnToSay(turns, progressKm, KMH50, said);
      if (call) said.set(call.index, call.stage);
      return call?.stage ?? null;
    };
    expect(at(0.5)).toBeNull();
    expect(at(0.65)).toBe('prepare');
    expect(at(0.7)).toBeNull();
    // The GPS jumps past the prepare distance straight to the turn: only "now".
    expect(at(0.93)).toBe('now');
    expect(at(0.95)).toBeNull();
    // Past the turn there is nothing left to say.
    expect(at(1.05)).toBeNull();
  });

  it('knows the next turn, and a turn right after it', () => {
    const turns = [turn(1, 'TURN_RIGHT'), turn(1.1, 'TURN_LEFT'), turn(3, 'TURN_LEFT')];
    expect(nextTurn(turns, 0.5)).toMatchObject({ index: 0, distanceM: 500, then: { maneuver: 'TURN_LEFT', km: 1.1 } });
    expect(nextTurn(turns, 1.05)).toMatchObject({ index: 1, then: null });
    expect(nextTurn(turns, 3.2)).toBeNull();
  });
});

describe('turn words', () => {
  it('says what to do and onto which road, in Romanian', () => {
    const right = turn(1, 'TURN_RIGHT', { street: 'Strada Lipscani' });
    expect(turnSentence({ index: 0, turn: right, distanceM: 290, then: null, stage: 'prepare' }, 'ro')).toBe('În 300 de metri, virează la dreapta pe Strada Lipscani.');
    expect(turnSentence({ index: 0, turn: right, distanceM: 60, then: turn(1.1, 'TURN_LEFT'), stage: 'now' }, 'ro')).toBe(
      'Virează la dreapta pe Strada Lipscani, apoi virează la stânga.',
    );
    expect(turnPhrase(turn(2, 'ROUNDABOUT_RIGHT', { roundaboutExit: 2, street: 'Bulevardul Unirii' }), 'ro')).toBe(
      'la sensul giratoriu, ia a doua ieșire pe Bulevardul Unirii',
    );
    expect(turnPhrase(turn(2, 'MOTORWAY_EXIT_RIGHT', { exit: '12', signpost: 'Pitești' }), 'ro')).toBe('ia ieșirea 12 spre Pitești');
    expect(turnPhrase(turn(2, 'ENTER_MOTORWAY', { road: 'A1, E81' }), 'ro')).toBe('intră pe A1');
    expect(capitalize(turnPhrase(turn(1, 'MAKE_UTURN'), 'ro'))).toBe('Întoarce-te');
  });

  it('says the destination side only ahead of it, and works in English', () => {
    const arrive = turn(3, 'ARRIVE_RIGHT');
    expect(turnSentence({ index: 2, turn: arrive, distanceM: 200, then: null, stage: 'prepare' }, 'ro')).toBe('În 200 de metri, destinația e pe dreapta.');
    expect(turnSentence({ index: 2, turn: arrive, distanceM: 30, then: null, stage: 'now' }, 'ro')).toBeNull();
    expect(turnSentence({ index: 0, turn: turn(1, 'TURN_LEFT', { street: 'High Street' }), distanceM: 1900, then: null, stage: 'early' }, 'en')).toBe(
      'In 1.9 kilometres, turn left onto High Street.',
    );
  });

  it('writes the distance for the screen', () => {
    expect(turnDistanceText(43, 'ro')).toBe('40 m');
    expect(turnDistanceText(237, 'ro')).toBe('250 m');
    expect(turnDistanceText(1234, 'ro')).toBe('1,2 km');
    expect(turnDistanceText(12_600, 'ro')).toBe('13 km');
  });
});

describe('rerouting', () => {
  const line = (lat0: number): RideRoute['coords'] => Array.from({ length: 11 }, (_, i) => ({ lat: lat0 + i / 111.195, lon: 26.1 }));
  const old: RideRoute = {
    name: 'Brașov',
    coords: line(44.4),
    cumKm: Array.from({ length: 11 }, (_, i) => i),
    distanceKm: 10,
    plannedKmh: 60,
    destination: { lat: 44.49, lon: 26.1 },
    ahead: [
      { km: 2, name: 'A', lat: 44.418, lon: 26.1, utcOffsetSeconds: 7200, slots: [] },
      { km: 8, name: 'Far', lat: 45.5, lon: 26.1, utcOffsetSeconds: 7200, slots: [] },
      { km: 5, name: 'Old', utcOffsetSeconds: 7200, slots: [] },
    ],
  };

  it('keeps the destination and places the weather points along the new line', () => {
    // The new line starts 3 km further on, along the same road.
    const coords = line(44.4 + 3 / 111.195).slice(0, 8);
    const next = reroutedRoute(old, { coords, cumKm: coords.map((_, i) => i), distanceKm: 7, turns: [turn(1, 'TURN_LEFT')] });
    expect(next.destination).toEqual(old.destination);
    expect(next.name).toBe('Brașov');
    expect(next.turns).toHaveLength(1);
    // "A" is 1 km behind the new start: it lands at its beginning. "Far" and the point with no place are dropped.
    expect(next.ahead?.map((p) => p.name)).toEqual(['A']);
    expect(next.ahead?.[0].km).toBe(0);
    expect(remapAhead(undefined, next)).toEqual([]);
  });

  it('waits between tries and stops after a while', () => {
    expect(mayReroute(null, 0, 1000)).toBe(true);
    expect(mayReroute(1000, 1, 1000 + REROUTE_GAP_MS - 1)).toBe(false);
    expect(mayReroute(1000, 1, 1000 + REROUTE_GAP_MS)).toBe(true);
    expect(mayReroute(null, MAX_REROUTES, 1000)).toBe(false);
  });
});
