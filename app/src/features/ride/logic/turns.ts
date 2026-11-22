import type { UrbanInstruction } from '../../../lib/types';

// Turn by turn on a route from "Drum": which turn comes next, and when to say
// it. A turn is said up to three times: early on a fast road (about 2 km
// ahead), to prepare (about 30 seconds ahead), and at the turn itself. Each
// is said once; one coming late (the GPS jumped, the app came back from the
// background) replaces the ones before it. Pure, so it is tested on its own.

/** A turn as the ride keeps it: where along the route, what to do, and onto what. */
export interface RouteTurn {
  /** Distance along the route, km (as RideRoute.cumKm). */
  km: number;
  /** TomTom's code: TURN_LEFT, ROUNDABOUT_RIGHT, TAKE_EXIT… */
  maneuver: string;
  street: string | null;
  road: string | null;
  exit: string | null;
  roundaboutExit: number | null;
  signpost: string | null;
}

export type TurnStage = 'early' | 'prepare' | 'now';

const STAGE_ORDER: Record<TurnStage, number> = { early: 0, prepare: 1, now: 2 };

/** Codes not worth a word: the start, a road changing name, waypoints. The arrival has its own sentence. */
const SILENT = new Set(['DEPART', 'STRAIGHT', 'FOLLOW', 'ARRIVE', 'WAYPOINT_LEFT', 'WAYPOINT_RIGHT', 'WAYPOINT_REACHED']);

export function turnsFromInstructions(instructions: readonly UrbanInstruction[]): RouteTurn[] {
  return instructions
    .filter((ins) => !SILENT.has(ins.maneuver) && ins.maneuver !== '')
    .map((ins) => ({
      km: ins.offset_m / 1000,
      maneuver: ins.maneuver,
      street: ins.street,
      road: ins.road,
      exit: ins.exit,
      roundaboutExit: ins.roundabout_exit,
      signpost: ins.signpost,
    }));
}

/** A turn this close behind the progress still counts as ahead: the projection lags a little. */
const PASSED_KM = 0.01;
/** "Now": within this many seconds, and never further than NOW_MAX_M or nearer than NOW_MIN_M. */
const NOW_S = 6;
const NOW_MIN_M = 35;
const NOW_MAX_M = 150;
/** "Prepare": about this many seconds ahead, between these distances. */
const PREPARE_S = 30;
const PREPARE_MIN_M = 150;
const PREPARE_MAX_M = 1200;
/** "Early": on a fast road, with a long way since the turn before. */
const EARLY_M = 2000;
const EARLY_MIN_SPEED_MS = 70 / 3.6;
const EARLY_MIN_LEG_M = 3000;
/** A second turn this soon after the first is said with it: "…, apoi la stânga". */
export const THEN_M = 150;

export interface NextTurn {
  index: number;
  turn: RouteTurn;
  distanceM: number;
  /** A turn that follows right after this one, or null. */
  then: RouteTurn | null;
}

export function nextTurn(turns: readonly RouteTurn[], progressKm: number): NextTurn | null {
  const index = turns.findIndex((t) => t.km > progressKm + PASSED_KM);
  if (index < 0) return null;
  const turn = turns[index];
  const after = turns[index + 1];
  return {
    index,
    turn,
    distanceM: Math.max(0, (turn.km - progressKm) * 1000),
    then: after && (after.km - turn.km) * 1000 <= THEN_M ? after : null,
  };
}

/** Which announcement the distance calls for at this speed, or null when it is too early for any. */
export function turnStage(distanceM: number, speedMs: number, legM: number): TurnStage | null {
  const speed = Math.max(0, speedMs);
  if (distanceM <= Math.min(NOW_MAX_M, Math.max(NOW_MIN_M, speed * NOW_S))) return 'now';
  if (distanceM <= Math.min(PREPARE_MAX_M, Math.max(PREPARE_MIN_M, speed * PREPARE_S))) return 'prepare';
  if (speed >= EARLY_MIN_SPEED_MS && legM >= EARLY_MIN_LEG_M && distanceM <= EARLY_M) return 'early';
  return null;
}

/** What has been said, by turn index: the furthest stage. */
export type TurnsSaid = ReadonlyMap<number, TurnStage>;

export interface TurnCall extends NextTurn {
  stage: TurnStage;
}

/**
 * The next turn, and the announcement to make now if there is one not made
 * yet. `said` is updated by the caller with the returned call.
 */
export function turnToSay(turns: readonly RouteTurn[], progressKm: number, speedMs: number, said: TurnsSaid): { next: NextTurn | null; call: TurnCall | null } {
  const next = nextTurn(turns, progressKm);
  if (!next) return { next: null, call: null };
  const previousKm = next.index > 0 ? turns[next.index - 1].km : 0;
  const stage = turnStage(next.distanceM, speedMs, (next.turn.km - previousKm) * 1000);
  if (!stage) return { next, call: null };
  const done = said.get(next.index);
  if (done !== undefined && STAGE_ORDER[done] >= STAGE_ORDER[stage]) return { next, call: null };
  return { next, call: { ...next, stage } };
}
