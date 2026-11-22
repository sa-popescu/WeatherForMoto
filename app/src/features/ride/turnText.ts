import { fmtNumber } from '../../lib/format';
import { fmt, type Lang } from '../../lib/i18n';
import type { RouteTurn, TurnCall } from './logic/turns';
import { TURN_S } from './turnStrings';
import { spokenDistance } from './voice';

// A turn in words: the action from TomTom's maneuver code (TomTom writes no
// Romanian, so the app words it itself), the road it leads onto, and the
// sentence for each moment it is said. Also the shape of the arrow on screen.

export type TurnShape =
  | 'right'
  | 'left'
  | 'bear-right'
  | 'bear-left'
  | 'sharp-right'
  | 'sharp-left'
  | 'uturn'
  | 'roundabout'
  | 'straight'
  | 'merge'
  | 'arrive'
  | 'ferry';

const SHAPE: Record<string, TurnShape> = {
  TURN_RIGHT: 'right',
  TURN_LEFT: 'left',
  BEAR_RIGHT: 'bear-right',
  KEEP_RIGHT: 'bear-right',
  MOTORWAY_EXIT_RIGHT: 'bear-right',
  TAKE_EXIT: 'bear-right',
  SWITCH_PARALLEL_ROAD: 'bear-right',
  BEAR_LEFT: 'bear-left',
  KEEP_LEFT: 'bear-left',
  MOTORWAY_EXIT_LEFT: 'bear-left',
  SWITCH_MAIN_ROAD: 'bear-left',
  SHARP_RIGHT: 'sharp-right',
  SHARP_LEFT: 'sharp-left',
  MAKE_UTURN: 'uturn',
  TRY_MAKE_UTURN: 'uturn',
  ROUNDABOUT_BACK: 'uturn',
  ROUNDABOUT_CROSS: 'roundabout',
  ROUNDABOUT_RIGHT: 'roundabout',
  ROUNDABOUT_LEFT: 'roundabout',
  ENTER_MOTORWAY: 'merge',
  ENTER_FREEWAY: 'merge',
  ENTER_HIGHWAY: 'merge',
  ENTRANCE_RAMP: 'merge',
  ARRIVE_RIGHT: 'arrive',
  ARRIVE_LEFT: 'arrive',
  ARRIVE: 'arrive',
  TAKE_FERRY: 'ferry',
};

export function turnShape(maneuver: string): TurnShape {
  return SHAPE[maneuver] ?? 'straight';
}

const EXITS = new Set(['TAKE_EXIT', 'MOTORWAY_EXIT_RIGHT', 'MOTORWAY_EXIT_LEFT', 'KEEP_RIGHT', 'KEEP_LEFT', 'BEAR_RIGHT', 'BEAR_LEFT']);
const ENTRIES = new Set(['ENTER_MOTORWAY', 'ENTER_FREEWAY', 'ENTER_HIGHWAY', 'ENTRANCE_RAMP']);

function ordinal(n: number, lang: Lang): string {
  const s = TURN_S[lang];
  const words = [s.nth1, s.nth2, s.nth3, s.nth4, s.nth5, s.nth6];
  return words[n - 1] ?? fmt(s.nthMany, { n });
}

/** The bare action: "virează la dreapta", "la sensul giratoriu, ia a doua ieșire". */
export function turnAction(turn: RouteTurn, lang: Lang): string {
  const s = TURN_S[lang];
  const exitNo = turn.exit?.trim();
  switch (turn.maneuver) {
    case 'TURN_RIGHT':
      return s.turnRight;
    case 'TURN_LEFT':
      return s.turnLeft;
    case 'BEAR_RIGHT':
      return s.bearRight;
    case 'BEAR_LEFT':
      return s.bearLeft;
    case 'SHARP_RIGHT':
      return s.sharpRight;
    case 'SHARP_LEFT':
      return s.sharpLeft;
    case 'KEEP_RIGHT':
      return s.keepRight;
    case 'KEEP_LEFT':
      return s.keepLeft;
    case 'MAKE_UTURN':
    case 'TRY_MAKE_UTURN':
      return s.uturn;
    case 'ROUNDABOUT_CROSS':
    case 'ROUNDABOUT_RIGHT':
    case 'ROUNDABOUT_LEFT':
    case 'ROUNDABOUT_BACK':
      if (turn.roundaboutExit && turn.roundaboutExit > 0) return fmt(s.roundaboutExit, { nth: ordinal(turn.roundaboutExit, lang) });
      if (turn.maneuver === 'ROUNDABOUT_CROSS') return s.roundaboutCross;
      if (turn.maneuver === 'ROUNDABOUT_RIGHT') return s.roundaboutRight;
      return turn.maneuver === 'ROUNDABOUT_LEFT' ? s.roundaboutLeft : s.roundaboutBack;
    case 'ENTER_MOTORWAY':
    case 'ENTER_FREEWAY':
    case 'ENTER_HIGHWAY':
    case 'ENTRANCE_RAMP':
      return turn.road ? fmt(s.enterRoad, { road: turn.road.split(',')[0].trim() }) : s.enterMotorway;
    case 'TAKE_EXIT':
      return exitNo ? fmt(s.exitNumber, { exit: exitNo }) : s.exitRight;
    case 'MOTORWAY_EXIT_RIGHT':
      return exitNo ? fmt(s.exitNumber, { exit: exitNo }) : s.exitRight;
    case 'MOTORWAY_EXIT_LEFT':
      return exitNo ? fmt(s.exitNumber, { exit: exitNo }) : s.exitLeft;
    case 'SWITCH_PARALLEL_ROAD':
      return s.parallel;
    case 'SWITCH_MAIN_ROAD':
      return s.mainRoad;
    case 'TAKE_FERRY':
      return s.ferry;
    case 'ARRIVE_RIGHT':
      return s.arriveRight;
    case 'ARRIVE_LEFT':
      return s.arriveLeft;
    default:
      return s.straight;
  }
}

/** The action with where it leads: "virează la dreapta pe Strada Lipscani", "ia ieșirea 12 spre Pitești". */
export function turnPhrase(turn: RouteTurn, lang: Lang): string {
  const s = TURN_S[lang];
  const action = turnAction(turn, lang);
  if (turn.maneuver.startsWith('ARRIVE') || ENTRIES.has(turn.maneuver)) return action;
  const sign = turn.signpost?.trim();
  if (EXITS.has(turn.maneuver) && sign) return fmt(s.toward, { action, sign });
  const street = (turn.street ?? turn.road?.split(',')[0] ?? '').trim();
  return street ? fmt(s.onto, { action, street }) : action;
}

export function capitalize(text: string): string {
  return text.charAt(0).toLocaleUpperCase() + text.slice(1);
}

/**
 * What to say for a call, or null when it is better left unsaid: an arrival
 * is only announced ahead, since "Ai ajuns" follows at the door.
 */
export function turnSentence(call: TurnCall, lang: Lang): string | null {
  const s = TURN_S[lang];
  if (call.turn.maneuver.startsWith('ARRIVE') && call.stage !== 'prepare') return null;
  if (call.stage === 'now') {
    const phrase = turnPhrase(call.turn, lang);
    const action = call.then ? fmt(s.sayThen, { action: phrase, next: turnAction(call.then, lang) }) : phrase;
    return capitalize(fmt(s.sayNow, { action }));
  }
  return fmt(s.sayIn, { dist: spokenDistance(call.distanceM / 1000, lang), action: turnPhrase(call.turn, lang) });
}

/** The distance on the banner: "40 m", "250 m", "1,2 km", "12 km". */
export function turnDistanceText(metres: number, lang: Lang): string {
  if (metres < 1000) return `${fmtNumber(metres < 100 ? Math.round(metres / 10) * 10 : Math.round(metres / 50) * 50, lang)} m`;
  const km = metres / 1000;
  return `${fmtNumber(km, lang, km < 10 ? 1 : 0)} km`;
}
