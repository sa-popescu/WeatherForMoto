import { fmt, type Lang } from '../../lib/i18n';
import type { LatLon } from '../route/types';
import { aheadSentence } from './aheadText';
import { warningAhead, type AheadWarning } from './logic/ahead';
import { NEW_FOLLOW, projectOnRoute, stepFollow, type FollowState, type RideRoute } from './logic/follow';
import type { RideFix } from './logic/track';
import { turnToSay, type NextTurn, type TurnStage } from './logic/turns';
import { RIDE_S } from './strings';
import { turnSentence } from './turnText';
import { TURN_S } from './turnStrings';
import { spokenDistance } from './voice';

// The ride's guide along a planned route: keeps the progress, and turns the
// moments that matter (off the route, back on it, arrived, the next turn,
// weather ahead) into sentences. Saying them is the recorder's job, so the
// voice can be muted in one place.

/** While off the route, a reminder this often. */
const OFF_REMINDER_MS = 60_000;
/** A warning ahead already said is said again once it is this close. */
const NEAR_MIN = 10;

export class RouteGuide {
  follow: FollowState = NEW_FOLLOW;
  route: RideRoute;
  private remindAt: number | null = null;
  /** Warnings ahead already said: once when they show up, once more when near. */
  private announced = new Map<string, 'far' | 'near'>();
  /** Turns already announced, by index: the furthest stage said. */
  private turnsSaid = new Map<number, TurnStage>();

  constructor(route: RideRoute) {
    this.route = route;
  }

  /** Leaving this route asks for a new one (routes from "Drum"). */
  get reroutable(): boolean {
    return this.route.destination !== undefined;
  }

  /** Swaps in a new route from where the rider is; the turns start over. */
  replaceRoute(route: RideRoute, at: LatLon): void {
    this.route = route;
    this.turnsSaid = new Map();
    this.remindAt = null;
    const p = projectOnRoute(route, at, null);
    this.follow = { ...NEW_FOLLOW, km: p.km, offM: p.offM };
  }

  newRouteSentence(lang: Lang): string {
    return fmt(TURN_S[lang].sayNewRoute, { dist: spokenDistance(this.remainingKm(), lang), name: this.route.name });
  }

  /** Picks the progress up from a stored position, saying nothing. */
  resumeAt(p: { lat: number; lon: number }): void {
    const at = projectOnRoute(this.route, p, null);
    this.follow = { ...NEW_FOLLOW, km: at.km, offM: at.offM };
  }

  remainingKm(): number {
    return Math.max(0, this.route.distanceKm - (this.follow.km ?? 0));
  }

  startSentence(lang: Lang): string {
    return fmt(RIDE_S[lang].sayStartRoute, { dist: spokenDistance(this.remainingKm(), lang), name: this.route.name });
  }

  /** Feeds one fix; returns what to say, if anything. */
  onFix(fix: RideFix, lang: Lang): string | null {
    const { state, event } = stepFollow(this.follow, this.route, fix);
    this.follow = state;
    const s = RIDE_S[lang];
    if (event === 'off') {
      this.remindAt = fix.t + OFF_REMINDER_MS;
      // A route from "Drum" is looked for again right away (the recorder does it).
      return this.reroutable ? TURN_S[lang].sayRerouting : s.sayOff;
    }
    if (event === 'back') {
      this.remindAt = null;
      return fmt(s.sayBack, { dist: spokenDistance(this.remainingKm(), lang) });
    }
    if (event === 'arrived') return fmt(s.sayArrived, { name: this.route.name });
    if (state.off && this.remindAt !== null && fix.t >= this.remindAt) {
      this.remindAt = fix.t + OFF_REMINDER_MS;
      return s.sayStillOff;
    }
    return null;
  }

  /**
   * The weather ahead at `speedKmh`, and a sentence the first time a warning
   * shows up, then once more when it is close, never every second. With
   * `canSpeak` false (something else is being said) the sentence waits.
   */
  weatherAhead(speedKmh: number, now: number, lang: Lang, canSpeak: boolean): { warning: AheadWarning | null; sentence: string | null } {
    const points = this.route.ahead;
    if (!points || points.length === 0 || this.follow.km === null || this.follow.arrived) return { warning: null, sentence: null };
    const warning = warningAhead(points, this.follow.km, speedKmh, now);
    if (!warning || !canSpeak) return { warning, sentence: null };
    const said = this.announced.get(warning.key);
    const near = warning.minutes <= NEAR_MIN;
    if (said === undefined || (said === 'far' && near)) {
      this.announced.set(warning.key, near ? 'near' : 'far');
      return { warning, sentence: aheadSentence(warning, lang) };
    }
    return { warning, sentence: null };
  }

  /**
   * The next turn, to show, and a sentence when one of its announcements is
   * due (logic/turns.ts). Nothing while off the route or after arriving.
   * `speedMs` is the speed right now.
   */
  turnAhead(speedMs: number, lang: Lang, canSpeak: boolean): { next: NextTurn | null; sentence: string | null } {
    const turns = this.route.turns;
    if (!turns || turns.length === 0 || this.follow.km === null || this.follow.off || this.follow.arrived) return { next: null, sentence: null };
    const { next, call } = turnToSay(turns, this.follow.km, speedMs, this.turnsSaid);
    // With something else being said, the announcement waits for the next fix.
    if (!call || !canSpeak) return { next, sentence: null };
    this.turnsSaid.set(call.index, call.stage);
    return { next, sentence: turnSentence(call, lang) };
  }
}
