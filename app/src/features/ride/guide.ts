import { fmt, type Lang } from '../../lib/i18n';
import { NEW_FOLLOW, projectOnRoute, stepFollow, type FollowState, type RideRoute } from './logic/follow';
import type { RideFix } from './logic/track';
import { RIDE_S } from './strings';
import { spokenDistance } from './voice';

// The ride's guide along a planned route: keeps the progress, and turns the
// moments that matter (off the route, back on it, arrived) into sentences.
// Saying them is the recorder's job, so the voice can be muted in one place.

/** While off the route, a reminder this often. */
const OFF_REMINDER_MS = 60_000;

export class RouteGuide {
  follow: FollowState = NEW_FOLLOW;
  private remindAt: number | null = null;

  constructor(readonly route: RideRoute) {}

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
      return s.sayOff;
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
}
