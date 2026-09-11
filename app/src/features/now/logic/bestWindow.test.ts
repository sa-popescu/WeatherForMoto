import { describe, expect, it } from 'vitest';
import { bestWindow, todayAndTomorrow } from './bestWindow';
import { day, hours } from './testFixtures';

const DAILY = [day('2026-09-11'), day('2026-09-12')];

describe('bestWindow', () => {
  it('picks the longest run of IDEAL daylight hours', () => {
    const scores = [90, 90, 70, 90, 91, 92, 90, 60];
    const w = bestWindow(hours('2026-09-11T08:00', scores.length, (i) => ({ moto_score: scores[i] })), DAILY);
    expect(w).toEqual({ from: '2026-09-11T11:00', to: '2026-09-11T15:00', avg: 91, hours: 4, strong: true });
  });

  it('falls back to OK hours when nothing reaches IDEAL', () => {
    const w = bestWindow(hours('2026-09-11T08:00', 5, () => ({ moto_score: 70 })), DAILY);
    expect(w).toMatchObject({ from: '2026-09-11T08:00', hours: 5, strong: false });
  });

  it('never extends into the night and ends at sunset', () => {
    const w = bestWindow(hours('2026-09-11T00:00', 24, () => ({ moto_score: 95 })), DAILY);
    expect(w).toMatchObject({ from: '2026-09-11T07:00', to: '2026-09-11T19:33', hours: 13 });
  });

  it('skips a window shorter than 90 minutes for a longer OK run', () => {
    const scores = [70, 90, 70, 72, 75, 50];
    const w = bestWindow(hours('2026-09-11T10:00', scores.length, (i) => ({ moto_score: scores[i] })), DAILY);
    expect(w).toEqual({ from: '2026-09-11T10:00', to: '2026-09-11T15:00', avg: 75, hours: 5, strong: false });
    expect(bestWindow(hours('2026-09-11T19:00', 1, () => ({ moto_score: 95 })), DAILY)).toBeNull();
  });

  it('returns null when no daylight hour is rideable', () => {
    expect(bestWindow(hours('2026-09-11T08:00', 6, () => ({ moto_score: 40 })), DAILY)).toBeNull();
  });

  it('splits today (from now) and tomorrow', () => {
    const hourly = hours('2026-09-11T00:00', 48, (i) => ({ moto_score: i < 24 ? (i >= 15 ? 88 : 50) : 72 }));
    const { today, tomorrow } = todayAndTomorrow(hourly, 12, DAILY);
    expect(today).toMatchObject({ from: '2026-09-11T15:00', strong: true });
    expect(tomorrow).toMatchObject({ from: '2026-09-12T07:00', strong: false, avg: 72 });
  });
});
