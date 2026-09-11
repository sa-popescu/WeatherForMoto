import { describe, expect, it } from 'vitest';
import {
  closestFrameIndex,
  FRAME_HOLD_MS,
  FRAME_STEP_MS,
  frameClock,
  frameDelayMs,
  labelledTicks,
  latestObservedIndex,
  nextFrameIndex,
  parseRadarIndex,
  radarTileUrl,
  startFrameIndex,
  type RadarFrame,
} from './radar';

const HOST = 'https://tilecache.rainviewer.com';
const past = (time: number): RadarFrame => ({ time, path: `/v2/radar/p${time}`, nowcast: false });
const cast = (time: number): RadarFrame => ({ time, path: `/v2/radar/n${time}`, nowcast: true });

describe('parseRadarIndex', () => {
  it('returns past and nowcast frames in time order', () => {
    const parsed = parseRadarIndex({
      host: HOST,
      radar: {
        past: [{ time: 1200, path: '/v2/radar/b' }, { time: 600, path: '/v2/radar/a' }],
        nowcast: [{ time: 1800, path: '/v2/radar/c' }],
      },
    });
    expect(parsed?.host).toBe(HOST);
    expect(parsed?.frames.map((f) => [f.time, f.nowcast])).toEqual([
      [600, false],
      [1200, false],
      [1800, true],
    ]);
  });

  it('drops malformed frames and keeps the observed copy of a duplicate time', () => {
    const parsed = parseRadarIndex({
      host: HOST,
      radar: {
        past: [{ time: 600, path: '/v2/radar/a' }, { time: 'x', path: '/v2/radar/b' }, { time: 700, path: 'https://evil.example/x' }, null],
        nowcast: [{ time: 600, path: '/v2/radar/dup' }, { time: 800, path: '/../../x' }],
      },
    });
    expect(parsed?.frames).toEqual([{ time: 600, path: '/v2/radar/a', nowcast: false }]);
  });

  it('works without a nowcast list, as the free API answers today', () => {
    expect(parseRadarIndex({ host: HOST, radar: { past: [{ time: 1, path: '/v2/radar/a' }] } })?.frames).toHaveLength(1);
  });

  it('rejects unsafe hosts, empty lists and junk', () => {
    const frames = { past: [{ time: 1, path: '/v2/radar/a' }] };
    expect(parseRadarIndex({ host: 'javascript:alert(1)', radar: frames })).toBeNull();
    expect(parseRadarIndex({ host: 'https://evil.example/x?', radar: frames })).toBeNull();
    expect(parseRadarIndex({ host: HOST, radar: { past: [], nowcast: [] } })).toBeNull();
    expect(parseRadarIndex(null)).toBeNull();
    expect(parseRadarIndex('nope')).toBeNull();
  });
});

describe('choosing frames', () => {
  const frames = [past(0), past(600), past(1200), cast(1800), cast(2400)];

  it('starts on the observed frame closest to now', () => {
    expect(startFrameIndex(frames, 1300)).toBe(2);
    expect(startFrameIndex(frames, 650)).toBe(1);
    // A forecast frame is closer here, but observed data wins.
    expect(startFrameIndex(frames, 1790)).toBe(2);
  });

  it('falls back to the closest forecast when nothing was observed', () => {
    expect(startFrameIndex([cast(100), cast(700)], 650)).toBe(1);
    expect(startFrameIndex([], 650)).toBe(-1);
  });

  it('breaks ties toward the earlier frame', () => {
    expect(closestFrameIndex(frames, 300)).toBe(0);
    expect(closestFrameIndex([], 300)).toBe(-1);
  });

  it('finds the newest observed frame', () => {
    expect(latestObservedIndex(frames)).toBe(2);
    expect(latestObservedIndex([cast(1)])).toBe(-1);
  });
});

describe('playback helpers', () => {
  it('wraps around the frame list', () => {
    expect(nextFrameIndex(4, 5)).toBe(0);
    expect(nextFrameIndex(1, 5)).toBe(2);
    expect(nextFrameIndex(0, 0)).toBe(-1);
  });

  it('holds the "now" frame and the last one longer', () => {
    expect(frameDelayMs(2, 5, 2)).toBe(FRAME_HOLD_MS);
    expect(frameDelayMs(4, 5, 2)).toBe(FRAME_HOLD_MS);
    expect(frameDelayMs(1, 5, 2)).toBe(FRAME_STEP_MS);
  });

  it('labels the last tick and every third one before it', () => {
    expect(labelledTicks(13, 3)).toEqual([0, 3, 6, 9, 12]);
    expect(labelledTicks(14, 3)).toEqual([1, 4, 7, 10, 13]);
    expect(labelledTicks(1, 3)).toEqual([0]);
  });
});

describe('tile URL and clock', () => {
  it('builds 256 px Universal Blue tile URLs', () => {
    expect(radarTileUrl(HOST, past(1))).toBe(`${HOST}/v2/radar/p1/256/{z}/{x}/{y}/2/1_1.png`);
  });

  it('shows the frame time on a 24 h clock in the given timezone', () => {
    const t = Date.UTC(2026, 8, 11, 14, 20) / 1000;
    expect(frameClock(t, 'ro', 'UTC')).toBe('14:20');
    expect(frameClock(t, 'en', 'Europe/Bucharest')).toBe('17:20');
  });
});
