import { describe, expect, it } from 'vitest';
import { FRAME_HOLD_MS, FRAME_STEP_MS, type RadarFrame } from './radar';
import { FORECAST_STEP_MS } from './forecast';
import { buildTimeline, entryDelayMs, nextIndex, startIndex } from './timeline';

const HOUR = 3600;
const NOW = Date.parse('2026-09-13T12:00:00Z') / 1000;

const frame = (offsetMin: number, nowcast = false): RadarFrame => ({
  time: NOW + offsetMin * 60,
  path: `/v2/radar/${offsetMin}`,
  nowcast,
});

const RADAR: RadarFrame[] = [frame(-60), frame(-30), frame(0), frame(10, true), frame(30, true)];
const FORECAST_TIMES = ['2026-09-13T12:00', '2026-09-13T13:00', '2026-09-13T14:00', '2026-09-13T15:00'];

describe('buildTimeline', () => {
  it('runs from the oldest radar frame to the last forecast hour', () => {
    const entries = buildTimeline(RADAR, FORECAST_TIMES);
    expect(entries[0].timeSec).toBe(NOW - 60 * 60);
    expect(entries[entries.length - 1].timeSec).toBe(NOW + 3 * HOUR);
    const times = entries.map((e) => e.timeSec);
    expect([...times].sort((a, b) => a - b)).toEqual(times);
  });

  it('drops forecast hours the radar already covers', () => {
    const entries = buildTimeline(RADAR, FORECAST_TIMES);
    const forecastTimes = entries.filter((e) => e.source === 'forecast').map((e) => e.timeSec);
    // 12:00 and 12:30 are inside the radar's own window, so the band picks up at 13:00.
    expect(forecastTimes).toEqual([NOW + HOUR, NOW + 2 * HOUR, NOW + 3 * HOUR]);
  });

  it('keeps every forecast hour when there is no radar', () => {
    const entries = buildTimeline([], FORECAST_TIMES);
    expect(entries).toHaveLength(4);
    expect(entries.every((e) => e.source === 'forecast' && e.forecast)).toBe(true);
    expect(entries[0].index).toBe(0);
  });

  it('keeps the radar alone when the forecast has not arrived', () => {
    expect(buildTimeline(RADAR, [])).toHaveLength(RADAR.length);
  });

  it('ignores unparseable hours', () => {
    expect(buildTimeline([], ['not a time', '2026-09-13T13:00'])).toHaveLength(1);
  });

  it('marks nowcast frames as forecast', () => {
    const entries = buildTimeline(RADAR, []);
    expect(entries.filter((e) => e.forecast)).toHaveLength(2);
  });
});

describe('startIndex', () => {
  it('opens on the newest observed moment', () => {
    const entries = buildTimeline(RADAR, FORECAST_TIMES);
    expect(entries[startIndex(entries, NOW)].timeSec).toBe(NOW);
  });

  it('falls back to the entry closest to now when nothing was observed', () => {
    const entries = buildTimeline([], FORECAST_TIMES);
    expect(entries[startIndex(entries, NOW + 70 * 60)].timeSec).toBe(NOW + HOUR);
  });

  it('has nothing to open on an empty band', () => {
    expect(startIndex([], NOW)).toBe(-1);
  });
});

describe('playback pacing', () => {
  const entries = buildTimeline(RADAR, FORECAST_TIMES);

  it('holds the present and the end, and gives forecast hours more time', () => {
    const hold = startIndex(entries, NOW);
    expect(entryDelayMs(entries, hold, hold)).toBe(FRAME_HOLD_MS);
    expect(entryDelayMs(entries, entries.length - 1, hold)).toBe(FRAME_HOLD_MS);
    expect(entryDelayMs(entries, 0, hold)).toBe(FRAME_STEP_MS);
    const firstForecast = entries.findIndex((e) => e.source === 'forecast');
    expect(entryDelayMs(entries, firstForecast, hold)).toBe(FORECAST_STEP_MS);
  });

  it('wraps back to the beginning', () => {
    expect(nextIndex(entries.length - 1, entries.length)).toBe(0);
    expect(nextIndex(0, entries.length)).toBe(1);
    expect(nextIndex(0, 0)).toBe(-1);
  });
});
