import { describe, expect, it } from 'vitest';
import { FRAME_HOLD_MS, FRAME_STEP_MS, type RadarFrame } from './radar';
import { FORECAST_HOURS, FORECAST_STEP_MS } from './forecast';
import { NOWCAST_STEP_S, NOWCAST_STEPS } from './nowcast';
import { buildTimeline, entryDelayMs, MODEL_FINE_HOURS, MODEL_FINE_STEP_S, nextIndex, planBand, startIndex, utcTimeSec } from './timeline';

const HOUR = 3600;
const NOW = Date.parse('2026-09-13T12:00:00Z') / 1000;

const frame = (offsetMin: number, nowcast = false): RadarFrame => ({
  time: NOW + offsetMin * 60,
  path: `/v2/radar/${offsetMin}`,
  nowcast,
});

const RADAR: RadarFrame[] = [frame(-60), frame(-30), frame(0), frame(10, true), frame(30, true)];
const FORECAST = ['2026-09-13T12:00', '2026-09-13T13:00', '2026-09-13T14:00', '2026-09-13T15:00'].map(utcTimeSec);

describe('buildTimeline', () => {
  it('runs from the oldest radar frame to the last forecast hour', () => {
    const entries = buildTimeline(RADAR, [], FORECAST);
    expect(entries[0].timeSec).toBe(NOW - 60 * 60);
    expect(entries[entries.length - 1].timeSec).toBe(NOW + 3 * HOUR);
    const times = entries.map((e) => e.timeSec);
    expect([...times].sort((a, b) => a - b)).toEqual(times);
  });

  it('drops forecast hours the radar already covers', () => {
    const entries = buildTimeline(RADAR, [], FORECAST);
    const forecastTimes = entries.filter((e) => e.source === 'forecast').map((e) => e.timeSec);
    // 12:00 and 12:30 are inside the radar's own window, so the band picks up at 13:00.
    expect(forecastTimes).toEqual([NOW + HOUR, NOW + 2 * HOUR, NOW + 3 * HOUR]);
  });

  it('puts the extrapolated radar between the radar and the forecast', () => {
    const observed = [frame(-20), frame(-10), frame(0)];
    const nowcast = Array.from({ length: NOWCAST_STEPS }, (_, i) => NOW + (i + 1) * NOWCAST_STEP_S);
    const entries = buildTimeline(observed, nowcast, FORECAST);
    expect(entries.map((e) => e.source)).toEqual([
      ...Array<string>(3).fill('radar'),
      ...Array<string>(NOWCAST_STEPS).fill('nowcast'),
      'forecast',
      'forecast',
    ]);
    // The extrapolation reaches 13:30, so the forecast carries on from 14:00.
    expect(entries.filter((e) => e.source === 'forecast').map((e) => e.timeSec)).toEqual([NOW + 2 * HOUR, NOW + 3 * HOUR]);
    expect(entries.filter((e) => e.source === 'nowcast').every((e) => e.forecast)).toBe(true);
  });

  it('keeps every forecast hour when there is no radar', () => {
    const entries = buildTimeline([], [], FORECAST);
    expect(entries).toHaveLength(4);
    expect(entries.every((e) => e.source === 'forecast' && e.forecast)).toBe(true);
    expect(entries[0].index).toBe(0);
  });

  it('keeps the radar alone when the forecast has not arrived', () => {
    expect(buildTimeline(RADAR, [], [])).toHaveLength(RADAR.length);
  });

  it('ignores unparseable hours', () => {
    expect(buildTimeline([], [], ['not a time', '2026-09-13T13:00'].map(utcTimeSec))).toHaveLength(1);
  });

  it('marks RainViewer nowcast frames as forecast', () => {
    const entries = buildTimeline(RADAR, [], []);
    expect(entries.filter((e) => e.forecast)).toHaveLength(2);
  });
});

describe('planBand', () => {
  const base = { nowSec: NOW + 5 * 60, lastObservedSec: NOW, lastRadarSec: NOW };

  it('extrapolates for an hour and a half, then hands the rest of the day to the model', () => {
    const plan = planBand({ ...base, nowcastBaseSec: NOW, nowcastExpected: true });
    expect(plan.nowcastTimes).toEqual(Array.from({ length: NOWCAST_STEPS }, (_, i) => NOW + (i + 1) * NOWCAST_STEP_S));
    expect(plan.modelTimes[0]).toBe(NOW + 2 * HOUR);
    expect(plan.modelTimes[plan.modelTimes.length - 1]).toBe(NOW + FORECAST_HOURS * HOUR);
    // The frames that fade into the model need its totals from 13:00 on.
    expect(plan.hourEnds[0]).toBe(NOW + HOUR);
    expect(plan.hourEnds[plan.hourEnds.length - 1]).toBe(NOW + (FORECAST_HOURS + 1) * HOUR);
  });

  it('steps every half hour for the first hours, then every hour', () => {
    const { modelTimes } = planBand({ ...base, nowcastBaseSec: NOW, nowcastExpected: true });
    const fine = MODEL_FINE_HOURS * 2;
    expect(modelTimes.slice(0, fine + 1).map((t) => t - modelTimes[0])).toEqual(
      Array.from({ length: fine + 1 }, (_, i) => i * MODEL_FINE_STEP_S),
    );
    for (let i = fine + 1; i < modelTimes.length; i += 1) expect(modelTimes[i] - modelTimes[i - 1]).toBe(HOUR);
    expect(new Set(modelTimes).size).toBe(modelTimes.length);
  });

  it('leaves room for an extrapolation still being computed, so the band does not shift', () => {
    const plan = planBand({ ...base, nowcastBaseSec: null, nowcastExpected: true });
    expect(plan.nowcastTimes).toEqual([]);
    expect(plan.modelTimes[0]).toBe(NOW + 2 * HOUR);
  });

  it('starts the model half an hour after the radar when there is no extrapolation', () => {
    expect(planBand({ ...base, nowcastBaseSec: null, nowcastExpected: false }).modelTimes[0]).toBe(NOW + HOUR / 2);
  });

  it('starts at the current hour without radar', () => {
    const plan = planBand({ nowSec: NOW + 40 * 60, lastObservedSec: null, lastRadarSec: null, nowcastBaseSec: null, nowcastExpected: false });
    expect(plan.modelTimes[0]).toBe(NOW);
    expect(plan.hourEnds[0]).toBe(NOW);
  });
});

describe('startIndex', () => {
  it('opens on the newest observed moment', () => {
    const entries = buildTimeline(RADAR, [], FORECAST);
    expect(entries[startIndex(entries, NOW)].timeSec).toBe(NOW);
  });

  it('falls back to the entry closest to now when nothing was observed', () => {
    const entries = buildTimeline([], [], FORECAST);
    expect(entries[startIndex(entries, NOW + 70 * 60)].timeSec).toBe(NOW + HOUR);
  });

  it('has nothing to open on an empty band', () => {
    expect(startIndex([], NOW)).toBe(-1);
  });
});

describe('playback pacing', () => {
  const entries = buildTimeline([frame(-10), frame(0)], [NOW + 600, NOW + 1200], FORECAST);

  it('holds the present and the end, flicks through extrapolated frames and gives forecast hours more time', () => {
    const hold = startIndex(entries, NOW);
    expect(entryDelayMs(entries, hold, hold)).toBe(FRAME_HOLD_MS);
    expect(entryDelayMs(entries, entries.length - 1, hold)).toBe(FRAME_HOLD_MS);
    expect(entryDelayMs(entries, 0, hold)).toBe(FRAME_STEP_MS);
    expect(entryDelayMs(entries, entries.findIndex((e) => e.source === 'nowcast'), hold)).toBe(FRAME_STEP_MS);
    expect(entryDelayMs(entries, entries.findIndex((e) => e.source === 'forecast'), hold)).toBe(FORECAST_STEP_MS);
  });

  it('wraps back to the beginning', () => {
    expect(nextIndex(entries.length - 1, entries.length)).toBe(0);
    expect(nextIndex(0, entries.length)).toBe(1);
    expect(nextIndex(0, 0)).toBe(-1);
  });
});
