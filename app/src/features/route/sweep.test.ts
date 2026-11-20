import { describe, expect, it } from 'vitest';
import { bestBar, bestDay, dayBests, departureSweep, type SweepBar } from './sweep';
import { dayForecast } from './testData';

const RO = 3 * 3600;
const DATE = '2026-09-12';
const BEFORE_DAWN = Date.UTC(2026, 8, 12, 0, 0); // 03:00 in Romania

// Origin always fine; a valley 150 km out gets rain from 10:00 local.
const points = [
  { km: 0, name: 'București', forecast: dayForecast(DATE, RO, () => 90) },
  { km: 150, name: 'Valea Oltului', forecast: dayForecast(DATE, RO, (h) => (h >= 10 ? 55 : 80)) },
];

describe('departureSweep', () => {
  const bars = departureSweep({ points, date: DATE, speedKmh: 75, originOffsetSec: RO, nowMs: BEFORE_DAWN });

  it('covers every half hour from 05:00 to 20:00', () => {
    expect(bars).toHaveLength(31);
    expect(bars[0].time).toBe('05:00');
  });

  it('takes the worst point at each point own ETA', () => {
    // 07:30 departure reaches the valley at 09:30: still dry.
    expect(bars.find((b) => b.time === '07:30')).toMatchObject({ worst: 80, worstName: 'Valea Oltului', complete: true });
    // 08:00 departure reaches it at 10:00: rain.
    expect(bars.find((b) => b.time === '08:00')).toMatchObject({ worst: 55, worstName: 'Valea Oltului' });
  });

  it('marks past departures and incomplete data', () => {
    const later = departureSweep({ points, date: DATE, speedKmh: 75, originOffsetSec: RO, nowMs: Date.UTC(2026, 8, 12, 6, 0) });
    expect(later.find((b) => b.time === '08:30')?.past).toBe(true);
    expect(later.find((b) => b.time === '09:30')?.past).toBe(false);
    // A point 400 km out, reached at 01:20 the next day, is past this one-day forecast.
    const far = [...points, { km: 400, name: 'Cluj', forecast: dayForecast(DATE, RO, () => 70) }];
    const lateBars = departureSweep({ points: far, date: DATE, speedKmh: 75, originOffsetSec: RO, nowMs: BEFORE_DAWN });
    expect(lateBars[30]).toMatchObject({ time: '20:00', complete: false, worst: 55 });
    expect(lateBars[0].complete).toBe(true);
  });
});

describe('bestBar', () => {
  const bar = (time: string, worst: number | null, extra: Partial<SweepBar> = {}): SweepBar => ({
    time,
    worst,
    worstName: 'x',
    complete: true,
    past: false,
    ...extra,
  });

  it('picks the highest worst score', () => {
    expect(bestBar([bar('06:00', 60), bar('07:00', 78), bar('08:00', 58)], '08:30')?.time).toBe('07:00');
  });

  it('prefers the chosen time on a tie, then the closest', () => {
    expect(bestBar([bar('06:00', 78), bar('08:30', 78)], '08:30')?.time).toBe('08:30');
    expect(bestBar([bar('06:00', 78), bar('10:00', 78)], '09:00')?.time).toBe('10:00');
  });

  it('skips past, incomplete and unscored departures', () => {
    const bars = [bar('06:00', 99, { past: true }), bar('07:00', 95, { complete: false }), bar('07:30', null), bar('08:00', 50)];
    expect(bestBar(bars, '08:00')?.time).toBe('08:00');
    expect(bestBar([bar('06:00', 90, { past: true })], '06:00')).toBeNull();
  });
});

describe('dayBests', () => {
  // Two days of forecast: the 12th is wet in the valley from 10:00, the 13th dry all day; nothing for the 14th.
  const twoDays = (scoreOn12: (h: number) => number, scoreOn13: (h: number) => number) => {
    const a = dayForecast(DATE, RO, scoreOn12);
    const b = dayForecast('2026-09-13', RO, scoreOn13);
    return { ...a, hourly: [...a.hourly, ...b.hourly] };
  };
  const route = [
    { km: 0, name: 'București', forecast: twoDays(() => 90, () => 90) },
    { km: 150, name: 'Valea Oltului', forecast: twoDays((h) => (h >= 10 ? 55 : 80), () => 85) },
  ];
  const input = { points: route, speedKmh: 75, originOffsetSec: RO, chosenTime: '09:00' };

  it('gives each day its best departure, or says why there is none', () => {
    const days = dayBests({ ...input, dates: [DATE, '2026-09-13', '2026-09-14'], nowMs: BEFORE_DAWN });
    expect(days.map((d) => [d.date, d.state, d.best?.time ?? null, d.best?.worst ?? null])).toEqual([
      [DATE, 'ready', '07:30', 80],
      ['2026-09-13', 'ready', '09:00', 85],
      ['2026-09-14', 'missing', null, null],
    ]);
    expect(bestDay(days)?.date).toBe('2026-09-13');
  });

  it('calls a day over once every departure is past', () => {
    const evening = Date.UTC(2026, 8, 12, 18, 0); // 21:00 in Romania
    const days = dayBests({ ...input, dates: [DATE, '2026-09-13'], nowMs: evening });
    expect(days[0]).toMatchObject({ state: 'over', best: null });
    expect(days[1].state).toBe('ready');
    expect(bestDay([days[0]])).toBeNull();
  });
});
