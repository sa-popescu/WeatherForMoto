import { describe, expect, it } from 'vitest';
import { bestDate, compareDates, compareRow } from './compare';
import { day } from './testFixtures';

const sinaia = { name: 'Sinaia', lat: 45.35, lon: 25.55 };

describe('compareDates', () => {
  it('runs seven days from today, across the month end', () => {
    expect(compareDates('2026-11-27')).toEqual(['2026-11-27', '2026-11-28', '2026-11-29', '2026-11-30', '2026-12-01', '2026-12-02', '2026-12-03']);
  });
});

describe('bestDate', () => {
  it('picks the highest score', () => {
    const cells = [
      { date: 'a', score: 40, code: 1 },
      { date: 'b', score: 81, code: 1 },
      { date: 'c', score: 65, code: 1 },
    ];
    expect(bestDate(cells)).toBe('b');
  });

  it('keeps the earlier day on a tie', () => {
    const cells = [
      { date: 'a', score: 70, code: 1 },
      { date: 'b', score: 70, code: 1 },
    ];
    expect(bestDate(cells)).toBe('a');
  });

  it('gives nothing when no day has a score', () => {
    expect(bestDate([{ date: 'a', score: null, code: null }])).toBeNull();
  });
});

describe('compareRow', () => {
  const dates = ['2026-11-21', '2026-11-22', '2026-11-23'];

  it('reads each date from the daily forecast', () => {
    const daily = [day('2026-11-21', { moto_score: 60, weather_code: 3 }), day('2026-11-22', { moto_score: 75, weather_code: 1 }), day('2026-11-23', { moto_score: 30 })];
    const row = compareRow(sinaia, daily, dates);
    expect(row.cells.map((c) => c.score)).toEqual([60, 75, 30]);
    expect(row.cells[0].code).toBe(3);
    expect(row.best).toBe('2026-11-22');
    expect(row.failed).toBe(false);
  });

  it('leaves a date the forecast does not reach empty', () => {
    const row = compareRow(sinaia, [day('2026-11-21', { moto_score: 50 })], dates);
    expect(row.cells.map((c) => c.score)).toEqual([50, null, null]);
    expect(row.best).toBe('2026-11-21');
  });

  it('marks a forecast that failed to load', () => {
    const row = compareRow(sinaia, null, dates);
    expect(row.failed).toBe(true);
    expect(row.best).toBeNull();
    expect(row.cells.every((c) => c.score == null)).toBe(true);
  });
});
