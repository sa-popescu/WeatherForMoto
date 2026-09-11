import { describe, expect, it } from 'vitest';
import { cumulativeKm, nearestOnLine, planSamples, pointAtKm, splitLineAtKm } from './geometry';

// A straight line along the equator: 1 degree of longitude is ~111.2 km.
const line = [0, 1, 2, 3].map((lon) => ({ lat: 0, lon }));
const cum = cumulativeKm(line);

describe('cumulativeKm', () => {
  it('adds up great-circle distances from 0', () => {
    expect(cum[0]).toBe(0);
    expect(cum[1]).toBeCloseTo(111.19, 1);
    expect(cum[3]).toBeCloseTo(333.58, 1);
  });

  it('handles empty and single-point lines', () => {
    expect(cumulativeKm([])).toEqual([]);
    expect(cumulativeKm([{ lat: 1, lon: 1 }])).toEqual([0]);
  });
});

describe('pointAtKm', () => {
  it('interpolates inside a segment', () => {
    const p = pointAtKm(line, cum, cum[1] + (cum[2] - cum[1]) / 2);
    expect(p.lat).toBeCloseTo(0, 6);
    expect(p.lon).toBeCloseTo(1.5, 6);
  });

  it('clamps outside the line', () => {
    expect(pointAtKm(line, cum, -5)).toEqual({ lat: 0, lon: 0 });
    expect(pointAtKm(line, cum, 10_000)).toEqual({ lat: 0, lon: 3 });
  });
});

describe('planSamples', () => {
  it('adds a point every ~40 km on a single leg, up to the cap', () => {
    const plan = planSamples([0, 272]);
    expect(plan).toHaveLength(8);
    expect(plan[0]).toEqual({ km: 0, stopIndex: 0 });
    expect(plan[7]).toEqual({ km: 272, stopIndex: 1 });
    const gaps = plan.slice(1).map((p, i) => p.km - plan[i].km);
    for (const gap of gaps) expect(gap).toBeCloseTo(272 / 7, 6);
  });

  it('keeps short routes to their stops', () => {
    expect(planSamples([0, 35])).toEqual([
      { km: 0, stopIndex: 0 },
      { km: 35, stopIndex: 1 },
    ]);
  });

  it('never exceeds the cap and gives the budget to the longest legs', () => {
    const plan = planSamples([0, 30, 530, 560]);
    expect(plan).toHaveLength(8);
    expect(plan.filter((p) => p.stopIndex !== null)).toHaveLength(4);
    const between = plan.filter((p) => p.stopIndex === null);
    expect(between.every((p) => p.km > 30 && p.km < 530)).toBe(true);
  });

  it('keeps every stop even when there are more stops than the cap allows extras', () => {
    const plan = planSamples([0, 100, 200, 300, 400], 40, 5);
    expect(plan.map((p) => p.stopIndex)).toEqual([0, 1, 2, 3, 4]);
  });
});

describe('splitLineAtKm', () => {
  it('returns one piece more than breaks, sharing boundaries', () => {
    const pieces = splitLineAtKm(line, cum, [cum[1] / 2, cum[2]]);
    expect(pieces).toHaveLength(3);
    expect(pieces[0][0]).toEqual(line[0]);
    expect(pieces[0][pieces[0].length - 1].lon).toBeCloseTo(0.5, 6);
    expect(pieces[1][0].lon).toBeCloseTo(0.5, 6);
    expect(pieces[2][pieces[2].length - 1]).toEqual(line[3]);
  });

  it('keeps the piece count when a break sits at the very end', () => {
    expect(splitLineAtKm(line, cum, [cum[3] + 1])).toHaveLength(2);
  });
});

describe('nearestOnLine', () => {
  it('finds the closest vertex and the distance off the line', () => {
    const near = nearestOnLine(line, cum, { lat: 0.1, lon: 2 });
    expect(near.km).toBeCloseTo(cum[2], 6);
    expect(near.offKm).toBeCloseTo(11.1, 1);
  });
});
