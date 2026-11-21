import { describe, expect, it } from 'vitest';
import { cumulativeKm } from '../route/geometry';
import { RouteGuide } from './guide';
import type { AheadSlot } from './logic/ahead';

const NOW = Date.UTC(2026, 10, 22, 8, 0);
const coords = [
  { lat: 45, lon: 25 },
  { lat: 45, lon: 25.5 },
];
const cumKm = cumulativeKm(coords);
const wet: AheadSlot[] = [9, 10, 11].map((h) => ({ time: `2026-11-22T${String(h).padStart(2, '0')}:00`, band: 'moderata', rainProb: 80, gustKmh: 20, frost: false, score: 50 }));

function guideAt(km: number): RouteGuide {
  const guide = new RouteGuide({
    name: 'X',
    coords,
    cumKm,
    distanceKm: cumKm[1],
    plannedKmh: 60,
    ahead: [{ km: 30, name: 'Pitești', utcOffsetSeconds: 7200, slots: wet }],
  });
  guide.follow = { ...guide.follow, km };
  return guide;
}

describe('RouteGuide.weatherAhead', () => {
  it('says a warning once, then once more when it is close', () => {
    const guide = guideAt(0);
    expect(guide.weatherAhead(60, NOW, 'ro', true).sentence).toContain('Ploaie moderată');
    expect(guide.weatherAhead(60, NOW + 1000, 'ro', true).sentence).toBeNull();
    guide.follow = { ...guide.follow, km: 24 };
    expect(guide.weatherAhead(60, NOW + 60_000, 'ro', true).sentence).toContain('6 minute');
    expect(guide.weatherAhead(60, NOW + 61_000, 'ro', true).sentence).toBeNull();
  });

  it('keeps the warning on screen but waits to say it while something else is said', () => {
    const guide = guideAt(0);
    const busy = guide.weatherAhead(60, NOW, 'ro', false);
    expect(busy.warning?.kind).toBe('rain');
    expect(busy.sentence).toBeNull();
    expect(guide.weatherAhead(60, NOW + 1000, 'ro', true).sentence).not.toBeNull();
  });
});
