import { describe, expect, it } from 'vitest';
import type { SourceStatus } from '../../../lib/types';
import { groupSources, minutesAgo } from './sources';

describe('groupSources', () => {
  const statuses: SourceStatus[] = [
    { id: 'open-meteo', status: 'used' },
    { id: 'pirate-weather', status: 'off' },
    { id: 'met-norway', status: 'no-data' },
    { id: 'model-ensemble', status: 'used', models: 5 },
    { id: 'official-stations', status: 'used', station: 'Bucuresti Filaret', distance_km: 1.8 },
    { id: 'netatmo', status: 'off' },
    { id: 'metar', status: 'none-nearby' },
    { id: 'anm-nowcast', status: 'used', count: 0 },
    { id: 'brand-new', status: 'used' },
  ];

  it('groups measurements, models and warnings, used first', () => {
    const groups = groupSources(statuses, null);
    expect(groups.map((g) => g.group)).toEqual(['stations', 'models', 'warnings']);
    expect(groups[0].items.map((s) => s.id)).toEqual(['official-stations', 'metar', 'netatmo']);
    expect(groups[1].items.map((s) => s.id)).toEqual(['open-meteo', 'model-ensemble', 'brand-new', 'met-norway', 'pirate-weather']);
    expect(groups[2].items.map((s) => s.id)).toEqual(['anm-nowcast']);
  });

  it('falls back to the list of used sources, and to Open-Meteo alone', () => {
    expect(groupSources(null, ['open-meteo', 'met-norway']).flatMap((g) => g.items.map((s) => `${s.id}:${s.status}`))).toEqual([
      'open-meteo:used',
      'met-norway:used',
    ]);
    expect(groupSources(undefined, null)).toEqual([{ group: 'models', items: [{ id: 'open-meteo', status: 'used' }] }]);
  });
});

describe('minutesAgo', () => {
  const now = Date.parse('2026-09-14T08:12:00Z');

  it('counts the minutes since a measurement', () => {
    expect(minutesAgo('2026-09-14T08:00:00Z', now)).toBe(12);
    expect(minutesAgo(null, now)).toBeNull();
    expect(minutesAgo('not a time', now)).toBeNull();
    expect(minutesAgo('2026-09-14T09:00:00Z', now)).toBeNull();
  });
});
