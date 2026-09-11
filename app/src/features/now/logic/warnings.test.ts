import { describe, expect, it } from 'vitest';
import { nowWarnings, warningText } from './warnings';
import { current, hours } from './testFixtures';

const NOW = '2026-09-11T10:00';

describe('nowWarnings', () => {
  it('stays quiet on a calm day', () => {
    expect(nowWarnings(current(), hours(NOW, 12), 0)).toEqual([]);
  });

  it('shows at most two, most dangerous first', () => {
    const hourly = hours(NOW, 12, (i) =>
      i === 2 ? { weather_code: 95, precipitation_probability: 80, precipitation_mm: 2 } : i === 3 ? { wind_gusts_kmh: 60 } : {},
    );
    const list = nowWarnings(current({ frost_risk: true }), hourly, 0);
    expect(list.map((w) => w.kind)).toEqual(['storm', 'frost']);
    expect(list[0].at).toBe('2026-09-11T12:00');
    expect(list[1].at).toBeNull();
  });

  it('ignores a stale storm code with no rain behind it', () => {
    const hourly = hours(NOW, 12, (i) => (i === 1 ? { weather_code: 95, precipitation_probability: 5, precipitation_mm: 0 } : {}));
    expect(nowWarnings(current(), hourly, 0)).toEqual([]);
  });

  it('reports the strongest gust from the first strong hour', () => {
    const hourly = hours(NOW, 12, (i) => ({ wind_gusts_kmh: i === 1 ? 55 : i === 3 ? 62 : 20 }));
    const [w] = nowWarnings(current(), hourly, 0);
    expect(w).toEqual({ kind: 'wind', at: '2026-09-11T11:00', value: 62 });
    expect(warningText(w, NOW, 'ro')).toBe('Rafale de până la 62 km/h de la 11:00');
    expect(warningText(w, NOW, 'en')).toBe('Gusts up to 62 km/h from 11:00');
  });

  it('warns about fog now from low visibility', () => {
    const [w] = nowWarnings(current({ visibility_km: 0.3 }), hours(NOW, 12), 0);
    expect(warningText(w, NOW, 'ro')).toBe('Ceață acum: vizibilitate 300 m');
  });
});
