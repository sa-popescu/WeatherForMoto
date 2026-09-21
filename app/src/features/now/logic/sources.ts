import type { SourceState, SourceStatus } from '../../../lib/types';

// Which sources to show on the "data sources" sheet, grouped and in order.
// Pure helpers, so the rules are testable without React.

export type SourceGroup = 'stations' | 'models' | 'warnings';

export const SOURCE_GROUPS: ReadonlyArray<{ group: SourceGroup; ids: readonly string[] }> = [
  { group: 'stations', ids: ['official-stations', 'metar', 'weatherxm', 'netatmo'] },
  { group: 'models', ids: ['open-meteo', 'model-ensemble', 'rain-ensemble', 'openweathermap', 'met-norway', 'pirate-weather'] },
  { group: 'warnings', ids: ['anm-nowcast', 'meteoalarm'] },
];

/** The map's own sources: always there, loaded by the browser, no status to report. */
export const MAP_SOURCES = ['rainviewer', 'dwd', 'eumetsat', 'osm'] as const;

export const SOURCE_URLS: Readonly<Record<string, string>> = {
  'official-stations': 'https://eumetnet.github.io/meteogate-documentation/',
  metar: 'https://aviationweather.gov/data/api/',
  weatherxm: 'https://weatherxm.com/',
  netatmo: 'https://weathermap.netatmo.com/',
  'open-meteo': 'https://open-meteo.com/',
  'model-ensemble': 'https://open-meteo.com/en/docs',
  'rain-ensemble': 'https://open-meteo.com/en/docs/ensemble-api',
  openweathermap: 'https://openweathermap.org/',
  'met-norway': 'https://api.met.no/',
  'pirate-weather': 'https://pirateweather.net/',
  'anm-nowcast': 'https://www.meteoromania.ro/',
  meteoalarm: 'https://meteoalarm.org/',
  rainviewer: 'https://www.rainviewer.com/api.html',
  dwd: 'https://www.dwd.de/',
  eumetsat: 'https://view.eumetsat.int/',
  osm: 'https://www.openstreetmap.org/copyright',
};

const STATE_ORDER: Record<SourceState, number> = { used: 0, 'none-nearby': 1, 'no-data': 1, off: 2 };

/**
 * Sources grouped for display, used ones first inside each group. Without
 * statuses (an older server, or the browser's direct fallback) the list of
 * used sources stands in, each marked as used. Ids the app does not know yet
 * are shown with the models rather than hidden.
 */
export function groupSources(
  statuses: readonly SourceStatus[] | null | undefined,
  used: readonly string[] | null | undefined,
): Array<{ group: SourceGroup; items: SourceStatus[] }> {
  const list: SourceStatus[] =
    statuses && statuses.length > 0 ? [...statuses] : (used && used.length > 0 ? used : ['open-meteo']).map((id) => ({ id, status: 'used' }));
  const byId = new Map(list.map((status) => [status.id, status]));
  const known = new Set(SOURCE_GROUPS.flatMap((g) => g.ids));
  const groups = SOURCE_GROUPS.map(({ group, ids }) => ({
    group,
    items: ids.map((id) => byId.get(id)).filter((s): s is SourceStatus => s !== undefined),
  }));
  groups[1].items.push(...list.filter((status) => !known.has(status.id)));
  for (const g of groups) g.items.sort((a, b) => (STATE_ORDER[a.status] ?? 3) - (STATE_ORDER[b.status] ?? 3));
  return groups.filter((g) => g.items.length > 0);
}

/** Whole minutes since a measurement, or null when unknown or in the future. */
export function minutesAgo(iso: string | null | undefined, nowMs: number): number | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  if (Number.isNaN(ms) || ms > nowMs + 60_000) return null;
  return Math.max(0, Math.round((nowMs - ms) / 60_000));
}
