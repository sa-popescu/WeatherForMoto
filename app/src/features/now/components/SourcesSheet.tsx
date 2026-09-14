import { fmt, useStrings } from '../../../lib/i18n';
import type { CurrentWeather, SourceState, SourceStatus } from '../../../lib/types';
import { Sheet } from '../../../ui/Sheet';
import { cx } from '../../../ui/primitives';
import { groupSources, MAP_SOURCES, minutesAgo, SOURCE_URLS, type SourceGroup } from '../logic/sources';
import { S_SOURCES } from '../sourcesStrings';

// "Surse de date": every service behind the numbers on "Acum", what it gives,
// and what it did for this answer. Opened from "N surse" in the header.

type Strings = { [K in keyof (typeof S_SOURCES)['ro']]: string };

const GROUP_TITLE: Record<SourceGroup, 'groupStations' | 'groupModels' | 'groupWarnings'> = {
  stations: 'groupStations',
  models: 'groupModels',
  warnings: 'groupWarnings',
};

const STATE_LABEL: Record<SourceState, 'statusUsed' | 'statusNoData' | 'statusNoneNearby' | 'statusOff'> = {
  used: 'statusUsed',
  'no-data': 'statusNoData',
  'none-nearby': 'statusNoneNearby',
  off: 'statusOff',
};

function text(s: Strings, id: string, part: 'name' | 'role'): string | null {
  const value = (s as Record<string, string>)[`${id}.${part}`];
  return value ?? (part === 'name' ? id : null);
}

function detail(s: Strings, source: SourceStatus, nowMs: number): string | null {
  if (source.status !== 'used') return null;
  if (source.station && source.distance_km != null) {
    const minutes = minutesAgo(source.observed_at, nowMs);
    if (minutes === null) return fmt(s.stationNoTime, { station: source.station, km: source.distance_km });
    const ago = minutes < 90 ? fmt(s.ago, { min: minutes }) : fmt(s.agoHours, { h: Math.round(minutes / 60) });
    return fmt(s.stationDetail, { station: source.station, km: source.distance_km, ago });
  }
  if (source.models != null) return fmt(s.modelsDetail, { n: source.models });
  if (source.count != null) return source.count > 0 ? fmt(s.warningsSome, { n: source.count }) : s.warningsNone;
  return null;
}

function SourceRow({ id, state, extra }: { id: string; state: SourceState | null; extra: string | null }) {
  const s = useStrings(S_SOURCES);
  const role = text(s, id, 'role');
  const url = SOURCE_URLS[id];
  return (
    <li className="now-source">
      <div className="now-source__head">
        <span className="now-source__name">{text(s, id, 'name')}</span>
        {state && (
          <span className={cx('now-source__state', `now-source__state--${state}`)}>{s[STATE_LABEL[state]]}</span>
        )}
      </div>
      {role && <p className="now-source__role">{role}</p>}
      {extra && <p className="now-source__detail num">{extra}</p>}
      {url && (
        <a className="now-source__link" href={url} target="_blank" rel="noopener noreferrer">
          {s.licence}
        </a>
      )}
    </li>
  );
}

export function SourcesSheet({ current, nowMs, onClose }: { current: CurrentWeather; nowMs: number; onClose: () => void }) {
  const s = useStrings(S_SOURCES);
  const statuses = current.source_status;
  const groups = groupSources(statuses, current.sources);
  // The browser's direct fallback knows one source and reports no statuses.
  const fallback = (!statuses || statuses.length === 0) && (current.sources?.length ?? 0) <= 1;

  return (
    <Sheet open onClose={onClose} title={s.title}>
      <p className="now-sheet__intro">{fallback ? s.fallback : s.intro}</p>
      {groups.map(({ group, items }) => (
        <section key={group} className="now-sheet__block" aria-labelledby={`sources-${group}`}>
          <h3 id={`sources-${group}`} className="eyebrow">
            {s[GROUP_TITLE[group]]}
          </h3>
          <ul className="now-sources">
            {items.map((source) => (
              <SourceRow key={source.id} id={source.id} state={source.status} extra={detail(s, source, nowMs)} />
            ))}
          </ul>
        </section>
      ))}
      <section className="now-sheet__block" aria-labelledby="sources-map">
        <h3 id="sources-map" className="eyebrow">
          {s.groupMap}
        </h3>
        <ul className="now-sources">
          {MAP_SOURCES.map((id) => (
            <SourceRow key={id} id={id} state={null} extra={null} />
          ))}
        </ul>
      </section>
    </Sheet>
  );
}
