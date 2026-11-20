import { useMemo } from 'react';
import { fmtNumber, fmtTemp, hourOf } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import { CORE } from '../../i18n/core';
import { Banner, cx, SectionLabel, Skeleton } from '../../ui/primitives';
import { crossWarning, roadWinds, type CrossWarning, type RoadWind, type WindSide } from './crosswind';
import { firstDark, type DarkAt } from './daylight';
import type { TimelineRow } from './plan';
import { rainLine } from './rainText';
import { fmtKm, scoreWord } from './routeFormat';
import { RS } from './strings';
import type { RouteLine } from './types';

// "Vremea pe drum": when you reach each point, its temperature, gusts (and
// the side they hit you from when they are strong, see crosswind.ts), the
// rain line (chance + amount + intensity, never a bare percentage) and the
// backend's moto score for that hour. Points reached in the dark are marked
// (see daylight.ts).

const SIDE_KEY = { head: 'windHead', tail: 'windTail', cross: 'windCross' } as const satisfies Record<WindSide, string>;

function CrossBanner({ warning }: { warning: CrossWarning }) {
  const s = useStrings(RS);
  const lang = useLang();
  // The place goes in brackets after the km: it is a town, a road or "after <town>".
  const main = fmt(s.crossWarn, {
    place: warning.row.point.name,
    km: fmtKm(warning.row.point.km, lang),
    time: hourOf(warning.row.etaLocal),
    v: fmtNumber(Math.round(warning.gustKmh), lang),
  });
  const more = warning.more === 0 ? '' : warning.more === 1 ? s.crossWarnOne : fmt(s.crossWarnMany, { n: warning.more });
  return (
    <Banner tone="warn" icon="wind">
      {more ? `${main} ${more}` : main}
    </Banner>
  );
}

/** The first point reached in the dark: leaving before sunrise or after sunset, or night falling on the way. */
function DarkBanner({ row }: { row: TimelineRow & { dark: DarkAt } }) {
  const s = useStrings(RS);
  const lang = useLang();
  const sun = hourOf(row.dark.sun);
  const before = row.dark.kind === 'before-sunrise';
  const text =
    row.point.stopIndex === 0
      ? fmt(before ? s.darkStartBefore : s.darkStartAfter, { place: row.point.name, sun })
      : fmt(before ? s.darkOnRoadBefore : s.darkOnRoad, {
          km: fmtKm(row.point.km, lang),
          place: row.point.name,
          eta: hourOf(row.etaLocal),
          sun,
        });
  return (
    <Banner tone="warn" icon="moon">
      {text}
    </Banner>
  );
}

function RowBody({ row, wind }: { row: TimelineRow; wind: RoadWind | null }) {
  const s = useStrings(RS);
  const lang = useLang();
  const { slot } = row;
  if (row.status === 'loading' && !slot) {
    return (
      <>
        <Skeleton height={13} width="70%" />
        <Skeleton height={13} width="85%" />
      </>
    );
  }
  if (!slot) return <span className="route-wp__meta">{row.status === 'error' ? s.wpError : s.wpNoData}</span>;
  const rain = rainLine(slot, lang);
  const gust = fmtNumber(slot.wind_gusts_kmh == null ? null : Math.round(slot.wind_gusts_kmh), lang);
  const meta = [
    fmt(s.wpKm, { km: fmtKm(row.point.km, lang) }),
    fmtTemp(slot.temperature, lang),
    wind ? fmt(s.wpGustSide, { v: gust, side: s[SIDE_KEY[wind.side]] }) : fmt(s.wpGust, { v: gust }),
  ].join(' · ');
  return (
    <>
      <span className="route-wp__meta">{meta}</span>
      <span className={cx('route-wp__rain', rain.wet && 'route-wp__rain--wet')}>{rain.text}</span>
    </>
  );
}

interface WaypointListProps {
  rows: readonly TimelineRow[];
  route: RouteLine;
}

export function WaypointList({ rows, route }: WaypointListProps) {
  const s = useStrings(RS);
  const core = useStrings(CORE);
  const winds = useMemo(() => roadWinds(route, rows), [route, rows]);
  const warning = useMemo(() => crossWarning(rows, winds), [rows, winds]);
  const dark = useMemo(() => firstDark(rows), [rows]);
  return (
    <section className="route-wp" aria-labelledby="route-wp-title">
      <SectionLabel id="route-wp-title">{s.wpTitle}</SectionLabel>
      {dark && <DarkBanner row={dark} />}
      {warning && <CrossBanner warning={warning} />}
      <ol className="route-wp__list">
        {rows.map((row, i) => {
          const score = row.slot?.moto_score ?? null;
          const color = tierColor(score);
          return (
            <li key={row.point.id} className="route-wp__row" aria-busy={row.status === 'loading' || undefined}>
              <span className="route-wp__time num">{hourOf(row.etaLocal)}</span>
              <span className="route-wp__rail" aria-hidden="true">
                <span className="route-wp__dot" style={{ background: color }} />
                {i < rows.length - 1 && <span className="route-wp__line" />}
              </span>
              <span className="route-wp__body">
                <span className={cx('route-wp__name', row.point.stopIndex === null && 'route-wp__name--between')}>
                  {row.point.name}
                  {row.dark && <span className="route-wp__dark">{` · ${s.wpDark}`}</span>}
                </span>
                <RowBody row={row} wind={winds[i]} />
              </span>
              <span className="route-wp__score num" style={{ color }}>
                {score == null ? (
                  <span aria-hidden="true">–</span>
                ) : (
                  <>
                    <span aria-hidden="true">{score}</span>
                    <span className="visually-hidden">{fmt(s.wpScore, { score, label: scoreWord(row.slot?.moto_label, score, core) })}</span>
                  </>
                )}
              </span>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
