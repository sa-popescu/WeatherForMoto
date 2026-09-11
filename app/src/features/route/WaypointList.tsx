import { fmtNumber, fmtTemp, hourOf } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { tierColor } from '../../lib/scoring';
import { CORE } from '../../i18n/core';
import { cx, SectionLabel, Skeleton } from '../../ui/primitives';
import type { TimelineRow } from './plan';
import { rainLine } from './rainText';
import { fmtKm, scoreWord } from './routeFormat';
import { RS } from './strings';

// "Vremea pe drum": when you reach each point, its temperature, gusts, the
// rain line (chance + amount + intensity, never a bare percentage) and the
// backend's moto score for that hour.

function RowBody({ row }: { row: TimelineRow }) {
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
  const meta = [
    fmt(s.wpKm, { km: fmtKm(row.point.km, lang) }),
    fmtTemp(slot.temperature, lang),
    fmt(s.wpGust, { v: fmtNumber(slot.wind_gusts_kmh == null ? null : Math.round(slot.wind_gusts_kmh), lang) }),
  ].join(' · ');
  return (
    <>
      <span className="route-wp__meta">{meta}</span>
      <span className={cx('route-wp__rain', rain.wet && 'route-wp__rain--wet')}>{rain.text}</span>
    </>
  );
}

export function WaypointList({ rows }: { rows: readonly TimelineRow[] }) {
  const s = useStrings(RS);
  const core = useStrings(CORE);
  return (
    <section className="route-wp" aria-labelledby="route-wp-title">
      <SectionLabel id="route-wp-title">{s.wpTitle}</SectionLabel>
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
                <span className={cx('route-wp__name', row.point.stopIndex === null && 'route-wp__name--between')}>{row.point.name}</span>
                <RowBody row={row} />
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
