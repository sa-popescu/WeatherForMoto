import { useMemo, useState } from 'react';
import { fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { useToast } from '../../state/toast';
import { cx } from '../../ui/primitives';
import { fmtDuration, fmtKm, fmtKmh, msToKmh } from './logic/rideFormat';
import { periodTotals, rideRecords, type Period, type RideRecord } from './logic/rideStats';
import { rideRecorder } from './recorder';
import type { RideListItem } from './store';
import { STORY_S } from './storyStrings';
import { RIDE_S } from './strings';

// The statistics on "Ture", from the rides on this phone: the totals of this
// week, month or year (or all of them), and the records, each opening the
// ride it belongs to.

const PERIODS: readonly Period[] = ['week', 'month', 'year', 'all'];

function shortDay(ms: number, lang: Lang): string {
  return new Date(ms).toLocaleDateString(lang === 'en' ? 'en-GB' : 'ro-RO', { day: 'numeric', month: 'short' }).replace('.', '');
}

export function RideTotals({ items }: { items: readonly RideListItem[] }) {
  const s = useStrings(STORY_S);
  const r = useStrings(RIDE_S);
  const lang = useLang();
  const toast = useToast();
  const [period, setPeriod] = useState<Period>('month');
  const now = Date.now();
  const totals = useMemo(() => periodTotals(items, period, now), [items, period, now]);
  const records = useMemo(() => rideRecords(items), [items]);

  if (items.length === 0) return <p className="ride-totals__empty">{s.noRides}</p>;

  const open = (id: string): void => {
    void rideRecorder.review(id).then((ok) => {
      if (!ok) toast(r.reviewErr, { tone: 'error' });
    });
  };
  const rows: Array<{ key: string; label: string; record: RideRecord | null; value: (v: number) => string }> = [
    { key: 'longest', label: s.recLongest, record: records.longest, value: (v) => `${fmtKm(v, lang)} km` },
    { key: 'time', label: s.recTime, record: records.longestTime, value: (v) => fmtDuration(v) },
    { key: 'fastest', label: s.recFastest, record: records.fastest, value: (v) => `${fmtKmh(msToKmh(v), lang)} km/h` },
    { key: 'lean', label: s.recLean, record: records.leanest, value: (v) => `${Math.round(v)}°` },
  ];

  return (
    <section className="ride-totals" aria-labelledby="ride-totals-title">
      <h2 id="ride-totals-title" className="eyebrow">
        {s.statsTitle}
      </h2>
      <div className="ride-totals__periods" role="group" aria-label={s.statsPeriod}>
        {PERIODS.map((p) => (
          <button key={p} type="button" className={cx('ride-totals__period', p === period && 'ride-totals__period--on')} aria-pressed={p === period} onClick={() => setPeriod(p)}>
            {s[p]}
          </button>
        ))}
      </div>
      <dl className="ride-totals__grid">
        <div>
          <dt>{s.rides}</dt>
          <dd className="num">{fmtNumber(totals.rides, lang)}</dd>
        </div>
        <div>
          <dt>{s.km}</dt>
          <dd className="num">{fmtKm(totals.distanceM, lang)}</dd>
        </div>
        <div>
          <dt>{s.moving}</dt>
          <dd className="num">{fmtDuration(totals.movingMs)}</dd>
        </div>
      </dl>
      <h3 className="eyebrow ride-totals__records-title">{s.recordsTitle}</h3>
      <ul className="ride-totals__records">
        {rows
          .filter((row) => row.record !== null)
          .map((row) => (
            <li key={row.key}>
              <button type="button" className="ride-totals__record" title={fmt(s.recOpen, { date: shortDay(row.record!.startedAt, lang) })} onClick={() => open(row.record!.id)}>
                <span className="ride-totals__record-label">{row.label}</span>
                <span className="ride-totals__record-value num">{row.value(row.record!.value)}</span>
                <span className="ride-totals__record-day">{shortDay(row.record!.startedAt, lang)}</span>
              </button>
            </li>
          ))}
      </ul>
    </section>
  );
}
