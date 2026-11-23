import { useEffect, useRef, useState } from 'react';
import { api } from '../../../lib/api';
import { dayMonth, dayName, localNowIso } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor } from '../../../lib/scoring';
import type { Place } from '../../../lib/types';
import { usePlace } from '../../../state/place';
import { WeatherIcon } from '../../../ui/WeatherIcon';
import { Banner, Button, Card, cx, SectionLabel, Spinner } from '../../../ui/primitives';
import { mapPool } from '../../account/lib/pool';
import { COMPARE_DAYS, compareDates, compareRow, type CompareRow } from '../logic/compare';
import { nextDate } from '../logic/daylight';
import { S_NOW } from '../strings';

/** Forecasts loading at once, as for the weekend ranking under "Eu". */
const CONCURRENCY = 2;

type Phase = 'idle' | 'loading' | 'done';

interface FavoritesCompareProps {
  /** Shows a favourite on "Acum", on the given day when a cell was clicked. */
  onShow: (place: Place, date: string | null) => void;
}

/**
 * Desktop only: the favourites one under the other, with the ride score of
 * each of the next 7 days. One forecast per favourite, so it loads only when
 * asked, and starts over whenever the list of favourites changes.
 */
export function FavoritesCompare({ onShow }: FavoritesCompareProps) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const { favorites } = usePlace();
  const [phase, setPhase] = useState<Phase>('idle');
  const [done, setDone] = useState(0);
  const [failed, setFailed] = useState(0);
  const [dates, setDates] = useState<string[]>([]);
  const [rows, setRows] = useState<CompareRow[]>([]);
  const controllerRef = useRef<AbortController | null>(null);
  const favKey = favorites.map((f) => `${f.lat.toFixed(3)},${f.lon.toFixed(3)}`).join('|');

  useEffect(() => {
    controllerRef.current?.abort();
    setPhase('idle');
    setRows([]);
    setFailed(0);
  }, [favKey]);
  useEffect(() => () => controllerRef.current?.abort(), []);

  const run = async (): Promise<void> => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    const list = [...favorites];
    setPhase('loading');
    setDone(0);
    setFailed(0);
    const results = await mapPool(
      list,
      CONCURRENCY,
      async (p, _index, signal) => {
        const { data } = await api.weather(p, signal, COMPARE_DAYS, 'forecast');
        return { daily: data.daily, today: localNowIso(data.utc_offset_seconds).slice(0, 10) };
      },
      controller.signal,
      () => setDone((n) => n + 1),
    );
    if (controller.signal.aborted) return;
    results.forEach((r) => r?.status === 'rejected' && console.warn('[now] favourite forecast failed', r.reason));
    const loaded = results.map((r) => (r?.status === 'fulfilled' ? r.value : null));
    setFailed(loaded.filter((r) => r === null).length);
    // The columns start at today where the first favourite is; with none
    // loaded there is nothing to lay out, only the warning above.
    const first = loaded.find((r) => r !== null);
    const columns = first ? compareDates(first.today) : [];
    setDates(columns);
    setRows(first ? list.map((place, i) => compareRow(place, loaded[i]?.daily ?? null, columns)) : []);
    setPhase('done');
  };

  const cancel = (): void => {
    controllerRef.current?.abort();
    setPhase(rows.length ? 'done' : 'idle');
  };

  if (!favorites.length) return null;

  const today = dates[0];
  const dayLabel = (date: string): string => (date === today ? s.today : date === nextDate(today) ? s.tomorrow : dayName(date, lang));

  return (
    <section className="now-section now-compare" aria-labelledby="now-compare-title">
      <SectionLabel id="now-compare-title">{s.compareTitle}</SectionLabel>
      <Card className="now-compare__intro">
        <p className="now-compare__lead">{rows.length ? s.compareHint : s.compareLead}</p>
        {phase === 'loading' ? (
          <div className="now-compare__progress" role="status">
            <Spinner size={18} />
            <span>{fmt(s.compareProgress, { done, total: favorites.length })}</span>
            <Button variant="ghost" onClick={cancel}>
              {s.compareCancel}
            </Button>
          </div>
        ) : (
          <Button icon={phase === 'done' ? 'refresh' : 'star'} onClick={() => void run()}>
            {phase === 'done' ? s.compareRerun : fmt(s.compareRun, { n: favorites.length })}
          </Button>
        )}
      </Card>
      {failed > 0 && (
        <Banner tone="warn" icon="alert">
          {fmt(s.compareFailed, { count: failed })}
        </Banner>
      )}
      {rows.length > 0 && (
        <div className="now-compare__wrap">
          <table className="now-compare__table">
            <caption className="visually-hidden">{s.compareTitle}</caption>
            <thead>
              <tr>
                <th scope="col">{s.comparePlace}</th>
                {dates.map((date) => (
                  <th key={date} scope="col" className="now-compare__day">
                    {dayLabel(date)}
                    <span className="now-compare__date">{dayMonth(date, lang)}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.place.lat},${row.place.lon}`}>
                  <th scope="row">
                    <button type="button" className="now-compare__place" onClick={() => onShow(row.place, null)}>
                      <span className="now-compare__name">{row.place.name}</span>
                      {row.failed && <span className="now-compare__nodata">{s.compareNoData}</span>}
                    </button>
                  </th>
                  {row.cells.map((cell) => {
                    const best = cell.date === row.best;
                    const label = `${dayLabel(cell.date)} ${dayMonth(cell.date, lang)}`;
                    return (
                      <td key={cell.date}>
                        <button
                          type="button"
                          className={cx('now-compare__cell', best && 'now-compare__cell--best')}
                          disabled={cell.score == null}
                          onClick={() => onShow(row.place, cell.date)}
                          aria-label={fmt(best ? s.compareCellBestAria : s.compareCellAria, { place: row.place.name, day: label, score: cell.score ?? '–' })}
                        >
                          {cell.code != null && <WeatherIcon code={cell.code} isDay size={22} />}
                          <span className="num" style={{ color: tierColor(cell.score) }}>
                            {cell.score ?? '–'}
                          </span>
                        </button>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
