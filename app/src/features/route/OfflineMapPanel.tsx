import { useMemo } from 'react';
import { dayMonth, fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { useComputer } from '../../lib/useMediaQuery';
import { Icon } from '../../ui/icons';
import { Button, SectionLabel } from '../../ui/primitives';
import { routeMapId } from './offlineMap';
import { estimateBytes, offlineMapsSupported, type OfflineMapErrorKind } from './offlineMapDownload';
import { deleteOfflineMap, startDownload, stopDownload, useDownloadState, useOfflineMaps } from './offlineMapStore';
import { OFFLINE_S } from './offlineStrings';
import { localDay } from './seasonalClosures';
import type { LatLon } from './types';

// The map along a route, for riding without signal: download it, follow the
// download, or delete it. A section in the planner, one button on the Drum card.
// Both stay off on a computer: the map is for the phone on the handlebar, and
// a download on the computer never reaches it.

type OfflineText = { [K in keyof (typeof OFFLINE_S)['ro']]: string };

export function megabytes(bytes: number, lang: Lang): string {
  return fmtNumber(Math.max(1, Math.round(bytes / (1024 * 1024))), lang);
}

function errorText(kind: OfflineMapErrorKind, s: OfflineText): string {
  return kind === 'space' ? s.space : kind === 'unsupported' ? s.unsupported : s.error;
}

/** The route's download: on the phone, under way, or neither. */
function useRouteMap(line: readonly LatLon[]) {
  const id = useMemo(() => routeMapId(line), [line]);
  const saved = useOfflineMaps().find((m) => m.id === id) ?? null;
  const { progress, error } = useDownloadState(id);
  const pct = progress && progress.total > 0 ? Math.round((100 * progress.done) / progress.total) : 0;
  return { id, saved, progress, error, pct };
}

interface Props {
  line: readonly LatLon[];
  /** How the download is named in Eu: "București → Sibiu". */
  name: string;
}

/** The planner's section. */
export function OfflineMapPanel({ line, name }: Props) {
  const s = useStrings(OFFLINE_S);
  const lang = useLang();
  const { id, saved, progress, error, pct } = useRouteMap(line);
  const estimate = useMemo(() => (saved ? null : estimateBytes(line)), [line, saved]);
  const computer = useComputer();
  const supported = offlineMapsSupported();
  const today = localDay(Date.now());
  if (computer) return null;

  return (
    <section className="route-offline" aria-labelledby="route-offline-title">
      <SectionLabel id="route-offline-title">{s.title}</SectionLabel>
      {!supported && <p className="route-note">{s.unsupported}</p>}
      {supported && progress && (
        <>
          <div className="route-offline__bar" role="progressbar" aria-label={s.title} aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}>
            <span style={{ width: `${pct}%` }} />
          </div>
          <p className="route-note">{fmt(s.progress, { pct })}</p>
          <Button icon="close" onClick={() => stopDownload(id)}>
            {s.stop}
          </Button>
        </>
      )}
      {supported && !progress && saved && (
        <>
          <p className="route-note">
            <Icon name="check" size={18} />
            {fmt(s.saved, { mb: megabytes(saved.bytes, lang), when: localDay(saved.savedAt) === today ? s.today : dayMonth(localDay(saved.savedAt), lang) })}
          </p>
          {saved.partial && <p className="route-note route-note--small">{s.partial}</p>}
          {!saved.partial && saved.reduced && <p className="route-note route-note--small">{s.reduced}</p>}
          <Button icon="trash" onClick={() => void deleteOfflineMap(id)}>
            {s.remove}
          </Button>
        </>
      )}
      {supported && !progress && !saved && (
        <>
          <p className="route-note route-offline__intro">
            {s.intro} {estimate && fmt(s.estimate, { mb: megabytes(estimate.bytes, lang) })}
          </p>
          {estimate?.reduced && <p className="route-note route-note--small">{s.reduced}</p>}
          {error && (
            <p className="route-note route-offline__error" role="alert">
              {errorText(error, s)}
            </p>
          )}
          <Button icon="download" onClick={() => startDownload(line, name)}>
            {s.download}
          </Button>
        </>
      )}
      <p className="route-note route-note--small">
        {s.limits} {s.credit}
      </p>
    </section>
  );
}

/** One line on the Drum card: download, the progress (tap to stop), or that the map is there. */
export function OfflineMapButton({ line, name }: Props) {
  const s = useStrings(OFFLINE_S);
  const { id, saved, progress, error, pct } = useRouteMap(line);
  const computer = useComputer();
  if (computer || !offlineMapsSupported()) return null;
  if (progress) {
    return (
      <button type="button" className="drive-sheet__detailed" onClick={() => stopDownload(id)} aria-label={`${fmt(s.progress, { pct })}. ${s.stop}`}>
        <Icon name="close" size={16} />
        {fmt(s.progressShort, { pct })}
      </button>
    );
  }
  if (saved) {
    return (
      <p className="drive-sheet__offline">
        <Icon name="check" size={16} />
        {s.savedShort}
      </p>
    );
  }
  return (
    <>
      <button type="button" className="drive-sheet__detailed" onClick={() => startDownload(line, name)}>
        <Icon name="download" size={16} />
        {s.downloadShort}
      </button>
      {error && (
        <p className="drive-sheet__note" role="alert">
          {errorText(error, s)}
        </p>
      )}
    </>
  );
}
