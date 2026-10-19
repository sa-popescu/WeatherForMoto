import { CORE } from '../../../i18n/core';
import { fmt, useStrings } from '../../../lib/i18n';
import type { WeatherErrorKind, WeatherSource } from '../../../state/weather';
import { Icon } from '../../../ui/icons';
import { Banner, Button, Card } from '../../../ui/primitives';
import { S_NOW } from '../strings';

interface StatusBannersProps {
  source: WeatherSource | null;
  offlineAgeMin: number | null;
  /** What the server said went wrong upstream, when it served older data. */
  staleReason?: string | null;
  /** A refresh failed while older data is still on screen. */
  failedRefresh: boolean;
  /** What each leg replied when it failed. Technical, shown so a screenshot diagnoses it. */
  errorDetail?: string | null;
  onRetry: () => void;
}

export function StatusBanners({ source, offlineAgeMin, staleReason, failedRefresh, errorDetail, onRetry }: StatusBannersProps) {
  const core = useStrings(CORE);
  const s = useStrings(S_NOW);
  return (
    <>
      {source === 'offline' && (
        <Banner tone="warn" icon="alert">
          {fmt(core.offlineAge, { min: offlineAgeMin ?? '?' })}
        </Banner>
      )}
      {source === 'stale' && (
        <Banner tone="warn" icon="alert">
          {fmt(core.staleAge, { min: offlineAgeMin ?? '?' })}
          {staleReason && <span className="status-detail">{staleReason}</span>}
        </Banner>
      )}
      {source === 'direct' && (
        <Banner tone="info" icon="info">
          {core.directSource}
        </Banner>
      )}
      {failedRefresh && (
        <Banner
          tone="warn"
          icon="refresh"
          action={
            <Button variant="ghost" onClick={onRetry}>
              {core.retry}
            </Button>
          }
        >
          {s.staleError}
          {errorDetail && <span className="status-detail">{errorDetail}</span>}
        </Banner>
      )}
    </>
  );
}

export function ErrorCard({
  kind,
  detail,
  busy,
  onRetry,
}: {
  kind: WeatherErrorKind | null;
  /** The technical cause of each failed leg, so a screenshot is a diagnosis. */
  detail?: string | null;
  busy: boolean;
  onRetry: () => void;
}) {
  const core = useStrings(CORE);
  const s = useStrings(S_NOW);
  return (
    <Card className="now-error" role="alert">
      <Icon name="alert" size={28} />
      <h2 className="now-error__title num">{s.errorTitle}</h2>
      <p className="muted">{kind === 'network' ? core.networkError : core.genericError}</p>
      {detail && <p className="status-detail status-detail--card">{detail}</p>}
      <Button variant="primary" size="lg" full icon="refresh" busy={busy} onClick={onRetry}>
        {core.retry}
      </Button>
    </Card>
  );
}
