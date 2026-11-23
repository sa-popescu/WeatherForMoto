import { useSyncExternalStore } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { shareLink } from '../../lib/share';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Button } from '../../ui/primitives';
import { liveShare } from './liveShare';
import { LIVE_S } from './liveStrings';

// Under the ride buttons: share the position by link, then send the link
// again or stop. The share ends by itself with the ride.

export function LiveShareControl({ now }: { now: number }) {
  const s = useStrings(LIVE_S);
  const lang = useLang();
  const toast = useToast();
  const live = useSyncExternalStore(liveShare.subscribe, liveShare.getSnapshot);

  const send = async (url: string): Promise<void> => {
    // The chat app's preview of the link speaks the rider's language (worker/preview.ts).
    const link = lang === 'en' ? `${url}?lang=en` : url;
    try {
      if ((await shareLink(link, s.shareText)) === 'copied') toast(s.copied, { tone: 'success' });
    } catch (err) {
      console.warn('[live] the link could not be shared', err);
      toast(s.shareFailed, { tone: 'error' });
    }
  };

  const onStart = async (): Promise<void> => {
    const url = await liveShare.start();
    if (url) await send(url);
  };

  if (live.status !== 'on' || !live.url) {
    return (
      <div className="ride-live">
        <Button full icon="share" busy={live.status === 'starting'} onClick={() => void onStart()}>
          {live.status === 'starting' ? s.starting : s.start}
        </Button>
        {live.error ? (
          <p className="ride-live__error" role="alert">
            {s[live.error]}
          </p>
        ) : (
          <p className="ride-live__note">{s.note}</p>
        )}
      </div>
    );
  }

  const url = live.url;
  return (
    <div className="ride-live ride-live--on" role="status">
      <span className="ride-live__dot" aria-hidden="true" />
      <span className="ride-live__text">
        <span className="ride-live__title">{s.on}</span>
        <span className="ride-live__meta">
          {live.lastSentAt === null ? s.waiting : fmt(s.sentAgo, { s: Math.max(0, Math.round((now - live.lastSentAt) / 1000)) })}
        </span>
      </span>
      <button type="button" className="ride-live__action" onClick={() => void send(url)} aria-label={s.sendLink} title={s.sendLink}>
        <Icon name="share" size={20} />
      </button>
      <button type="button" className="ride-live__action" onClick={() => void liveShare.stop()} aria-label={s.stop} title={s.stop}>
        <Icon name="close" size={20} />
      </button>
    </div>
  );
}
