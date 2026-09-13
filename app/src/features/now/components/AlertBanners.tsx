import { useState } from 'react';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import type { AlertLevel, OfficialAlert } from '../../../lib/types';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import { S_NOW } from '../strings';

// Official warnings from the national weather service, above everything else
// on the screen: they come from people whose job is to issue them, so they are
// shown as they are and never folded into our own score.

const TONE: Record<AlertLevel, string> = {
  red: 'now-alert--red',
  orange: 'now-alert--orange',
  yellow: 'now-alert--yellow',
  green: 'now-alert--green',
};

/** "13 sep 09:00" in the reader's locale, from the issuer's own offset. */
function when(iso: string | null, lang: 'ro' | 'en'): string | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  if (Number.isNaN(ms)) return null;
  return new Date(ms).toLocaleString(lang === 'ro' ? 'ro-RO' : 'en-GB', {
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  });
}

export function AlertBanners({ alerts }: { alerts: readonly OfficialAlert[] }) {
  const s = useStrings(S_NOW);
  if (alerts.length === 0) return null;
  return (
    <section className="now-alerts" aria-label={s.alertsTitle}>
      {alerts.map((alert) => (
        <AlertCard key={alert.id} alert={alert} />
      ))}
    </section>
  );
}

function AlertCard({ alert }: { alert: OfficialAlert }) {
  const s = useStrings(S_NOW);
  const lang = useLang();
  const [open, setOpen] = useState(false);
  const title = alert.headline || alert.event || s.alertFallbackTitle;
  const from = when(alert.onset, lang);
  const to = when(alert.expires, lang);
  const period = from && to ? fmt(s.alertPeriod, { from, to }) : (to ? fmt(s.alertUntil, { to }) : null);
  const body = [alert.description, alert.instruction].filter(Boolean).join('\n\n');

  return (
    <article className={cx('now-alert', TONE[alert.level])} role="alert">
      <div className="now-alert__head">
        <Icon name="alert" size={20} />
        <h2 className="now-alert__title">{title}</h2>
      </div>
      <p className="now-alert__meta">
        {[alert.areas.join(', '), period].filter(Boolean).join(' · ')}
      </p>
      {body && (
        <>
          {open && <p className="now-alert__body">{body}</p>}
          <button type="button" className="now-alert__more" aria-expanded={open} onClick={() => setOpen(!open)}>
            {open ? s.alertLess : s.alertMore}
          </button>
        </>
      )}
      {alert.sender && <p className="now-alert__source">{fmt(s.alertSource, { sender: alert.sender })}</p>}
    </article>
  );
}
