import { useStrings } from '../../lib/i18n';
import { Icon, type IconName } from '../../ui/icons';
import { cx } from '../../ui/primitives';
import { AS } from './actionStrings';

// Save, GPX, log ride, report hazard. Signed-out riders still see the
// buttons; the ones needing an account explain it instead of failing.

export type ActionKind = 'save' | 'gpx' | 'log' | 'hazard';

interface RouteActionsProps {
  signedIn: boolean;
  onAction: (kind: ActionKind) => void;
}

export function RouteActions({ signedIn, onAction }: RouteActionsProps) {
  const as = useStrings(AS);
  const items: { kind: ActionKind; icon: IconName; label: string; aria: string; warn?: boolean }[] = [
    { kind: 'save', icon: 'bookmark', label: as.actSave, aria: as.actSaveAria },
    { kind: 'gpx', icon: 'download', label: as.actGpx, aria: as.actGpxAria },
    { kind: 'log', icon: 'bike', label: as.actLog, aria: as.actLogAria },
    { kind: 'hazard', icon: 'alert', label: as.actHazard, aria: as.actHazardAria, warn: true },
  ];
  return (
    <section className="route-actions-wrap" aria-label={as.actSaveAria}>
      <div className="route-actions">
        {items.map((item) => (
          <button
            key={item.kind}
            type="button"
            className={cx('route-action', item.warn && 'route-action--warn')}
            aria-label={item.aria}
            onClick={() => onAction(item.kind)}
          >
            <Icon name={item.icon} size={22} />
            <span aria-hidden="true">{item.label}</span>
          </button>
        ))}
      </div>
      {!signedIn && (
        <p className="route-note route-note--small">
          {as.signedOutNote} <a href="#/eu">{as.goToAccount}</a>
        </p>
      )}
    </section>
  );
}
