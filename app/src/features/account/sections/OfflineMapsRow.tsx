import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { useDesktop } from '../../../lib/useMediaQuery';
import { useToast } from '../../../state/toast';
import { Icon } from '../../../ui/icons';
import { megabytes } from '../../route/OfflineMapPanel';
import { deleteAllOfflineMaps, useOfflineMaps } from '../../route/offlineMapStore';
import { OFFLINE_S } from '../../route/offlineStrings';

// Eu, Aplicație: how many route maps are kept for offline use, and a way to
// delete them all (each one can also go from its route in the planner).
// A desktop cannot download them (OfflineMapPanel.tsx), so there the row only
// shows while maps saved before are still kept, to delete them.

export function OfflineMapsRow() {
  const s = useStrings(OFFLINE_S);
  const lang = useLang();
  const toast = useToast();
  const desktop = useDesktop();
  const maps = useOfflineMaps();
  const bytes = maps.reduce((sum, m) => sum + m.bytes, 0);
  if (desktop && maps.length === 0) return null;

  const onRemoveAll = async (): Promise<void> => {
    try {
      await deleteAllOfflineMaps();
      toast(s.removedAll, { tone: 'success' });
    } catch (err) {
      console.warn('[offline map] could not delete the maps', err);
      toast(s.error, { tone: 'error' });
    }
  };

  return (
    <div className="acct-row acct-row--static">
      <Icon name="map" size={22} />
      <span className="acct-row__label">
        {s.rowTitle}
        <span className="acct-row__sub">{maps.length === 0 ? s.rowNone : fmt(s.rowSome, { n: maps.length, mb: megabytes(bytes, lang) })}</span>
      </span>
      {maps.length > 0 && (
        <button type="button" className="acct-row__action" onClick={() => void onRemoveAll()}>
          {s.removeAll}
        </button>
      )}
    </div>
  );
}
