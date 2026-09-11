import { useState } from 'react';
import { useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Card } from '../../../ui/primitives';
import { DeleteSheet } from '../sheets/DeleteSheet';
import { EmailSheet } from '../sheets/EmailSheet';
import { NameSheet } from '../sheets/NameSheet';
import { PasswordSheet } from '../sheets/PasswordSheet';
import { PROFILE } from '../strings/profile';
import { Group, RowButton } from '../ui/controls';

type OpenSheet = 'name' | 'email' | 'password' | 'delete' | null;

/** Name, email, password, sign-out and delete. Sign-out stays available even if the account failed to load. */
export function AccountSection() {
  const s = useStrings(PROFILE);
  const { me, logout } = useAuth();
  const toast = useToast();
  const [sheet, setSheet] = useState<OpenSheet>(null);
  const [leaving, setLeaving] = useState(false);
  const close = (): void => setSheet(null);

  const onLogout = async (): Promise<void> => {
    setLeaving(true);
    try {
      // logout() also removes this device's push subscription.
      await logout();
      toast(s.logoutDone);
    } catch (err) {
      console.warn('[account] logout failed', err);
    } finally {
      setLeaving(false);
    }
  };

  return (
    <Group title={s.section}>
      <Card className="acct-rows">
        {me && <RowButton icon="user" label={s.nameRow} value={me.display_name || '–'} onClick={() => setSheet('name')} />}
        {me && <RowButton icon="mail" label={s.emailRow} value={me.email} onClick={() => setSheet('email')} />}
        {me && <RowButton icon="lock" label={s.passwordRow} value={s.passwordRowValue} onClick={() => setSheet('password')} />}
        <RowButton icon="logout" label={s.logout} busy={leaving} onClick={() => void onLogout()} />
        {me && <RowButton icon="trash" label={s.deleteRow} tone="danger" onClick={() => setSheet('delete')} />}
      </Card>
      {me && (
        <>
          <NameSheet open={sheet === 'name'} me={me} onClose={close} />
          <EmailSheet open={sheet === 'email'} me={me} onClose={close} />
          <PasswordSheet open={sheet === 'password'} me={me} onClose={close} />
          <DeleteSheet open={sheet === 'delete'} onClose={close} />
        </>
      )}
    </Group>
  );
}
