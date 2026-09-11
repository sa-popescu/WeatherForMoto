import { useEffect, useId, useState, type FormEvent } from 'react';
import { CORE } from '../../../i18n/core';
import { api } from '../../../lib/api';
import { fmt, useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { Banner, Button } from '../../../ui/primitives';
import { Sheet } from '../../../ui/Sheet';
import { EmailInput } from '../auth/EmailInput';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { isValidEmail } from '../lib/validation';
import { COMMON } from '../strings/common';
import { PROFILE } from '../strings/profile';
import { PasswordField } from '../ui/PasswordField';
import type { AccountSheetProps } from './NameSheet';
import { NoPasswordHelp } from './NoPasswordHelp';

/**
 * The backend marks the new address unconfirmed, sends a confirmation link
 * and signs out every other session; the sheet ends on that notice.
 */
export function EmailSheet({ open, me, onClose }: AccountSheetProps) {
  const s = useStrings(PROFILE);
  const c = useStrings(COMMON);
  const core = useStrings(CORE);
  const { token, refreshMe } = useAuth();
  const kindText = useKindText();
  const { busy, error, setError, run } = useSubmit('changeEmail');
  const formId = useId();
  const [newEmail, setNewEmail] = useState('');
  const [password, setPassword] = useState('');
  const [checked, setChecked] = useState(false);
  const [sentTo, setSentTo] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setNewEmail('');
    setPassword('');
    setChecked(false);
    setSentTo(null);
    setError(null);
  }, [open, setError]);

  const address = newEmail.trim();
  const same = address.toLowerCase() === me.email.toLowerCase();
  const emailError = !checked ? null : !isValidEmail(address) ? c.emailInvalid : same ? s.emailSame : null;
  const passwordError = checked && !password ? s.passwordRequired : error === 'wrongPassword' ? kindText(error) : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!token || !isValidEmail(address) || same || !password) return;
    void run(async () => {
      await api.changeEmail(token, address, password);
      setSentTo(address.toLowerCase());
      await refreshMe();
    });
  };

  const footer = sentTo ? (
    <Button variant="primary" onClick={onClose}>
      {s.understood}
    </Button>
  ) : (
    <>
      <Button onClick={onClose}>{core.cancel}</Button>
      <Button type="submit" form={formId} variant="primary" busy={busy}>
        {s.emailChange}
      </Button>
    </>
  );

  return (
    <Sheet open={open} onClose={onClose} title={s.emailTitle} footer={footer}>
      {sentTo ? (
        <Banner tone="info" icon="mail">
          {fmt(s.emailChanged, { email: sentTo })}
        </Banner>
      ) : (
        <form id={formId} className="acct-form" onSubmit={submit} noValidate>
          <EmailInput label={s.newEmail} value={newEmail} onChange={setNewEmail} error={emailError} />
          <PasswordField label={s.currentPassword} value={password} onChange={setPassword} autoComplete="current-password" error={passwordError} />
          {error && error !== 'wrongPassword' && (
            <Banner tone="error" icon="alert">
              {kindText(error)}
            </Banner>
          )}
          <NoPasswordHelp email={me.email} />
        </form>
      )}
    </Sheet>
  );
}
