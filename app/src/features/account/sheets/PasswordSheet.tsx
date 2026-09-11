import { useEffect, useId, useState, type FormEvent } from 'react';
import { CORE } from '../../../i18n/core';
import { api } from '../../../lib/api';
import { useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button } from '../../../ui/primitives';
import { Sheet } from '../../../ui/Sheet';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { passwordProblem } from '../lib/validation';
import { AUTH } from '../strings/auth';
import { COMMON } from '../strings/common';
import { PROFILE } from '../strings/profile';
import { PasswordField } from '../ui/PasswordField';
import type { AccountSheetProps } from './NameSheet';
import { NoPasswordHelp } from './NoPasswordHelp';

export function PasswordSheet({ open, me, onClose }: AccountSheetProps) {
  const s = useStrings(PROFILE);
  const a = useStrings(AUTH);
  const c = useStrings(COMMON);
  const core = useStrings(CORE);
  const { token } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const { busy, error, setError, run } = useSubmit('changePassword');
  const formId = useId();
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    if (!open) return;
    setCurrent('');
    setNext('');
    setChecked(false);
    setError(null);
  }, [open, setError]);

  const problem = passwordProblem(next);
  const currentError = checked && !current ? s.passwordRequired : error === 'wrongPassword' ? kindText(error) : null;
  const nextError = checked && problem ? (problem === 'short' ? c.passwordShort : c.passwordLong) : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!token || !current || problem) return;
    void run(async () => {
      await api.changePassword(token, current, next);
      toast(s.passwordChanged, { tone: 'success', durationMs: 6000 });
      onClose();
    });
  };

  const footer = (
    <>
      <Button onClick={onClose}>{core.cancel}</Button>
      <Button type="submit" form={formId} variant="primary" busy={busy}>
        {s.passwordChange}
      </Button>
    </>
  );

  return (
    <Sheet open={open} onClose={onClose} title={s.passwordTitle} footer={footer}>
      <form id={formId} className="acct-form" onSubmit={submit} noValidate>
        {/* Lets password managers update the right saved login. */}
        <input type="email" className="visually-hidden" autoComplete="username" value={me.email} readOnly tabIndex={-1} aria-hidden="true" />
        <PasswordField label={s.currentPassword} value={current} onChange={setCurrent} autoComplete="current-password" error={currentError} />
        <PasswordField label={a.newPassword} value={next} onChange={setNext} autoComplete="new-password" hint={a.passwordNewHint} error={nextError} />
        {error && error !== 'wrongPassword' && (
          <Banner tone="error" icon="alert">
            {kindText(error)}
          </Banner>
        )}
        <NoPasswordHelp email={me.email} />
      </form>
    </Sheet>
  );
}
