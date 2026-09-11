import { useState, type FormEvent } from 'react';
import { api } from '../../../lib/api';
import { fmt, useStrings } from '../../../lib/i18n';
import { Banner, Button } from '../../../ui/primitives';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { isValidEmail } from '../lib/validation';
import { AUTH } from '../strings/auth';
import { COMMON } from '../strings/common';
import { EmailInput } from './EmailInput';

interface ForgotFormProps {
  email: string;
  onEmail: (value: string) => void;
  onBack?: () => void;
  /** Intro sentence; defaults to the generic "we send you a link". */
  lead?: string;
}

/**
 * Requests a reset link. The confirmation is neutral on purpose: the backend
 * answers the same whether or not the address has an account.
 */
export function ForgotForm({ email, onEmail, onBack, lead }: ForgotFormProps) {
  const s = useStrings(AUTH);
  const c = useStrings(COMMON);
  const kindText = useKindText();
  const { busy, error, run } = useSubmit('requestReset');
  const [checked, setChecked] = useState(false);
  const [sentTo, setSentTo] = useState<string | null>(null);

  const emailError = checked && !isValidEmail(email) ? c.emailInvalid : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!isValidEmail(email)) return;
    const address = email.trim();
    void run(async () => {
      await api.requestReset(address);
      setSentTo(address);
    });
  };

  if (sentTo) {
    return (
      <div className="acct-form" role="status">
        <Banner tone="info" icon="mail">
          {fmt(s.resetSent, { email: sentTo })}
        </Banner>
        {onBack && (
          <Button full onClick={onBack}>
            {s.backToLogin}
          </Button>
        )}
      </div>
    );
  }

  return (
    <form className="acct-form" onSubmit={submit} noValidate>
      <p className="acct-lead">{lead ?? s.forgotLead}</p>
      <EmailInput value={email} onChange={onEmail} error={emailError} />
      {error && (
        <Banner tone="error" icon="alert">
          {kindText(error)}
        </Banner>
      )}
      <Button type="submit" variant="primary" size="lg" full busy={busy} icon="mail">
        {s.sendReset}
      </Button>
      {onBack && (
        <Button variant="ghost" full onClick={onBack}>
          {s.backToLogin}
        </Button>
      )}
    </form>
  );
}
