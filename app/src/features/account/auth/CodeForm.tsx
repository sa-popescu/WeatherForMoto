import { useState, type FormEvent } from 'react';
import { fmt, useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button, TextField } from '../../../ui/primitives';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { isCompleteCode, isValidEmail, normalizeCode } from '../lib/validation';
import { AUTH } from '../strings/auth';
import { COMMON } from '../strings/common';
import { Note } from '../ui/controls';
import { EmailInput } from './EmailInput';

interface CodeFormProps {
  email: string;
  onEmail: (value: string) => void;
}

/** Passwordless sign-in: request a 6-digit code, then type it. */
export function CodeForm({ email, onEmail }: CodeFormProps) {
  const s = useStrings(AUTH);
  const c = useStrings(COMMON);
  const { requestCode, verifyCode } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const send = useSubmit('requestCode');
  const verify = useSubmit('verifyCode');
  const [step, setStep] = useState<'email' | 'code'>('email');
  const [code, setCode] = useState('');
  const [checked, setChecked] = useState(false);

  const emailError = checked && !isValidEmail(email) ? c.emailInvalid : null;
  const codeError = checked && step === 'code' && !isCompleteCode(code) ? s.codeHint : null;

  const sendCode = (event?: FormEvent): void => {
    event?.preventDefault();
    setChecked(true);
    if (!isValidEmail(email)) return;
    void send.run(async () => {
      await requestCode(email.trim());
      setStep('code');
      setCode('');
      setChecked(false);
      verify.setError(null);
    });
  };

  const submitCode = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!isCompleteCode(code)) return;
    void verify.run(async () => {
      await verifyCode(email.trim(), code);
      toast(s.welcome, { tone: 'success' });
    });
  };

  if (step === 'email') {
    return (
      <form className="acct-form" onSubmit={sendCode} noValidate>
        <p className="acct-lead">{s.codeLead}</p>
        <EmailInput value={email} onChange={onEmail} error={emailError} />
        {send.error && (
          <Banner tone="error" icon="alert">
            {kindText(send.error)}
          </Banner>
        )}
        <Button type="submit" variant="primary" size="lg" full busy={send.busy} icon="mail">
          {s.sendCode}
        </Button>
      </form>
    );
  }

  return (
    <form className="acct-form" onSubmit={submitCode} noValidate>
      <p className="acct-lead" role="status">
        {fmt(s.codeSent, { email: email.trim() })}
      </p>
      <TextField
        className="acct-code"
        label={s.codeLabel}
        hint={s.codeHint}
        error={codeError}
        value={code}
        onChange={(value) => setCode(normalizeCode(value))}
        inputMode="numeric"
        autoComplete="one-time-code"
        pattern="[0-9]*"
        autoFocus
      />
      {(verify.error || send.error) && (
        <Banner tone="error" icon="alert">
          {kindText(verify.error ?? send.error ?? 'generic')}
        </Banner>
      )}
      <Button type="submit" variant="primary" size="lg" full busy={verify.busy}>
        {s.verifyCode}
      </Button>
      <div className="acct-inline-actions">
        <Button variant="ghost" icon="refresh" busy={send.busy} onClick={() => sendCode()}>
          {s.resendCode}
        </Button>
        <Button variant="ghost" onClick={() => setStep('email')}>
          {s.otherEmail}
        </Button>
      </div>
      <Note>{s.codeNote}</Note>
    </form>
  );
}
