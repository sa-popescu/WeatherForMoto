import { useState, type FormEvent } from 'react';
import { useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button, TextField } from '../../../ui/primitives';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { cleanDisplayName, isValidEmail, NAME_MAX, passwordProblem } from '../lib/validation';
import { AUTH } from '../strings/auth';
import { COMMON } from '../strings/common';
import { PasswordField } from '../ui/PasswordField';
import type { AuthMode } from './AuthCard';
import { EmailInput } from './EmailInput';

interface SignupFormProps {
  email: string;
  onEmail: (value: string) => void;
  onMode: (mode: AuthMode) => void;
}

/**
 * The backend signs the new account in right away; the address is confirmed
 * later through the emailed link (the signed-in header keeps reminding).
 */
export function SignupForm({ email, onEmail, onMode }: SignupFormProps) {
  const s = useStrings(AUTH);
  const c = useStrings(COMMON);
  const { signup } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const { busy, error, run } = useSubmit('signup');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [checked, setChecked] = useState(false);

  const problem = passwordProblem(password);
  const emailError = checked && !isValidEmail(email) ? c.emailInvalid : null;
  const passwordError = checked && problem ? (problem === 'short' ? c.passwordShort : c.passwordLong) : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!isValidEmail(email) || problem) return;
    void run(async () => {
      const res = await signup(email.trim(), password, cleanDisplayName(name) || undefined);
      toast(res.verification_email_sent ? s.signupDone : s.signupDoneNoMail, { tone: 'success', durationMs: 8000 });
    });
  };

  return (
    <form className="acct-form" onSubmit={submit} noValidate>
      <EmailInput value={email} onChange={onEmail} error={emailError} />
      <PasswordField
        label={c.passwordLabel}
        value={password}
        onChange={setPassword}
        autoComplete="new-password"
        hint={s.passwordNewHint}
        error={passwordError}
      />
      <TextField label={s.nameLabel} value={name} onChange={setName} autoComplete="nickname" maxLength={NAME_MAX} />
      {error && (
        <Banner tone="error" icon="alert">
          {kindText(error)}
        </Banner>
      )}
      {error === 'emailExists' && (
        <div className="acct-inline-actions">
          <Button onClick={() => onMode('login')}>{s.existsLogin}</Button>
          <Button onClick={() => onMode('forgot')}>{s.existsForgot}</Button>
        </div>
      )}
      <Button type="submit" variant="primary" size="lg" full busy={busy}>
        {s.signup}
      </Button>
    </form>
  );
}
