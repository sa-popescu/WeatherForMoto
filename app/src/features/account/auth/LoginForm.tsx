import { useState, type FormEvent } from 'react';
import { useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button } from '../../../ui/primitives';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { isValidEmail, passwordProblem } from '../lib/validation';
import { AUTH } from '../strings/auth';
import { COMMON } from '../strings/common';
import { PasswordField } from '../ui/PasswordField';
import type { AuthMode } from './AuthCard';
import { EmailInput } from './EmailInput';

interface LoginFormProps {
  email: string;
  onEmail: (value: string) => void;
  onMode: (mode: AuthMode) => void;
}

export function LoginForm({ email, onEmail, onMode }: LoginFormProps) {
  const s = useStrings(AUTH);
  const c = useStrings(COMMON);
  const { login } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const { busy, error, run } = useSubmit('login');
  const [password, setPassword] = useState('');
  const [checked, setChecked] = useState(false);

  const emailError = checked && !isValidEmail(email) ? c.emailInvalid : null;
  const passwordError = checked && passwordProblem(password) ? c.passwordShort : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!isValidEmail(email) || passwordProblem(password)) return;
    void run(async () => {
      await login(email.trim(), password);
      toast(s.welcome, { tone: 'success' });
    });
  };

  return (
    <form className="acct-form" onSubmit={submit} noValidate>
      <EmailInput value={email} onChange={onEmail} error={emailError} />
      <PasswordField label={c.passwordLabel} value={password} onChange={setPassword} autoComplete="current-password" error={passwordError} />
      {error && (
        <Banner tone="error" icon="alert">
          {kindText(error)}
        </Banner>
      )}
      <Button type="submit" variant="primary" size="lg" full busy={busy}>
        {s.login}
      </Button>
      <Button variant="ghost" full onClick={() => onMode('forgot')}>
        {s.forgotLink}
      </Button>
    </form>
  );
}
