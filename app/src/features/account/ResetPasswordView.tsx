import './account.css';
import { useState, type FormEvent } from 'react';
import { api } from '../../lib/api';
import { useStrings } from '../../lib/i18n';
import { useAuth } from '../../state/auth';
import { Banner, Button, Card } from '../../ui/primitives';
import { ForgotForm } from './auth/ForgotForm';
import { useKindText } from './hooks/useErrorText';
import { useSubmit } from './hooks/useSubmit';
import { classifyError } from './lib/errors';
import { passwordProblem } from './lib/validation';
import { AUTH } from './strings/auth';
import { COMMON } from './strings/common';
import { PasswordField } from './ui/PasswordField';

interface ResetPasswordViewProps {
  /** Token from the ?reset_token= link; only ever sent to the API, never written into markup. */
  token: string;
  /** Called after a successful reset or when the user leaves the view. */
  onDone: () => void;
}

type Phase = 'form' | 'done' | 'invalid';

export default function ResetPasswordView({ token, onDone }: ResetPasswordViewProps) {
  const s = useStrings(AUTH);
  const c = useStrings(COMMON);
  const kindText = useKindText();
  const { token: sessionToken, refreshMe } = useAuth();
  const { busy, error, run } = useSubmit('resetPassword');
  const [phase, setPhase] = useState<Phase>('form');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [checked, setChecked] = useState(false);
  const [email, setEmail] = useState('');

  const problem = passwordProblem(password);
  const passwordError = checked && problem ? (problem === 'short' ? c.passwordShort : c.passwordLong) : null;
  const confirmError = checked && !problem && confirm !== password ? s.mismatch : null;

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (problem || confirm !== password) return;
    void run(async () => {
      try {
        await api.resetPassword(token, password);
      } catch (err) {
        if (classifyError(err, 'resetPassword') !== 'resetLinkInvalid') throw err;
        setPhase('invalid');
        return;
      }
      setPhase('done');
      // The backend revoked every session; let this device notice its stale one.
      if (sessionToken) void refreshMe();
    });
  };

  return (
    <div className="screen acct acct-reset">
      <header className="acct-head">
        <span className="eyebrow">MotoMeteo</span>
        <h1 className="acct-head__title num">{s.resetTitle}</h1>
      </header>
      <Card className="acct-stack">
        {phase === 'form' && (
          <form className="acct-form" onSubmit={submit} noValidate>
            <p className="acct-lead">{s.resetLead}</p>
            <PasswordField
              label={s.newPassword}
              value={password}
              onChange={setPassword}
              autoComplete="new-password"
              hint={s.passwordNewHint}
              error={passwordError}
              autoFocus
            />
            <PasswordField label={s.confirmPassword} value={confirm} onChange={setConfirm} autoComplete="new-password" error={confirmError} />
            {error && (
              <Banner tone="error" icon="alert">
                {kindText(error)}
              </Banner>
            )}
            <Button type="submit" variant="primary" size="lg" full busy={busy}>
              {s.savePassword}
            </Button>
          </form>
        )}
        {phase === 'done' && (
          <>
            <div role="status">
              <Banner tone="info" icon="check">
                {s.resetDone}
              </Banner>
            </div>
            <Button variant="primary" size="lg" full onClick={onDone}>
              {s.goSignIn}
            </Button>
          </>
        )}
        {phase === 'invalid' && (
          <>
            <Banner tone="error" icon="alert">
              {c.errResetLink}
            </Banner>
            <ForgotForm email={email} onEmail={setEmail} lead={s.resetInvalidLead} />
          </>
        )}
      </Card>
      {phase !== 'done' && (
        <Button variant="ghost" full onClick={onDone}>
          {s.backToApp}
        </Button>
      )}
    </div>
  );
}
