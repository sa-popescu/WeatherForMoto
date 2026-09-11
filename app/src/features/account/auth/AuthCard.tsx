import { useId, useState } from 'react';
import { useStrings } from '../../../lib/i18n';
import { Icon, type IconName } from '../../../ui/icons';
import { Card, Segmented } from '../../../ui/primitives';
import { AUTH } from '../strings/auth';
import { CodeForm } from './CodeForm';
import { ForgotForm } from './ForgotForm';
import { LoginForm } from './LoginForm';
import { SignupForm } from './SignupForm';

export type AuthMode = 'login' | 'code' | 'signup' | 'forgot';

/** Signed-out state: one card, four ways in. The email typed carries across modes. */
export function AuthCard() {
  const s = useStrings(AUTH);
  const titleId = useId();
  const [mode, setMode] = useState<AuthMode>('login');
  const [email, setEmail] = useState('');

  const modes: ReadonlyArray<{ value: AuthMode; label: string }> = [
    { value: 'login', label: s.modeLogin },
    { value: 'code', label: s.modeCode },
    { value: 'signup', label: s.modeSignup },
    { value: 'forgot', label: s.modeForgot },
  ];

  return (
    <>
      <Card className="acct-auth" aria-labelledby={titleId}>
        <h2 id={titleId} className="acct-card-title num">
          {s.authTitle}
        </h2>
        <p className="acct-lead">{s.authLead}</p>
        <div className="acct-seg4">
          <Segmented label={s.modeLabel} options={modes} value={mode} onChange={setMode} />
        </div>
        {mode === 'login' && <LoginForm email={email} onEmail={setEmail} onMode={setMode} />}
        {mode === 'code' && <CodeForm email={email} onEmail={setEmail} />}
        {mode === 'signup' && <SignupForm email={email} onEmail={setEmail} onMode={setMode} />}
        {mode === 'forgot' && <ForgotForm email={email} onEmail={setEmail} onBack={() => setMode('login')} />}
      </Card>
      <UnlocksCard />
    </>
  );
}

function UnlocksCard() {
  const s = useStrings(AUTH);
  const titleId = useId();
  const items: { icon: IconName; text: string }[] = [
    { icon: 'bell', text: s.unlockAlerts },
    { icon: 'route', text: s.unlockRoutes },
    { icon: 'gauge', text: s.unlockStats },
    { icon: 'alert', text: s.unlockHazards },
  ];
  return (
    <Card aria-labelledby={titleId}>
      <h2 id={titleId} className="acct-sub">
        {s.unlocksTitle}
      </h2>
      <ul className="acct-unlocks">
        {items.map((item) => (
          <li key={item.icon} className="acct-unlock">
            <span className="acct-unlock__icon" aria-hidden="true">
              <Icon name={item.icon} size={20} />
            </span>
            {item.text}
          </li>
        ))}
      </ul>
    </Card>
  );
}
