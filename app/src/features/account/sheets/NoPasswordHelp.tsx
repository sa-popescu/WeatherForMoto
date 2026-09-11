import { useState } from 'react';
import { api } from '../../../lib/api';
import { fmt, useStrings } from '../../../lib/i18n';
import { Banner, Button } from '../../../ui/primitives';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { PROFILE } from '../strings/profile';

/**
 * A code login on an unconfirmed account clears its password, so some users
 * have none. The reset link sets one (and signs every session out afterwards).
 */
export function NoPasswordHelp({ email }: { email: string }) {
  const s = useStrings(PROFILE);
  const kindText = useKindText();
  const { busy, error, run } = useSubmit('requestReset');
  const [sent, setSent] = useState(false);

  if (sent) {
    return (
      <Banner tone="info" icon="mail">
        {fmt(s.setByEmailSent, { email })}
      </Banner>
    );
  }

  const send = (): void => {
    void run(async () => {
      await api.requestReset(email);
      setSent(true);
    });
  };

  return (
    <div className="acct-help">
      <p className="acct-hint">{s.noPasswordHint}</p>
      <Button variant="ghost" icon="mail" busy={busy} onClick={send}>
        {s.setByEmail}
      </Button>
      {error && (
        <p className="field__hint field__hint--error" role="alert">
          {kindText(error)}
        </p>
      )}
    </div>
  );
}
