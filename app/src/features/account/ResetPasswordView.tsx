import { CORE } from '../../i18n/core';
import { useStrings } from '../../lib/i18n';
import { Card } from '../../ui/primitives';

interface ResetPasswordViewProps {
  /** Token from the ?reset_token= link; never written into markup. */
  token: string;
  /** Called after a successful reset or when the user leaves the view. */
  onDone: () => void;
}

// Placeholder, replaced by the account feature implementation.
export default function ResetPasswordView({ token, onDone }: ResetPasswordViewProps) {
  const s = useStrings(CORE);
  void token;
  return (
    <div className="screen">
      <Card>
        <p className="muted">{s.comingSoon}</p>
        <button type="button" className="btn btn--secondary" onClick={onDone}>
          {s.close}
        </button>
      </Card>
    </div>
  );
}
