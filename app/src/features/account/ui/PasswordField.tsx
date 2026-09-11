import { useId, useState } from 'react';
import { useStrings } from '../../../lib/i18n';
import { Icon } from '../../../ui/icons';
import { cx } from '../../../ui/primitives';
import { PASSWORD_MAX } from '../lib/validation';
import { COMMON } from '../strings/common';

interface PasswordFieldProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  autoComplete: 'current-password' | 'new-password';
  hint?: string;
  error?: string | null;
  autoFocus?: boolean;
}

/** Password input with a show/hide button (typing with gloves on is error-prone). */
export function PasswordField({ label, value, onChange, autoComplete, hint, error, autoFocus }: PasswordFieldProps) {
  const s = useStrings(COMMON);
  const [shown, setShown] = useState(false);
  const inputId = useId();
  const hintId = `${inputId}-hint`;
  return (
    <div className={cx('field', error && 'field--error')}>
      <label className="field__label" htmlFor={inputId}>
        {label}
      </label>
      <div className="acct-pw">
        <input
          id={inputId}
          className="field__input acct-pw__input"
          type={shown ? 'text' : 'password'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          autoComplete={autoComplete}
          autoCapitalize="none"
          autoCorrect="off"
          spellCheck={false}
          maxLength={PASSWORD_MAX}
          autoFocus={autoFocus}
          aria-invalid={error ? true : undefined}
          aria-describedby={hint || error ? hintId : undefined}
        />
        <button
          type="button"
          className="acct-pw__toggle"
          aria-pressed={shown}
          aria-controls={inputId}
          aria-label={shown ? s.hidePassword : s.showPassword}
          title={shown ? s.hidePassword : s.showPassword}
          onClick={() => setShown((v) => !v)}
        >
          <Icon name="eye" size={22} />
        </button>
      </div>
      {(error || hint) && (
        <span id={hintId} className={cx('field__hint', error && 'field__hint--error')}>
          {error ?? hint}
        </span>
      )}
    </div>
  );
}
