import { useStrings } from '../../../lib/i18n';
import { TextField } from '../../../ui/primitives';
import { COMMON } from '../strings/common';

interface EmailInputProps {
  value: string;
  onChange: (value: string) => void;
  error?: string | null;
  label?: string;
  autoFocus?: boolean;
}

export function EmailInput({ value, onChange, error, label, autoFocus }: EmailInputProps) {
  const c = useStrings(COMMON);
  return (
    <TextField
      label={label ?? c.emailLabel}
      type="email"
      inputMode="email"
      autoComplete="email"
      autoCapitalize="none"
      autoCorrect="off"
      spellCheck={false}
      maxLength={254}
      value={value}
      onChange={onChange}
      error={error}
      autoFocus={autoFocus}
    />
  );
}
