import { useCallback, useState } from 'react';
import { useAuth } from '../../../state/auth';
import { classifyError, isSessionError, type AuthAction, type ErrorKind } from '../lib/errors';

// Busy flag + classified error for one form. A lost session signs the device
// out; every other failure stays inline so the user can fix and retry.

export interface Submit {
  busy: boolean;
  error: ErrorKind | null;
  setError: (kind: ErrorKind | null) => void;
  run: (task: () => Promise<void>) => Promise<void>;
}

export function useSubmit(action: AuthAction): Submit {
  const { handleAuthError } = useAuth();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ErrorKind | null>(null);

  const run = useCallback(
    async (task: () => Promise<void>) => {
      setBusy(true);
      setError(null);
      try {
        await task();
      } catch (err) {
        console.warn(`[account] ${action} failed`, err);
        setError(classifyError(err, action));
        if (isSessionError(err, action)) handleAuthError(err);
      } finally {
        setBusy(false);
      }
    },
    [action, handleAuthError],
  );

  return { busy, error, setError, run };
}
