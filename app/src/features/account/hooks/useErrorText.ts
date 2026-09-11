import { useCallback } from 'react';
import { useStrings } from '../../../lib/i18n';
import { classifyError, type AuthAction, type ErrorKind } from '../lib/errors';
import { COMMON } from '../strings/common';

type CommonKey = keyof typeof COMMON.ro;

const MESSAGE_KEY: Record<ErrorKind, CommonKey> = {
  network: 'errNetwork',
  rateLimit: 'errRateLimit',
  wrongCredentials: 'errWrongCredentials',
  emailExists: 'errEmailExists',
  codeInvalid: 'errCodeInvalid',
  codeExpired: 'errCodeExpired',
  codeExhausted: 'errCodeExhausted',
  resetLinkInvalid: 'errResetLink',
  wrongPassword: 'errWrongPassword',
  emailTaken: 'errEmailTaken',
  invalidInput: 'errInvalidInput',
  sessionExpired: 'errSession',
  unavailable: 'errUnavailable',
  generic: 'errGeneric',
};

/** Returns a function giving the user-facing sentence for an API failure. */
export function useErrorText(): (err: unknown, action: AuthAction) => string {
  const s = useStrings(COMMON);
  return useCallback((err: unknown, action: AuthAction) => s[MESSAGE_KEY[classifyError(err, action)]], [s]);
}

export function useKindText(): (kind: ErrorKind) => string {
  const s = useStrings(COMMON);
  return useCallback((kind: ErrorKind) => s[MESSAGE_KEY[kind]], [s]);
}
