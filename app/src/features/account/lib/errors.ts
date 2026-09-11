import { ApiError } from '../../../lib/api';

// Maps an API failure to what the user should be told. The same status code
// means different things per endpoint (401 on login = wrong password, 401 on
// /me/prefs = expired session), so the caller names the action.

export type ErrorKind =
  | 'network'
  | 'rateLimit'
  | 'wrongCredentials'
  | 'emailExists'
  | 'codeInvalid'
  | 'codeExpired'
  | 'codeExhausted'
  | 'resetLinkInvalid'
  | 'wrongPassword'
  | 'emailTaken'
  | 'invalidInput'
  | 'sessionExpired'
  | 'unavailable'
  | 'generic';

export type AuthAction =
  | 'login'
  | 'signup'
  | 'requestCode'
  | 'verifyCode'
  | 'requestReset'
  | 'resetPassword'
  | 'changePassword'
  | 'changeEmail'
  | 'other';

/** Session errors from get_current_user all mention the token. */
function isTokenDetail(detail: string): boolean {
  return /token/i.test(detail);
}

function classify401(err: ApiError, action: AuthAction): ErrorKind {
  if (action === 'login') return 'wrongCredentials';
  if (action === 'changePassword' && !isTokenDetail(err.detail)) return 'wrongPassword';
  return 'sessionExpired';
}

function classify400(err: ApiError, action: AuthAction): ErrorKind {
  if (action === 'verifyCode') return /invalid code/i.test(err.detail) ? 'codeInvalid' : 'codeExpired';
  if (action === 'resetPassword') return 'resetLinkInvalid';
  if (action === 'changeEmail') return 'wrongPassword';
  return 'invalidInput';
}

export function classifyError(err: unknown, action: AuthAction): ErrorKind {
  if (!(err instanceof ApiError)) return 'generic';
  switch (err.status) {
    case 0:
      return 'network';
    case 400:
      return classify400(err, action);
    case 401:
      return classify401(err, action);
    case 403:
      return 'sessionExpired';
    case 409:
      return action === 'changeEmail' ? 'emailTaken' : action === 'signup' ? 'emailExists' : 'generic';
    case 422:
      return 'invalidInput';
    case 429:
      // A burnt code needs a new one; other 429s just need a pause.
      return action === 'verifyCode' && /cod nou/i.test(err.detail) ? 'codeExhausted' : 'rateLimit';
    case 502:
    case 503:
      return 'unavailable';
    default:
      return 'generic';
  }
}

/** true when the error means this device's session is gone and should be cleared. */
export function isSessionError(err: unknown, action: AuthAction): boolean {
  return classifyError(err, action) === 'sessionExpired';
}
