import { describe, expect, it } from 'vitest';
import { ApiError } from '../../../lib/api';
import { classifyError, isSessionError } from './errors';

describe('classifyError', () => {
  it('reads 401 per action', () => {
    expect(classifyError(new ApiError(401, 'Email sau parolă invalidă'), 'login')).toBe('wrongCredentials');
    expect(classifyError(new ApiError(401, 'Parola curentă este incorectă'), 'changePassword')).toBe('wrongPassword');
    expect(classifyError(new ApiError(401, 'Token expired'), 'changePassword')).toBe('sessionExpired');
    expect(classifyError(new ApiError(401, 'Invalid token'), 'other')).toBe('sessionExpired');
  });

  it('reads 409 per action', () => {
    expect(classifyError(new ApiError(409, 'Account already exists'), 'signup')).toBe('emailExists');
    expect(classifyError(new ApiError(409, 'Adresa de email este deja folosită.'), 'changeEmail')).toBe('emailTaken');
  });

  it('separates wrong, expired and burnt codes', () => {
    expect(classifyError(new ApiError(400, 'Invalid code'), 'verifyCode')).toBe('codeInvalid');
    expect(classifyError(new ApiError(400, 'Code expired'), 'verifyCode')).toBe('codeExpired');
    expect(classifyError(new ApiError(400, 'No code requested'), 'verifyCode')).toBe('codeExpired');
    expect(classifyError(new ApiError(429, 'Prea multe încercări. Solicită un cod nou.'), 'verifyCode')).toBe('codeExhausted');
    expect(classifyError(new ApiError(429, 'Prea multe încercări. Încearcă din nou mai târziu.'), 'verifyCode')).toBe('rateLimit');
  });

  it('maps the rest', () => {
    expect(classifyError(new ApiError(400, 'Link de resetare invalid sau expirat.'), 'resetPassword')).toBe('resetLinkInvalid');
    expect(classifyError(new ApiError(400, 'Parolă incorectă.'), 'changeEmail')).toBe('wrongPassword');
    expect(classifyError(new ApiError(429, 'Prea multe cereri.'), 'requestReset')).toBe('rateLimit');
    expect(classifyError(new ApiError(0, 'timeout'), 'login')).toBe('network');
    expect(classifyError(new ApiError(422, 'value is not a valid email'), 'signup')).toBe('invalidInput');
    expect(classifyError(new ApiError(503, 'Serviciu temporar indisponibil'), 'login')).toBe('unavailable');
    expect(classifyError(new Error('boom'), 'login')).toBe('generic');
  });

  it('flags only real session loss', () => {
    expect(isSessionError(new ApiError(401, 'Email sau parolă invalidă'), 'login')).toBe(false);
    expect(isSessionError(new ApiError(403, 'Forbidden'), 'other')).toBe(true);
  });
});
