import { describe, expect, it } from 'vitest';
import { ApiError } from './api';
import { DETAIL_MAX, apiLeg, directLeg, failureDetail } from './failureDetail';

describe('failureDetail', () => {
  it('names the status and the message our API answered with', () => {
    const err = new ApiError(502, 'Weather data temporarily unavailable (Open-Meteo error 429: limit)');
    expect(apiLeg(err)).toBe('API HTTP 502: Weather data temporarily unavailable (Open-Meteo error 429: limit)');
  });

  it('separates an API that never answered from one that answered badly', () => {
    expect(apiLeg(new ApiError(0, 'timeout'))).toBe('API unreachable: timeout');
  });

  it('does not repeat the source name the direct call already used', () => {
    expect(directLeg(new Error('Open-Meteo HTTP 429'))).toBe('direct: Open-Meteo HTTP 429');
    expect(directLeg(new TypeError('Failed to fetch'))).toBe('direct Open-Meteo: Failed to fetch');
  });

  it('puts both legs on one line, so one screenshot shows the whole chain', () => {
    const line = failureDetail(new ApiError(502, 'upstream refused'), new Error('Open-Meteo timeout'));
    expect(line).toBe('API HTTP 502: upstream refused · direct: Open-Meteo timeout');
  });

  it('stays short enough to read on a phone', () => {
    const line = failureDetail(new ApiError(502, 'x'.repeat(400)), new Error('Open-Meteo HTTP 429'));
    expect(line.length).toBeLessThanOrEqual(DETAIL_MAX);
    expect(line.endsWith('…')).toBe(true);
  });

  it('survives something that is not an Error at all', () => {
    expect(failureDetail('boom', null)).toBe('API: boom · direct Open-Meteo: unknown');
  });
});
