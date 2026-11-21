import { describe, expect, it } from 'vitest';
import { spokenDistance } from './voice';

describe('spokenDistance', () => {
  it('says Romanian distances the way they are said', () => {
    expect(spokenDistance(0.48, 'ro')).toBe('500 de metri');
    expect(spokenDistance(1.02, 'ro')).toBe('un kilometru');
    expect(spokenDistance(4.53, 'ro')).toBe('4,5 kilometri');
    expect(spokenDistance(12.4, 'ro')).toBe('12 kilometri');
    expect(spokenDistance(112, 'ro')).toBe('112 kilometri');
    expect(spokenDistance(124.3, 'ro')).toBe('124 de kilometri');
    expect(spokenDistance(200, 'ro')).toBe('200 de kilometri');
  });

  it('says English distances too', () => {
    expect(spokenDistance(0.3, 'en')).toBe('300 metres');
    expect(spokenDistance(1, 'en')).toBe('1 kilometre');
    expect(spokenDistance(4.53, 'en')).toBe('4.5 kilometres');
  });
});
