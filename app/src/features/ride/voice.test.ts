import { describe, expect, it } from 'vitest';
import { spokenDistance, spokenKmh, spokenMinutes, spokenWhere } from './voice';

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

  it('says minutes, speeds and places the Romanian way', () => {
    expect(spokenMinutes(1, 'ro')).toBe('un minut');
    expect(spokenMinutes(15, 'ro')).toBe('15 minute');
    expect(spokenMinutes(25, 'ro')).toBe('25 de minute');
    expect(spokenKmh(62, 'ro')).toBe('62 de kilometri pe oră');
    expect(spokenKmh(18, 'ro')).toBe('18 kilometri pe oră');
    expect(spokenWhere('Pitești', 'ro')).toBe('pe la Pitești');
    expect(spokenWhere('DN7', 'ro')).toBe('pe DN7');
    expect(spokenWhere('A1', 'en')).toBe('on A1');
  });

  it('says English distances too', () => {
    expect(spokenDistance(0.3, 'en')).toBe('300 metres');
    expect(spokenDistance(1, 'en')).toBe('1 kilometre');
    expect(spokenDistance(4.53, 'en')).toBe('4.5 kilometres');
  });
});
