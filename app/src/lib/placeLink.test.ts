import { describe, expect, it } from 'vitest';
import { parsePlaceLink, placeLinkUrl } from './placeLink';

describe('place links', () => {
  it('round-trips a landmark with its coordinates', () => {
    const url = new URL(placeLinkUrl({ name: 'Promenada Mall & Co', lat: 44.478191, lon: 26.103413 }));
    expect(url.origin).toBe('https://weatherformoto.bluemouse.cc');
    expect(parsePlaceLink(url.searchParams)).toEqual({ kind: 'place', place: { name: 'Promenada Mall & Co', lat: 44.4782, lon: 26.1034 } });
  });

  it('falls back to the name for older links and for coordinates that make no sense', () => {
    expect(parsePlaceLink(new URLSearchParams('q=Sibiu'))).toEqual({ kind: 'name', name: 'Sibiu' });
    expect(parsePlaceLink(new URLSearchParams('q=Sibiu&ll=95,24'))).toEqual({ kind: 'name', name: 'Sibiu' });
    expect(parsePlaceLink(new URLSearchParams('q=Sibiu&ll=45.79;24.15'))).toEqual({ kind: 'name', name: 'Sibiu' });
    expect(parsePlaceLink(new URLSearchParams('q=Sibiu&ll=javascript:1,2'))).toEqual({ kind: 'name', name: 'Sibiu' });
  });

  it('ignores links without a name and caps long names', () => {
    expect(parsePlaceLink(new URLSearchParams('ll=45.79,24.15'))).toBeNull();
    expect(parsePlaceLink(new URLSearchParams('q=%20%20'))).toBeNull();
    const long = parsePlaceLink(new URLSearchParams(`q=${'a'.repeat(200)}&ll=-33.9,151.2`));
    expect(long).toEqual({ kind: 'place', place: { name: 'a'.repeat(80), lat: -33.9, lon: 151.2 } });
  });
});
