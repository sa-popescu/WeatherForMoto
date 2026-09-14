import { describe, expect, it } from 'vitest';
import { MAX_SUGGESTIONS, parsePhoton, photonUrl, type PhotonFeature } from './photon';

// Shapes taken from real Photon answers (photon.komoot.io, September 2026).
function feature(lat: number, lon: number, properties: Record<string, string>): PhotonFeature {
  return { geometry: { coordinates: [lon, lat] }, properties };
}

const mallBucharest = feature(44.4782, 26.1034, {
  name: 'Promenada Mall', type: 'house', osm_key: 'shop', osm_value: 'mall', district: 'Sector 1', city: 'București', country: 'România', countrycode: 'RO',
});
const mallSibiu = feature(45.797, 24.163, {
  name: 'Promenada Mall', type: 'house', osm_key: 'shop', osm_value: 'mall', district: 'Trei Stejari', city: 'Sibiu', county: 'Sibiu', country: 'România', countrycode: 'RO',
});
const parking = feature(44.4784, 26.1034, {
  name: 'Parcare Promenada Mall', type: 'house', osm_key: 'amenity', osm_value: 'parking', city: 'București', countrycode: 'RO',
});

describe('parsePhoton', () => {
  it('keeps landmarks with where they are and drops parking, stops and blocks of flats', () => {
    const busStop = feature(44.4282, 26.0491, { name: 'Sibiu', type: 'house', osm_key: 'highway', osm_value: 'bus_stop', city: 'București', countrycode: 'RO' });
    const tram = feature(44.4283, 26.0489, { name: 'Sibiu', type: 'house', osm_key: 'railway', osm_value: 'tram_stop', city: 'București', countrycode: 'RO' });
    const flats = feature(44.4698, 26.1121, { name: 'Floreasca Residence Bloc 5', type: 'house', osm_key: 'building', osm_value: 'apartments', countrycode: 'RO' });
    const results = parsePhoton([mallBucharest, parking, busStop, tram, flats]);
    expect(results).toEqual([
      { name: 'Promenada Mall', lat: 44.4782, lon: 26.1034, region: 'Sector 1, București', country: 'România', countryCode: 'RO' },
    ]);
  });

  it('merges one street split in segments, but not the same name in another city', () => {
    const district = feature(44.4198, 26.0284, { name: 'Drumul Taberei', type: 'district', osm_key: 'place', osm_value: 'suburb', city: 'București', countrycode: 'RO' });
    const segmentA = feature(44.4242, 26.047, { name: 'Drumul Taberei', type: 'street', osm_key: 'highway', osm_value: 'secondary', district: 'Drumul Taberei', city: 'București', countrycode: 'RO' });
    const segmentB = feature(44.4187, 26.0278, { name: 'Drumul Taberei', type: 'street', osm_key: 'highway', osm_value: 'residential', city: 'București', countrycode: 'RO' });
    expect(parsePhoton([district, segmentB]).map((r) => r.region)).toEqual(['București']);
    // Segment A is about 1.6 km from the district centre: a separate row.
    expect(parsePhoton([district, segmentA, segmentB])).toHaveLength(2);
    expect(parsePhoton([mallBucharest, mallSibiu]).map((r) => r.region)).toEqual(['Sector 1, București', 'Trei Stejari, Sibiu']);
  });

  it('narrows to the county after a comma and ignores a hint that matches nothing', () => {
    const village = feature(45.8096, 24.8164, { name: 'Sâmbăta de Jos', type: 'district', osm_key: 'place', osm_value: 'village', city: 'Voila', county: 'Brașov', countrycode: 'RO' });
    const elsewhere = feature(47.1, 22.9, { name: 'Sâmbăta', type: 'city', osm_key: 'place', osm_value: 'village', county: 'Bihor', countrycode: 'RO' });
    expect(parsePhoton([elsewhere, village], 'Brasov').map((r) => r.name)).toEqual(['Sâmbăta de Jos']);
    expect(parsePhoton([elsewhere, village], 'Atlantis')).toHaveLength(2);
  });

  it('drops a county that shares its name with a town and puts Romania first', () => {
    const town = feature(45.7974, 24.1519, { name: 'Sibiu', type: 'city', osm_key: 'place', osm_value: 'city', county: 'Sibiu', country: 'România', countrycode: 'RO' });
    const county = feature(45.8888, 24.2374, { name: 'Sibiu', type: 'county', osm_key: 'place', osm_value: 'county', country: 'România', countrycode: 'RO' });
    const abroad = feature(48.8566, 2.3522, { name: 'Paris', type: 'city', osm_key: 'place', osm_value: 'city', country: 'France', countrycode: 'fr' });
    const results = parsePhoton([abroad, county, town]);
    expect(results.map((r) => `${r.name}/${r.countryCode}`)).toEqual(['Sibiu/RO', 'Paris/FR']);
    expect(results[0]?.region).toBeNull();
  });

  it('names an address by street and number, skips nameless or broken entries and caps the list', () => {
    const address = feature(44.43, 26.1, { street: 'Strada Lipscani', housenumber: '12', city: 'București', countrycode: 'RO' });
    const nameless = feature(44.43, 26.1, { osm_key: 'place', osm_value: 'suburb' });
    const broken: PhotonFeature = { properties: { name: 'No geometry' } };
    expect(parsePhoton([address, nameless, broken]).map((r) => r.name)).toEqual(['Strada Lipscani 12']);
    const many = Array.from({ length: 12 }, (_, i) => feature(44 + i, 26, { name: `Loc ${i}`, countrycode: 'RO' }));
    expect(parsePhoton(many)).toHaveLength(MAX_SUGGESTIONS);
  });
});

describe('photonUrl', () => {
  it('uses local names in Romanian, English labels in English and a coarse location bias', () => {
    const ro = new URL(photonUrl('promenada mall', 'ro', { lat: 44.43278, lon: 26.10389 }));
    expect(ro.searchParams.get('q')).toBe('promenada mall');
    expect(ro.searchParams.get('lang')).toBeNull();
    expect([ro.searchParams.get('lat'), ro.searchParams.get('lon')]).toEqual(['44.43', '26.10']);
    const en = new URL(photonUrl('Sibiu', 'en'));
    expect(en.searchParams.get('lang')).toBe('en');
    expect(en.searchParams.has('lat')).toBe(false);
  });
});
