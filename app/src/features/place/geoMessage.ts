import type { GeoError } from '../../lib/geo';
import type { S_PLACE } from './strings';

type PlaceStrings = (typeof S_PLACE)['ro'];

/** User-facing reason a GPS fix failed. */
export function geoMessage(reason: GeoError['reason'], s: { [K in keyof PlaceStrings]: string }): string {
  switch (reason) {
    case 'denied':
      return s.geoDenied;
    case 'timeout':
      return s.geoTimeout;
    case 'unsupported':
      return s.geoUnsupported;
    case 'unavailable':
      return s.geoUnavailable;
  }
}
