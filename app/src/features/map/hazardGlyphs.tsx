import type { HazardType } from '../../lib/types';
import { severityTone } from './hazards';

// Stroke glyphs per hazard type (24 px grid, same style as ui/icons.tsx). Kept
// as path data so one set of shapes feeds both React and the Leaflet markers,
// which need plain HTML strings.

const GLYPHS: Record<HazardType, readonly string[]> = {
  gravel: [
    'M3.5 19.5h17',
    'M5.5 15.5a2 2 0 1 0 4 0a2 2 0 1 0 -4 0',
    'M10.5 11a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0 -5 0',
    'M15.5 16a1.8 1.8 0 1 0 3.6 0a1.8 1.8 0 1 0 -3.6 0',
    'M7 7.5a1.3 1.3 0 1 0 2.6 0a1.3 1.3 0 1 0 -2.6 0',
  ],
  ice: ['M12 3v18', 'M4.2 7.5l15.6 9', 'M4.2 16.5l15.6-9', 'M9.8 4.8 12 7l2.2-2.2', 'M9.8 19.2 12 17l2.2 2.2'],
  flood: [
    'M12 3s-2.5 2.8-2.5 4.5a2.5 2.5 0 0 0 5 0C14.5 5.8 12 3 12 3Z',
    'M3 14c1.5 0 2.2-1.5 4.5-1.5S10 14 12 14s2.2-1.5 4.5-1.5S19.5 14 21 14',
    'M3 19c1.5 0 2.2-1.5 4.5-1.5S10 19 12 19s2.2-1.5 4.5-1.5S19.5 19 21 19',
  ],
  accident: ['M12 2.5l1.9 5.1 5.1-1.6-2.6 4.6 4.6 2.4-5.2 1 .8 5.3-4.6-2.9-4.6 2.9.8-5.3-5.2-1 4.6-2.4-2.6-4.6 5.1 1.6Z'],
  animals: [
    'M7.5 8.5a1.6 2.1 0 1 0 3.2 0a1.6 2.1 0 1 0 -3.2 0',
    'M13.3 8.5a1.6 2.1 0 1 0 3.2 0a1.6 2.1 0 1 0 -3.2 0',
    'M3.8 12.5a1.5 1.9 0 1 0 3 0a1.5 1.9 0 1 0 -3 0',
    'M17.2 12.5a1.5 1.9 0 1 0 3 0a1.5 1.9 0 1 0 -3 0',
    'M12 12.8c-2.6 0-4.8 3.2-4.8 5.2 0 1.4 1.1 2.2 2.3 2 .9-.2 1.6-.6 2.5-.6s1.6.4 2.5.6c1.2.2 2.3-.6 2.3-2 0-2-2.2-5.2-4.8-5.2Z',
  ],
  roadworks: ['M10 4h4l4.5 15h-13Z', 'M8.3 10h7.4', 'M6.9 14.5h10.2', 'M3.5 19h17'],
  other: ['M12 3.5 21.5 20h-19Z', 'M12 10v4.5', 'M12 17.5v.01'],
};

const SVG_ATTRS =
  'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"';

/** Marker HTML for L.divIcon. Built only from constants, never from report text. */
export function hazardMarkerHtml(type: HazardType, severity: number): string {
  const paths = GLYPHS[type].map((d) => `<path d="${d}"/>`).join('');
  return `<span class="map-hazard__pin map-tone--${severityTone(severity)}"><svg width="22" height="22" ${SVG_ATTRS}>${paths}</svg></span>`;
}

export function HazardGlyph({ type, size = 22 }: { type: HazardType; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {GLYPHS[type].map((d) => (
        <path key={d} d={d} />
      ))}
    </svg>
  );
}
