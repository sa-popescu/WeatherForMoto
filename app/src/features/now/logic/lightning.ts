import type { Lightning } from '../../../lib/types';

// When and how the satellite's lightning is shown. Pure, so the rules are testable without React.

export type LightningTone = 'near' | 'around';

/** Nothing to show without flashes; "near" when one struck within 10 km. */
export function lightningTone(lightning: Lightning | null | undefined): LightningTone | null {
  if (!lightning || lightning.count <= 0) return null;
  return lightning.near_count > 0 ? 'near' : 'around';
}

const POINTS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'] as const;
export type CompassPoint = (typeof POINTS)[number];

/** The eight-point compass direction of a bearing in degrees (0 = north). */
export function compassPoint(bearing: number): CompassPoint {
  const i = Math.round((((bearing % 360) + 360) % 360) / 45) % 8;
  return POINTS[i];
}
