import { describe, expect, it } from 'vitest';
import { boxMatrix, placement, unionBox } from './valueLayer';

const IDENTITY = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1];

/** A column-major matrix applied to (x, y, 0, 1). */
function apply(m: ArrayLike<number>, x: number, y: number): [number, number] {
  return [m[0] * x + m[4] * y + m[12], m[1] * x + m[5] * y + m[13]];
}

describe('placing the rain on the map', () => {
  const box = { x: 0.55, y: 0.35, width: 0.02, height: 0.01 };

  it('narrows the map matrix to the drawn box', () => {
    const m = boxMatrix(IDENTITY, box);
    expect(apply(m, 0, 0)[0]).toBeCloseTo(0.55, 6);
    expect(apply(m, 1, 1)[1]).toBeCloseTo(0.36, 6);
  });

  it('draws over both fields while one moves into the other', () => {
    const other = { x: 0.56, y: 0.3, width: 0.02, height: 0.02 };
    const both = unionBox(box, other);
    expect(both).toEqual({ x: 0.55, y: 0.3, width: expect.closeTo(0.03, 9), height: expect.closeTo(0.06, 9) });
    expect(unionBox(box, null)).toBe(box);
  });

  it('finds each field within the drawn box', () => {
    const drawn = { x: 0.5, y: 0.3, width: 0.1, height: 0.1 };
    const [sx, sy, ox, oy] = placement(box, drawn);
    // The field's own corners, as positions in the drawn box, come out at 0 and 1.
    expect(((box.x - drawn.x) / drawn.width) * sx + ox).toBeCloseTo(0, 9);
    expect(((box.x + box.width - drawn.x) / drawn.width) * sx + ox).toBeCloseTo(1, 9);
    expect(((box.y + box.height - drawn.y) / drawn.height) * sy + oy).toBeCloseTo(1, 9);
  });
});
