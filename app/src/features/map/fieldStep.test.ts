import { describe, expect, it } from 'vitest';
import { modelFrame, pictureFrame } from './bandFrames';
import type { FieldGeometry } from './mercator';
import { fieldStep } from './useFieldLayer';

const GEOMETRY: FieldGeometry = { bounds: { south: 43, west: 20, north: 48, east: 30 }, z: 6, width: 8, height: 6 } as FieldGeometry;

describe('the field layer while the band moves', () => {
  it('leaves the picture on screen while the next one is still on its way', () => {
    // Canvas 0 shows "10:00"; "10:10" has no pixels yet.
    expect(fieldStep(['10:00', null], 0, '10:10', false)).toEqual({ kind: 'leave' });
  });

  it('paints a new picture on the canvas not on screen, so it can fade in over the other', () => {
    expect(fieldStep(['10:00', null], 0, '10:10', true)).toEqual({ kind: 'paint', buffer: 1 });
    expect(fieldStep(['10:00', '10:10'], 1, '10:20', true)).toEqual({ kind: 'paint', buffer: 0 });
    expect(fieldStep([null, null], null, '10:00', true)).toEqual({ kind: 'paint', buffer: 0 });
  });

  it('keeps the picture already on screen, and goes back to the one just before without painting it again', () => {
    expect(fieldStep(['10:00', '10:10'], 1, '10:10', true)).toEqual({ kind: 'keep', buffer: 1 });
    expect(fieldStep(['10:00', '10:10'], 1, '10:00', false)).toEqual({ kind: 'reshow', buffer: 0 });
  });

  it('has a pending frame, not none, for a moment whose data has not come yet', () => {
    const waiting = modelFrame(1_800_000_000, GEOMETRY, null);
    expect(waiting.paint()).toBeNull();
    expect(waiting.key).toContain('pending');
    expect(pictureFrame('satellite|10:00', undefined).paint()).toBeNull();
  });
});
