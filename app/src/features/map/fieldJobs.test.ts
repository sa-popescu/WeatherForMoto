import { describe, expect, it } from 'vitest';
import { runJob, type Drawing } from './fieldJobs';
import { CHANNELS, dbzToCode } from './rainValues';

/** A canvas stand-in: remembers what was drawn and hands back fixed pixels. */
function fakeCanvas(width: number, height: number, rgba: [number, number, number, number]) {
  const drawn: unknown[][] = [];
  const data = new Uint8ClampedArray(width * height * 4);
  for (let i = 0; i < data.length; i += 4) data.set(rgba, i);
  const ctx = {
    imageSmoothingEnabled: false,
    imageSmoothingQuality: 'low',
    drawImage: (...args: unknown[]) => drawn.push(args),
    clearRect: () => undefined,
    getImageData: () => ({ data }),
  };
  return { ctx: ctx as unknown as Drawing, drawn };
}

const image = {} as CanvasImageSource;

describe('runJob', () => {
  it('lays radar tiles at their place and reads them back into rain values', async () => {
    const { ctx, drawn } = fakeCanvas(4, 4, [0xff, 0xee, 0x00, 0xff]);
    const tiles = [
      { url: 'a', left: 0, top: 0 },
      { url: 'b', left: -100, top: 0 },
    ];
    const field = await runJob({ kind: 'radar', width: 4, height: 4, tiles }, ctx, async () => image);
    expect(drawn.map((args) => args.slice(1))).toEqual([
      [0, 0],
      [-100, 0],
    ]);
    // An even field stays even through the smoothing.
    expect(field[5 * CHANNELS]).toBe(dbzToCode(35));
  });

  it('gives up when most tiles fail, but not over one', async () => {
    const { ctx } = fakeCanvas(2, 2, [0, 0, 0, 0]);
    const tiles = ['a', 'b', 'c'].map((url) => ({ url, left: 0, top: 0 }));
    const failing = (bad: string[]) => async (url: string) => {
      if (bad.includes(url)) throw new Error('tile failed');
      return image;
    };
    await expect(runJob({ kind: 'radar', width: 2, height: 2, tiles }, ctx, failing(['a']))).resolves.toBeDefined();
    await expect(runJob({ kind: 'radar', width: 2, height: 2, tiles }, ctx, failing(['a', 'b']))).rejects.toThrow('2 of 3');
  });

  it('stretches a satellite picture over the whole canvas', async () => {
    const { ctx, drawn } = fakeCanvas(3, 2, [0, 0, 0, 0]);
    const field = await runJob({ kind: 'lightning', width: 3, height: 2, url: 'x' }, ctx, async () => image);
    expect(drawn[0].slice(1)).toEqual([0, 0, 3, 2]);
    expect(field).toHaveLength(3 * 2 * CHANNELS);
  });
});
