import { cloudField } from './cloudValues';
import { lightningField } from './lightningValues';
import { CHANNELS, decodePixels, smoothRadarField, type ValueField } from './rainValues';

// Reading observed images into fields (rainValues.ts), the same way in a
// worker (fieldWorker.ts) or on the page when workers cannot draw: the images
// are drawn on a canvas of the field's size, read back as pixels and turned
// into values. A job says what to draw; the caller hands in the canvas and
// how to load an image.

export interface TileDraw {
  url: string;
  /** Where the tile's top-left corner lands on the canvas; tiles are drawn at their own size. */
  left: number;
  top: number;
}

export type FieldJob =
  | { kind: 'radar'; width: number; height: number; tiles: TileDraw[] }
  | { kind: 'clouds'; width: number; height: number; ir: string; mask: string }
  | { kind: 'lightning'; width: number; height: number; url: string };

export type Drawing = CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D;
export type ImageLoader = (url: string) => Promise<CanvasImageSource>;

/** Frees an image's memory at once where it can (ImageBitmap); a page image is left to the browser. */
function release(image: CanvasImageSource): void {
  if (typeof ImageBitmap !== 'undefined' && image instanceof ImageBitmap) image.close();
}

/** The whole canvas as RGBA. Throws if an image came without CORS: the canvas is then unreadable. */
function pixels(ctx: Drawing, width: number, height: number): Uint8ClampedArray {
  return ctx.getImageData(0, 0, width, height).data;
}

/** An image stretched over the whole canvas, with smoothing, read back. */
function stretched(ctx: Drawing, image: CanvasImageSource, width: number, height: number): Uint8ClampedArray {
  ctx.clearRect(0, 0, width, height);
  ctx.drawImage(image, 0, 0, width, height);
  release(image);
  return pixels(ctx, width, height);
}

export async function runJob(job: FieldJob, ctx: Drawing, load: ImageLoader): Promise<ValueField> {
  const { width, height } = job;
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  if (job.kind === 'radar') {
    const results = await Promise.allSettled(
      job.tiles.map(async (tile) => {
        const image = await load(tile.url);
        ctx.drawImage(image, tile.left, tile.top);
        release(image);
      }),
    );
    const failed = results.filter((r) => r.status === 'rejected').length;
    if (failed > job.tiles.length / 2) throw new Error(`${failed} of ${job.tiles.length} radar tiles failed`);
    const field: ValueField = new Uint8ClampedArray(width * height * CHANNELS);
    decodePixels(pixels(ctx, width, height), field);
    smoothRadarField(field, width, height);
    return field;
  }

  if (job.kind === 'lightning') return lightningField(stretched(ctx, await load(job.url), width, height), width, height);

  const [ir, mask] = await Promise.all([load(job.ir), load(job.mask)]);
  return cloudField(stretched(ctx, ir, width, height), stretched(ctx, mask, width, height), width, height);
}
