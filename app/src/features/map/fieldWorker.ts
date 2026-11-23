import { runJob, type FieldJob } from './fieldJobs';
import type { ValueField } from './rainValues';

// Reads observed images into fields off the main thread (fieldJobs.ts), so
// the map keeps panning and the band keeps playing while a band of radar
// frames and satellite pictures is being read.

export interface FieldRequest {
  id: number;
  job: FieldJob;
}

export type FieldReply = { id: number; field: ValueField } | { id: number; error: string };

interface WorkerScope {
  onmessage: ((event: MessageEvent<FieldRequest>) => void) | null;
  postMessage: (message: FieldReply, transfer: Transferable[]) => void;
}

const scope = self as unknown as WorkerScope;

const IMAGE_TIMEOUT_MS = 15_000;

/** One image, through the browser's own cache like any other request. */
async function load(url: string): Promise<ImageBitmap> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), IMAGE_TIMEOUT_MS);
  try {
    const res = await fetch(url, { mode: 'cors', signal: controller.signal });
    if (!res.ok) throw new Error(`image answered ${res.status}`);
    return await createImageBitmap(await res.blob());
  } finally {
    clearTimeout(timer);
  }
}

scope.onmessage = (event) => {
  const { id, job } = event.data;
  void (async () => {
    try {
      const ctx = new OffscreenCanvas(job.width, job.height).getContext('2d', { willReadFrequently: true });
      if (!ctx) throw new Error('no 2D canvas in the worker');
      const field = await runJob(job, ctx, load);
      scope.postMessage({ id, field }, [field.buffer]);
    } catch (err) {
      scope.postMessage({ id, error: err instanceof Error ? err.message : String(err) }, []);
    }
  })();
};
