import { runJob, type FieldJob } from './fieldJobs';
import type { FieldReply, FieldRequest } from './fieldWorker';
import { loadImage } from './pictures';
import type { ValueField } from './rainValues';

// Reads a field job (fieldJobs.ts) in the worker when the browser can draw
// there (OffscreenCanvas and createImageBitmap), else on the page. A worker
// that cannot start hands its waiting jobs back to the page.

interface Waiting {
  job: FieldJob;
  resolve: (field: ValueField) => void;
  reject: (error: Error) => void;
}

let worker: Worker | null | undefined;
let nextId = 0;
const waiting = new Map<number, Waiting>();

/** A signal nothing aborts: a job asked for is always finished (radarValues.ts, satelliteValues.ts share the results). */
const NEVER = new AbortController().signal;

async function readOnPage(job: FieldJob): Promise<ValueField> {
  const canvas = document.createElement('canvas');
  canvas.width = job.width;
  canvas.height = job.height;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) throw new Error('canvas 2D is not available');
  return runJob(job, ctx, (url) => loadImage(url, NEVER));
}

function giveBackToPage(): void {
  worker = null;
  const jobs = [...waiting.values()];
  waiting.clear();
  for (const { job, resolve, reject } of jobs) readOnPage(job).then(resolve, reject);
}

function fieldWorker(): Worker | null {
  if (worker !== undefined) return worker;
  if (typeof OffscreenCanvas === 'undefined' || typeof createImageBitmap === 'undefined' || typeof Worker === 'undefined') {
    worker = null;
    return worker;
  }
  try {
    const created = new Worker(new URL('./fieldWorker.ts', import.meta.url), { type: 'module' });
    created.addEventListener('message', (event: MessageEvent<FieldReply>) => {
      const entry = waiting.get(event.data.id);
      if (!entry) return;
      waiting.delete(event.data.id);
      if ('field' in event.data) entry.resolve(event.data.field);
      else entry.reject(new Error(event.data.error));
    });
    created.addEventListener('error', (event) => {
      console.warn('[map] the field worker stopped, reading on the page', event.message);
      created.terminate();
      giveBackToPage();
    });
    worker = created;
  } catch (err) {
    console.warn('[map] the field worker cannot start, reading on the page', err);
    worker = null;
  }
  return worker;
}

export function readField(job: FieldJob): Promise<ValueField> {
  const target = fieldWorker();
  if (!target) return readOnPage(job);
  return new Promise((resolve, reject) => {
    const id = (nextId += 1);
    waiting.set(id, { job, resolve, reject });
    const request: FieldRequest = { id, job };
    target.postMessage(request);
  });
}
