// Runs async work over a list with a fixed number of requests in flight.
// Abort stops picking new items; items already running get the signal too.

export async function mapPool<T, R>(
  items: ReadonlyArray<T>,
  limit: number,
  worker: (item: T, index: number, signal?: AbortSignal) => Promise<R>,
  signal?: AbortSignal,
  onSettled?: (index: number, result: PromiseSettledResult<R>) => void,
): Promise<PromiseSettledResult<R>[]> {
  const results: PromiseSettledResult<R>[] = new Array(items.length);
  let next = 0;

  const run = async (): Promise<void> => {
    while (next < items.length && !signal?.aborted) {
      const index = next;
      next += 1;
      let result: PromiseSettledResult<R>;
      try {
        result = { status: 'fulfilled', value: await worker(items[index], index, signal) };
      } catch (reason) {
        result = { status: 'rejected', reason };
      }
      results[index] = result;
      if (!signal?.aborted) onSettled?.(index, result);
    }
  };

  const lanes = Math.max(1, Math.min(limit, items.length));
  await Promise.all(Array.from({ length: lanes }, run));
  return results;
}
