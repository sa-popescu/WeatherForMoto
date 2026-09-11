// Runs async work over a list with a fixed number of lanes, so the weather
// API never gets more than `limit` requests at once from this screen.

export async function runPool<T>(
  items: readonly T[],
  limit: number,
  worker: (item: T, index: number) => Promise<void>,
  signal?: AbortSignal,
): Promise<void> {
  let next = 0;
  const lane = async (): Promise<void> => {
    while (next < items.length && !signal?.aborted) {
      const index = next;
      next += 1;
      try {
        await worker(items[index], index);
      } catch (err) {
        // Workers report their own failures; this only keeps the lane alive.
        console.warn('[route] pool worker failed', err);
      }
    }
  };
  const lanes = Math.max(1, Math.min(limit, items.length));
  await Promise.all(Array.from({ length: lanes }, lane));
}
