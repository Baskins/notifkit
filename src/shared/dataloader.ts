export interface DataLoaderOptions {
  /**
   * How long to collect keys before loading. 0 (the default) batches only the
   * keys requested in the same microtask; callers that reach the loader after
   * independent I/O rarely line up that closely, so a few milliseconds turns
   * one query per message into one query per burst.
   */
  batchDelayMs?: number;
  /** Load immediately once this many keys are waiting. */
  maxBatchSize?: number;
}

export class DataLoader<K, V> {
  private keys: K[] = [];
  private promises: Array<{ resolve: (value: V | Error) => void }> = [];
  private scheduled = false;
  private timer: NodeJS.Timeout | null = null;
  private readonly batchDelayMs: number;
  private readonly maxBatchSize: number;

  constructor(
    private readonly batchLoadFn: (keys: K[]) => Promise<(V | Error)[]>,
    { batchDelayMs = 0, maxBatchSize = Infinity }: DataLoaderOptions = {},
  ) {
    this.batchDelayMs = batchDelayMs;
    this.maxBatchSize = maxBatchSize;
  }

  load(key: K): Promise<V> {
    return new Promise((resolve, reject) => {
      this.keys.push(key);
      this.promises.push({
        resolve: (value) => {
          if (value instanceof Error) reject(value);
          else resolve(value);
        },
      });

      if (this.keys.length >= this.maxBatchSize) {
        this.dispatch();
      } else if (!this.scheduled) {
        this.scheduled = true;
        if (this.batchDelayMs > 0) {
          this.timer = setTimeout(() => this.dispatch(), this.batchDelayMs);
        } else {
          void Promise.resolve().then(() => this.dispatch());
        }
      }
    });
  }

  private dispatch(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.scheduled = false;
    if (this.keys.length === 0) return;

    const keysToLoad = this.keys;
    const currentPromises = this.promises;
    this.keys = [];
    this.promises = [];

    this.batchLoadFn(keysToLoad)
      .then((results) => {
        for (let i = 0; i < currentPromises.length; i++) {
          currentPromises[i]!.resolve(results[i] as V | Error);
        }
      })
      .catch((err) => {
        for (const p of currentPromises) {
          p.resolve(err);
        }
      });
  }
}
