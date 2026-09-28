// Shared between the profiling server (writer) and the runner (reader).

/** Upper bounds (ms) of the end-to-end latency histogram buckets. */
export const LATENCY_BUCKETS_MS = [
  5,
  10,
  25,
  50,
  75,
  100,
  150,
  200,
  250,
  300,
  400,
  500,
  750,
  1_000,
  1_500,
  2_000,
  3_000,
  5_000,
  7_500,
  10_000,
  15_000,
  20_000,
  30_000,
  45_000,
  60_000,
  90_000,
  120_000,
  180_000,
  300_000,
  Infinity,
] as const;

export const STATS_KEYS = {
  /** Hash: delivered, failed. */
  counters: "prof:stats",
  /** Hash: bucket upper bound → count. */
  latency: "prof:lat",
  /** Hash: node → earliest delivery timestamp this phase. */
  first: "prof:first",
  /** Hash: node → latest delivery timestamp this phase. */
  last: "prof:last",
  /** Hash per node: event loop delay and memory, refreshed every second. */
  node: (name: string) => `prof:node:${name}`,
  nodePattern: "prof:node:*",
  /** String: "open" once worker services may start. */
  gate: "prof:gate",
} as const;

export function bucketFor(latencyMs: number): number {
  for (const bound of LATENCY_BUCKETS_MS) {
    if (latencyMs <= bound) return bound;
  }
  return Infinity;
}

/** Percentile from a bucket histogram, interpolated linearly within a bucket. */
export function histogramPercentile(counts: Map<number, number>, p: number): number {
  const total = [...counts.values()].reduce((a, b) => a + b, 0);
  if (total === 0) return 0;

  const target = (p / 100) * total;
  let seen = 0;
  let lower = 0;
  for (const bound of LATENCY_BUCKETS_MS) {
    const count = counts.get(bound) ?? 0;
    if (seen + count >= target && count > 0) {
      if (!Number.isFinite(bound)) return lower;
      const fraction = (target - seen) / count;
      return Math.round(lower + (bound - lower) * fraction);
    }
    seen += count;
    if (Number.isFinite(bound)) lower = bound;
  }
  return lower;
}
