import {
  NotifkitServer,
  RedisClient,
  type Transport,
  type NotificationDispatchedPayload,
  type DeliveryResult,
} from "notifkit";
import http from "node:http";
import os from "node:os";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { STATS_KEYS, bucketFor } from "./stats.js";

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const NODE_NAME = process.env.NODE_NAME || os.hostname();
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// ─── Services ───────────────────────────────────────────────────────────────

const validServices = ["api", "enricher", "engine", "delivery", "scheduler", "events"] as const;
type ServiceType = (typeof validServices)[number];

const rawServices = process.env.SERVICES?.trim();
const services: ServiceType[] =
  rawServices && rawServices !== "all"
    ? rawServices
        .split(",")
        .map((s) => s.trim())
        .filter((s): s is ServiceType => (validServices as readonly string[]).includes(s))
    : [...validServices];

const apiServices = services.filter((s) => s === "api");
const workerServices = services.filter((s) => s !== "api");

// ─── Simulated provider ─────────────────────────────────────────────────────

function parseLatency(val?: string): { min: number; max: number } {
  if (!val) return { min: 0, max: 0 };
  if (val.includes("-")) {
    const [minStr, maxStr] = val.split("-");
    const min = parseInt(minStr!, 10) || 0;
    const max = parseInt(maxStr!, 10) || min;
    return { min, max };
  }
  const exact = parseInt(val, 10) || 0;
  return { min: exact, max: exact };
}

const latencyRange = parseLatency(process.env.PROVIDER_LATENCY_MS);
const failureRate = Number(process.env.PROVIDER_FAILURE_RATE || 0);

/**
 * Delivery counters, kept in memory and flushed to Redis four times a second.
 * One pipeline per flush instead of a log line per message, so measuring
 * delivery costs the node next to nothing.
 */
class DeliveryStats {
  private delivered = 0;
  private failed = 0;
  private buckets = new Map<number, number>();
  private first: number | null = null;
  private last: number | null = null;

  constructor(private readonly redis: RedisClient["native"]) {
    setInterval(() => void this.flush(), 250).unref();
  }

  record(latencyMs: number): void {
    const now = Date.now();
    this.delivered++;
    const bucket = bucketFor(latencyMs);
    this.buckets.set(bucket, (this.buckets.get(bucket) ?? 0) + 1);
    if (this.first === null) this.first = now;
    this.last = now;
  }

  recordFailure(): void {
    this.failed++;
  }

  private async flush(): Promise<void> {
    if (this.delivered === 0 && this.failed === 0) return;
    const { delivered, failed, buckets, first, last } = this;
    this.delivered = 0;
    this.failed = 0;
    this.buckets = new Map();
    this.first = null;
    this.last = null;

    const pipeline = this.redis.pipeline();
    if (delivered) pipeline.hincrby(STATS_KEYS.counters, "delivered", delivered);
    if (failed) pipeline.hincrby(STATS_KEYS.counters, "failed", failed);
    for (const [bucket, count] of buckets) {
      pipeline.hincrby(STATS_KEYS.latency, String(bucket), count);
    }
    if (first !== null) pipeline.hsetnx(STATS_KEYS.first, NODE_NAME, String(first));
    if (last !== null) pipeline.hset(STATS_KEYS.last, NODE_NAME, String(last));
    await pipeline.exec().catch(() => {});
  }
}

class ProfilingTransport implements Transport {
  readonly channel = "email";
  readonly limits = { limit: 1_000_000, windowSeconds: 1 };

  constructor(private readonly stats: DeliveryStats) {}

  async send(task: NotificationDispatchedPayload): Promise<DeliveryResult> {
    if (latencyRange.max > 0) {
      const delay =
        latencyRange.min === latencyRange.max
          ? latencyRange.min
          : Math.floor(Math.random() * (latencyRange.max - latencyRange.min + 1)) +
            latencyRange.min;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    if (failureRate > 0 && Math.random() < failureRate) {
      this.stats.recordFailure();
      return { success: false, error: "simulated provider error" };
    }

    const requestTime = (task.templateVariables as Record<string, unknown> | undefined)
      ?.requestTime;
    this.stats.record(typeof requestTime === "number" ? Date.now() - requestTime : 0);

    return { success: true, providerMessageId: `prof-${task.taskId}` };
  }
}

// ─── Node health sampling ───────────────────────────────────────────────────

/** Event loop delay is the clearest sign that a node's one JS thread is saturated. */
function startNodeSampler(redis: RedisClient["native"]): void {
  const eld = monitorEventLoopDelay({ resolution: 10 });
  eld.enable();
  setInterval(() => {
    const mem = process.memoryUsage();
    void redis
      .hset(STATS_KEYS.node(NODE_NAME), {
        services: services.join(","),
        eldP50: (eld.percentile(50) / 1e6).toFixed(1),
        eldP99: (eld.percentile(99) / 1e6).toFixed(1),
        eldMax: (eld.max / 1e6).toFixed(1),
        rssMb: (mem.rss / 1048576).toFixed(0),
        heapMb: (mem.heapUsed / 1048576).toFixed(0),
        at: String(Date.now()),
      })
      .catch(() => {});
    eld.reset();
  }, 1_000).unref();
}

// ─── Startup ────────────────────────────────────────────────────────────────

const baseOptions = {
  redisUrl: REDIS_URL,
  databaseUrl: process.env.DATABASE_URL || "postgres://notifkit:password@localhost:5432/notifkit",
  logLevel: (process.env.LOG_LEVEL as any) || "info",
  autoMigrate: false,
  port: PORT,
};

async function waitForGate(redis: RedisClient["native"]): Promise<void> {
  if (process.env.WORKER_GATE !== "1") return;
  while ((await redis.get(STATS_KEYS.gate).catch(() => null)) !== "open") {
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

async function main(): Promise<void> {
  const redis = new RedisClient({ url: REDIS_URL, name: `profiling-${NODE_NAME}` });
  const stats = new DeliveryStats(redis.native);
  startNodeSampler(redis.native);

  if (apiServices.length > 0) {
    await new NotifkitServer({ ...baseOptions, services: apiServices }).start();
  } else {
    // Worker-only node: a health endpoint for the compose healthcheck.
    http
      .createServer((req, res) => {
        if (req.url === "/health") {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "ok", services }));
        } else {
          res.writeHead(404);
          res.end();
        }
      })
      .listen(PORT, "0.0.0.0");
  }

  console.log(`profiling node ${NODE_NAME} up [services: ${services.join(",")}]`);

  if (workerServices.length > 0) {
    await waitForGate(redis.native);
    await new NotifkitServer({
      ...baseOptions,
      services: workerServices,
      providers: [new ProfilingTransport(stats)],
    }).start();
    console.log(`profiling node ${NODE_NAME} workers started [${workerServices.join(",")}]`);
  }
}

main().catch((err) => {
  console.error("Failed to start profiling server:", err);
  process.exit(1);
});
