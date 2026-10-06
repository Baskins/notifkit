import fs from "node:fs";
import path from "node:path";
import { createDatabase, runMigrations, RedisClient } from "notifkit";
import {
  API_URL,
  DB_URL,
  REDIS_URL,
  RESULTS_DIR,
  ResourceSampler,
  buildImage,
  compose,
  composeEnv,
  cpuLimits,
  dockerCpuCount,
  nodeNames,
  provision,
  readDelivery,
  readLatency,
  resetStats,
  resourceRows,
  round,
  runK6,
  sleep,
  table,
  type Percentiles,
  type PhaseResources,
  type ProfileTier,
  type RunOptions,
} from "./runner.js";
import { STATS_KEYS, WEBHOOK_PATH, WEBHOOK_SECRET } from "./stats.js";

// Sustained-capacity benchmark: how many notifications/s one tier delivers
// without the queue in Redis growing, with production noise switched on —
// a provider that fails some sends, recipients opening mail (webhooks writing
// message_logs), and people reading the delivery log — and how fast Postgres
// grows while it does.
//
//  1. ramp — stepped open-model arrival rates. A step passes if the API takes
//     what is offered and the pipeline backlog does not trend upwards.
//  2. soak — the highest passing rate held for a long window (15 min by
//     default), sampling backlog, Redis memory and Postgres size throughout.

export interface SustainedOptions extends RunOptions {
  /** First ramp rate, req/s. */
  startRate: number;
  /** Ramp increment, req/s. */
  stepRate: number;
  /** Seconds per ramp step. */
  stepSec: number;
  soakSec: number;
  /** Fixed soak rate, or "auto" for the highest rate the ramp passed. */
  soakRate: number | "auto";
  /** Seconds between delivery and the open being reported. */
  openDelaySec: number;
  /** Share of opens that also click. */
  clickShare: number;
  /** Dashboard users polling the delivery log. */
  logReaders: number;
  logReadIntervalMs: number;
  skipRamp: boolean;
}

// ─── Redis & Postgres probes ────────────────────────────────────────────────

const DLQ = "notifkit:stream:dlq";
const SCHEDULED_SHARDS = 16;

interface BacklogSample {
  /** Unprocessed entries across every pipeline stream (acked entries are deleted). */
  total: number;
  byStream: Record<string, number>;
  /** Sends parked for a retry or a deferral. */
  scheduled: number;
  dlq: number;
  redisMemMb: number;
}

async function sampleBacklog(redis: RedisClient["native"]): Promise<BacklogSample> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = await redis.scan(cursor, "MATCH", "notifkit:stream:*", "COUNT", 200);
    cursor = next;
    keys.push(...batch);
  } while (cursor !== "0");

  const pipe = redis.pipeline();
  for (const k of keys) pipe.xlen(k);
  for (let i = 0; i < SCHEDULED_SHARDS; i++) pipe.zcard(`notif:scheduled:zset:${i}`);
  pipe.info("memory");
  const res = (await pipe.exec()) ?? [];

  const byStream: Record<string, number> = {};
  let total = 0;
  let dlq = 0;
  keys.forEach((k, i) => {
    const n = Number(res[i]?.[1] ?? 0);
    if (k === DLQ) {
      dlq = n;
      return;
    }
    const short = k.slice("notifkit:stream:".length).replace(/:(critical|normal|low)$/, "");
    byStream[short] = (byStream[short] ?? 0) + n;
    total += n;
  });
  let scheduled = 0;
  for (let i = 0; i < SCHEDULED_SHARDS; i++) scheduled += Number(res[keys.length + i]?.[1] ?? 0);
  const info = String(res[keys.length + SCHEDULED_SHARDS]?.[1] ?? "");
  const used = /used_memory:(\d+)/.exec(info);
  return {
    total,
    byStream,
    scheduled,
    dlq,
    redisMemMb: used ? round(Number(used[1]) / 1048576) : 0,
  };
}

interface DbSnapshot {
  at: number;
  dbBytes: number;
  walDirBytes: number;
  walBytesGenerated: number;
  tables: Record<string, { bytes: number; heapBytes: number; indexBytes: number; inserts: number }>;
}

async function snapshotDb(sql: any): Promise<DbSnapshot> {
  const [db] = await sql`SELECT pg_database_size(current_database())::bigint AS size`;
  const [wal] = await sql`SELECT coalesce(sum(size), 0)::bigint AS size FROM pg_ls_waldir()`;
  const [walStat] = await sql`SELECT wal_bytes::bigint AS bytes FROM pg_stat_wal`.catch(() => [
    { bytes: 0 },
  ]);
  const rows = await sql`
    SELECT relname,
           pg_total_relation_size(relid)::bigint AS total,
           pg_relation_size(relid)::bigint AS heap,
           pg_indexes_size(relid)::bigint AS idx,
           n_tup_ins::bigint AS ins
    FROM pg_stat_user_tables`;
  const tables: DbSnapshot["tables"] = {};
  for (const r of rows) {
    tables[r.relname] = {
      bytes: Number(r.total),
      heapBytes: Number(r.heap),
      indexBytes: Number(r.idx),
      inserts: Number(r.ins),
    };
  }
  return {
    at: Date.now(),
    dbBytes: Number(db.size),
    walDirBytes: Number(wal.size),
    walBytesGenerated: Number(walStat?.bytes ?? 0),
    tables,
  };
}

/** Least-squares slope of y over x (seconds), in units per second. */
function slope(points: { t: number; v: number }[]): number {
  if (points.length < 2) return 0;
  const t0 = points[0]!.t;
  const xs = points.map((p) => (p.t - t0) / 1000);
  const ys = points.map((p) => p.v);
  const mx = xs.reduce((a, b) => a + b, 0) / xs.length;
  const my = ys.reduce((a, b) => a + b, 0) / ys.length;
  let num = 0;
  let den = 0;
  xs.forEach((x, i) => {
    num += (x - mx) * (ys[i]! - my);
    den += (x - mx) ** 2;
  });
  return den === 0 ? 0 : num / den;
}

function percentiles(values: number[]): Percentiles {
  if (values.length === 0) return { p50: 0, p95: 0, p99: 0, max: 0 };
  const s = [...values].sort((a, b) => a - b);
  const at = (p: number) => s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]!;
  return {
    p50: round(at(50)),
    p95: round(at(95)),
    p99: round(at(99)),
    max: round(s[s.length - 1]!),
  };
}

// ─── Background traffic: opens and log reads ────────────────────────────────

/**
 * Plays the email provider posting engagement events back: messages the
 * transport picked to be opened are reported after a delay, in batches, the
 * way ESPs deliver webhooks. Each event is a lookup by provider_message_id
 * and a message_logs insert in the API.
 */
class EngagementDriver {
  private pending: { id: string; at: number }[] = [];
  private running = false;
  private loop: Promise<void> | null = null;
  opensSent = 0;
  clicksSent = 0;
  posts = 0;
  errors = 0;
  private latencies: number[] = [];

  constructor(
    private readonly redis: RedisClient["native"],
    private readonly delayMs: number,
    private readonly clickShare: number,
  ) {}

  start(): void {
    this.running = true;
    this.loop = this.run();
  }

  /** Stops once every queued open has been reported, or after `graceMs`. */
  async stop(graceMs: number): Promise<void> {
    const deadline = Date.now() + graceMs;
    while (Date.now() < deadline) {
      const queued = await this.redis.llen(STATS_KEYS.opens).catch(() => 0);
      if (queued === 0 && this.pending.length === 0) break;
      await sleep(1000);
    }
    this.running = false;
    await this.loop;
  }

  takeLatencies(): Percentiles {
    const p = percentiles(this.latencies);
    this.latencies = [];
    return p;
  }

  private async run(): Promise<void> {
    while (this.running) {
      const popped = ((await this.redis.lpop(STATS_KEYS.opens, 5000).catch(() => null)) ??
        []) as string[];
      for (const entry of popped) {
        const [id, at] = entry.split("|");
        if (id) this.pending.push({ id, at: Number(at) });
      }

      const cutoff = Date.now() - this.delayMs;
      let ready = 0;
      while (ready < this.pending.length && this.pending[ready]!.at <= cutoff) ready++;
      const due = this.pending.splice(0, ready);

      for (let i = 0; i < due.length; i += 500) {
        const events: { id: string; status: string; url?: string }[] = [];
        for (const { id } of due.slice(i, i + 500)) {
          events.push({ id, status: "opened" });
          if (Math.random() < this.clickShare) {
            events.push({ id, status: "clicked", url: "https://example.com/orders" });
          }
        }
        const t = performance.now();
        try {
          const res = await fetch(`${API_URL}${WEBHOOK_PATH}`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-profiling-secret": WEBHOOK_SECRET },
            body: JSON.stringify({ events }),
          });
          await res.arrayBuffer();
          if (!res.ok) throw new Error(String(res.status));
          this.latencies.push(performance.now() - t);
          this.posts++;
          for (const e of events) {
            if (e.status === "opened") this.opensSent++;
            else this.clicksSent++;
          }
        } catch {
          this.errors++;
        }
      }
      await sleep(500);
    }
  }
}

/**
 * Dashboard users reading the delivery log while traffic runs: the latest
 * page, filtered views, and a single message looked up by task id.
 */
class LogReaders {
  private running = false;
  private loops: Promise<void>[] = [];
  private latencies = new Map<string, number[]>();
  errors = 0;
  reads = 0;
  recentTaskIds: string[] = [];

  constructor(
    private readonly apiKey: string,
    private readonly readers: number,
    private readonly intervalMs: number,
  ) {}

  start(): void {
    this.running = true;
    for (let i = 0; i < this.readers; i++) this.loops.push(this.run(i));
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.all(this.loops);
  }

  takeLatencies(): Record<string, Percentiles & { count: number }> {
    const out: Record<string, Percentiles & { count: number }> = {};
    for (const [kind, values] of this.latencies) {
      out[kind] = { ...percentiles(values), count: values.length };
    }
    this.latencies = new Map();
    return out;
  }

  private async run(reader: number): Promise<void> {
    const templates = ["order_confirmation", "tracking_update", "promotional_blast"];
    for (let n = reader; this.running; n++) {
      const views: [string, string][] = [
        ["latest", "limit=50"],
        ["by template", `limit=50&templateId=${templates[n % templates.length]}`],
        ["failed", "limit=50&status=failed"],
      ];
      const taskId = this.recentTaskIds[Math.floor(Math.random() * this.recentTaskIds.length)];
      if (taskId) views.push(["by task", `taskId=${encodeURIComponent(taskId)}`]);
      const [kind, query] = views[n % views.length]!;

      const t = performance.now();
      try {
        const res = await fetch(`${API_URL}/v1/notifications/logs?${query}`, {
          headers: { Authorization: `Bearer ${this.apiKey}` },
        });
        await res.arrayBuffer();
        if (!res.ok) throw new Error(String(res.status));
        if (!this.latencies.has(kind)) this.latencies.set(kind, []);
        this.latencies.get(kind)!.push(performance.now() - t);
        this.reads++;
      } catch {
        this.errors++;
      }
      await sleep(this.intervalMs);
    }
  }
}

// ─── Ramp ───────────────────────────────────────────────────────────────────

export interface StepResult {
  rate: number;
  achievedRate: number;
  dropped: number;
  failedRequests: number;
  apiLatency: Percentiles;
  deliveryRate: number;
  /** Backlog trend over the last two thirds of the step, msgs/s. */
  backlogSlope: number;
  backlogEnd: number;
  pass: boolean;
}

interface Ctx {
  env: Record<string, string>;
  apiKey: string;
  targets: string;
  users: number;
  redis: RedisClient["native"];
}

function k6RateEnv(ctx: Ctx, rate: number, sec: number): Record<string, string> {
  return {
    MODE: "rate",
    RATE: String(rate),
    VUS: String(Math.min(2000, Math.max(100, Math.ceil(rate / 10)))),
    MAX_VUS: String(Math.min(4000, Math.max(400, Math.ceil(rate / 2)))),
    DURATION: String(sec),
    TARGETS: ctx.targets,
    API_KEY: ctx.apiKey,
    USERS: String(ctx.users),
  };
}

/** Samples backlog and deliveries every `everyMs` until stopped. */
function startSeries(redis: RedisClient["native"], everyMs: number) {
  const points: { t: number; backlog: BacklogSample; delivered: number; failed: number }[] = [];
  let on = true;
  const loop = (async () => {
    while (on) {
      const [backlog, d] = await Promise.all([sampleBacklog(redis), readDelivery(redis)]);
      points.push({ t: Date.now(), backlog, delivered: d.delivered, failed: d.failed });
      await sleep(everyMs);
    }
  })();
  return {
    points,
    stop: async () => {
      on = false;
      await loop;
    },
  };
}

async function runStep(ctx: Ctx, rate: number, sec: number): Promise<StepResult> {
  const series = startSeries(ctx.redis, 2000);
  const start = Date.now();
  const k6 = await runK6(ctx.env, k6RateEnv(ctx, rate, sec));
  const end = Date.now();
  await series.stop();

  const window = series.points.filter((p) => p.t >= start + (end - start) / 3 && p.t <= end);
  const backlogSlope = round(slope(window.map((p) => ({ t: p.t, v: p.backlog.total }))));
  const a = window[0];
  const b = window[window.length - 1];
  const deliveryRate =
    a && b && b.t > a.t ? round((b.delivered - a.delivered) / ((b.t - a.t) / 1000)) : 0;
  const achievedRate = round(k6.accepted / sec);
  const backlogEnd = series.points[series.points.length - 1]?.backlog.total ?? 0;
  // Growing by more than 2% of the arrival rate is a queue that never empties;
  // below that is noise from batching and retries landing.
  const pass =
    backlogSlope <= Math.max(3, rate * 0.02) &&
    achievedRate >= 0.97 * rate &&
    k6.dropped <= rate * sec * 0.01;
  return {
    rate,
    achievedRate,
    dropped: k6.dropped,
    failedRequests: k6.failed,
    apiLatency: k6.latency,
    deliveryRate,
    backlogSlope,
    backlogEnd,
    pass,
  };
}

async function waitForDrain(
  redis: RedisClient["native"],
  below: number,
  timeoutMs: number,
): Promise<number | null> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const b = await sampleBacklog(redis);
    if (b.total <= below) return round((Date.now() - start) / 1000);
    process.stdout.write(`\r   draining: ${b.total} queued   `);
    await sleep(2000);
  }
  process.stdout.write("\n");
  return null;
}

function logStep(s: StepResult): void {
  console.log(
    `   ${String(s.rate).padStart(5)} req/s → in ${s.achievedRate}/s, out ${s.deliveryRate}/s, ` +
      `backlog ${s.backlogEnd} (trend ${s.backlogSlope >= 0 ? "+" : ""}${s.backlogSlope}/s), ` +
      `API p95 ${s.apiLatency.p95}ms${s.dropped ? `, dropped ${s.dropped}` : ""} → ${s.pass ? "✅" : "❌"}`,
  );
}

async function ramp(
  ctx: Ctx,
  opts: SustainedOptions,
): Promise<{ steps: StepResult[]; best: number }> {
  const steps: StepResult[] = [];
  let best = 0;
  let rate = opts.startRate;

  // Unjudged warm-up: a cold node (JIT, caches, pools) fails a first step it
  // would pass a minute later.
  console.log(`   warm-up: ${Math.ceil(rate / 2)} req/s for 30s`);
  await runK6(ctx.env, k6RateEnv(ctx, Math.ceil(rate / 2), 30));
  await waitForDrain(ctx.redis, 200, 120_000);

  // Climb until a step fails.
  for (;;) {
    const s = await runStep(ctx, rate, opts.stepSec);
    steps.push(s);
    logStep(s);
    if (s.pass) {
      best = rate;
      rate += opts.stepRate;
      continue;
    }
    if (best === 0 && rate > 25) {
      // Even the first step was too much: back off before climbing again.
      await waitForDrain(ctx.redis, 500, 300_000);
      rate = Math.max(25, Math.floor(rate / 2));
      continue;
    }
    break;
  }

  // One step halfway between the last pass and the first fail.
  const half = Math.floor(opts.stepRate / 2);
  if (best > 0 && half >= 10) {
    await waitForDrain(ctx.redis, Math.max(500, best), 300_000);
    const s = await runStep(ctx, best + half, opts.stepSec);
    steps.push(s);
    logStep(s);
    if (s.pass) best += half;
  }
  return { steps, best };
}

// ─── Soak ───────────────────────────────────────────────────────────────────

export interface SoakResult {
  targetRate: number;
  durationSec: number;
  requests: number;
  accepted: number;
  droppedIterations: number;
  achievedRate: number;
  apiLatency: Percentiles;
  /** Successful sends/s over the soak, retries included. */
  deliveryRate: number;
  deliveryRateSecondHalf: number;
  providerFailures: number;
  /** Backlog trend over the last two thirds, msgs/s; flat means it keeps up. */
  backlogSlope: number;
  backlogMax: number;
  backlogAtEnd: number;
  scheduledMax: number;
  dlqAtEnd: number;
  tailDrainSec: number | null;
  e2eLatency: Percentiles;
  redisMemMb: { start: number; max: number; end: number };
  /**
   * Longest gap between 5s samples. Far above 5s means the host slept or
   * Docker was paused, and every rate in this result is meaningless.
   */
  maxSampleGapSec: number;
  keepsUp: boolean;
  series: {
    sec: number;
    backlog: number;
    byStream: Record<string, number>;
    scheduled: number;
    redisMemMb: number;
    delivered: number;
    failed: number;
  }[];
  resources: PhaseResources;
}

export interface DbGrowth {
  durationSec: number;
  notifications: number;
  dbStartMb: number;
  dbEndMb: number;
  growthMb: number;
  bytesPerNotification: number;
  messageLogRowsPerNotification: number;
  walGeneratedMb: number;
  walDirMb: number;
  tables: {
    table: string;
    startMb: number;
    endMb: number;
    growthMb: number;
    rowsAdded: number;
    bytesPerRow: number;
  }[];
  series: { sec: number; dbMb: number; messageLogsMb: number }[];
  /** Projections at the soak's delivery rate, with no retention. */
  perHourGb: number;
  perDayGb: number;
  per30DaysGb: number;
}

export interface EngagementResult {
  opensSent: number;
  clicksSent: number;
  webhookPosts: number;
  webhookErrors: number;
  webhookLatency: Percentiles;
  engagementRowsWritten: number;
  logReads: number;
  logReadErrors: number;
  logReadLatency: Record<string, Percentiles & { count: number }>;
}

export interface SustainedResult {
  tier: ProfileTier;
  options: SustainedOptions;
  ramp: StepResult[];
  sustainableRate: number;
  soak: SoakResult;
  db: DbGrowth;
  engagement: EngagementResult;
  dbSizeMbAtEnd: number;
}

const mb = (bytes: number) => round(bytes / 1048576, 1);

export async function runSustained(
  tier: ProfileTier,
  opts: SustainedOptions,
): Promise<SustainedResult> {
  const env = composeEnv(tier, opts);
  const limits = cpuLimits(tier);

  console.log("🧹 Resetting environment...");
  await compose(["down", "-v", "--remove-orphans"], env);
  console.log(`🚀 Starting db + redis...`);
  await compose(["up", "-d", "--wait", "db", "redis"], env);

  const { db, sql } = createDatabase({ url: DB_URL, maxConnections: 3 });
  const redisClient = new RedisClient({ url: REDIS_URL, name: "profiling-runner" });
  const redis = redisClient.native;
  const sampler = new ResourceSampler(redis);
  let engagement: EngagementDriver | null = null;
  let readers: LogReaders | null = null;

  try {
    for (let attempt = 1; ; attempt++) {
      try {
        await runMigrations(db);
        break;
      } catch (err) {
        if (attempt >= 30) throw err;
        await sleep(1000);
      }
    }
    await sql`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`;
    // No ingest-only phase here: workers run from the start.
    await redis.set(STATS_KEYS.gate, "open");

    console.log(`🚀 Starting nodes [${tier.nodes.join(", ")}]...`);
    await compose(["up", "-d", "--wait", ...tier.nodes], env);
    console.log(`🏗️  Provisioning project, templates, ${opts.users} users...`);
    const apiKey = await provision(sql, opts.users);
    const names = await nodeNames();
    const ctx: Ctx = {
      env,
      apiKey,
      targets: tier.apiNodes.map((n) => `${n}:3000`).join(","),
      users: opts.users,
      redis,
    };

    engagement = new EngagementDriver(redis, opts.openDelaySec * 1000, opts.clickShare);
    readers = new LogReaders(apiKey, opts.logReaders, opts.logReadIntervalMs);
    engagement.start();
    readers.start();
    sampler.start();
    // Feed the "by task" reader with ids of messages that were really sent.
    const taskIdFeed = setInterval(() => {
      void redis.lrange(STATS_KEYS.opens, -20, -1).then((entries) => {
        if (!readers || entries.length === 0) return;
        readers.recentTaskIds = entries.map((e) => e.split("|")[0]!.replace(/^prof-/, ""));
      });
    }, 5000);

    // ── Ramp ──────────────────────────────────────────────────────────────
    let steps: StepResult[] = [];
    let best = typeof opts.soakRate === "number" ? opts.soakRate : 0;
    if (!opts.skipRamp) {
      console.log(
        `\n📶 [ramp] from ${opts.startRate} req/s in +${opts.stepRate} steps of ${opts.stepSec}s ` +
          `(${Math.round((opts.providerFailureRate ?? 0) * 100)}% provider failures, ` +
          `${Math.round((opts.openRate ?? 0) * 100)}% opened, ${opts.logReaders} log readers)`,
      );
      sampler.phase = "ramp";
      const r = await ramp(ctx, opts);
      steps = r.steps;
      if (opts.soakRate === "auto") best = r.best;
      console.log(`   → highest rate that held: ${r.best} req/s`);
    }
    if (best <= 0) throw new Error("no sustainable rate found; pass --soak-rate=N to force one");

    console.log(`\n⏳ Letting the ramp's backlog drain before the soak...`);
    const drained = await waitForDrain(redis, Math.max(200, best / 2), 600_000);
    console.log(
      `\n   ${drained === null ? "⚠️ still not drained after 10 min" : `drained in ${drained}s`}`,
    );

    // ── Soak ──────────────────────────────────────────────────────────────
    console.log(
      `\n🕰️  [soak] ${best} req/s for ${Math.round(opts.soakSec / 60)} min ` +
        `(${Math.round(best * opts.soakSec).toLocaleString()} notifications)...`,
    );
    sampler.phase = "soak";
    await redis.config("RESETSTAT");
    await resetStats(redis);
    engagement.takeLatencies();
    readers.takeLatencies();
    const [{ n: engagementBefore }] =
      await sql`SELECT count(*)::bigint AS n FROM message_logs WHERE kind IN ('opened', 'clicked')`;
    const dbStart = await snapshotDb(sql);
    const soakStart = Date.now();

    const series = startSeries(redis, 5000);
    const dbSeries: DbGrowth["series"] = [];
    let dbSampling = true;
    const dbLoop = (async () => {
      while (dbSampling) {
        const s = await snapshotDb(sql).catch(() => null);
        if (s) {
          dbSeries.push({
            sec: round((s.at - soakStart) / 1000, 0),
            dbMb: mb(s.dbBytes),
            messageLogsMb: mb(s.tables.message_logs?.bytes ?? 0),
          });
        }
        for (let i = 0; i < 30 && dbSampling; i++) await sleep(1000);
      }
    })();
    const progress = setInterval(() => {
      const p = series.points[series.points.length - 1];
      if (!p) return;
      const elapsed = Math.round((p.t - soakStart) / 1000);
      process.stdout.write(
        `\r   ${elapsed}s: delivered ${p.delivered}, provider failures ${p.failed}, ` +
          `backlog ${p.backlog.total}, retries parked ${p.backlog.scheduled}, ` +
          `Redis ${p.backlog.redisMemMb}MB, DB ${dbSeries[dbSeries.length - 1]?.dbMb ?? "?"}MB   `,
      );
    }, 5000);

    const k6 = await runK6(env, k6RateEnv(ctx, best, opts.soakSec));
    const soakEnd = Date.now();
    clearInterval(progress);
    process.stdout.write("\n");
    const atEnd = await readDelivery(redis);

    // Let the tail through, then let the last opens be reported.
    console.log("   letting the tail drain...");
    const tailDrainSec = await waitForDrain(redis, 0, 300_000);
    await series.stop();
    await engagement.stop(Math.max(60_000, opts.openDelaySec * 3000));
    dbSampling = false;
    await dbLoop;
    clearInterval(taskIdFeed);
    // Let autovacuum and the stats collector settle before the final size.
    await sleep(5000);
    const dbEnd = await snapshotDb(sql);
    const e2eLatency = await readLatency(redis);
    const final = await readDelivery(redis);
    const [{ n: engagementAfter }] =
      await sql`SELECT count(*)::bigint AS n FROM message_logs WHERE kind IN ('opened', 'clicked')`;

    // ── Soak results ──────────────────────────────────────────────────────
    const pts = series.points.filter((p) => p.t <= soakEnd);
    const trend = pts.filter((p) => p.t >= soakStart + (soakEnd - soakStart) / 3);
    const backlogSlope = round(slope(trend.map((p) => ({ t: p.t, v: p.backlog.total }))), 2);
    const mid = pts.filter((p) => p.t >= soakStart + (soakEnd - soakStart) / 2);
    const m0 = mid[0];
    const m1 = mid[mid.length - 1];
    const soakSec = (soakEnd - soakStart) / 1000;
    const achievedRate = round(k6.accepted / opts.soakSec);
    const backlogAtEnd = pts[pts.length - 1]?.backlog.total ?? 0;
    const mem = pts.map((p) => p.backlog.redisMemMb);

    const soak: SoakResult = {
      targetRate: best,
      durationSec: opts.soakSec,
      requests: k6.requests,
      accepted: k6.accepted,
      droppedIterations: k6.dropped,
      achievedRate,
      apiLatency: k6.latency,
      deliveryRate: round(atEnd.delivered / soakSec),
      deliveryRateSecondHalf:
        m0 && m1 && m1.t > m0.t ? round((m1.delivered - m0.delivered) / ((m1.t - m0.t) / 1000)) : 0,
      providerFailures: final.failed,
      backlogSlope,
      backlogMax: Math.max(0, ...pts.map((p) => p.backlog.total)),
      backlogAtEnd,
      scheduledMax: Math.max(0, ...pts.map((p) => p.backlog.scheduled)),
      dlqAtEnd: series.points[series.points.length - 1]?.backlog.dlq ?? 0,
      tailDrainSec,
      e2eLatency,
      redisMemMb: { start: mem[0] ?? 0, max: Math.max(0, ...mem), end: mem[mem.length - 1] ?? 0 },
      maxSampleGapSec: round(
        Math.max(0, ...pts.slice(1).map((p, i) => (p.t - pts[i]!.t) / 1000)),
        0,
      ),
      keepsUp:
        backlogSlope <= Math.max(1, best * 0.005) &&
        achievedRate >= 0.97 * best &&
        backlogAtEnd <= Math.max(500, 2 * best),
      series: series.points.map((p) => ({
        sec: round((p.t - soakStart) / 1000, 0),
        backlog: p.backlog.total,
        byStream: p.backlog.byStream,
        scheduled: p.backlog.scheduled,
        redisMemMb: p.backlog.redisMemMb,
        delivered: p.delivered,
        failed: p.failed,
      })),
      resources: sampler.report("soak", limits, names),
    };

    // ── DB growth ─────────────────────────────────────────────────────────
    const growth = dbEnd.dbBytes - dbStart.dbBytes;
    // Rows are written per delivery: messages still queued at the end have not
    // grown the database yet, so dividing by accepted would understate it.
    const notifications = Math.max(1, final.delivered);
    const logRows =
      (dbEnd.tables.message_logs?.inserts ?? 0) - (dbStart.tables.message_logs?.inserts ?? 0);
    const tables = Object.entries(dbEnd.tables)
      .map(([t, e]) => {
        const s = dbStart.tables[t] ?? { bytes: 0, inserts: 0, heapBytes: 0, indexBytes: 0 };
        const rows = e.inserts - s.inserts;
        return {
          table: t,
          startMb: mb(s.bytes),
          endMb: mb(e.bytes),
          growthMb: mb(e.bytes - s.bytes),
          rowsAdded: rows,
          bytesPerRow: rows > 0 ? Math.round((e.bytes - s.bytes) / rows) : 0,
        };
      })
      .filter((t) => t.growthMb > 0 || t.rowsAdded > 0)
      .sort((a, b) => b.growthMb - a.growthMb);
    const bytesPerNotification = Math.round(growth / notifications);
    // Projected from what the tier actually delivered, not what was offered.
    const perSecBytes = bytesPerNotification * Math.min(soak.deliveryRate, achievedRate);
    const dbGrowth: DbGrowth = {
      durationSec: opts.soakSec,
      notifications: final.delivered,
      dbStartMb: mb(dbStart.dbBytes),
      dbEndMb: mb(dbEnd.dbBytes),
      growthMb: mb(growth),
      bytesPerNotification,
      messageLogRowsPerNotification: round(logRows / notifications, 2),
      walGeneratedMb: mb(dbEnd.walBytesGenerated - dbStart.walBytesGenerated),
      walDirMb: mb(dbEnd.walDirBytes),
      tables,
      series: dbSeries,
      perHourGb: round((perSecBytes * 3600) / 1073741824, 2),
      perDayGb: round((perSecBytes * 86400) / 1073741824, 1),
      per30DaysGb: round((perSecBytes * 86400 * 30) / 1073741824, 0),
    };

    await readers.stop();
    const eng: EngagementResult = {
      opensSent: engagement.opensSent,
      clicksSent: engagement.clicksSent,
      webhookPosts: engagement.posts,
      webhookErrors: engagement.errors,
      webhookLatency: engagement.takeLatencies(),
      engagementRowsWritten: Number(engagementAfter) - Number(engagementBefore),
      logReads: readers.reads,
      logReadErrors: readers.errors,
      logReadLatency: readers.takeLatencies(),
    };

    sampler.phase = "done";
    await sampler.stop();
    return {
      tier,
      options: opts,
      ramp: steps,
      sustainableRate: best,
      soak,
      db: dbGrowth,
      engagement: eng,
      dbSizeMbAtEnd: mb(dbEnd.dbBytes),
    };
  } finally {
    await readers?.stop().catch(() => {});
    await engagement?.stop(0).catch(() => {});
    await sampler.stop().catch(() => {});
    await sql.end().catch(() => {});
    await redisClient.disconnect().catch(() => {});
    if (!opts.keep) await compose(["down", "-v", "--remove-orphans"], env).catch(() => {});
  }
}

// ─── Reporting ──────────────────────────────────────────────────────────────

const indent = (s: string) =>
  s
    .split("\n")
    .map((l) => "  " + l)
    .join("\n");

export function printSustained(r: SustainedResult): void {
  const bar = "═".repeat(110);
  const s = r.soak;
  const d = r.db;
  const e = r.engagement;
  console.log(
    `\n${bar}\n 📊 ${r.tier.budget} — ${r.tier.description} — sustained capacity\n${bar}`,
  );

  if (r.ramp.length) {
    console.log("\n  Ramp:");
    console.log(
      indent(
        table(
          ["offered", "accepted", "delivered", "backlog trend", "backlog", "API p95", "result"],
          r.ramp.map((x) => [
            `${x.rate}/s`,
            `${x.achievedRate}/s`,
            `${x.deliveryRate}/s`,
            `${x.backlogSlope >= 0 ? "+" : ""}${x.backlogSlope}/s`,
            x.backlogEnd,
            `${x.apiLatency.p95}ms`,
            x.pass ? "✅" : "❌",
          ]),
        ),
      ),
    );
  }

  const minutes = Math.round(s.durationSec / 60);
  if (s.maxSampleGapSec > 20) {
    console.log(
      `
  ⚠️⚠️ INVALID RUN: ${s.maxSampleGapSec}s passed between two 5s samples — the host ` +
        `slept or Docker paused. Discard these numbers and re-run with the machine awake.`,
    );
  }
  console.log(`\n  Soak at ${s.targetRate} req/s for ${minutes} min:`);
  console.log(
    indent(
      table(
        ["metric", "value"],
        [
          [
            "accepted",
            `${s.accepted.toLocaleString()} (${s.achievedRate}/s, dropped ${s.droppedIterations})`,
          ],
          [
            "delivered (successful sends)",
            `${s.deliveryRate}/s overall, ${s.deliveryRateSecondHalf}/s second half`,
          ],
          [
            "provider failures (→ retried)",
            `${s.providerFailures} (${round((100 * s.providerFailures) / Math.max(1, s.accepted), 2)}% of sends)`,
          ],
          ["retries parked (max)", s.scheduledMax],
          ["dead-lettered", s.dlqAtEnd],
          [
            "backlog trend / max / end",
            `${s.backlogSlope >= 0 ? "+" : ""}${s.backlogSlope}/s / ${s.backlogMax} / ${s.backlogAtEnd}`,
          ],
          ["tail drained after", s.tailDrainSec === null ? "not drained" : `${s.tailDrainSec}s`],
          ["API p50/p95/p99", `${s.apiLatency.p50}/${s.apiLatency.p95}/${s.apiLatency.p99}ms`],
          ["E2E p50/p95/p99", `${s.e2eLatency.p50}/${s.e2eLatency.p95}/${s.e2eLatency.p99}ms`],
          [
            "Redis used_memory start/max/end",
            `${s.redisMemMb.start}/${s.redisMemMb.max}/${s.redisMemMb.end}MB (limit ${r.tier.redis.memory})`,
          ],
          ["verdict", s.keepsUp ? "✅ keeps up" : "❌ backlog grows"],
          ["longest gap between samples", `${s.maxSampleGapSec}s`],
        ],
      ),
    ),
  );

  console.log(`\n  Resources during soak:`);
  console.log(
    indent(
      table(
        ["container", "CPU cores avg/max", "of limit", "mem", "event loop p99 avg/max (ms)"],
        resourceRows(s.resources),
      ),
    ),
  );

  console.log(`\n  Opens & log reads:`);
  const reads = Object.entries(e.logReadLatency).map(
    ([k, p]) => `${k} ${p.p50}/${p.p95}/${p.max}ms (${p.count})`,
  );
  console.log(
    indent(
      table(
        ["metric", "value"],
        [
          [
            "opened / clicked events posted",
            `${e.opensSent} / ${e.clicksSent} in ${e.webhookPosts} webhook posts (${e.webhookErrors} errors)`,
          ],
          [
            "webhook p50/p95/max",
            `${e.webhookLatency.p50}/${e.webhookLatency.p95}/${e.webhookLatency.max}ms`,
          ],
          ["engagement rows in message_logs", e.engagementRowsWritten],
          ["log reads (errors)", `${e.logReads} (${e.logReadErrors})`],
          ["log read p50/p95/max", reads.join("; ") || "—"],
        ],
      ),
    ),
  );

  console.log(`\n  Postgres growth over the soak:`);
  console.log(
    indent(
      table(
        ["metric", "value"],
        [
          ["database size", `${d.dbStartMb} → ${d.dbEndMb}MB (+${d.growthMb}MB)`],
          [
            "per notification",
            `${d.bytesPerNotification} bytes, ${d.messageLogRowsPerNotification} message_logs rows`,
          ],
          ["WAL generated", `${d.walGeneratedMb}MB (pg_wal on disk ${d.walDirMb}MB)`],
          [
            "projected at this rate",
            `${d.perHourGb} GB/hour, ${d.perDayGb} GB/day, ${d.per30DaysGb} GB/30 days`,
          ],
        ],
      ),
    ),
  );
  console.log(
    indent(
      table(
        ["table", "start", "end", "growth", "rows added", "bytes/row"],
        d.tables
          .slice(0, 8)
          .map((t) => [
            t.table,
            `${t.startMb}MB`,
            `${t.endMb}MB`,
            `+${t.growthMb}MB`,
            t.rowsAdded,
            t.bytesPerRow,
          ]),
      ),
    ),
  );

  const perMonth = Math.min(s.deliveryRate, s.achievedRate) * 86400 * 30;
  console.log(
    `\n  ➜ ${r.tier.budget}: ${s.keepsUp ? "sustains" : "does NOT sustain"} ${s.targetRate} notifications/s ` +
      `(≈ ${(perMonth / 1e6).toFixed(0)}M/month at 100% duty) with ${Math.round((r.options.providerFailureRate ?? 0) * 100)}% provider failures.\n`,
  );
}

export function saveSustained(r: SustainedResult): string {
  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const out = path.join(RESULTS_DIR, `sustained-${Date.now()}.json`);
  fs.writeFileSync(out, JSON.stringify(r, null, 2), "utf-8");
  return out;
}

// ─── CLI ────────────────────────────────────────────────────────────────────

export async function runSustainedSuite(tier: ProfileTier): Promise<void> {
  const opts: SustainedOptions = {
    ingestSec: 0,
    steadySec: 0,
    steadyRate: "auto",
    users: 100_000,
    providerLatency: process.env.PROVIDER_LATENCY_MS || "150-250",
    fused: false,
    logLevel: "info",
    keep: false,
    cpuProf: false,
    providerFailureRate: 0.03,
    openRate: 0.3,
    startRate: 150,
    stepRate: 50,
    stepSec: 60,
    soakSec: 900,
    soakRate: "auto",
    openDelaySec: 20,
    clickShare: 0.15,
    logReaders: 2,
    logReadIntervalMs: 2000,
    skipRamp: false,
  };
  let skipBuild = false;
  let skipDist = false;

  for (const arg of process.argv.slice(2)) {
    const [key, val = ""] = arg.replace(/^--/, "").split("=");
    const num = Number(val);
    switch (key) {
      case "soak":
        opts.soakSec = Math.round(num * 60);
        break;
      case "soak-rate":
        opts.soakRate = val === "auto" ? "auto" : num;
        if (val !== "auto") opts.skipRamp = true;
        break;
      case "start":
        opts.startRate = num;
        break;
      case "step":
        opts.stepRate = num;
        break;
      case "step-sec":
        opts.stepSec = num;
        break;
      case "failure-rate":
        opts.providerFailureRate = num;
        break;
      case "open-rate":
        opts.openRate = num;
        break;
      case "click-share":
        opts.clickShare = num;
        break;
      case "log-readers":
        opts.logReaders = num;
        break;
      case "users":
        opts.users = num;
        break;
      case "latency":
        opts.providerLatency = val;
        break;
      case "fused":
        opts.fused = val !== "false";
        break;
      case "keep":
        opts.keep = true;
        break;
      case "skip-build":
        skipBuild = true;
        break;
      case "skip-dist":
        skipDist = true;
        break;
      default:
        console.warn(`unknown option: ${arg}`);
    }
  }

  const bar = "═".repeat(100);
  console.log(
    `${bar}\n 🚀 NOTIFKIT SUSTAINED CAPACITY — ${tier.budget} (${tier.description})\n${bar}`,
  );
  console.log(
    `ramp ${opts.skipRamp ? "skipped" : `${opts.startRate}+${opts.stepRate}/s × ${opts.stepSec}s`} → ` +
      `soak ${opts.soakSec / 60} min @ ${opts.soakRate} | provider ${opts.providerLatency}ms, ` +
      `${opts.providerFailureRate! * 100}% failures | ${opts.openRate! * 100}% opened ` +
      `(${opts.clickShare * 100}% of those click) | ${opts.logReaders} log readers | ${opts.users} users`,
  );

  const hostCpus = await dockerCpuCount();
  if (!hostCpus) throw new Error("Docker is not reachable — start Docker Desktop first.");
  if (!skipBuild) await buildImage(skipDist);

  const result = await runSustained(tier, opts);
  printSustained(result);
  console.log(`📁 Results saved to ${saveSustained(result)}`);
}
