import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createDatabase, runMigrations, RedisClient } from "notifkit";
import { STATS_KEYS, LATENCY_BUCKETS_MS, histogramPercentile } from "./stats.js";

// ─── Paths & constants ──────────────────────────────────────────────────────

export const PROFILING_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPO_ROOT = path.resolve(PROFILING_DIR, "..");
const COMPOSE_FILE = path.join(PROFILING_DIR, "docker-compose.yml");
export const RESULTS_DIR = path.join(PROFILING_DIR, "results");
const K6_OUT_DIR = path.join(RESULTS_DIR, ".k6");
const IMAGE = "notifkit-prof-node:latest";
const CONTAINER_PREFIX = "notifkit-prof-";

// Host ports, overridable because Windows (Hyper-V/WinNAT) reserves port
// ranges at boot that can swallow the defaults; compose reads the same vars.
const DB_PORT = process.env.PROF_DB_PORT || "35432";
const REDIS_PORT = process.env.PROF_REDIS_PORT || "36379";
const API_PORT = process.env.PROF_API_PORT || "35678";
export const DB_URL = `postgres://notifkit:password@localhost:${DB_PORT}/notifkit`;
export const REDIS_URL = `redis://localhost:${REDIS_PORT}`;
export const API_URL = `http://localhost:${API_PORT}`;
export const ADMIN_API_KEY = "perf-admin-key";

const TEMPLATES = [
  { id: "order_confirmation", topic: [] as string[] },
  { id: "payment_receipt", topic: [] },
  { id: "password_reset", topic: [] },
  { id: "tracking_update", topic: ["shipping"] },
  { id: "invoice_ready", topic: ["billing"] },
  { id: "security_alert", topic: [] },
  { id: "stock_replenished", topic: ["inventory"] },
  { id: "promotional_blast", topic: ["promotions"] },
];

// ─── Types ──────────────────────────────────────────────────────────────────

export interface Resources {
  cpus: string;
  memory: string;
}

export interface ProfileTier {
  budget: string;
  monthlyCost: number;
  description: string;
  /** Compose services running notifkit. */
  nodes: string[];
  /** The subset of `nodes` that serve the API; the load generator spreads across them. */
  apiNodes: string[];
  server?: Resources;
  api?: Resources;
  worker?: Resources;
  db: Resources;
  redis: Resources;
  /** Per-stage concurrency for the CPU-bound workers (enricher, engine). */
  workerConcurrency: number;
  /** In-flight sends per delivery worker; provider latency makes this the big one. */
  deliveryConcurrency: number;
  /** Pool size per service per process. */
  dbMaxConnections: number;
  /** Closed-loop callers used to find ingest capacity. */
  ingestVus: number;
}

export interface RunOptions {
  ingestSec: number;
  steadySec: number;
  /** Requests/s for the steady phase, or "auto" for 80% of the measured capacity. */
  steadyRate: number | "auto";
  users: number;
  providerLatency: string;
  fused: boolean;
  logLevel: string;
  keep: boolean;
  /** Write a V8 CPU profile per node to results/cpuprof. */
  cpuProf: boolean;
  /** Share of sends the simulated provider refuses, 0–1. */
  providerFailureRate?: number;
  /** Share of delivered messages reported back as opened through the webhook, 0–1. */
  openRate?: number;
}

export interface Percentiles {
  p50: number;
  p95: number;
  p99: number;
  max?: number;
}

export interface ContainerUsage {
  /** Mean CPU in cores. */
  avgCores: number;
  maxCores: number;
  /** Mean CPU as a share of the container's limit, 0–1. */
  avgUtil: number;
  avgMemMb: number;
}

export interface NodeHealth {
  services: string;
  /** Worst per-second event loop delay p99 seen, ms. */
  eldP99Max: number;
  eldP99Avg: number;
  rssMbMax: number;
}

export interface PhaseResources {
  containers: Record<string, ContainerUsage>;
  nodes: Record<string, NodeHealth>;
}

export interface IngestResult {
  durationSec: number;
  vus: number;
  requests: number;
  accepted: number;
  failed: number;
  rate: number;
  apiLatency: Percentiles;
  resources: PhaseResources;
}

export interface DrainResult {
  backlog: number;
  delivered: number;
  failed: number;
  /** First to last delivery: excludes worker start-up. */
  durationSec: number;
  rate: number;
  logRows: number;
  /** Seconds from the last delivery until message_logs caught up. */
  logCatchUpSec: number | null;
  logRate: number;
  resources: PhaseResources;
}

export interface SteadyResult {
  targetRate: number;
  durationSec: number;
  requests: number;
  accepted: number;
  /** Arrivals k6 could not issue because every VU was stuck on a slow API. */
  droppedIterations: number;
  achievedRate: number;
  apiLatency: Percentiles;
  /** Delivery rate over the second half of the window, once warmed up. */
  deliveryRate: number;
  /** message_logs rows/s over the second half of the window. */
  logRate: number;
  backlogAtEnd: number;
  logLagAtEnd: number;
  drainAfterSec: number | null;
  e2eLatency: Percentiles;
  keepsUp: boolean;
  resources: PhaseResources;
}

export interface PgReport {
  topStatements: { query: string; calls: number; totalMs: number; meanMs: number; rows: number }[];
  tables: { table: string; inserts: number; deadTuples: number; sizeMb: number }[];
  walMb: number | null;
  commits: number;
}

/**
 * CPU cores each component burns per 1,000 notifications/s, and what that
 * extrapolates to at the target rate. API cost comes from the saturated ingest
 * phase (at low utilisation per-request overheads are not amortised and the
 * container's CPU overstates it); the rest from the steady phase.
 */
export interface CapacityPlan {
  perThousand: { api: number; pipeline: number; redis: number; db: number };
  target: number;
  needed: { api: number; pipeline: number; redis: number; db: number };
}

export interface TierRunResult {
  ingest: IngestResult;
  drain: DrainResult;
  steady: SteadyResult;
  pg: PgReport;
  bottleneck: string;
  capacity: CapacityPlan;
  redisCommands: RedisCommandStat[];
}

/** Per-command Redis cost over the steady phase, from INFO commandstats. */
export interface RedisCommandStat {
  command: string;
  calls: number;
  /** Calls per delivered notification. */
  perMessage: number;
  /** Share of all command execution time. */
  share: number;
  usecPerCall: number;
}

export interface TierSummary {
  tier: ProfileTier;
  options: RunOptions;
  runs: TierRunResult[];
  median: {
    ingestRate: number;
    drainRate: number;
    steadyTarget: number;
    steadyDelivery: number;
    steadyLogRate: number;
    e2eP95: number;
    apiP95: number;
    keepsUp: boolean;
    bottleneck: string;
  };
}

// ─── Process helpers ────────────────────────────────────────────────────────

interface RunResult {
  code: number;
  stdout: string;
  stderr: string;
}

export function run(
  cmd: string,
  args: string[],
  opts: { env?: Record<string, string>; cwd?: string; quiet?: boolean; allowFail?: boolean } = {},
): Promise<RunResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: opts.cwd ?? PROFILING_DIR,
      env: { ...process.env, ...opts.env },
      // npm is a .cmd shim on Windows.
      shell: process.platform === "win32" && cmd === "npm",
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => {
      stdout += d;
      if (!opts.quiet) process.stdout.write(d);
    });
    child.stderr.on("data", (d) => {
      stderr += d;
      if (!opts.quiet) process.stderr.write(d);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      const result = { code: code ?? 1, stdout, stderr };
      if (code !== 0 && !opts.allowFail) {
        reject(new Error(`${cmd} ${args.join(" ")} exited with ${code}\n${stderr.slice(-2000)}`));
      } else {
        resolve(result);
      }
    });
  });
}

export function compose(args: string[], env: Record<string, string>, quiet = true) {
  return run("docker", ["compose", "-f", COMPOSE_FILE, ...args], { env, quiet });
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function memoryMb(mem: string): number {
  const m = /^([\d.]+)\s*([GMK]?)/i.exec(mem.trim());
  if (!m) return 1024;
  const n = parseFloat(m[1]!);
  const unit = m[2]!.toUpperCase();
  return unit === "G" ? n * 1024 : unit === "K" ? n / 1024 : n;
}

export function round(n: number, digits = 1): number {
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

export function median(values: number[]): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid]! : (s[mid - 1]! + s[mid]!) / 2;
}

// ─── Build ──────────────────────────────────────────────────────────────────

/**
 * Builds notifkit's dist/ and the node image from it. The image copies dist/
 * rather than building it, so skipping the first step benchmarks whatever
 * stale build happens to be lying around.
 */
export async function buildImage(skipDist: boolean): Promise<void> {
  if (!skipDist) {
    console.log("🔨 Building notifkit dist/ ...");
    await run("npm", ["run", "build"], { cwd: REPO_ROOT, quiet: true });
  }
  console.log(`🐳 Building ${IMAGE} ...`);
  await run("docker", ["build", "-t", IMAGE, "-f", "profiling/Dockerfile", "."], {
    cwd: REPO_ROOT,
    quiet: true,
  });
}

export async function dockerCpuCount(): Promise<number> {
  const res = await run("docker", ["info", "--format", "{{.NCPU}}"], {
    quiet: true,
    allowFail: true,
  });
  return parseInt(res.stdout.trim(), 10) || 0;
}

// ─── Resource sampling ──────────────────────────────────────────────────────

interface CpuSample {
  phase: string;
  name: string;
  cores: number;
  memMb: number;
}

/**
 * Samples `docker stats` for the whole run, tagging each sample with the phase
 * in progress, so each phase reports which container was working hardest.
 */
export class ResourceSampler {
  phase = "setup";
  private samples: CpuSample[] = [];
  private nodeSamples: { phase: string; node: string; data: Record<string, string> }[] = [];
  private running = false;
  private loops: Promise<void>[] = [];

  constructor(private readonly redis: RedisClient["native"]) {}

  start(): void {
    this.running = true;
    this.loops.push(this.dockerLoop(), this.nodeLoop());
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.all(this.loops);
  }

  private async dockerLoop(): Promise<void> {
    while (this.running) {
      const phase = this.phase;
      const res = await run(
        "docker",
        ["stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
        { quiet: true, allowFail: true },
      );
      for (const line of res.stdout.split("\n")) {
        const [name, cpu, mem] = line.trim().split("\t");
        if (!name?.startsWith(CONTAINER_PREFIX) || !cpu || !mem) continue;
        const cores = parseFloat(cpu) / 100;
        const used = mem.split("/")[0]!.trim();
        const memMb = used.endsWith("GiB")
          ? parseFloat(used) * 1024
          : used.endsWith("KiB")
            ? parseFloat(used) / 1024
            : parseFloat(used);
        const short = name
          .slice(CONTAINER_PREFIX.length)
          .replace(/-1$/, "")
          .replace(/^loadgen-run-.*/, "loadgen");
        this.samples.push({ phase, name: short, cores, memMb });
      }
    }
  }

  private async nodeLoop(): Promise<void> {
    while (this.running) {
      const phase = this.phase;
      const keys = await this.redis.keys(STATS_KEYS.nodePattern).catch(() => [] as string[]);
      for (const key of keys) {
        const data: Record<string, string> = await this.redis.hgetall(key).catch(() => ({}));
        if (data.at && Date.now() - Number(data.at) < 3000) {
          this.nodeSamples.push({ phase, node: key.slice("prof:node:".length), data });
        }
      }
      await sleep(1000);
    }
  }

  report(
    phase: string,
    limits: Record<string, number>,
    names: Map<string, string>,
  ): PhaseResources {
    const containers: Record<string, ContainerUsage> = {};
    const byName = new Map<string, CpuSample[]>();
    for (const s of this.samples.filter((s) => s.phase === phase)) {
      if (!byName.has(s.name)) byName.set(s.name, []);
      byName.get(s.name)!.push(s);
    }
    for (const [name, samples] of byName) {
      const avg = samples.reduce((a, s) => a + s.cores, 0) / samples.length;
      const limit = limits[name] ?? 1;
      containers[name] = {
        avgCores: round(avg, 2),
        maxCores: round(Math.max(...samples.map((s) => s.cores)), 2),
        avgUtil: round(avg / limit, 2),
        avgMemMb: round(samples.reduce((a, s) => a + s.memMb, 0) / samples.length, 0),
      };
    }

    const nodes: Record<string, NodeHealth> = {};
    const byNode = new Map<string, Record<string, string>[]>();
    for (const s of this.nodeSamples.filter((s) => s.phase === phase)) {
      const name = names.get(s.node) ?? s.node;
      if (!byNode.has(name)) byNode.set(name, []);
      byNode.get(name)!.push(s.data);
    }
    for (const [name, samples] of byNode) {
      const p99s = samples.map((d) => Number(d.eldP99 ?? 0));
      nodes[name] = {
        services: samples[0]!.services ?? "",
        eldP99Max: round(Math.max(...p99s), 1),
        eldP99Avg: round(p99s.reduce((a, b) => a + b, 0) / p99s.length, 1),
        rssMbMax: Math.max(...samples.map((d) => Number(d.rssMb ?? 0))),
      };
    }
    return { containers, nodes };
  }
}

// ─── Stats readers ──────────────────────────────────────────────────────────

export async function resetStats(redis: RedisClient["native"]): Promise<void> {
  await redis.del(STATS_KEYS.counters, STATS_KEYS.latency, STATS_KEYS.first, STATS_KEYS.last);
}

export async function readDelivery(redis: RedisClient["native"]) {
  const [counters, first, last] = await Promise.all([
    redis.hgetall(STATS_KEYS.counters),
    redis.hvals(STATS_KEYS.first),
    redis.hvals(STATS_KEYS.last),
  ]);
  return {
    delivered: Number(counters.delivered ?? 0),
    failed: Number(counters.failed ?? 0),
    first: first.length ? Math.min(...first.map(Number)) : null,
    last: last.length ? Math.max(...last.map(Number)) : null,
  };
}

export async function readLatency(redis: RedisClient["native"]): Promise<Percentiles> {
  const raw = await redis.hgetall(STATS_KEYS.latency);
  const counts = new Map<number, number>();
  for (const bound of LATENCY_BUCKETS_MS) {
    const v = raw[String(bound)];
    if (v) counts.set(bound, Number(v));
  }
  return {
    p50: histogramPercentile(counts, 50),
    p95: histogramPercentile(counts, 95),
    p99: histogramPercentile(counts, 99),
  };
}

/** Rows inserted into message_logs since the stats were last reset. */
async function logRowsInserted(sql: any): Promise<number> {
  const rows = await sql`
    SELECT n_tup_ins::bigint AS n FROM pg_stat_user_tables WHERE relname = 'message_logs'`;
  return Number(rows[0]?.n ?? 0);
}

/**
 * Exact row count. pg_stat counters are cheap but an idle backend only
 * reports them after ~10s, which reads as log lag that is not there.
 */
async function logRowCount(sql: any): Promise<number> {
  const rows = await sql`SELECT count(*)::bigint AS n FROM message_logs`;
  return Number(rows[0]?.n ?? 0);
}

async function inboundBacklog(redis: RedisClient["native"]): Promise<number> {
  const lens = await Promise.all(
    ["critical", "normal", "low"].map((p) => redis.xlen(`notifkit:stream:inbound:${p}`)),
  );
  return lens.reduce((a, b) => a + b, 0);
}

// ─── k6 ─────────────────────────────────────────────────────────────────────

interface K6Result {
  requests: number;
  accepted: number;
  failed: number;
  dropped: number;
  latency: Percentiles;
}

export async function runK6(
  env: Record<string, string>,
  k6Env: Record<string, string>,
): Promise<K6Result> {
  fs.mkdirSync(K6_OUT_DIR, { recursive: true });
  const file = `summary-${Date.now()}.json`;
  const args = ["run", "--rm", "--no-deps"];
  for (const [k, v] of Object.entries({ ...k6Env, OUT: `/out/${file}` })) {
    args.push("-e", `${k}=${v}`);
  }
  args.push("loadgen");
  await compose(args, env);

  const summary = JSON.parse(fs.readFileSync(path.join(K6_OUT_DIR, file), "utf-8"));
  fs.rmSync(path.join(K6_OUT_DIR, file), { force: true });
  const m = summary.metrics;
  const dur = m.http_req_duration?.values ?? {};
  const requests = m.http_reqs?.values?.count ?? 0;
  const accepted = m.checks?.values?.passes ?? 0;
  return {
    requests,
    accepted,
    failed: requests - accepted,
    dropped: m.dropped_iterations?.values?.count ?? 0,
    latency: {
      p50: round(dur.med ?? 0),
      p95: round(dur["p(95)"] ?? 0),
      p99: round(dur["p(99)"] ?? 0),
      max: round(dur.max ?? 0),
    },
  };
}

// ─── Environment ────────────────────────────────────────────────────────────

export function composeEnv(tier: ProfileTier, opts: RunOptions): Record<string, string> {
  const dbMb = memoryMb(tier.db.memory);
  const env: Record<string, string> = {
    DB_CPUS: tier.db.cpus,
    DB_MEMORY: tier.db.memory,
    REDIS_CPUS: tier.redis.cpus,
    REDIS_MEMORY: tier.redis.memory,
    PG_SHARED_BUFFERS: `${Math.floor(dbMb * 0.25)}MB`,
    PG_EFFECTIVE_CACHE_SIZE: `${Math.floor(dbMb * 0.7)}MB`,
    PG_MAINTENANCE_WORK_MEM: `${Math.max(32, Math.floor(dbMb * 0.05))}MB`,
    WORKER_CONCURRENCY: String(tier.workerConcurrency),
    DELIVERY_CONCURRENCY: String(tier.deliveryConcurrency),
    DB_MAX_CONNECTIONS: String(tier.dbMaxConnections),
    PROVIDER_LATENCY_MS: opts.providerLatency,
    PROVIDER_FAILURE_RATE: String(opts.providerFailureRate ?? 0),
    PROFILING_OPEN_RATE: String(opts.openRate ?? 0),
    PIPELINE_FUSED: String(opts.fused),
    LOG_LEVEL: opts.logLevel,
    NODE_OPTIONS: opts.cpuProf ? "--cpu-prof --cpu-prof-dir=/prof" : "",
  };
  for (const [role, res] of [
    ["SERVER", tier.server],
    ["API", tier.api],
    ["WORKER", tier.worker],
  ] as const) {
    if (res) {
      env[`${role}_CPUS`] = res.cpus;
      env[`${role}_MEMORY`] = res.memory;
    }
  }
  return env;
}

export function cpuLimits(tier: ProfileTier): Record<string, number> {
  const limits: Record<string, number> = {
    db: parseFloat(tier.db.cpus),
    redis: parseFloat(tier.redis.cpus),
    loadgen: 4,
  };
  for (const node of tier.nodes) {
    const res = node === "server" ? tier.server : node.startsWith("api") ? tier.api : tier.worker;
    limits[node] = parseFloat(res?.cpus ?? "1");
  }
  return limits;
}

/** Container id (the node's hostname) → compose service name. */
export async function nodeNames(): Promise<Map<string, string>> {
  const res = await run("docker", ["ps", "--format", "{{.ID}}\t{{.Names}}"], {
    quiet: true,
    allowFail: true,
  });
  const map = new Map<string, string>();
  for (const line of res.stdout.split("\n")) {
    const [id, name] = line.trim().split("\t");
    if (id && name?.startsWith(CONTAINER_PREFIX)) {
      map.set(id, name.slice(CONTAINER_PREFIX.length).replace(/-1$/, ""));
    }
  }
  return map;
}

// ─── Setup ──────────────────────────────────────────────────────────────────

export async function provision(sql: any, users: number): Promise<string> {
  const projectRes = await fetch(`${API_URL}/v1/projects`, {
    method: "POST",
    headers: { Authorization: `Bearer ${ADMIN_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ name: "Profiling Project" }),
  });
  if (!projectRes.ok) throw new Error(`project creation failed: ${projectRes.status}`);
  const project = (await projectRes.json()) as { id: string; apiKey: string };

  // Before the key's first use: the API caches the limit alongside the key.
  await sql`UPDATE projects SET rate_limit_rpm = 100000000`;

  const tplRes = await fetch(`${API_URL}/v1/templates`, {
    method: "PUT",
    headers: { Authorization: `Bearer ${project.apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      templates: TEMPLATES.map((t) => ({
        id: t.id,
        channel: "email",
        ...(t.topic.length ? { topic: t.topic } : {}),
        content: {
          subject: `${t.id.replace(/_/g, " ")} for {{name}}`,
          html: `<h1>Hello {{name}}</h1><p>Your ${t.id.replace(/_/g, " ")} for order {{orderId}} ({{amount}}) is ready.</p>`,
          text: `Hello {{name}}, order {{orderId}}: {{amount}}`,
        },
      })),
    }),
  });
  if (!tplRes.ok) throw new Error(`template sync failed: ${tplRes.status} ${await tplRes.text()}`);

  // Bulk-seed recipients straight into Postgres: going through the API would
  // take longer than the benchmark itself.
  const projectId = (await sql`SELECT id FROM projects LIMIT 1`)[0].id;
  // The client's 10s statement_timeout is for request paths, not a bulk load.
  await sql.begin(async (tx: any) => {
    await tx`SET LOCAL statement_timeout = 0`;
    await tx`
        INSERT INTO users (project_id, external_id, attributes)
        SELECT ${projectId}, 'perf-user-' || g,
               jsonb_build_object('language', 'en', 'timezone', 'UTC',
                                  'email', 'perf-' || g || '@example.com')
        FROM generate_series(0, ${users - 1}) AS g`;
    await tx`
        INSERT INTO user_contacts (user_id, channel, target, is_primary)
        SELECT id, 'email', 'perf-' || substr(external_id, 11) || '@example.com', true
        FROM users WHERE project_id = ${projectId}`;
  });
  await sql`ANALYZE`;

  return project.apiKey;
}

// ─── Tier run ───────────────────────────────────────────────────────────────

/**
 * One run of one tier, in three phases on a fresh environment:
 *
 *  1. ingest — API nodes only (worker services held behind a gate). Closed-loop
 *     callers find the most the API accepts; everything accepted is queued.
 *  2. drain  — the gate opens and workers chew through that backlog: the most
 *     the pipeline delivers, message_logs included.
 *  3. steady — an open-model arrival rate with everything running, the way
 *     production sees traffic. Reports whether delivery keeps up and the
 *     end-to-end latency a user would see.
 */
export async function runTier(tier: ProfileTier, opts: RunOptions): Promise<TierRunResult> {
  const env = composeEnv(tier, opts);
  if (opts.cpuProf) fs.mkdirSync(path.join(RESULTS_DIR, "cpuprof"), { recursive: true });
  const limits = cpuLimits(tier);

  console.log(`🧹 Resetting environment...`);
  await compose(["down", "-v", "--remove-orphans"], env);
  console.log(`🚀 Starting db + redis (db ${tier.db.cpus} vCPU / ${tier.db.memory})...`);
  await compose(["up", "-d", "--wait", "db", "redis"], env);

  const { db, sql } = createDatabase({ url: DB_URL, maxConnections: 3 });
  const redisClient = new RedisClient({ url: REDIS_URL, name: "profiling-runner" });
  const redis = redisClient.native;
  const sampler = new ResourceSampler(redis);

  try {
    // The postgres image restarts once after initdb, so the first healthy
    // pg_isready can land on a server that is about to go away.
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

    console.log(`🚀 Starting nodes [${tier.nodes.join(", ")}]...`);
    await compose(["up", "-d", "--wait", ...tier.nodes], env);

    console.log(`🏗️  Provisioning project, ${TEMPLATES.length} templates, ${opts.users} users...`);
    const apiKey = await provision(sql, opts.users);
    const names = await nodeNames();
    const targets = tier.apiNodes.map((n) => `${n}:3000`).join(",");

    await sql`SELECT pg_stat_statements_reset()`;
    await sql`SELECT pg_stat_reset()`;
    sampler.start();

    // ── Phase 1: ingest ────────────────────────────────────────────────────
    console.log(`\n🔥 [ingest] ${tier.ingestVus} closed-loop callers for ${opts.ingestSec}s...`);
    sampler.phase = "ingest";
    await resetStats(redis);
    const k6Ingest = await runK6(env, {
      MODE: "closed",
      VUS: String(tier.ingestVus),
      DURATION: String(opts.ingestSec),
      TARGETS: targets,
      API_KEY: apiKey,
      USERS: String(opts.users),
    });
    const backlog = await inboundBacklog(redis);
    const ingest: IngestResult = {
      durationSec: opts.ingestSec,
      vus: tier.ingestVus,
      requests: k6Ingest.requests,
      accepted: k6Ingest.accepted,
      failed: k6Ingest.failed,
      rate: round(k6Ingest.accepted / opts.ingestSec),
      apiLatency: k6Ingest.latency,
      resources: sampler.report("ingest", limits, names),
    };
    console.log(
      `   accepted ${ingest.accepted} (${ingest.rate} req/s), failed ${ingest.failed}, ` +
        `API p50/p95/p99 ${ingest.apiLatency.p50}/${ingest.apiLatency.p95}/${ingest.apiLatency.p99}ms, ` +
        `queued ${backlog}`,
    );

    // ── Phase 2: drain ─────────────────────────────────────────────────────
    console.log(`\n🌊 [drain] opening worker gate, draining ${backlog} queued notifications...`);
    sampler.phase = "drain";
    await resetStats(redis);
    const logsBefore = await logRowCount(sql);
    await redis.set(STATS_KEYS.gate, "open");
    const drainDeadline = Date.now() + Math.max(180_000, (backlog / 50) * 1000);

    let lastProgress = { at: Date.now(), delivered: 0 };
    let d = await readDelivery(redis);
    while (d.delivered + d.failed < backlog && Date.now() < drainDeadline) {
      await sleep(1000);
      d = await readDelivery(redis);
      if (d.delivered > lastProgress.delivered) {
        lastProgress = { at: Date.now(), delivered: d.delivered };
      } else if (d.delivered > 0 && Date.now() - lastProgress.at > 20_000) {
        console.log(`   ⚠️ no progress for 20s at ${d.delivered}/${backlog}; moving on`);
        break;
      }
      process.stdout.write(`\r   delivered ${d.delivered}/${backlog}`);
    }
    process.stdout.write("\n");

    // message_logs trails delivery: wait for it, but not forever.
    const expectedLogs = d.delivered * 2;
    const lastDelivery = d.last ?? Date.now();
    let logs = (await logRowCount(sql)) - logsBefore;
    let logCatchUpSec: number | null = null;
    const logDeadline = Date.now() + 60_000;
    while (logs < expectedLogs && Date.now() < logDeadline) {
      await sleep(1000);
      logs = (await logRowCount(sql)) - logsBefore;
    }
    if (logs >= expectedLogs) logCatchUpSec = round(Math.max(0, Date.now() - lastDelivery) / 1000);

    const drainSpan = d.first && d.last && d.last > d.first ? (d.last - d.first) / 1000 : 1;
    const drain: DrainResult = {
      backlog,
      delivered: d.delivered,
      failed: d.failed,
      durationSec: round(drainSpan),
      rate: round(d.delivered / drainSpan),
      logRows: logs,
      logCatchUpSec,
      logRate: round(logs / Math.max(1, (Date.now() - (d.first ?? Date.now())) / 1000)),
      resources: sampler.report("drain", limits, names),
    };
    console.log(
      `   ${drain.delivered} delivered in ${drain.durationSec}s → ${drain.rate} msg/s; ` +
        `${drain.logRows}/${expectedLogs} log rows` +
        (logCatchUpSec !== null
          ? `, caught up ${logCatchUpSec}s after last send`
          : ", NOT caught up"),
    );

    // ── Phase 3: steady ────────────────────────────────────────────────────
    const targetRate =
      opts.steadyRate === "auto"
        ? Math.max(50, Math.floor(0.8 * Math.min(ingest.rate, drain.rate)))
        : opts.steadyRate;
    // Enough VUs that a slow API shows up as latency, not as arrivals k6
    // could not issue ("dropped").
    const preVus = Math.min(2000, Math.max(100, Math.ceil(targetRate / 10)));
    console.log(`\n📈 [steady] ${targetRate} req/s open-model arrivals for ${opts.steadySec}s...`);
    sampler.phase = "steady";
    await redis.config("RESETSTAT");
    await resetStats(redis);
    const steadyLogsBefore = await logRowsInserted(sql);
    const steadyCountBefore = await logRowCount(sql);

    const series: { t: number; delivered: number; logs: number }[] = [];
    let sampling = true;
    const seriesLoop = (async () => {
      while (sampling) {
        const dd = await readDelivery(redis);
        series.push({
          t: Date.now(),
          delivered: dd.delivered,
          logs: (await logRowsInserted(sql)) - steadyLogsBefore,
        });
        await sleep(1000);
      }
    })();

    const steadyStart = Date.now();
    const k6Steady = await runK6(env, {
      MODE: "rate",
      RATE: String(targetRate),
      VUS: String(preVus),
      MAX_VUS: String(Math.min(4000, Math.max(400, Math.ceil(targetRate / 2)))),
      DURATION: String(opts.steadySec),
      TARGETS: targets,
      API_KEY: apiKey,
      USERS: String(opts.users),
    });
    const steadyEnd = Date.now();
    sampling = false;
    await seriesLoop;

    const atEnd = await readDelivery(redis);
    const logsAtEnd = (await logRowCount(sql)) - steadyCountBefore;
    // Second half of the window: past warm-up, before the tail.
    const mid = steadyStart + (steadyEnd - steadyStart) / 2;
    const inWindow = series.filter((s) => s.t >= mid && s.t <= steadyEnd);
    const a = inWindow[0];
    const b = inWindow[inWindow.length - 1];
    const span = a && b && b.t > a.t ? (b.t - a.t) / 1000 : 1;
    const deliveryRate = a && b ? round((b.delivered - a.delivered) / span) : 0;
    const logRate = a && b ? round((b.logs - a.logs) / span) : 0;
    const backlogAtEnd = Math.max(0, k6Steady.accepted - atEnd.delivered - atEnd.failed);

    // Let the tail drain so the latency histogram covers every message.
    let drainAfterSec: number | null = null;
    const tailDeadline = Date.now() + 120_000;
    let tail = atEnd;
    while (tail.delivered + tail.failed < k6Steady.accepted && Date.now() < tailDeadline) {
      await sleep(1000);
      tail = await readDelivery(redis);
    }
    if (tail.delivered + tail.failed >= k6Steady.accepted) {
      drainAfterSec = round((Date.now() - steadyEnd) / 1000);
    }

    const e2eLatency = await readLatency(redis);
    const achievedRate = round(k6Steady.accepted / opts.steadySec);
    const steady: SteadyResult = {
      targetRate,
      durationSec: opts.steadySec,
      requests: k6Steady.requests,
      accepted: k6Steady.accepted,
      droppedIterations: k6Steady.dropped,
      achievedRate,
      apiLatency: k6Steady.latency,
      deliveryRate,
      logRate,
      backlogAtEnd,
      logLagAtEnd: Math.max(0, atEnd.delivered * 2 - logsAtEnd),
      drainAfterSec,
      e2eLatency,
      // Kept up if under two seconds of work was queued when arrivals stopped,
      // and the API took (nearly) everything offered.
      keepsUp: backlogAtEnd <= Math.max(2 * targetRate, 500) && achievedRate >= 0.95 * targetRate,
      resources: sampler.report("steady", limits, names),
    };
    console.log(
      `   achieved ${steady.achievedRate} req/s (dropped ${steady.droppedIterations}), ` +
        `delivery ${steady.deliveryRate} msg/s, logs ${steady.logRate} rows/s, ` +
        `backlog at end ${steady.backlogAtEnd}, e2e p50/p95/p99 ` +
        `${e2eLatency.p50}/${e2eLatency.p95}/${e2eLatency.p99}ms → ${steady.keepsUp ? "✅ keeps up" : "❌ falls behind"}`,
    );

    const redisCommands = await redisCommandStats(redis, tail.delivered);
    sampler.phase = "done";
    await sampler.stop();
    const pg = await pgReport(sql);

    const busiest = Object.entries(steady.resources.containers)
      .filter(([name]) => name !== "loadgen")
      .sort((x, y) => y[1].avgUtil - x[1].avgUtil)[0];
    const bottleneck = busiest
      ? `${busiest[0]} (${Math.round(busiest[1].avgUtil * 100)}% of ${limits[busiest[0]] ?? "?"} vCPU)`
      : "unknown";

    const capacity = capacityPlan(tier, ingest, steady);
    return { ingest, drain, steady, pg, bottleneck, capacity, redisCommands };
  } finally {
    await sampler.stop().catch(() => {});
    await sql.end().catch(() => {});
    await redisClient.disconnect().catch(() => {});
    if (opts.cpuProf) {
      // A profile is only written on a clean exit.
      await compose(["stop", "-t", "60", ...tier.nodes], env).catch(() => {});
      console.log(`🔬 CPU profiles written to ${path.join(RESULTS_DIR, "cpuprof")}`);
    }
    if (!opts.keep) await compose(["down", "-v", "--remove-orphans"], env).catch(() => {});
  }
}

export async function redisCommandStats(
  redis: RedisClient["native"],
  delivered: number,
): Promise<RedisCommandStat[]> {
  const info = await redis.info("commandstats");
  const stats: { command: string; calls: number; usec: number }[] = [];
  for (const line of info.split("\n")) {
    const m = /^cmdstat_([^:]+):calls=(\d+),usec=(\d+)/.exec(line.trim());
    if (m) stats.push({ command: m[1]!, calls: Number(m[2]), usec: Number(m[3]) });
  }
  // The runner's own polling is not the workload.
  const own = new Set(["hgetall", "hvals", "keys", "info", "config", "xlen"]);
  const work = stats.filter((s) => !own.has(s.command));
  const total = work.reduce((a, s) => a + s.usec, 0) || 1;
  return work
    .sort((a, b) => b.usec - a.usec)
    .map((s) => ({
      command: s.command,
      calls: s.calls,
      perMessage: delivered > 0 ? round(s.calls / delivered, 2) : 0,
      share: round(s.usec / total, 3),
      usecPerCall: round(s.usec / Math.max(1, s.calls), 1),
    }));
}

export const CAPACITY_TARGET = 5_000;

function capacityPlan(tier: ProfileTier, ingest: IngestResult, steady: SteadyResult): CapacityPlan {
  const cores = (res: PhaseResources, pick: (name: string) => boolean) =>
    Object.entries(res.containers)
      .filter(([name]) => pick(name))
      .reduce((a, [, c]) => a + c.avgCores, 0);
  const isApiOnly = (n: string) => tier.apiNodes.includes(n) && n !== "server";
  const isPipeline = (n: string) => tier.nodes.includes(n) && !isApiOnly(n);

  const perK = (c: number, rate: number) => (rate > 0 ? round((c / rate) * 1000, 2) : 0);
  const apiPerK = perK(
    cores(ingest.resources, (n) => tier.apiNodes.includes(n)),
    ingest.rate,
  );
  // A monolith's steady CPU includes its own API work; take that back out.
  const monolithApi = tier.apiNodes.includes("server") ? (apiPerK * steady.achievedRate) / 1000 : 0;
  const perThousand = {
    api: apiPerK,
    pipeline: perK(
      Math.max(0, cores(steady.resources, isPipeline) - monolithApi),
      steady.deliveryRate,
    ),
    redis: perK(
      cores(steady.resources, (n) => n === "redis"),
      steady.deliveryRate,
    ),
    db: perK(
      cores(steady.resources, (n) => n === "db"),
      steady.deliveryRate,
    ),
  };
  const scale = CAPACITY_TARGET / 1000;
  return {
    perThousand,
    target: CAPACITY_TARGET,
    needed: {
      api: round(perThousand.api * scale, 1),
      pipeline: round(perThousand.pipeline * scale, 1),
      redis: round(perThousand.redis * scale, 1),
      db: round(perThousand.db * scale, 1),
    },
  };
}

export async function pgReport(sql: any): Promise<PgReport> {
  const statements = await sql`
    SELECT query, calls::bigint AS calls, total_exec_time AS total, mean_exec_time AS mean, rows::bigint AS rows
    FROM pg_stat_statements
    WHERE query NOT ILIKE '%pg_stat%'
    ORDER BY total_exec_time DESC
    LIMIT 12`;
  const tables = await sql`
    SELECT relname, n_tup_ins::bigint AS ins, n_dead_tup::bigint AS dead,
           pg_total_relation_size(relid) AS size
    FROM pg_stat_user_tables
    WHERE n_tup_ins > 0 OR n_tup_upd > 0
    ORDER BY n_tup_ins DESC`;
  const wal = await sql`SELECT wal_bytes FROM pg_stat_wal`.catch(() => []);
  const db = await sql`
    SELECT xact_commit::bigint AS commits FROM pg_stat_database WHERE datname = 'notifkit'`;

  return {
    topStatements: statements.map((s: any) => ({
      query: String(s.query).replace(/\s+/g, " ").slice(0, 160),
      calls: Number(s.calls),
      totalMs: round(Number(s.total), 0),
      meanMs: round(Number(s.mean), 2),
      rows: Number(s.rows),
    })),
    tables: tables.map((t: any) => ({
      table: t.relname,
      inserts: Number(t.ins),
      deadTuples: Number(t.dead),
      sizeMb: round(Number(t.size) / 1048576),
    })),
    walMb: wal[0] ? round(Number(wal[0].wal_bytes) / 1048576) : null,
    commits: Number(db[0]?.commits ?? 0),
  };
}

// ─── Reporting ──────────────────────────────────────────────────────────────

export function table(headers: string[], rows: (string | number)[][]): string {
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => String(r[i] ?? "").length)),
  );
  const line = (cols: (string | number)[]) =>
    cols.map((c, i) => String(c).padEnd(widths[i]!)).join(" │ ");
  return [line(headers), widths.map((w) => "─".repeat(w)).join("─┼─"), ...rows.map(line)].join(
    "\n",
  );
}

export function resourceRows(res: PhaseResources): (string | number)[][] {
  return Object.entries(res.containers)
    .sort((a, b) => b[1].avgUtil - a[1].avgUtil)
    .map(([name, c]) => {
      const node = res.nodes[name];
      return [
        name,
        `${c.avgCores} / ${c.maxCores}`,
        `${Math.round(c.avgUtil * 100)}%`,
        `${c.avgMemMb}MB`,
        node ? `${node.eldP99Avg} / ${node.eldP99Max}` : "",
      ];
    });
}

export function printTierRun(tier: ProfileTier, r: TierRunResult): void {
  const bar = "═".repeat(110);
  console.log(`\n${bar}\n 📊 ${tier.budget} — ${tier.description}\n${bar}`);
  console.log(
    table(
      ["Phase", "Result", "API p50/p95/p99 (ms)", "E2E p50/p95/p99 (ms)", "message_logs"],
      [
        [
          "ingest (max)",
          `${r.ingest.rate} req/s accepted (${r.ingest.failed} failed)`,
          `${r.ingest.apiLatency.p50}/${r.ingest.apiLatency.p95}/${r.ingest.apiLatency.p99}`,
          "—",
          "—",
        ],
        [
          "drain (max)",
          `${r.drain.rate} msg/s (${r.drain.delivered}/${r.drain.backlog})`,
          "—",
          "— (queued)",
          `${r.drain.logRate} rows/s, ${r.drain.logCatchUpSec === null ? "behind" : `+${r.drain.logCatchUpSec}s`}`,
        ],
        [
          `steady @${r.steady.targetRate}/s`,
          `${r.steady.achievedRate} in → ${r.steady.deliveryRate} out msg/s, backlog ${r.steady.backlogAtEnd} ${r.steady.keepsUp ? "✅" : "❌"}`,
          `${r.steady.apiLatency.p50}/${r.steady.apiLatency.p95}/${r.steady.apiLatency.p99}`,
          `${r.steady.e2eLatency.p50}/${r.steady.e2eLatency.p95}/${r.steady.e2eLatency.p99}`,
          `${r.steady.logRate} rows/s, lag ${r.steady.logLagAtEnd}`,
        ],
      ],
    ),
  );

  for (const phase of ["ingest", "drain", "steady"] as const) {
    console.log(`\n  Resources during ${phase}:`);
    console.log(
      table(
        ["container", "CPU cores avg/max", "of limit", "mem", "event loop p99 avg/max (ms)"],
        resourceRows(r[phase].resources),
      )
        .split("\n")
        .map((l) => "  " + l)
        .join("\n"),
    );
  }

  console.log(`\n  Postgres (whole run): ${r.pg.commits} commits, ${r.pg.walMb ?? "?"}MB WAL`);
  console.log(
    table(
      ["table", "inserts", "dead tuples", "size"],
      r.pg.tables.slice(0, 8).map((t) => [t.table, t.inserts, t.deadTuples, `${t.sizeMb}MB`]),
    )
      .split("\n")
      .map((l) => "  " + l)
      .join("\n"),
  );
  console.log(`\n  Top statements by total time:`);
  console.log(
    table(
      ["total ms", "calls", "mean ms", "rows", "query"],
      r.pg.topStatements
        .slice(0, 8)
        .map((s) => [s.totalMs, s.calls, s.meanMs, s.rows, s.query.slice(0, 90)]),
    )
      .split("\n")
      .map((l) => "  " + l)
      .join("\n"),
  );
  console.log(`\n  Redis commands during steady state (per delivered message):`);
  console.log(
    table(
      ["command", "per msg", "share of Redis time", "µs/call"],
      r.redisCommands
        .slice(0, 12)
        .map((c) => [c.command, c.perMessage, `${Math.round(c.share * 100)}%`, c.usecPerCall]),
    )
      .split("\n")
      .map((l) => "  " + l)
      .join("\n"),
  );
  const perMsg = r.redisCommands.reduce((a, c) => a + c.perMessage, 0);
  console.log(`  ≈ ${round(perMsg, 1)} Redis commands per delivered notification`);

  console.log(`\n  Busiest container in steady state: ${r.bottleneck}`);
  const c = r.capacity;
  console.log(
    `\n  CPU cores per 1k msg/s: API ${c.perThousand.api}, pipeline ${c.perThousand.pipeline}, ` +
      `Redis ${c.perThousand.redis}, Postgres ${c.perThousand.db}`,
  );
  console.log(
    `  → ${c.target}/s in and out would need ≈ API ${c.needed.api}, pipeline ${c.needed.pipeline}, ` +
      `Redis ${c.needed.redis}${c.needed.redis > 1 ? " (one Redis runs commands on ONE core)" : ""}, ` +
      `Postgres ${c.needed.db} cores\n`,
  );
}

export function summarise(
  tier: ProfileTier,
  options: RunOptions,
  runs: TierRunResult[],
): TierSummary {
  return {
    tier,
    options,
    runs,
    median: {
      ingestRate: median(runs.map((r) => r.ingest.rate)),
      drainRate: median(runs.map((r) => r.drain.rate)),
      steadyTarget: median(runs.map((r) => r.steady.targetRate)),
      steadyDelivery: median(runs.map((r) => r.steady.deliveryRate)),
      steadyLogRate: median(runs.map((r) => r.steady.logRate)),
      e2eP95: median(runs.map((r) => r.steady.e2eLatency.p95)),
      apiP95: median(runs.map((r) => r.steady.apiLatency.p95)),
      keepsUp: runs.filter((r) => r.steady.keepsUp).length > runs.length / 2,
      bottleneck: runs[runs.length - 1]!.bottleneck,
    },
  };
}

export function printSummary(title: string, all: TierSummary[]): void {
  const bar = "═".repeat(140);
  console.log(`\n${bar}\n 💰 ${title}\n${bar}`);
  console.log(
    table(
      [
        "Budget",
        "Topology",
        "Ingest max",
        "Drain max",
        "Steady in→out",
        "E2E p95",
        "API p95",
        "Logs",
        "Keeps up",
        "msg/s per $",
        "Busiest (steady)",
      ],
      all.map((s) => [
        s.tier.budget,
        s.tier.description,
        `${s.median.ingestRate}/s`,
        `${s.median.drainRate}/s`,
        `${s.median.steadyTarget}→${s.median.steadyDelivery}/s`,
        `${s.median.e2eP95}ms`,
        `${s.median.apiP95}ms`,
        `${s.median.steadyLogRate} rows/s`,
        s.median.keepsUp ? "✅" : "❌",
        round(s.median.steadyDelivery / s.tier.monthlyCost),
        s.median.bottleneck,
      ]),
    ),
  );
  console.log(bar + "\n");
}

export function saveResults(filename: string, data: unknown): void {
  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const out = path.join(RESULTS_DIR, filename);
  fs.writeFileSync(out, JSON.stringify(data, null, 2), "utf-8");
  console.log(`📁 Results saved to ${out}`);
}

// ─── CLI ────────────────────────────────────────────────────────────────────

export async function runSuite(
  name: string,
  title: string,
  allTiers: ProfileTier[],
): Promise<void> {
  const opts: RunOptions = {
    ingestSec: 15,
    steadySec: 30,
    steadyRate: "auto",
    users: 100_000,
    providerLatency: process.env.PROVIDER_LATENCY_MS || "150-250",
    fused: false,
    logLevel: "info",
    keep: false,
    cpuProf: false,
  };
  let runs = 1;
  let tiers = allTiers;
  let skipBuild = false;
  let skipDist = false;

  for (const arg of process.argv.slice(2)) {
    const [key, val = ""] = arg.replace(/^--/, "").split("=");
    switch (key) {
      case "quick":
        opts.ingestSec = 5;
        opts.steadySec = 10;
        opts.users = 10_000;
        break;
      case "ingest":
        opts.ingestSec = parseInt(val, 10);
        break;
      case "steady":
        opts.steadySec = parseInt(val, 10);
        break;
      case "rate":
        opts.steadyRate = val === "auto" ? "auto" : parseInt(val, 10);
        break;
      case "users":
        opts.users = parseInt(val, 10);
        break;
      case "latency":
        opts.providerLatency = val;
        break;
      case "fused":
        opts.fused = val !== "false";
        break;
      case "log-level":
        opts.logLevel = val;
        break;
      case "runs":
        runs = parseInt(val, 10);
        break;
      case "keep":
        opts.keep = true;
        break;
      case "cpu-prof":
        opts.cpuProf = true;
        break;
      case "skip-build":
        skipBuild = true;
        break;
      case "skip-dist":
        skipDist = true;
        break;
      case "tiers": {
        const wanted = val.split(",").map((s) => s.replace(/[$/mo]/g, "").trim());
        tiers = allTiers.filter((t) => wanted.includes(String(t.monthlyCost)));
        break;
      }
      default:
        console.warn(`unknown option: ${arg}`);
    }
  }

  const bar = "═".repeat(100);
  console.log(`${bar}\n 🚀 ${title}\n${bar}`);
  console.log(
    `Phases: ingest ${opts.ingestSec}s → drain → steady ${opts.steadySec}s @ ${opts.steadyRate} req/s | ` +
      `${opts.users} users | provider latency ${opts.providerLatency}ms | ` +
      `pipeline ${opts.fused ? "FUSED" : "streamed"} | LOG_LEVEL=${opts.logLevel} | runs ${runs}`,
  );
  console.log(`Tiers: ${tiers.map((t) => t.budget).join(" ➜ ")}\n${bar}`);

  const hostCpus = await dockerCpuCount();
  if (!hostCpus) throw new Error("Docker is not reachable — start Docker Desktop first.");
  for (const t of tiers) {
    const need = Object.values(cpuLimits(t)).reduce((a, b) => a + b, 0);
    if (need > hostCpus) {
      console.warn(
        `⚠️ ${t.budget} limits add up to ${need} vCPU but Docker has ${hostCpus}: ` +
          `containers will contend and the numbers will understate the tier.`,
      );
    }
  }

  if (!skipBuild) await buildImage(skipDist);

  const all: TierSummary[] = [];
  for (const tier of tiers) {
    const tierRuns: TierRunResult[] = [];
    for (let i = 0; i < runs; i++) {
      console.log(`\n>>> ${tier.budget} — ${tier.description} — run ${i + 1}/${runs}`);
      const r = await runTier(tier, opts);
      printTierRun(tier, r);
      tierRuns.push(r);
    }
    all.push(summarise(tier, opts, tierRuns));
  }

  printSummary(title, all);
  saveResults(`${name}-${opts.fused ? "fused-" : ""}${Date.now()}.json`, all);
}
