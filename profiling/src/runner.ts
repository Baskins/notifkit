import { DockerComposeEnvironment } from "testcontainers";
import { createDatabase, runMigrations } from "notifkit";
import { setGlobalDispatcher, Agent } from "undici";
import fs from "node:fs";
import path from "node:path";

// High-concurrency connection dispatcher
setGlobalDispatcher(
  new Agent({
    connections: 2000,
    pipelining: 1,
    keepAliveTimeout: 30000,
    keepAliveMaxTimeout: 60000,
  }),
);

export interface ServerNodeConfig {
  name: string;
  port: number;
  isApi?: boolean;
}

export interface ProfileTier {
  budget: string;
  monthlyCost: number;
  description: string;
  architecture: string;
  serverCpus: string;
  serverMemory: string;
  dbCpus: string;
  dbMemory: string;
  redisCpus: string;
  redisMemory: string;
  workerConcurrency: number;
  dbMaxConnections?: number;
  concurrency: number;
  nodes: ServerNodeConfig[];
  envOverrides?: Record<string, string>;
}

export interface SingleRunResult {
  durationSec: number;
  concurrency: number;
  requestsSent: number;
  successCount: number;
  failCount: number;
  ingestionThroughput: number;
  firstDelivOffset: number;
  lastDelivOffset: number;
  deliverySpanSec: number;
  activeDeliveryRate: number;
  totalDeliveryRate: number;
  apiP50: number;
  apiP95: number;
  apiP99: number;
  deliveryP50: number;
  deliveryP95: number;
  deliveryP99: number;
  totalDelivered: number;
}

export interface MetricStats {
  mean: number;
  median: number;
  min: number;
  max: number;
  stdDev: number;
}

export interface TierSummary {
  tier: ProfileTier;
  runs: SingleRunResult[];
  ingestionThroughput: MetricStats;
  activeDeliveryRate: MetricStats;
  totalDeliveryRate: MetricStats;
  apiP50: MetricStats;
  apiP95: MetricStats;
  apiP99: MetricStats;
  deliveryP50: MetricStats;
  deliveryP95: MetricStats;
  deliveryP99: MetricStats;
  costEfficiency: MetricStats;
}

export function calculateStats(values: number[]): MetricStats {
  if (values.length === 0) {
    return { mean: 0, median: 0, min: 0, max: 0, stdDev: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const mean = sum / sorted.length;
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 !== 0 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
  const min = sorted[0]!;
  const max = sorted[sorted.length - 1]!;
  const variance = sorted.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / sorted.length;
  const stdDev = Math.sqrt(variance);

  return {
    mean: Number(mean.toFixed(2)),
    median: Number(median.toFixed(2)),
    min: Number(min.toFixed(2)),
    max: Number(max.toFixed(2)),
    stdDev: Number(stdDev.toFixed(2)),
  };
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)]!;
}

const MICROSERVICES = [
  { name: "order-service", priority: "critical", event: "order_confirmation" },
  { name: "payments-service", priority: "critical", event: "payment_receipt" },
  { name: "auth-service", priority: "critical", event: "password_reset" },
  { name: "shipping-service", priority: "normal", event: "tracking_update" },
  { name: "billing-service", priority: "normal", event: "invoice_ready" },
  { name: "account-service", priority: "normal", event: "security_alert" },
  { name: "inventory-service", priority: "low", event: "stock_replenished" },
  { name: "marketing-service", priority: "low", event: "promotional_blast" },
];

export async function runTierBenchmark(
  tier: ProfileTier,
  options: {
    durationSec: number;
    providerLatencyMs?: number | string;
    adminApiKey?: string;
  },
): Promise<SingleRunResult> {
  const durationSec = options.durationSec;
  const providerLatency = options.providerLatencyMs ?? process.env.PROVIDER_LATENCY_MS ?? 0;
  const adminApiKey = options.adminApiKey ?? "perf-admin-key";
  const dbPort = 35432;

  const composeEnv: Record<string, string> = {
    SERVER_CPUS: tier.serverCpus,
    SERVER_MEMORY: tier.serverMemory,
    DB_CPUS: tier.dbCpus,
    DB_MEMORY: tier.dbMemory,
    REDIS_CPUS: tier.redisCpus,
    REDIS_MEMORY: tier.redisMemory,
    WORKER_CONCURRENCY: String(tier.workerConcurrency),
    DB_MAX_CONNECTIONS: String(tier.dbMaxConnections ?? 10),
    PROVIDER_LATENCY_MS: String(providerLatency),
    ...(tier.envOverrides || {}),
  };

  const activeServices = ["db", "redis", ...tier.nodes.map((n) => n.name)];

  console.log(
    `🚀 Starting containers [${activeServices.join(", ")}] (Server: ${tier.serverCpus} vCPU / ${tier.serverMemory}, DB: ${tier.dbCpus} vCPU, Redis: ${tier.redisCpus} vCPU)...`,
  );

  const environment = await new DockerComposeEnvironment(".", "docker-compose.yml")
    .withEnvironment(composeEnv)
    .withBuild()
    .up(activeServices);

  const dbUrl = `postgres://notifkit:password@localhost:${dbPort}/notifkit`;
  const apiNodes = tier.nodes.filter((n) => n.isApi !== false);
  const apiUrls = (apiNodes.length > 0 ? apiNodes : [tier.nodes[0]!]).map(
    (n) => `http://localhost:${n.port}`,
  );

  const apiLatencies: number[] = [];
  const deliveryLatencies: number[] = [];
  let firstDeliveryTimestamp: number | null = null;
  let lastDeliveryTimestamp: number | null = null;

  try {
    // 1. Health check all server containers
    console.log(`⏳ Waiting for ${tier.nodes.length} node(s) health check...`);
    for (const node of tier.nodes) {
      let ready = false;
      const url = `http://localhost:${node.port}/health`;
      while (!ready) {
        try {
          const res = await fetch(url);
          if (res.ok) ready = true;
          else await new Promise((r) => setTimeout(r, 300));
        } catch {
          await new Promise((r) => setTimeout(r, 300));
        }
      }
    }

    // 2. Attach log streaming to capture delivery events
    for (const node of tier.nodes) {
      const container = environment.getContainer(`${node.name}-1`);
      const stream = await container.logs();
      let logBuffer = "";
      stream.on("data", (chunk) => {
        logBuffer += chunk.toString();
        const lines = logBuffer.split("\n");
        logBuffer = lines.pop() || "";
        for (const line of lines) {
          if (line.includes("push delivered (console transport)")) {
            const now = Date.now();
            if (firstDeliveryTimestamp === null) firstDeliveryTimestamp = now;
            lastDeliveryTimestamp = now;
            try {
              const parsed = JSON.parse(line);
              deliveryLatencies.push(typeof parsed.latency === "number" ? parsed.latency : 0);
            } catch {
              deliveryLatencies.push(0);
            }
          }
        }
      });
    }

    // 3. Database migrations
    console.log("🛠️ Running database migrations...");
    const dbData = createDatabase({ url: dbUrl });
    await runMigrations(dbData.db);

    // 4. Provision project & template
    console.log("🏗️ Provisioning benchmark project and templates...");
    const projectRes = await fetch(`${apiUrls[0]}/v1/projects`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${adminApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "Profiling Project" }),
    });
    const project = await projectRes.json();
    const projectApiKey = project.apiKey;

    await dbData.sql`UPDATE projects SET rate_limit_rpm = 10000000`;

    await fetch(`${apiUrls[0]}/v1/templates`, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${projectApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        templates: [
          {
            id: "perf-email",
            channel: "email",
            topic: ["transactional"],
            content: {
              subject: "Performance Benchmark Email",
              html: "<p>Hello {{name}} from {{service}}</p>",
            },
          },
        ],
      }),
    });

    await fetch(`${apiUrls[0]}/v1/users`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${projectApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        id: "perf-user-1",
        email: ["perf@example.com"],
      }),
    });

    // 5. High-concurrency client load generation
    console.log(
      `\n🔥 Generating load: ${tier.concurrency} parallel virtual callers for ${durationSec}s across ${apiUrls.length} API node(s)...`,
    );

    let requestsSent = 0;
    let successCount = 0;
    let failCount = 0;

    const startTime = Date.now();
    const endTime = startTime + durationSec * 1000;

    const progressInterval = setInterval(() => {
      const elapsedSec = Math.max(1, Math.round((Date.now() - startTime) / 1000));
      const remainingSec = Math.max(0, Math.round((endTime - Date.now()) / 1000));
      const curReqRate = (requestsSent / elapsedSec).toFixed(1);
      console.log(
        `⚡ Blasting: ${elapsedSec}s elapsed (${remainingSec}s left) | Sent: ${requestsSent} (${curReqRate} req/s) | Delivered: ${deliveryLatencies.length} msgs`,
      );
    }, 5000);

    async function worker(workerId: number) {
      const targetUrl = apiUrls[workerId % apiUrls.length]!;
      const svc = MICROSERVICES[workerId % MICROSERVICES.length]!;

      while (Date.now() < endTime) {
        requestsSent++;
        const reqStart = Date.now();
        try {
          const res = await fetch(`${targetUrl}/v1/notify`, {
            method: "POST",
            headers: {
              Authorization: `Bearer ${projectApiKey}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              user: "perf-user-1",
              template: "perf-email",
              channels: ["email"],
              priority: svc.priority,
              data: {
                name: "Load Tester",
                service: svc.name,
                event: svc.event,
                requestTime: reqStart,
              },
            }),
          });

          apiLatencies.push(Date.now() - reqStart);
          if (res.status === 202) {
            successCount++;
          } else {
            failCount++;
          }
        } catch {
          failCount++;
        }
      }
    }

    const workers = Array.from({ length: tier.concurrency }).map((_, i) => worker(i));
    await Promise.all(workers);
    clearInterval(progressInterval);

    const activeWindowSec = durationSec;
    const ingestionThroughput = Number((requestsSent / activeWindowSec).toFixed(2));

    console.log(
      `\n⏳ Load generation finished (${requestsSent} sent in ${activeWindowSec}s, ${successCount} accepted). Awaiting delivery queue flush...`,
    );

    // 6. Queue flush
    const flushStart = Date.now();
    let lastLogged = 0;
    const flushTimeoutMs = Math.max(120000, durationSec * 3000);

    while (deliveryLatencies.length < successCount) {
      if (Date.now() - flushStart > flushTimeoutMs) {
        console.log(
          `⚠️ Flush timeout! Delivered ${deliveryLatencies.length}/${successCount} messages.`,
        );
        break;
      }
      if (Date.now() - lastLogged > 2000) {
        lastLogged = Date.now();
        const pct = ((deliveryLatencies.length / Math.max(1, successCount)) * 100).toFixed(1);
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = (deliveryLatencies.length / Math.max(1, elapsed)).toFixed(0);
        console.log(
          `⏳ Flushing: [${deliveryLatencies.length} / ${successCount}] (${pct}%) ~ ${rate} delivered/sec`,
        );
      }
      await new Promise((r) => setTimeout(r, 200));
    }

    const totalElapsed = (Date.now() - startTime) / 1000;
    const firstDelivOffset = firstDeliveryTimestamp
      ? Number(((firstDeliveryTimestamp - startTime) / 1000).toFixed(2))
      : 0;
    const lastDelivOffset = lastDeliveryTimestamp
      ? Number(((lastDeliveryTimestamp - startTime) / 1000).toFixed(2))
      : totalElapsed;
    const deliverySpanSec =
      firstDeliveryTimestamp &&
      lastDeliveryTimestamp &&
      lastDeliveryTimestamp > firstDeliveryTimestamp
        ? Number(((lastDeliveryTimestamp - firstDeliveryTimestamp) / 1000).toFixed(2))
        : totalElapsed;

    const activeDeliveryRate = Number(
      (deliveryLatencies.length / Math.max(1, deliverySpanSec)).toFixed(2),
    );
    const totalDeliveryRate = Number(
      (deliveryLatencies.length / Math.max(1, totalElapsed)).toFixed(2),
    );

    const apiP50 = percentile(apiLatencies, 50);
    const apiP95 = percentile(apiLatencies, 95);
    const apiP99 = percentile(apiLatencies, 99);
    const deliveryP50 = percentile(deliveryLatencies, 50);
    const deliveryP95 = percentile(deliveryLatencies, 95);
    const deliveryP99 = percentile(deliveryLatencies, 99);

    console.log(
      `✅ Tier Run Complete: Ingestion: ${ingestionThroughput} req/s | Active Delivery: ${activeDeliveryRate} msg/s | API p95: ${apiP95}ms | Deliv p95: ${deliveryP95}ms`,
    );

    await dbData.sql.end().catch(() => {});
    await environment.down().catch(() => {});

    return {
      durationSec: activeWindowSec,
      concurrency: tier.concurrency,
      requestsSent,
      successCount,
      failCount,
      ingestionThroughput,
      firstDelivOffset,
      lastDelivOffset,
      deliverySpanSec,
      activeDeliveryRate,
      totalDeliveryRate,
      apiP50,
      apiP95,
      apiP99,
      deliveryP50,
      deliveryP95,
      deliveryP99,
      totalDelivered: deliveryLatencies.length,
    };
  } catch (err) {
    await environment.down().catch(() => {});
    throw err;
  }
}

export function printTierBreakdown(stat: TierSummary, runsCount: number) {
  console.log("\n" + "=".repeat(165));
  console.log(
    ` 📊 TIER STATISTICAL BREAKDOWN: ${stat.tier.budget} [${stat.tier.description}] (${runsCount} Runs)`,
  );
  console.log("=".repeat(165));

  const headers = [
    "Metric",
    ...stat.runs.map((_, i) => `Run ${i + 1}`),
    "Avg (Mean)",
    "Median",
    "Min",
    "Max",
    "StdDev",
  ];

  const colWidths = [30, ...stat.runs.map(() => 14), 14, 14, 14, 14, 12];
  const formatRow = (cols: string[]) =>
    cols.map((col, idx) => col.padEnd(colWidths[idx] ?? 14)).join(" | ");

  console.log(formatRow(headers));
  console.log("-".repeat(165));

  const rows = [
    {
      label: "Ingestion Throughput",
      values: stat.runs.map((r) => r.ingestionThroughput),
      s: stat.ingestionThroughput,
      unit: " req/s",
    },
    {
      label: "Active Delivery Rate",
      values: stat.runs.map((r) => r.activeDeliveryRate),
      s: stat.activeDeliveryRate,
      unit: " msg/s",
    },
    {
      label: "Wall Drain Rate",
      values: stat.runs.map((r) => r.totalDeliveryRate),
      s: stat.totalDeliveryRate,
      unit: " msg/s",
    },
    {
      label: "API Latency p50",
      values: stat.runs.map((r) => r.apiP50),
      s: stat.apiP50,
      unit: "ms",
    },
    {
      label: "API Latency p95",
      values: stat.runs.map((r) => r.apiP95),
      s: stat.apiP95,
      unit: "ms",
    },
    {
      label: "API Latency p99",
      values: stat.runs.map((r) => r.apiP99),
      s: stat.apiP99,
      unit: "ms",
    },
    {
      label: "Delivery Latency p50",
      values: stat.runs.map((r) => r.deliveryP50),
      s: stat.deliveryP50,
      unit: "ms",
    },
    {
      label: "Delivery Latency p95",
      values: stat.runs.map((r) => r.deliveryP95),
      s: stat.deliveryP95,
      unit: "ms",
    },
    {
      label: "Delivery Latency p99",
      values: stat.runs.map((r) => r.deliveryP99),
      s: stat.deliveryP99,
      unit: "ms",
    },
    {
      label: "Cost Efficiency",
      values: stat.runs.map((r) =>
        Number((r.activeDeliveryRate / stat.tier.monthlyCost).toFixed(2)),
      ),
      s: stat.costEfficiency,
      unit: " msg/s/$",
    },
  ];

  for (const row of rows) {
    const cols = [
      row.label,
      ...row.values.map((v) => `${v}${row.unit}`),
      `${row.s.mean}${row.unit}`,
      `${row.s.median}${row.unit}`,
      `${row.s.min}${row.unit}`,
      `${row.s.max}${row.unit}`,
      `${row.s.stdDev}`,
    ];
    console.log(formatRow(cols));
  }

  console.log("=".repeat(165) + "\n");
}

export function printSummaryTable(title: string, allStats: TierSummary[]) {
  console.log("\n" + "=".repeat(180));
  console.log(`                                      💰 ${title} SUMMARY 💰`);
  console.log("=".repeat(180));

  const headers = [
    "Budget",
    "Architecture & Topology",
    "Ingestion (Avg ± Std)",
    "Active Delivery (Avg / Med)",
    "Wall Drain (Avg)",
    "API p95",
    "Delivery p95",
    "Cost Efficiency",
  ];

  const colWidths = [10, 52, 26, 30, 18, 12, 14, 16];
  const formatRow = (cols: string[]) =>
    cols.map((col, idx) => col.padEnd(colWidths[idx]!)).join(" | ");

  console.log(formatRow(headers));
  console.log("-".repeat(180));

  for (const stat of allStats) {
    const row = [
      stat.tier.budget,
      stat.tier.architecture,
      `${stat.ingestionThroughput.mean} ± ${stat.ingestionThroughput.stdDev} req/s`,
      `${stat.activeDeliveryRate.mean} / ${stat.activeDeliveryRate.median} msg/s`,
      `${stat.totalDeliveryRate.mean} msg/s`,
      `${stat.apiP95.mean}ms`,
      `${stat.deliveryP95.mean}ms`,
      `${stat.costEfficiency.mean} msg/s/$`,
    ];
    console.log(formatRow(row));
  }

  console.log("=".repeat(180) + "\n");
}

export function saveResults(filename: string, data: any) {
  try {
    const resultsDir = path.resolve(process.cwd(), "results");
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }
    const outputPath = path.join(resultsDir, filename);
    fs.writeFileSync(outputPath, JSON.stringify(data, null, 2), "utf-8");
    console.log(`📁 Benchmark results saved to: ${outputPath}`);
  } catch (err) {
    console.warn("⚠️ Failed to save results to JSON:", err);
  }
}
