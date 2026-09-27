import {
  runTierBenchmark,
  calculateStats,
  printTierBreakdown,
  printSummaryTable,
  saveResults,
  type ProfileTier,
  type TierSummary,
  type SingleRunResult,
} from "../src/runner.js";

export const VERTICAL_TIERS: ProfileTier[] = [
  {
    budget: "$20/mo",
    monthlyCost: 20,
    description: "1x Monolith (1.0 vCPU / 1GB RAM)",
    architecture: "1x Server (All Services: API, Enricher, Engine, Delivery, Scheduler)",
    serverCpus: "1.0",
    serverMemory: "1G",
    dbCpus: "0.5",
    dbMemory: "512M",
    redisCpus: "0.5",
    redisMemory: "256M",
    workerConcurrency: 50,
    dbMaxConnections: 5,
    concurrency: 80,
    nodes: [{ name: "server", port: 35678, isApi: true }],
  },
  {
    budget: "$40/mo",
    monthlyCost: 40,
    description: "1x Monolith (2.0 vCPU / 2GB RAM)",
    architecture: "1x Server (All Services: API, Enricher, Engine, Delivery, Scheduler)",
    serverCpus: "2.0",
    serverMemory: "2G",
    dbCpus: "1.0",
    dbMemory: "1G",
    redisCpus: "0.5",
    redisMemory: "512M",
    workerConcurrency: 100,
    dbMaxConnections: 10,
    concurrency: 160,
    nodes: [{ name: "server", port: 35678, isApi: true }],
  },
  {
    budget: "$60/mo",
    monthlyCost: 60,
    description: "1x Monolith (3.0 vCPU / 3GB RAM)",
    architecture: "1x Server (All Services: API, Enricher, Engine, Delivery, Scheduler)",
    serverCpus: "3.0",
    serverMemory: "3G",
    dbCpus: "1.5",
    dbMemory: "1.5G",
    redisCpus: "1.0",
    redisMemory: "1G",
    workerConcurrency: 150,
    dbMaxConnections: 15,
    concurrency: 240,
    nodes: [{ name: "server", port: 35678, isApi: true }],
  },
  {
    budget: "$80/mo",
    monthlyCost: 80,
    description: "1x Monolith (4.0 vCPU / 4GB RAM)",
    architecture: "1x Server (All Services: API, Enricher, Engine, Delivery, Scheduler)",
    serverCpus: "4.0",
    serverMemory: "4G",
    dbCpus: "2.0",
    dbMemory: "2G",
    redisCpus: "1.0",
    redisMemory: "1G",
    workerConcurrency: 200,
    dbMaxConnections: 20,
    concurrency: 320,
    nodes: [{ name: "server", port: 35678, isApi: true }],
  },
  {
    budget: "$100/mo",
    monthlyCost: 100,
    description: "1x Monolith (5.0 vCPU / 6GB RAM)",
    architecture: "1x Server (All Services: API, Enricher, Engine, Delivery, Scheduler)",
    serverCpus: "5.0",
    serverMemory: "6G",
    dbCpus: "2.5",
    dbMemory: "3G",
    redisCpus: "1.5",
    redisMemory: "1.5G",
    workerConcurrency: 300,
    dbMaxConnections: 25,
    concurrency: 400,
    nodes: [{ name: "server", port: 35678, isApi: true }],
  },
];

function parseArgs() {
  const args = process.argv.slice(2);
  let durationSec = 10;
  let runs = 3;
  let providerLatencyMs: number | string | undefined = process.env.PROVIDER_LATENCY_MS || "150-250";
  let tiers = VERTICAL_TIERS;

  for (const arg of args) {
    if (arg.startsWith("--duration=")) {
      durationSec = parseInt(arg.split("=")[1]!, 10);
    } else if (arg.startsWith("--runs=")) {
      runs = parseInt(arg.split("=")[1]!, 10);
    } else if (arg === "--quick") {
      durationSec = 5;
      runs = 1;
    } else if (arg.startsWith("--latency=")) {
      const val = arg.split("=")[1]!;
      providerLatencyMs = val.includes("-") ? val : parseInt(val, 10);
    } else if (arg.startsWith("--workers=")) {
      const val = parseInt(arg.split("=")[1]!, 10);
      tiers = tiers.map((t) => ({ ...t, workerConcurrency: val }));
    } else if (arg.startsWith("--tiers=")) {
      const requested = arg
        .split("=")[1]!
        .split(",")
        .map((s) => s.trim().replace("$", "").replace("/mo", ""));
      tiers = VERTICAL_TIERS.filter((t) =>
        requested.includes(t.budget.replace("$", "").replace("/mo", "")),
      );
    }
  }

  return { durationSec, runs, providerLatencyMs, tiers };
}

async function main() {
  const { durationSec, runs, providerLatencyMs, tiers } = parseArgs();

  console.log(
    "==========================================================================================",
  );
  console.log(
    "             🚀 NOTIFKIT VERTICAL SCALING PROFILER ($20 -> $100 SETUP)                   ",
  );
  console.log(
    "==========================================================================================",
  );
  console.log(
    `Duration per run: ${durationSec}s | Iterations per tier: ${runs} | Total Runs: ${tiers.length * runs}`,
  );
  console.log(`Tiers to test:    ${tiers.map((t) => t.budget).join("  ➜  ")}`);
  if (providerLatencyMs && providerLatencyMs !== "0") {
    console.log(`Provider Latency: ${providerLatencyMs}ms`);
  }
  console.log(
    "==========================================================================================\n",
  );

  const allStats: TierSummary[] = [];

  for (let tIdx = 0; tIdx < tiers.length; tIdx++) {
    const tier = tiers[tIdx]!;
    const tierRuns: SingleRunResult[] = [];

    console.log(
      `\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>`,
    );
    console.log(`[Tier ${tIdx + 1}/${tiers.length}] Budget: ${tier.budget} — ${tier.description}`);
    console.log(
      `Resources: Server ${tier.serverCpus} vCPU / ${tier.serverMemory} | DB ${tier.dbCpus} vCPU | Redis ${tier.redisCpus} vCPU`,
    );
    console.log(
      `>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n`,
    );

    for (let rIdx = 0; rIdx < runs; rIdx++) {
      console.log(`\n--- [${tier.budget}] Run ${rIdx + 1} of ${runs} (${durationSec}s) ---`);
      const res = await runTierBenchmark(tier, { durationSec, providerLatencyMs });
      tierRuns.push(res);

      if (rIdx < runs - 1) {
        console.log("⏳ Cooling down for 3s before next run...");
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }

    const tierStats: TierSummary = {
      tier,
      runs: tierRuns,
      ingestionThroughput: calculateStats(tierRuns.map((r) => r.ingestionThroughput)),
      activeDeliveryRate: calculateStats(tierRuns.map((r) => r.activeDeliveryRate)),
      totalDeliveryRate: calculateStats(tierRuns.map((r) => r.totalDeliveryRate)),
      apiP50: calculateStats(tierRuns.map((r) => r.apiP50)),
      apiP95: calculateStats(tierRuns.map((r) => r.apiP95)),
      apiP99: calculateStats(tierRuns.map((r) => r.apiP99)),
      deliveryP50: calculateStats(tierRuns.map((r) => r.deliveryP50)),
      deliveryP95: calculateStats(tierRuns.map((r) => r.deliveryP95)),
      deliveryP99: calculateStats(tierRuns.map((r) => r.deliveryP99)),
      costEfficiency: calculateStats(
        tierRuns.map((r) => Number((r.activeDeliveryRate / tier.monthlyCost).toFixed(2))),
      ),
    };

    allStats.push(tierStats);
    printTierBreakdown(tierStats, runs);

    if (tIdx < tiers.length - 1) {
      console.log("⏳ Cooling down for 4s before next tier...");
      await new Promise((resolve) => setTimeout(resolve, 4000));
    }
  }

  printSummaryTable("VERTICAL SCALING BENCHMARK ($20 -> $100)", allStats);
  saveResults(`vertical-scaling-${Date.now()}.json`, allStats);
}

main().catch((err) => {
  console.error("Vertical scaling benchmark failed:", err);
  process.exit(1);
});
