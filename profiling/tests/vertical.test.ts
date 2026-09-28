import { runSuite, type ProfileTier } from "../src/runner.js";

// One monolith per tier running every service, events included. Node runs JS
// on one core, so without cluster mode extra vCPUs mostly go to GC, libuv and
// the other containers — this suite shows how much that costs.
const monolith = (
  budget: number,
  cpus: string,
  memory: string,
  db: [string, string],
  redis: [string, string],
  workerConcurrency: number,
  deliveryConcurrency: number,
  dbMaxConnections: number,
  ingestVus: number,
): ProfileTier => ({
  budget: `$${budget}/mo`,
  monthlyCost: budget,
  description: `1x monolith (${cpus} vCPU / ${memory})`,
  nodes: ["server"],
  apiNodes: ["server"],
  server: { cpus, memory },
  db: { cpus: db[0], memory: db[1] },
  redis: { cpus: redis[0], memory: redis[1] },
  workerConcurrency,
  deliveryConcurrency,
  dbMaxConnections,
  ingestVus,
});

export const VERTICAL_TIERS: ProfileTier[] = [
  monolith(20, "1.0", "1G", ["0.5", "512M"], ["0.5", "256M"], 50, 400, 5, 64),
  monolith(40, "2.0", "2G", ["1.0", "1G"], ["0.5", "512M"], 100, 800, 8, 128),
  monolith(60, "3.0", "3G", ["1.5", "1536M"], ["1.0", "1G"], 150, 1200, 10, 192),
  monolith(80, "4.0", "4G", ["2.0", "2G"], ["1.0", "1G"], 200, 1600, 12, 256),
  monolith(100, "5.0", "6G", ["2.5", "3G"], ["1.5", "1536M"], 250, 2000, 15, 320),
];

runSuite("vertical", "NOTIFKIT VERTICAL SCALING ($20 → $100)", VERTICAL_TIERS).catch((err) => {
  console.error("Vertical benchmark failed:", err);
  process.exit(1);
});
