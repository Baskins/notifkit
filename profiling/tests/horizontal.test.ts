import { runSuite, type ProfileTier } from "../src/runner.js";

// 1 vCPU / 1GB nodes. API nodes serve ingress only; every worker node runs the
// whole pipeline (enricher, engine, delivery, scheduler, events), so capacity
// grows by adding identical workers — the way it would be deployed, and the
// shape that lets `--fused` apply on every worker.
const NODE = { cpus: "1.0", memory: "1G" };

const tier = (
  budget: number,
  apis: number,
  workers: number,
  db: [string, string],
  redis: [string, string],
): ProfileTier => {
  const apiNodes = Array.from({ length: apis }, (_, i) => `api-${i + 1}`);
  const workerNodes = Array.from({ length: workers }, (_, i) => `worker-${i + 1}`);
  return {
    budget: `$${budget}/mo`,
    monthlyCost: budget,
    description: `${apis} API + ${workers} worker node${workers > 1 ? "s" : ""}`,
    nodes: [...apiNodes, ...workerNodes],
    apiNodes,
    api: NODE,
    worker: NODE,
    db: { cpus: db[0], memory: db[1] },
    redis: { cpus: redis[0], memory: redis[1] },
    workerConcurrency: 100,
    deliveryConcurrency: 800,
    dbMaxConnections: 6,
    ingestVus: 128 * apis,
  };
};

export const HORIZONTAL_TIERS: ProfileTier[] = [
  {
    budget: "$20/mo",
    monthlyCost: 20,
    description: "1x monolith (baseline)",
    nodes: ["server"],
    apiNodes: ["server"],
    server: NODE,
    db: { cpus: "0.5", memory: "512M" },
    redis: { cpus: "0.5", memory: "256M" },
    workerConcurrency: 50,
    deliveryConcurrency: 400,
    dbMaxConnections: 5,
    ingestVus: 64,
  },
  tier(40, 1, 1, ["1.0", "1G"], ["0.5", "512M"]),
  tier(60, 1, 2, ["1.5", "1536M"], ["1.0", "1G"]),
  tier(80, 1, 3, ["2.0", "2G"], ["1.0", "1G"]),
  tier(100, 2, 3, ["2.5", "3G"], ["1.5", "1536M"]),
];

runSuite("horizontal", "NOTIFKIT HORIZONTAL SCALING ($20 → $100)", HORIZONTAL_TIERS).catch(
  (err) => {
    console.error("Horizontal benchmark failed:", err);
    process.exit(1);
  },
);
