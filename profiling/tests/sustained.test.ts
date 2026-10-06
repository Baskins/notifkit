import { runSustainedSuite } from "../src/sustained.js";
import type { ProfileTier } from "../src/runner.js";

// The $20 monolith with Redis at 512MB instead of 256MB: about what a $25
// box gives you (2 vCPU / 2GB split between notifkit, Postgres and Redis).
// 256MB is not enough to hold an hour of idempotency markers at a few
// hundred messages/s, and Redis runs with noeviction, so it would be
// OOM-killed partway through a long soak rather than measured.
const TIER_25: ProfileTier = {
  budget: "$25/mo",
  monthlyCost: 25,
  description: "1x monolith (1 vCPU / 1G), Postgres 0.5 vCPU / 512M, Redis 0.5 vCPU / 512M",
  nodes: ["server"],
  apiNodes: ["server"],
  server: { cpus: "1.0", memory: "1G" },
  db: { cpus: "0.5", memory: "512M" },
  redis: { cpus: "0.5", memory: "512M" },
  workerConcurrency: 50,
  deliveryConcurrency: 400,
  dbMaxConnections: 5,
  ingestVus: 64,
};

runSustainedSuite(TIER_25).catch((err) => {
  console.error("Sustained benchmark failed:", err);
  process.exit(1);
});
