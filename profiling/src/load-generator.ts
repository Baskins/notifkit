import { setGlobalDispatcher, Agent } from "undici";

// High concurrency undici dispatcher
setGlobalDispatcher(
  new Agent({
    connections: 4000,
    pipelining: 1,
    keepAliveTimeout: 30000,
    keepAliveMaxTimeout: 60000,
  }),
);

const API_URL = process.env.API_URL || "http://api-server-1:3000";
const API_KEY = process.env.PROJECT_API_KEY || "";
const DURATION_SEC = Number(process.env.DURATION_SEC || "120");
const CONCURRENCY = Number(process.env.CONCURRENCY || "240");
const SERVICES_COUNT = Number(process.env.SERVICES_COUNT || "8");

const MICROSERVICES = [
  { name: "order-service", priority: "critical", event: "order_confirmation" },
  { name: "payments-service", priority: "critical", event: "payment_receipt" },
  { name: "billing-service", priority: "normal", event: "invoice_ready" },
  { name: "auth-service", priority: "critical", event: "password_reset" },
  { name: "shipping-service", priority: "normal", event: "tracking_update" },
  { name: "account-service", priority: "normal", event: "security_alert" },
  { name: "inventory-service", priority: "low", event: "stock_replenished" },
  { name: "fraud-service", priority: "critical", event: "suspicious_login" },
  { name: "marketing-service", priority: "low", event: "promotional_blast" },
  { name: "support-service", priority: "normal", event: "ticket_reply" },
];

async function main() {
  console.log(`\n🚀 In-Docker Load Generator starting...`);
  console.log(`Target: ${API_URL}`);
  console.log(`Concurrency: ${CONCURRENCY} workers | Duration: ${DURATION_SEC}s`);

  const activeServices = MICROSERVICES.slice(0, Math.min(SERVICES_COUNT, MICROSERVICES.length));

  let requestsSent = 0;
  let successCount = 0;
  let failCount = 0;
  const apiLatencies: number[] = [];

  const startTime = Date.now();
  const endTime = startTime + DURATION_SEC * 1000;

  const progressInterval = setInterval(() => {
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(0);
    const remainingSec = Math.max(0, Math.round((endTime - Date.now()) / 1000));
    const curReqRate = (requestsSent / Math.max(1, Number(elapsedSec))).toFixed(1);
    console.log(
      `⚡ [In-Docker Generator] ${elapsedSec}s elapsed (${remainingSec}s left) | Sent: ${requestsSent} (${curReqRate} req/s) | Success: ${successCount}`,
    );
  }, 10000);

  async function worker(workerIndex: number) {
    const svc = activeServices[workerIndex % activeServices.length]!;
    while (Date.now() < endTime) {
      requestsSent++;
      try {
        const reqStart = Date.now();
        const res = await fetch(`${API_URL}/v1/notify`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${API_KEY}`,
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
              requestTime: reqStart,
            },
          }),
        });

        const lat = Date.now() - reqStart;
        apiLatencies.push(lat);

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

  const workers = Array.from({ length: CONCURRENCY }).map((_, i) => worker(i));
  await Promise.all(workers);
  clearInterval(progressInterval);

  function percentile(arr: number[], p: number) {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[idx];
  }

  const durationActual = (Date.now() - startTime) / 1000;
  const throughput = (requestsSent / durationActual).toFixed(2);

  const stats = {
    requestsSent,
    successCount,
    failCount,
    throughput: Number(throughput),
    apiP50: percentile(apiLatencies, 50),
    apiP95: percentile(apiLatencies, 95),
    apiP99: percentile(apiLatencies, 99),
  };

  console.log(`\n✅ [In-Docker Generator] Finished:`, JSON.stringify(stats));
  console.log(`__BENCHMARK_RESULT__:${JSON.stringify(stats)}`);
}

main().catch((err) => {
  console.error("Load generator failed:", err);
  process.exit(1);
});
