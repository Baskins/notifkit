// Load generator for POST /v1/notify, run by src/runner.ts inside the compose
// network so the numbers do not include Docker Desktop's host port proxy.
//
// MODE=closed  — VUS callers in a tight loop for DURATION seconds: the most the
//                API can accept (ingest capacity).
// MODE=rate    — RATE requests/s for DURATION seconds regardless of response
//                time (an open model, like real traffic).
import http from "k6/http";
import { check } from "k6";

const TARGETS = (__ENV.TARGETS || "api-1:3000").split(",");
const API_KEY = __ENV.API_KEY;
const USERS = parseInt(__ENV.USERS || "1000", 10);
const DURATION = `${__ENV.DURATION || "10"}s`;
const VUS = parseInt(__ENV.VUS || "100", 10);

// Weighted like a production tenant: mostly transactional, a marketing tail.
const TRAFFIC = [
  { template: "order_confirmation", priority: "critical", weight: 20 },
  { template: "payment_receipt", priority: "critical", weight: 10 },
  { template: "password_reset", priority: "critical", weight: 5 },
  { template: "tracking_update", priority: "normal", weight: 25 },
  { template: "invoice_ready", priority: "normal", weight: 10 },
  { template: "security_alert", priority: "normal", weight: 5 },
  { template: "stock_replenished", priority: "low", weight: 10 },
  { template: "promotional_blast", priority: "low", weight: 15 },
];
const TOTAL_WEIGHT = TRAFFIC.reduce((sum, t) => sum + t.weight, 0);

function pickTraffic() {
  let r = Math.random() * TOTAL_WEIGHT;
  for (const t of TRAFFIC) {
    r -= t.weight;
    if (r < 0) return t;
  }
  return TRAFFIC[TRAFFIC.length - 1];
}

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
  scenarios:
    __ENV.MODE === "rate"
      ? {
          steady: {
            executor: "constant-arrival-rate",
            rate: parseInt(__ENV.RATE || "1000", 10),
            timeUnit: "1s",
            duration: DURATION,
            preAllocatedVUs: VUS,
            maxVUs: parseInt(__ENV.MAX_VUS || String(VUS * 4), 10),
          },
        }
      : {
          max: {
            executor: "constant-vus",
            vus: VUS,
            duration: DURATION,
          },
        },
};

const HEADERS = {
  Authorization: `Bearer ${API_KEY}`,
  "Content-Type": "application/json",
};

export default function () {
  const target = TARGETS[(__VU + __ITER) % TARGETS.length];
  const traffic = pickTraffic();
  const userId = `perf-user-${Math.floor(Math.random() * USERS)}`;

  const res = http.post(
    `http://${target}/v1/notify`,
    JSON.stringify({
      user: userId,
      template: traffic.template,
      channels: ["email"],
      priority: traffic.priority,
      data: {
        name: "Load Tester",
        orderId: `ord-${__VU}-${__ITER}`,
        amount: "42.00",
        requestTime: Date.now(),
      },
    }),
    { headers: HEADERS, tags: { name: "notify" } },
  );

  check(res, { accepted: (r) => r.status === 202 });
}

export function handleSummary(data) {
  return { [__ENV.OUT || "/out/summary.json"]: JSON.stringify(data) };
}
