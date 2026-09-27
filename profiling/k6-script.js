import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';
import exec from 'k6/execution';

const API_URL = __ENV.API_URL || 'http://api-server-1:3000';
const API_KEY = __ENV.PROJECT_API_KEY || '';
const DURATION = __ENV.DURATION || '120s';
const VUS = parseInt(__ENV.VUS || '240', 10);
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '45s';

const steadyReqDuration = new Trend('steady_http_req_duration', true);
const steadyReqs = new Counter('steady_http_reqs');

export const options = {
  scenarios: {
    warmup: {
      executor: 'constant-vus',
      vus: VUS,
      duration: WARMUP_DURATION,
      gracefulStop: '5s',
    },
    steady_state: {
      executor: 'constant-vus',
      vus: VUS,
      startTime: WARMUP_DURATION,
      duration: DURATION,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    'http_req_failed{scenario:steady_state}': ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)'],
};

const MICROSERVICES = [
  { name: 'order-service', priority: 'critical', event: 'order_confirmation' },
  { name: 'payments-service', priority: 'critical', event: 'payment_receipt' },
  { name: 'billing-service', priority: 'normal', event: 'invoice_ready' },
  { name: 'auth-service', priority: 'critical', event: 'password_reset' },
  { name: 'shipping-service', priority: 'normal', event: 'tracking_update' },
  { name: 'account-service', priority: 'normal', event: 'security_alert' },
  { name: 'inventory-service', priority: 'low', event: 'stock_replenished' },
  { name: 'fraud-service', priority: 'critical', event: 'suspicious_login' },
];

const headers = {
  'Content-Type': 'application/json',
  'Authorization': `Bearer ${API_KEY}`,
};

export default function () {
  const svc = MICROSERVICES[__VU % MICROSERVICES.length];
  
  const payload = JSON.stringify({
    user: 'perf-user-1',
    template: 'perf-email',
    channels: ['email'],
    priority: svc.priority,
    data: {
      name: 'Load Tester',
      service: svc.name,
      event: svc.event,
    },
  });

  const res = http.post(`${API_URL}/v1/notify`, payload, { headers });

  if (exec.scenario.name === 'steady_state') {
    steadyReqDuration.add(res.timings.duration);
    steadyReqs.add(1);
  }

  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}

