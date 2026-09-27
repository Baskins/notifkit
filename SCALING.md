# Scaling Past the Single Event Loop

## The Problem

All five services (API, enricher, engine, delivery, scheduler) run on one Node.js event loop in monolith mode. The delivery worker's `setTimeout` callbacks (simulating 150–250ms provider latency) compete for the same event loop as enricher/engine DB queries, template rendering, and API request handling.

Numbers from the vertical benchmark ($100 tier, 5 vCPU / 6GB, concurrency 300):

| Metric                                             | Value       |
| -------------------------------------------------- | ----------- |
| Theoretical max delivery (300 slots × 5 calls/sec) | ~1500 msg/s |
| Actual delivery                                    | ~884 msg/s  |
| Utilisation                                        | 59%         |
| Delivery p95 latency                               | 10.3s       |

During load, delivery runs at ~280 msg/s. Once ingestion stops and the event loop clears, delivery climbs to 630 msg/s. The enricher/engine are eating ~40% of the event loop budget while the delivery `setTimeout` queue backs up.

The 5 vCPUs assigned to the container are wasted — Node uses one core for JS, the rest sit idle handling libuv I/O and OS scheduling.

---

## Option 1: Cluster Mode in `NotifkitServer`

Fork each service group into its own child process using Node's `cluster` module. The user sees no change — `NotifkitServer` still takes a `services` array and a single port. Internally, the primary process manages forks.

### How it works

```
Primary process (coordinator)
├── Fork 1: API (serves HTTP, proxied from primary)
├── Fork 2: enricher
├── Fork 3: engine + delivery (share template/contact state)
└── Fork 4: scheduler
```

Each fork calls the same `server.ts` entrypoint with a `SERVICES` env override. The primary uses `cluster.fork()` and forwards the HTTP socket to the API fork.

### Implementation sketch

**`src/server.ts`** — add a `cluster` option:

```typescript
interface NotifkitOptions {
  // ...existing fields
  cluster?:
    | boolean
    | {
        /** Group services into processes. Default splits by natural boundaries. */
        groups?: string[][];
      };
}
```

When `cluster` is truthy and `cluster.isPrimary`:

1. Determine service groups (default: `[["api"], ["enricher"], ["engine", "delivery"], ["scheduler"]]`)
2. `cluster.fork({ SERVICES: group.join(",") })` for each group
3. Forward the listening socket to the API fork
4. Relay `SIGINT`/`SIGTERM` to children, await clean exits

When `cluster.isWorker`:

1. Read `SERVICES` from env
2. Run the normal `start()` path with only those services

### Changes required

| File                      | Change                                                       |
| ------------------------- | ------------------------------------------------------------ |
| `src/server.ts`           | Add fork logic in `start()`, socket forwarding, signal relay |
| `src/server.ts`           | `stop()` sends shutdown message to children                  |
| `profiling/src/server.ts` | Pass `cluster: true`                                         |
| Docs                      | Document the option                                          |

### Tradeoffs

- **Each fork gets its own event loop and V8 heap** — 5 vCPUs are actually used.
- **Redis/Postgres connections multiply** — 4 forks × the current pool sizes. At small scale this is fine; at scale, tune pool sizes down per fork.
- **`globalEmitter` no longer works cross-service** — events emitted by delivery don't reach the API fork. Need IPC relay for the handful of events (`delivery:delivered`, etc.) that the `NotifkitServer` instance re-emits. Alternatively, the event bus already goes through Redis streams, so the `events` service handles cross-process observation.
- **Custom `providers` registered in the primary** — must be serialisable or registered in each fork. The `registerTransport()` call happens before forking, so the transport class must be importable by path rather than passed as an instance.

### Estimated effort

Medium — ~200–300 lines of cluster coordination, IPC for events, and a provider registration refactor.

---

## Option 2: Cluster Mode in the Profiling Server Only

Leave `NotifkitServer` unchanged. Fork only inside `profiling/src/server.ts`.

### How it works

```typescript
// profiling/src/server.ts
import cluster from "node:cluster";

if (cluster.isPrimary) {
  const groups = [["api"], ["enricher"], ["engine", "delivery"], ["scheduler"]];
  for (const group of groups) {
    cluster.fork({ SERVICES: group.join(",") });
  }
} else {
  // existing NotifkitServer startup with services from env
}
```

The profiling docker-compose already passes `SERVICES` as an env var, so this fits naturally.

### Changes required

| File                           | Change                                       |
| ------------------------------ | -------------------------------------------- |
| `profiling/src/server.ts`      | Wrap in cluster fork/worker branch           |
| `profiling/docker-compose.yml` | No change — `SERVICES` already flows through |

### Tradeoffs

- **Fastest to implement** — ~30 lines.
- **Doesn't help real users** — only the benchmark benefits.
- **Good for proving the ceiling lift** before investing in Option 1.

### Estimated effort

Small — an afternoon.

---

## Option 3: `worker_threads`

Run heavy services (enricher, engine, delivery) as worker threads instead of forked processes.

### How it works

```typescript
import { Worker } from "node:worker_threads";

const enricherThread = new Worker("./services/enricher/main.js");
const engineThread = new Worker("./services/engine/main.js");
const deliveryThread = new Worker("./services/delivery/main.js");
```

Each thread gets its own V8 isolate and event loop but shares the process memory space (via `SharedArrayBuffer` if needed).

### Changes required

| File                      | Change                                                                           |
| ------------------------- | -------------------------------------------------------------------------------- |
| Each `services/*/main.ts` | Wrap `start*Worker()` in a `isMainThread` guard or accept a `parentPort` message |
| `src/server.ts`           | Spawn `Worker` instances instead of direct `await start*()` calls                |
| Provider registration     | Must happen inside each thread, or pass config via `workerData`                  |

### Tradeoffs

- **Same event loop isolation as cluster**, one V8 heap per thread.
- **Lighter than `cluster`** — no full process overhead, shared libuv thread pool.
- **Debugging is harder** — stack traces, inspector ports, and `console.log` all need routing.
- **Cannot share object references** — Redis clients, DB pools, and transport instances must be created per thread. Same connection multiplication as Option 1.
- **`globalEmitter` breaks the same way** — need `MessagePort` relay instead of IPC.
- **Module loading quirks** — ESM + `worker_threads` requires care with `--loader` flags and `import.meta.url` resolution.

### Estimated effort

Medium-High — more wiring than cluster for the same isolation benefit. The ESM loader situation adds friction.

---

## Comparison

|                       | Option 1: Cluster in Server | Option 2: Cluster in Profiler | Option 3: worker_threads |
| --------------------- | --------------------------- | ----------------------------- | ------------------------ |
| Event loop isolation  | ✅ Full                     | ✅ Full                       | ✅ Full                  |
| Multi-core usage      | ✅ Yes                      | ✅ Yes                        | ✅ Yes                   |
| Benefits real users   | ✅ Yes                      | ❌ Benchmark only             | ✅ Yes                   |
| Implementation effort | Medium                      | Small                         | Medium-High              |
| Connection overhead   | 4× pools                    | 4× pools                      | 4× pools                 |
| globalEmitter         | IPC relay needed            | IPC relay needed              | MessagePort relay needed |
| Provider registration | Needs refactor              | Trivial                       | Needs refactor           |
| Debugging experience  | Good (separate PIDs)        | Good                          | Harder                   |
| Memory overhead       | Higher (separate heaps)     | Higher                        | Slightly lower           |

## Recommended Phasing

1. **Start with Option 2** — prove the ceiling lift in the benchmark with minimal code. If delivery jumps from ~880 to ~1400+ msg/s on the $100 tier, the thesis is confirmed.
2. **Ship Option 1** — cluster mode behind an opt-in flag in `NotifkitServer`. Default service grouping covers 90% of cases; power users can override groups.
3. **Skip Option 3** — worker_threads buys the same isolation as cluster with more wiring and worse DX. Not worth it unless you later need shared memory for a specific use case (e.g., a shared token cache that avoids Redis).
