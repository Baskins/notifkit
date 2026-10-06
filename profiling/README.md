# Notifkit Profiling

Production-shaped throughput benchmarks for notifkit, run on Docker with CPU and
memory limits per container.

```bash
npm run profile:vertical      # one monolith per tier, 1 → 5 vCPU
npm run profile:horizontal    # API nodes + identical 1 vCPU pipeline workers
npm run profile:sustained     # $25 box: highest rate that holds, then a 15 min soak
```

Docker must be running. Each suite first builds notifkit's `dist/` and the node
image from it (`--skip-build` / `--skip-dist` to reuse them).

## What a run does

Every tier gets a fresh environment and three phases:

| Phase      | Setup                                                                                                    | Answers                                                                                               |
| ---------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| **ingest** | Only the API serves; worker services wait behind a gate. k6 closed-loop VUs.                             | The most `POST /v1/notify` the API accepts. Everything accepted is queued.                            |
| **drain**  | Gate opens; workers work through that backlog.                                                           | The most the pipeline delivers per second, and whether `message_logs` keeps up.                       |
| **steady** | Everything running; k6 open-model arrivals at a fixed rate (default: 80% of the lower of the two above). | Whether delivery keeps up with production-style traffic, and the end-to-end latency a recipient sees. |

What makes it production-shaped:

- **Load comes from inside the compose network.** It never goes through Docker Desktop's host port proxy, and the load generator doesn't share an event loop with anything being measured.
- **Deliveries are counted in-process.** The profiling transport counts them and flushes to Redis four times a second, with no per-message logging.
- **The traffic is realistic.** It spreads across 100k seeded users (more than the repositories' 5k-entry caches hold) and 8 templates, some with topics, so they get signed unsubscribe headers. The mix is 35% critical, 40% normal and 25% low priority.
- **The providers are realistic.** The simulated provider takes 150–250 ms (`--latency=`).
- **The `events` service runs**, so `message_logs` gets two rows per delivery, as in production.
- **Configuration matches production:**
  - `NODE_ENV=production` and `LOG_LEVEL=info`.
  - Postgres runs with `shared_buffers` and friends scaled to the tier's memory, plus `pg_stat_statements`.
  - Redis runs with AOF, `appendfsync everysec`.

## Output

For each phase you get:

- API latency percentiles from k6.
- End-to-end latency, from request to provider accept.
- Delivery rate and `message_logs` rows/s.
- Per-container CPU (cores, and share of the container's limit) and memory.
- Per-node event-loop delay.

For the whole run you get Postgres table inserts, sizes, WAL, and the top statements by total time. A final table compares the tiers. Raw results are saved to `results/<suite>-<timestamp>.json`.

The "busiest container" is the one closest to its CPU limit during steady state. That's usually the bottleneck. If a notifkit node sits at ~100% of one core with event-loop p99 climbing, that node is out of CPU.

## Options

```bash
--quick                 # 5s ingest, 10s steady, 10k users
--ingest=15 --steady=30 # phase durations (s)
--rate=5000             # steady-phase arrival rate instead of auto
--users=100000          # seeded recipients
--latency=150-250       # simulated provider latency (ms), fixed or range
--fused                 # PIPELINE_FUSED=true: enricher → engine → delivery in-process
--log-level=info
--runs=3                # repeat each tier, report medians
--tiers=20,60,100
--keep                  # leave containers up afterwards
--skip-build / --skip-dist
```

## Sustained capacity

`profile:sustained` answers "how many notifications/s does a $25 box really
send": the $20 monolith with Redis at 512MB, everything running, with the noise
production has.

1. **ramp**: an unjudged warm-up, then open-model arrivals from `--start` (150/s)
   in `--step` (+50/s) steps of `--step-sec` (60s), plus one step halfway past the last pass. A step
   passes if the API takes what is offered and the backlog across every pipeline
   stream does not trend upwards by more than 2% of the rate.
2. **soak**: the highest passing rate (or `--soak-rate=N`) held for `--soak`
   minutes (15). Backlog, parked retries, DLQ and Redis memory are sampled every 5s,
   and Postgres sizes every 30s.

While it runs:

- The provider refuses `--failure-rate` (3%) of sends. They go through the real retry path (30s, then 2 min).
- `--open-rate` (30%) of delivered messages are reported back as opened through the transport's webhook 20s later, in batches of 500. `--click-share` (15%) of those also click.
- `--log-readers` (2) dashboard users read `GET /v1/notifications/logs` every 2s: the latest page, by template, failed, and by task.

The report covers the ramp, the soak verdict, and API, end-to-end, webhook and log-read latency. It also shows Postgres growth: per notification, per table, and projected per hour, day and 30 days (there is no `message_logs` retention). Results go to `results/sustained-<timestamp>.json`.

## Host ports

The runner reaches Postgres, Redis and the API through host ports 35432, 36379
and 35678. Windows sometimes reserves those at boot (`netsh interface ipv4 show
excludedportrange protocol=tcp`). If so, override them:

```bash
PROF_DB_PORT=45432 PROF_REDIS_PORT=46379 PROF_API_PORT=45678 npm run profile:sustained
```

## Topologies

**Vertical**: a single `server` container running `api, enricher, engine,
delivery, scheduler, events`. It scales from 1 vCPU / 1 GB to 5 vCPU / 6 GB, with Postgres and Redis growing alongside.

**Horizontal**: 1 vCPU / 1 GB nodes.

| Budget | Nodes                        |
| ------ | ---------------------------- |
| $20    | the monolith baseline        |
| $40    | `api-1` + `worker-1`         |
| $60    | `api-1` + 2 workers          |
| $80    | `api-1` + 3 workers          |
| $100   | `api-1`, `api-2` + 3 workers |

Every worker runs the whole pipeline. That is how you'd scale it in production, and it lets `--fused` apply on every worker.

The notifkit, Postgres and Redis limits of the larger tiers add up to more than
a laptop has. Treat numbers from a machine where the containers contend as a lower bound; the runner warns when the limits exceed Docker's CPUs.
