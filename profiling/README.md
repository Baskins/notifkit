# Notifkit Profiling & Scaling Benchmarks

Simplified profiling suite for Notifkit with two focused scaling profilers.

---

## 1. Vertical Scaling Profiler ($20 to $100)

Scales a single monolith server from 1 vCPU up to 5 vCPUs with matching database and Redis capacities.

### Architecture by Tier

| Budget      | Server (Monolith)  | Database         | Redis            |
| ----------- | ------------------ | ---------------- | ---------------- |
| **$20/mo**  | 1.0 vCPU, 1 GB RAM | 0.5 vCPU, 512 MB | 0.5 vCPU, 256 MB |
| **$40/mo**  | 2.0 vCPU, 2 GB RAM | 1.0 vCPU, 1 GB   | 0.5 vCPU, 512 MB |
| **$60/mo**  | 3.0 vCPU, 3 GB RAM | 1.5 vCPU, 1.5 GB | 1.0 vCPU, 1 GB   |
| **$80/mo**  | 4.0 vCPU, 4 GB RAM | 2.0 vCPU, 2 GB   | 1.0 vCPU, 1 GB   |
| **$100/mo** | 5.0 vCPU, 6 GB RAM | 2.5 vCPU, 3 GB   | 1.5 vCPU, 1.5 GB |

### Run Command

```bash
npm run profile:vertical
```

Options:

```bash
npm run profile:vertical -- --quick                    # 5s test per tier, 1 run
npm run profile:vertical -- --duration=15 --runs=3     # Custom duration and iterations
npm run profile:vertical -- --tiers=20,40,100          # Run specific tiers
```

---

## 2. Horizontal Scaling Profiler ($20 to $100)

Scales horizontally by distributing distinct microservices across dedicated server nodes.

### Service Distribution by Tier

| Budget      | Topology  | Distributed Nodes & Roles                                                                                                                           |
| ----------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **$20/mo**  | 1 Server  | `monolith-server` (api, enricher, engine, delivery, scheduler)                                                                                      |
| **$40/mo**  | 2 Servers | `api-server-1` (api)<br>`worker-server` (enricher, engine, delivery, scheduler)                                                                     |
| **$60/mo**  | 3 Servers | `api-server-1` (api)<br>`pipeline-worker` (enricher, engine, scheduler)<br>`delivery-worker` (delivery)                                             |
| **$80/mo**  | 4 Servers | `api-server-1` (api)<br>`enricher-worker` (enricher, scheduler)<br>`engine-worker` (engine)<br>`delivery-worker` (delivery)                         |
| **$100/mo** | 5 Servers | `api-server-1` (api)<br>`api-server-2` (api)<br>`enricher-worker` (enricher, scheduler)<br>`engine-worker` (engine)<br>`delivery-worker` (delivery) |

### Run Command

```bash
npm run profile:horizontal
```

Options:

```bash
npm run profile:horizontal -- --quick                  # 5s test per tier, 1 run
npm run profile:horizontal -- --duration=15 --runs=3   # Custom duration and iterations
npm run profile:horizontal -- --tiers=40,80,100        # Run specific tiers
```

---

## Output Metrics

Both profilers output:

- **Ingestion Throughput** (`req/s`)
- **Active Delivery Rate** (`msg/s`)
- **Wall Drain Rate** (`msg/s`)
- **API Latency** (`p50`, `p95`, `p99`)
- **Delivery Latency** (`p50`, `p95`, `p99`)
- **Cost Efficiency** (`msg/s per $`)
- Statistical aggregation across runs (Mean, Median, Min, Max, StdDev)
- JSON export in `results/`
