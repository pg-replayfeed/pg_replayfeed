# pg_replayfeed — Modular Benchmark Architecture

The `bench/` directory contains an end-to-end benchmark suite for `pg_replayfeed` 2.0, providing both quick validation runs and a dedicated, long-running sustained matrix that measures write throughput, latency percentiles, WAL dynamics, consumer lag, and payload independence against Supabase Realtime baselines.

---

## Directory Layout

```
bench/
├── bench.sh                      # Quick CLI benchmark orchestrator (`make bench`)
├── bench_dedicated.sh            # Dedicated sustained long-running matrix (`make bench-dedicated`)
├── common.sh                     # Shared utilities: timing, portable timeout, psql wrappers
├── workloads/                    # Modular benchmark workloads
│   ├── 01_write_throughput.sh    # Multi-row bulk INSERTs + WAL overhead
│   ├── 02_discrete_commits.sh     # Discrete 1-op transaction commits + streaming
│   ├── 03_replay_streaming.sh    # Logical decoding streaming throughput
│   ├── 04_payload_impact.sh      # Key-only stream behavior across 50B, 1KB, 10KB, and 50KB rows
│   └── 05_sustained_load.sh      # Sustained concurrent writers + streaming consumers soak
├── reports/                      # Output formatting & analytical comparisons
│   ├── summary_report.sh         # Standard summary report & Supabase Realtime comparison
│   └── dedicated_matrix_report.sh # Comprehensive matrix with high-resolution latency percentiles
└── README.md                     # Modular benchmark documentation
```

---

## Benchmark Modes

### 1. Standard Quick Benchmark (`make bench`)
Runs a fast (~2s) end-to-end suite across all workloads on a throwaway instance:
```bash
make bench
# or with custom parameters:
bench/bench.sh --rows 50000 --replay-rows 10000 --discrete-txns 500 --payload-rows 2000
```

### 2. Dedicated Long-Running Matrix (`make bench-dedicated`)
Executes a sustained multi-client soak test with concurrent writers and parallel replication consumers over a sustained duration (e.g., 30s, 60s, 5m), measuring:
- **Write Statement Latency**: p50, p90, p95, p99, p99.9, max
- **End-to-End Consumer Lag**: Commit-to-receive latency percentiles (p50, p90, p95, p99, p99.9, max)
- **Sustained Throughput**: Commits/sec and aggregate changes delivered/sec
- **WAL Sustained Bandwidth**: Exact MB/s write bandwidth and bytes per commit
- **Delivery Verification**: 100% loss-free, duplicate-free delivery check across independent slots
- **Side-by-Side Matrix**: Full comparative synthesis with Supabase Realtime benchmarks

```bash
make bench-dedicated
# or customize concurrency and duration:
bench/bench_dedicated.sh --duration 60s --writers 16 --consumers 8
```
