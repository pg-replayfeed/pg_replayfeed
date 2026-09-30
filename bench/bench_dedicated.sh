#!/bin/bash
# bench/bench_dedicated.sh -- dedicated long-running sustained benchmark matrix for pg_replayfeed 2.0.
#
# Runs a dedicated multi-client soak test with concurrent writers and streaming
# consumers over a sustained duration (e.g. 30s, 60s, 5m), measuring:
#   - Sustained discrete write throughput & commit latency percentiles
#   - End-to-end commit-to-consumer lag distribution (p50, p90, p95, p99, p99.9, max)
#   - Exact WAL generation rate & bandwidth under continuous concurrent load
#   - Delivery verification across all independent consumer slots (zero loss/duplicates)
#   - Dedicated comparative matrix against Supabase Realtime long-running scenarios
#
# Usage:
#   bench/bench_dedicated.sh [--duration 30s] [--writers 8] [--consumers 4]
#   or: make bench-dedicated
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR_MODULES="$ROOT/bench"

source "$ROOT/scripts/pg_throwaway.sh"
source "$BENCH_DIR_MODULES/common.sh"

LOAD_DURATION="30s"
LOAD_WRITERS=8
LOAD_CONSUMERS=4
LOAD_DRAIN="5s"

while [ $# -gt 0 ]; do
  case "$1" in
    --duration) LOAD_DURATION="${2:?}"; shift 2 ;;
    --writers) LOAD_WRITERS="${2:?}"; shift 2 ;;
    --consumers) LOAD_CONSUMERS="${2:?}"; shift 2 ;;
    --drain) LOAD_DRAIN="${2:?}"; shift 2 ;;
    -h|--help)
      echo "usage: bench/bench_dedicated.sh [--duration 30s] [--writers 8] [--consumers 4]"
      echo "Dedicated sustained benchmark matrix measuring latency percentiles, consumer lag, and WAL dynamics."
      exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 1 ;;
  esac
done

pg_throwaway_require_tools
BENCH_DIR=/tmp/pg_replayfeed_bench_dedicated
BENCH_PORT=55428
pg_throwaway_start "$BENCH_DIR" "$BENCH_PORT"
trap pg_throwaway_stop EXIT

export PGDATABASE=bench_dedicated_db
psql -q -X -d postgres -c "CREATE DATABASE $PGDATABASE;"

q "CREATE EXTENSION pg_replayfeed;" >/dev/null

echo "Starting dedicated long-running benchmark matrix ($LOAD_DURATION, $LOAD_WRITERS writers, $LOAD_CONSUMERS consumers)..."

# Run sustained concurrent load workload
source "$BENCH_DIR_MODULES/workloads/05_sustained_load.sh"

# Format and display dedicated matrix report
source "$BENCH_DIR_MODULES/reports/dedicated_matrix_report.sh"
