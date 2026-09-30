#!/bin/bash
# bench/bench.sh -- modular benchmark orchestrator for pg_replayfeed 2.0.
#
# Workload modules:
#   bench/workloads/01_write_throughput.sh  -- Bulk multi-row INSERTs + WAL overhead
#   bench/workloads/02_discrete_commits.sh   -- Discrete 1-op transaction commits
#   bench/workloads/03_replay_streaming.sh  -- Logical decoding stream throughput
#   bench/workloads/04_payload_impact.sh    -- Key-only stream vs 1KB/10KB row bodies
# Report module:
#   bench/reports/summary_report.sh         -- Metrics & Supabase Realtime comparison
#
# Usage:
#   bench/bench.sh [--rows N] [--replay-rows K] [--discrete-txns D] [--payload-rows P]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR_MODULES="$ROOT/bench"

source "$ROOT/scripts/pg_throwaway.sh"
source "$BENCH_DIR_MODULES/common.sh"

ROWS=50000
REPLAY_ROWS=10000
DISCRETE_TXNS=500
PAYLOAD_ROWS=2000

while [ $# -gt 0 ]; do
  case "$1" in
    --rows) ROWS="${2:?}"; shift 2 ;;
    --replay-rows) REPLAY_ROWS="${2:?}"; shift 2 ;;
    --discrete-txns) DISCRETE_TXNS="${2:?}"; shift 2 ;;
    --payload-rows) PAYLOAD_ROWS="${2:?}"; shift 2 ;;
    -h|--help)
      echo "usage: bench/bench.sh [--rows N] [--replay-rows K] [--discrete-txns D] [--payload-rows P]"
      echo "Modular benchmark suite comparing pg_replayfeed 2.0 with Supabase Realtime baselines."
      exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 1 ;;
  esac
done

pg_throwaway_require_tools
BENCH_DIR=/tmp/pg_replayfeed_bench
BENCH_PORT=55418
pg_throwaway_start "$BENCH_DIR" "$BENCH_PORT"
trap pg_throwaway_stop EXIT

export PGDATABASE=benchdb
psql -q -X -d postgres -c "CREATE DATABASE benchdb;"

q "CREATE EXTENSION pg_replayfeed;" >/dev/null
q "CREATE TABLE bench_docs(id bigint PRIMARY KEY, tenant text NOT NULL, payload text);" >/dev/null
q "SELECT replayfeed.create_feed('bench', 'bench_docs', 'id', 'tenant');" >/dev/null

echo "Running pg_replayfeed 2.0 modular benchmark suite against Supabase Realtime baselines..."

# Execute modular workloads
source "$BENCH_DIR_MODULES/workloads/01_write_throughput.sh"
source "$BENCH_DIR_MODULES/workloads/02_discrete_commits.sh"
source "$BENCH_DIR_MODULES/workloads/03_replay_streaming.sh"
source "$BENCH_DIR_MODULES/workloads/04_payload_impact.sh"

# Format and render report
source "$BENCH_DIR_MODULES/reports/summary_report.sh"
