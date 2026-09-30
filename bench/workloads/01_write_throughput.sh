#!/bin/bash
# bench/workloads/01_write_throughput.sh -- measures multi-row write throughput & WAL generation.
set -euo pipefail

# Warmup: settle caches, relation init forks, autovacuum
q "INSERT INTO bench_docs SELECT g, 't1', 'warmup' FROM generate_series(1, 1000) g;" >/dev/null
q "TRUNCATE bench_docs;" >/dev/null

# 1. Timed multi-row INSERT
t0=$(ms_now)
q "INSERT INTO bench_docs SELECT g, 't1', 'payload-data' FROM generate_series(1, $ROWS) g;" >/dev/null
t1=$(ms_now)
write_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")

# 2. WAL bytes per row (measured over a clean checkpointed batch)
q "CHECKPOINT;" >/dev/null
w0=$(q "SELECT pg_current_wal_lsn();")
q "INSERT INTO bench_docs SELECT $ROWS + g, 't1', 'payload-data' FROM generate_series(1, $ROWS) g;" >/dev/null
w1=$(q "SELECT pg_current_wal_lsn();")
wal_bytes=$(q "SELECT pg_wal_lsn_diff('$w1', '$w0');")
