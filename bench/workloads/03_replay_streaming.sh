#!/bin/bash
# bench/workloads/03_replay_streaming.sh -- measures logical decoding streaming throughput.
set -euo pipefail

# Provision slot before writes to capture WAL changes
q "SELECT pg_create_logical_replication_slot('bench_slot', 'pg_replayfeed');" >/dev/null
q "INSERT INTO bench_docs SELECT 2 * $ROWS + g, 't1', 'payload-data' FROM generate_series(1, $REPLAY_ROWS) g;" >/dev/null
end_lsn=$(q "SELECT pg_current_wal_lsn();")

replay_out="$BENCH_DIR/replay.out"
t0=$(ms_now)
# Stream changes up to end_lsn and exit cleanly via -E
_run_with_timeout 60 pg_recvlogical -d benchdb --slot bench_slot --start \
  -P pg_replayfeed -o feed='bench' -n -E "$end_lsn" -f "$replay_out" 2>/dev/null
t1=$(ms_now)
replay_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
n_bytes=$(wc -c < "$replay_out" | awk '{print $1}')
