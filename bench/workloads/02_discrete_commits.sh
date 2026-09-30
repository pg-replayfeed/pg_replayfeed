#!/bin/bash
# bench/workloads/02_discrete_commits.sh -- measures 1-op-per-transaction throughput & streaming.
set -euo pipefail

# Create stored procedure that executes discrete commits
q "CREATE OR REPLACE PROCEDURE run_discrete_bench(n int) LANGUAGE plpgsql AS \$\$
BEGIN
  FOR i IN 1..n LOOP
    INSERT INTO bench_docs VALUES (3 * $ROWS + i, 't1', 'discrete');
    COMMIT;
  END LOOP;
END;
\$\$;" >/dev/null

q "SELECT pg_create_logical_replication_slot('bench_discrete_slot', 'pg_replayfeed');" >/dev/null

# Time discrete transaction commits
t0=$(ms_now)
q "CALL run_discrete_bench($DISCRETE_TXNS);" >/dev/null
t1=$(ms_now)
discrete_write_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
discrete_end_lsn=$(q "SELECT pg_current_wal_lsn();")

# Time discrete envelope replay via logical decoding
discrete_out="$BENCH_DIR/discrete.out"
t0=$(ms_now)
_run_with_timeout 60 pg_recvlogical -d benchdb --slot bench_discrete_slot --start \
  -P pg_replayfeed -o feed='bench' -n -E "$discrete_end_lsn" -f "$discrete_out" 2>/dev/null
t1=$(ms_now)
discrete_replay_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
discrete_envelopes=$(wc -l < "$discrete_out" | awk '{print $1}')
