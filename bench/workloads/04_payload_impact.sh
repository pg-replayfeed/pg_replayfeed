#!/bin/bash
# bench/workloads/04_payload_impact.sh -- measures stream performance across compact, 1KB, and 10KB row bodies.
set -euo pipefail

q "CREATE TABLE IF NOT EXISTS bench_payload(id bigint PRIMARY KEY, tenant text NOT NULL, body text);" >/dev/null
q "SELECT replayfeed.create_feed('bench_pl', 'bench_payload', 'id', 'tenant');" >/dev/null

# 1. Compact payload (~50B row)
q "SELECT pg_create_logical_replication_slot('slot_pl_compact', 'pg_replayfeed');" >/dev/null
q "INSERT INTO bench_payload SELECT g, 't1', 'small' FROM generate_series(1, $PAYLOAD_ROWS) g;" >/dev/null
lsn_pl_compact=$(q "SELECT pg_current_wal_lsn();")
pl_compact_out="$BENCH_DIR/pl_compact.out"
t0=$(ms_now)
_run_with_timeout 60 pg_recvlogical -d benchdb --slot slot_pl_compact --start \
  -P pg_replayfeed -o feed='bench_pl' -n -E "$lsn_pl_compact" -f "$pl_compact_out" 2>/dev/null
t1=$(ms_now)
pl_compact_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
pl_compact_bytes=$(wc -c < "$pl_compact_out" | awk '{print $1}')

# 2. 1KB row payload
q "SELECT pg_create_logical_replication_slot('slot_pl_1k', 'pg_replayfeed');" >/dev/null
q "INSERT INTO bench_payload SELECT $PAYLOAD_ROWS + g, 't1', repeat('x', 1024) FROM generate_series(1, $PAYLOAD_ROWS) g;" >/dev/null
lsn_pl_1k=$(q "SELECT pg_current_wal_lsn();")
pl_1k_out="$BENCH_DIR/pl_1k.out"
t0=$(ms_now)
_run_with_timeout 60 pg_recvlogical -d benchdb --slot slot_pl_1k --start \
  -P pg_replayfeed -o feed='bench_pl' -n -E "$lsn_pl_1k" -f "$pl_1k_out" 2>/dev/null
t1=$(ms_now)
pl_1k_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
pl_1k_bytes=$(wc -c < "$pl_1k_out" | awk '{print $1}')

# 3. 10KB row payload
q "SELECT pg_create_logical_replication_slot('slot_pl_10k', 'pg_replayfeed');" >/dev/null
q "INSERT INTO bench_payload SELECT 2 * $PAYLOAD_ROWS + g, 't1', repeat('y', 10240) FROM generate_series(1, $PAYLOAD_ROWS) g;" >/dev/null
lsn_pl_10k=$(q "SELECT pg_current_wal_lsn();")
pl_10k_out="$BENCH_DIR/pl_10k.out"
t0=$(ms_now)
_run_with_timeout 60 pg_recvlogical -d benchdb --slot slot_pl_10k --start \
  -P pg_replayfeed -o feed='bench_pl' -n -E "$lsn_pl_10k" -f "$pl_10k_out" 2>/dev/null
t1=$(ms_now)
pl_10k_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
pl_10k_bytes=$(wc -c < "$pl_10k_out" | awk '{print $1}')

# 4. 50KB row payload (matches Supabase Realtime 50KB benchmark)
q "SELECT pg_create_logical_replication_slot('slot_pl_50k', 'pg_replayfeed');" >/dev/null
q "INSERT INTO bench_payload SELECT 3 * $PAYLOAD_ROWS + g, 't1', repeat('z', 51200) FROM generate_series(1, $PAYLOAD_ROWS) g;" >/dev/null
lsn_pl_50k=$(q "SELECT pg_current_wal_lsn();")
pl_50k_out="$BENCH_DIR/pl_50k.out"
t0=$(ms_now)
_run_with_timeout 60 pg_recvlogical -d benchdb --slot slot_pl_50k --start \
  -P pg_replayfeed -o feed='bench_pl' -n -E "$lsn_pl_50k" -f "$pl_50k_out" 2>/dev/null
t1=$(ms_now)
pl_50k_ms=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
pl_50k_bytes=$(wc -c < "$pl_50k_out" | awk '{print $1}')
