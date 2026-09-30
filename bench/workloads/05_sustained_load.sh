#!/bin/bash
# bench/workloads/05_sustained_load.sh -- dedicated sustained concurrent writer + streaming consumer workload.
set -euo pipefail

LOAD_TABLE="${LOAD_TABLE:-bench_load_orders}"
LOAD_FEED="${LOAD_FEED:-bench_load}"
LOAD_TENANT="${LOAD_TENANT:-acme}"

q "CREATE TABLE IF NOT EXISTS $LOAD_TABLE(id bigint PRIMARY KEY, tenant text NOT NULL, total bigint);" >/dev/null
if ! q "SELECT 1 FROM replayfeed.feed WHERE name='$LOAD_FEED';" | grep -q 1; then
  q "SELECT replayfeed.create_feed('$LOAD_FEED', '$LOAD_TABLE', 'id', 'tenant');" >/dev/null
fi

# Build loadtest binary
cd "$ROOT/e2e"
[ -f go.sum ] || go get github.com/jackc/pgx/v5@latest
go build -o "$BENCH_DIR/bin-loadtest" ./cmd/loadtest

LOAD_METRICS_JSON="$BENCH_DIR/loadtest_metrics.json"

q "CHECKPOINT;" >/dev/null
wal_start_lsn=$(q "SELECT pg_current_wal_lsn();")

echo "Starting sustained soak: $LOAD_WRITERS writers, $LOAD_CONSUMERS consumers for $LOAD_DURATION..."
"$BENCH_DIR/bin-loadtest" \
  --sock "$PGHOST" --port "$PGPORT" --user "$PGUSER" --db "$PGDATABASE" \
  --table "$LOAD_TABLE" --feed "$LOAD_FEED" --tenant "$LOAD_TENANT" \
  --writers "$LOAD_WRITERS" --consumers "$LOAD_CONSUMERS" \
  --duration "$LOAD_DURATION" --drain "${LOAD_DRAIN:-5s}" \
  --json-out "$LOAD_METRICS_JSON"

wal_end_lsn=$(q "SELECT pg_current_wal_lsn();")
sustained_wal_bytes=$(q "SELECT pg_wal_lsn_diff('$wal_end_lsn', '$wal_start_lsn');")
