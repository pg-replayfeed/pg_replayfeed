#!/bin/bash
# e2e/run_loadtest.sh -- load test: N writers hammer INSERTs while M consumers
# replay over the replication protocol. Reports throughput, latency
# percentiles (incl. tail), consumer end-to-end lag, and connection setup
# latency.
#
#   LOAD_WRITERS=16 LOAD_CONSUMERS=8 LOAD_DURATION=60s make loadtest
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "$ROOT/e2e/dev_feed.sh" up
source "$(bash "$ROOT/e2e/dev_feed.sh" env-file)"

LOAD_TABLE="${LOAD_TABLE:-load_orders}"
LOAD_FEED="${LOAD_FEED:-load}"

export PGHOST PGPORT PGUSER
if ! psql -d "$DEVDB" -q -t -A -c "SELECT 1 FROM pg_database WHERE datname='$DEVDB';" >/dev/null; then
  echo "run_loadtest: dev database missing" >&2
  exit 1
fi
psql -d "$DEVDB" -q -c "CREATE TABLE IF NOT EXISTS $LOAD_TABLE(id bigint NOT NULL, tenant text NOT NULL, total integer, PRIMARY KEY(id));"
if ! psql -d "$DEVDB" -q -t -A -c "SELECT 1 FROM replayfeed.feed WHERE name='$LOAD_FEED';" | grep -q 1; then
  psql -d "$DEVDB" -q -c "SELECT replayfeed.create_feed('$LOAD_FEED', '$LOAD_TABLE', 'id', 'tenant');"
fi

cd "$ROOT/e2e"
[ -f go.sum ] || go get github.com/jackc/pgx/v5@latest
go build -o "$DEVDIR/bin-loadtest" ./cmd/loadtest

echo "loadtest: ${LOAD_WRITERS:-8} writers, ${LOAD_CONSUMERS:-4} consumers, ${LOAD_DURATION:-30s} (table $LOAD_TABLE, feed $LOAD_FEED)"
exec "$DEVDIR/bin-loadtest" \
  --sock "$PGHOST" --port "$PGPORT" --user "$PGUSER" --db "$DEVDB" \
  --table "$LOAD_TABLE" --feed "$LOAD_FEED" --tenant "$DEVTENANT" \
  --writers "${LOAD_WRITERS:-8}" --consumers "${LOAD_CONSUMERS:-4}" \
  --duration "${LOAD_DURATION:-30s}"
