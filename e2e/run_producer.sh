#!/bin/bash
# e2e/run_producer.sh -- interactive demo producer: leaderboard ticks (1/sec)
# against the persistent dev feed (see e2e/dev_feed.sh). Only real score
# changes are written. Ctrl-C to stop; restart any time.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "$ROOT/e2e/dev_feed.sh" up
source "$(bash "$ROOT/e2e/dev_feed.sh" env-file)"

cd "$ROOT/e2e"
[ -f go.sum ] || go get github.com/jackc/pgx/v5@latest
go build -o "$DEVDIR/bin-producer" ./cmd/producer

echo "producer: leaderboard, 1 tick/sec, 10 players (feed $DEVFEED, tenant $DEVTENANT). Only real score changes are written. Ctrl-C to stop."
exec "$DEVDIR/bin-producer" \
  --sock "$PGHOST" --port "$PGPORT" --user "$PGUSER" --db "$DEVDB" \
  --table "$DEVTABLE" --tenant "$DEVTENANT" --ops 0 --rate 1 --players 10
