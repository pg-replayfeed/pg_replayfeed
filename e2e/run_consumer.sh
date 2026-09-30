#!/bin/bash
# e2e/run_consumer.sh -- interactive demo consumer: live leaderboard redrawn
# from the source table whenever the feed reports a change. The cursor is
# saved to $DEVDIR/consumer.cursor after each batch, so restarting replays
# everything missed while it was down, then continues live.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "$ROOT/e2e/dev_feed.sh" up
source "$(bash "$ROOT/e2e/dev_feed.sh" env-file)"

cd "$ROOT/e2e"
[ -f go.sum ] || go get github.com/jackc/pgx/v5@latest
go build -o "$DEVDIR/bin-consumer" ./cmd/consumer

CONSUMER_ID="${CONSUMER_ID:-demo}"
if [ "$CONSUMER_ID" = "demo" ]; then
  CONSUMER_SLOT="${CONSUMER_SLOT:-demo_consumer}"
  CONSUMER_CURSOR="${CONSUMER_CURSOR:-$DEVDIR/consumer.cursor}"
else
  CONSUMER_SLOT="${CONSUMER_SLOT:-demo_consumer_${CONSUMER_ID}}"
  CONSUMER_CURSOR="${CONSUMER_CURSOR:-$DEVDIR/consumer_${CONSUMER_ID}.cursor}"
fi

active_pid="$(psql -d "$DEVDB" -q -t -A -c "SELECT active_pid FROM pg_replication_slots WHERE slot_name='$CONSUMER_SLOT';" 2>/dev/null || true)"
if [ -n "$active_pid" ] && [ "$active_pid" != "" ]; then
  echo "error: replication slot '$CONSUMER_SLOT' is already active (PID $active_pid)." >&2
  echo "hint: a consumer is already running in another terminal. Stop it with Ctrl-C," >&2
  echo "      or run a concurrent consumer with: CONSUMER_ID=demo2 make consumer" >&2
  exit 1
fi

echo "consumer: live leaderboard of feed $DEVFEED; cursor file $CONSUMER_CURSOR."
echo "Ctrl-C to stop; restart to resume where it left off."
exec "$DEVDIR/bin-consumer" \
  --sock "$PGHOST" --port "$PGPORT" --user "$PGUSER" --db "$DEVDB" \
  --feed "$DEVFEED" --id "$CONSUMER_ID" --slot "$CONSUMER_SLOT" \
  --cursor-file "$CONSUMER_CURSOR" --leaderboard --table "$DEVTABLE"
