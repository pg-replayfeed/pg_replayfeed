#!/bin/bash
# e2e/run_e2e.sh -- continuous Go producer + N streaming Go consumers.
#
# Starts a throwaway PostgreSQL instance, creates the extension + feed, then
# runs one producer pushing INSERTs/UPDATEs continuously while N consumer
# processes each stream the WAL via the replication protocol on their own
# slot. Consumers log throughput every 5s.
#
# Ctrl-C stops the producer first, drains the consumers, then verifies every
# consumer saw every produced change (count == produced).
#
# Finite deterministic mode: E2E_OPS=<n> runs the producer for exactly n ops
# and each consumer exits once it has replayed all n (a CI-style gate).
#
# Logs live in $LOGDIR (/tmp/pg_replayfeed_e2e_logs), OUTSIDE the throwaway
# work dir, so they survive pg_throwaway_stop and can be inspected after a
# failure.
#
# Usage: E2E_CONSUMERS=5 E2E_RATE=2000 bash e2e/run_e2e.sh   (or `make e2e`)
set -euo pipefail

command -v go >/dev/null 2>&1 || {
  echo "error: go not found (macOS: brew install go)" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/pg_throwaway.sh"
pg_throwaway_require_tools

CONSUMERS="${E2E_CONSUMERS:-5}"
OPS="${E2E_OPS:-0}"      # 0 = continuous until Ctrl-C
RATE="${E2E_RATE:-2000}" # target producer ops/sec (continuous mode)
WORK=/tmp/pg_replayfeed_e2e
LOGDIR=/tmp/pg_replayfeed_e2e_logs
FEED=e2e
TABLE=e2e_orders
TENANT=acme
DB=e2edb

# Extension must be installed for CREATE EXTENSION.
make -C "$ROOT" -s >/dev/null
make -C "$ROOT" install >/dev/null 2>&1 || sudo make -C "$ROOT" install >/dev/null

pg_throwaway_start "$WORK" 55419
trap pg_throwaway_stop EXIT
rm -rf "$LOGDIR" "$WORK"/produced.count "$WORK"/bin-producer "$WORK"/bin-consumer
mkdir -p "$LOGDIR"

psql -d postgres -q -c "CREATE DATABASE $DB;"
psql -d "$DB" -q -c "CREATE EXTENSION pg_replayfeed;"
psql -d "$DB" -q <<SQL
CREATE TABLE $TABLE(id bigint NOT NULL, tenant text NOT NULL, total integer, PRIMARY KEY(id));
SELECT replayfeed.create_feed('$FEED', '$TABLE', 'id', 'tenant');
SQL

cd "$ROOT/e2e"
[ -f go.sum ] || go get github.com/jackc/pgx/v5@latest
go build -o "$WORK/bin-producer" ./cmd/producer
go build -o "$WORK/bin-consumer" ./cmd/consumer

DB_ARGS=(--sock "$PGHOST" --port "$PGPORT" --user "$PGUSER" --db "$DB")

# Print the tail of every log; called on failure so the cause is visible
# even though the throwaway instance (but not $LOGDIR) is torn down.
dump_logs() {
  echo "--- producer.log (tail) ---"
  tail -20 "$LOGDIR/producer.log" 2>/dev/null || echo "(no producer.log)"
  for i in $(seq 1 "$CONSUMERS"); do
    echo "--- consumer-$i.log (tail) ---"
    tail -5 "$LOGDIR/consumer-$i.log" 2>/dev/null || echo "(no consumer-$i.log)"
  done
}

if [ "$OPS" -gt 0 ]; then
  # ---------------- finite deterministic mode ----------------
  echo "starting $CONSUMERS consumers (expect $OPS changes each)..."
  for i in $(seq 1 "$CONSUMERS"); do
    "$WORK/bin-consumer" "${DB_ARGS[@]}" \
      --feed "$FEED" --id "c$i" --slot "e2e_c$i" --count-file "$WORK/produced.count" \
      >"$LOGDIR/consumer-$i.log" 2>&1 &
    echo $! >"$LOGDIR/consumer-$i.pid"
  done
  sleep 1 # let consumers start replication before the first write

  echo "starting producer ($OPS ops)..."
  "$WORK/bin-producer" "${DB_ARGS[@]}" \
    --table "$TABLE" --tenant "$TENANT" --ops "$OPS" --count-file "$WORK/produced.count" \
    2>&1 | tee "$LOGDIR/producer.log"

  echo "waiting for consumers..."
  FAIL=0
  for i in $(seq 1 "$CONSUMERS"); do
    pid="$(cat "$LOGDIR/consumer-$i.pid")"
    if wait "$pid"; then
      grep -h "DONE" "$LOGDIR/consumer-$i.log" | tail -1 || true
    else
      echo "consumer c$i FAILED:"
      tail -5 "$LOGDIR/consumer-$i.log"
      FAIL=1
    fi
  done
  if [ "$FAIL" -eq 0 ]; then
    echo "E2E OK: $CONSUMERS consumers each replayed $OPS events"
  else
    echo "E2E FAILED"
    dump_logs
    exit 1
  fi
  exit 0
fi

# ---------------- continuous mode ----------------
echo "starting $CONSUMERS consumers (continuous)..."
for i in $(seq 1 "$CONSUMERS"); do
  "$WORK/bin-consumer" "${DB_ARGS[@]}" \
    --feed "$FEED" --id "c$i" --slot "e2e_c$i" --stats-interval 5s \
    >"$LOGDIR/consumer-$i.log" 2>&1 &
  echo $! >"$LOGDIR/consumer-$i.pid"
done
sleep 1 # let consumers LISTEN before the first write

"$WORK/bin-producer" "${DB_ARGS[@]}" \
  --table "$TABLE" --tenant "$TENANT" --ops 0 --rate "$RATE" \
  >"$LOGDIR/producer.log" 2>&1 &
PRODUCER_PID=$!
echo "producer running (pid $PRODUCER_PID, ~$RATE ops/s). Ctrl-C to stop and verify."
echo "consumer stats: tail -f $LOGDIR/consumer-1.log"

shutdown() {
  [ "${SHUTDOWN_DONE:-0}" -eq 1 ] && return 0
  SHUTDOWN_DONE=1
  trap - INT TERM
  echo
  echo "stopping producer..."
  kill -TERM "$PRODUCER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  # NOTE: the || true inside the substitution matters: grep exits 1 on no
  # match, and under `set -e` that would otherwise kill the script silently.
  done_line="$(grep 'producer DONE' "$LOGDIR/producer.log" 2>/dev/null | tail -1 || true)"
  if [[ "$done_line" =~ ops=([0-9]+) ]]; then
    PRODUCED="${BASH_REMATCH[1]}"
  else
    echo "could not read producer total (log follows):"
    dump_logs
    exit 1
  fi
  echo "producer total: $PRODUCED"

  echo "draining consumers (up to 15s)..."
  kill -TERM $(cat "$LOGDIR"/consumer-*.pid) 2>/dev/null || true
  for _ in $(seq 1 15); do
    sleep 1
    n="$(grep -l 'FINAL' "$LOGDIR"/consumer-*.log 2>/dev/null | wc -l || true)"
    [ "$n" -eq "$CONSUMERS" ] && break
  done
  for f in "$LOGDIR"/consumer-*.pid; do
    p="$(cat "$f")"
    kill -0 "$p" 2>/dev/null && kill -KILL "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true

  echo "--- results (produced: $PRODUCED) ---"
  FAIL=0
  for i in $(seq 1 "$CONSUMERS"); do
    line="$(grep 'FINAL' "$LOGDIR/consumer-$i.log" 2>/dev/null | tail -1 || true)"
    echo "c$i: $line"
    if [[ "$line" =~ count=([0-9]+)\ last_lsn= ]]; then
      [ "${BASH_REMATCH[1]}" = "$PRODUCED" ] || FAIL=1
    else
      FAIL=1
    fi
  done
  if [ "$FAIL" -eq 0 ]; then
    echo "E2E OK: all $CONSUMERS consumers replayed $PRODUCED changes"
    exit 0
  else
    echo "E2E FAILED"
    dump_logs
    exit 1
  fi
}

trap shutdown INT TERM
wait "$PRODUCER_PID" || true
shutdown
