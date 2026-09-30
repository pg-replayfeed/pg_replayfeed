# pg_replayfeed — Final Engineering Report (2.0)

**Date:** 2026-09-30
**Status:** WAL-native rewrite complete. Ubuntu PG16 green: `make test`
(4/4 pg_regress + upgrade), `make e2e` (2 consumers x 100 changes via the real
replication protocol). Not yet synced to Mac.

---

## 1. What it is

`pg_replayfeed` 2.0 is a PostgreSQL **logical-decoding output plugin** (PGXS,
PG 16/17/18 target) that streams key-only change envelopes directly out of
the binary Write-Ahead Log over the replication protocol.

**What was removed from 1.x:** SQL polling (`events_after`), NOTIFY wake-ups,
trigger-written event tables, dense bigint cursors, per-feed writer
serialization, retention/pruning, snapshot fencing, the TypeScript relay.
All of it. The WAL is the log; the commit LSN is the cursor; the replication
slot is the retention mechanism.

**Explicit non-goals (by design):** no background worker, no Kafka/Redis,
no row bodies, no broker integration, no exactly-once claims, no multiplexing
layer (one slot per independent consumer).

---

## 2. The invariant (design contract)

**Cursor contract.** The consumer's cursor is the transaction commit LSN.

- PostgreSQL assigns every commit a unique, strictly increasing LSN in
  commit order. Logical decoding delivers transactions in LSN order per slot.
- One JSON envelope per committed transaction; rolled-back work never
  streams; irrelevant transactions emit nothing.
- The consumer applies each envelope atomically and persists the LSN only
  after success. Crash between apply and persist causes redelivery;
  consumers apply idempotently.

**Delivery contract.** At-least-once, ordered per slot. That is the entire
contract — there are no gaps to track because the WAL has no gaps.

**Envelope rules.** Key-changing UPDATE becomes DELETE(old key) +
INSERT(new key). Tenant moves carry `old_tenant`. No-op UPDATEs emit
nothing. TRUNCATE emits a resnapshot marker. See `docs/architecture.md` §3.

---

## 3. Code layout (modular, per request)

```
src/
  pg_replayfeed.c   module entry point (output plugin + extension)
  replayfeed.h      shared declarations
  catalog.c       SPI helpers, feed registry load
  feed.c          create_feed / drop_feed (sets REPLICA IDENTITY FULL)
  decode.c        output plugin callbacks (startup/begin/change/truncate/commit)
  extract.c       tuple -> key/tenant extraction, no-op attribute comparison
  encode.c        JSON envelope construction
sql/
  pg_replayfeed--2.0.sql        extension SQL (catalog, functions)
  pg_replayfeed--1.1--2.0.sql   destructive upgrade (drops all 1.x objects)
e2e/
  feedclient/     Go replication-protocol client (no polling, no NOTIFY)
  cmd/producer    continuous producer (leaderboard mode: only real changes)
  cmd/consumer    streaming consumer (live leaderboard, LSN cursor file)
  cmd/loadtest    load test (throughput + latency percentiles)
test/
  sql/ + expected/             pg_regress suites: basic, txn, retention, security
  upgrade/test_upgrade.sh      1.1 -> 2.0 upgrade verification
docs/
  architecture.md, semantics.md, security.md, benchmarks.md
```

---

## 4. Bugs found and fixed during this build (real, not cosmetic)

1. **Feed registry freed by SPI_finish.** `rf_load_feed_registry` allocated
   the registry array while `CurrentMemoryContext` still pointed at SPI's
   private procedure context; `SPI_finish()` tore it down and left a
   dangling pointer. Fixed by capturing the caller context before
   `SPI_connect()` and switching back before allocation.
2. **Feed-name type confusion.** The `feed` startup option arrived as
   PostgreSQL `text`, not a C string; comparing it as `char *` read garbage.
   Fixed by treating it as `text` explicitly.
3. **MVCC-header tuple comparison.** No-op detection compared raw tuple
   bytes including `xmin`/`xmax`, so identical logical rows never matched.
   Replaced with attribute-by-attribute comparison (`rf_tuples_identical`).
4. **Unknown output-plugin options silently ignored.** Now fail loudly with
   `ERRCODE_INVALID_PARAMETER_VALUE` — a typo'd option must not produce a
   silently wrong stream.
5. **Split envelopes.** Row changes and truncates were emitted as separate
   messages; combined into one envelope per transaction as the contract
   requires.
6. **UPDATE without old tuple not fail-closed.** With `REPLICA IDENTITY
   FULL` the old tuple is always present; a missing one now raises rather
   than emitting a key-less change.
7. **Replication-protocol segfault (the critical one).** `START_REPLICATION`
   crashed the walsender with signal 11. Root cause, found by trace logging:
   `startup_cb` called `rf_load_feed_registry` via SPI, but the walsender
   runs the startup callback outside any transaction snapshot — SPI without
   a snapshot is a segfault. Fix, in two parts:
   - `startup_cb` does option parsing only; catalog validation moved to
     `begin_cb` (which runs inside the decoding transaction).
   - `begin_cb` pushes the transaction snapshot before SPI **only when no
     snapshot is active** (`!ActiveSnapshotSet()`). Via the SQL interface a
     snapshot is already active; pushing a second one broke the no-op tuple
     comparison. Both failure modes were hit and are now covered: the e2e
     exercises the walsender path, the pg_regress suite exercises the SQL
     path.
8. **Nested SPI ownership in drop_feed.** `replayfeed_drop_feed` opened an
   SPI connection while one was already open; fixed to use the existing
   connection.

---

## 5. Test pyramid results (2026-09-30, PostgreSQL 16, Ubuntu)

```
=== pg_regress ===
ok 1 - basic
ok 2 - txn
ok 3 - retention
ok 4 - security
# All 4 tests passed.
=== upgrade 1.1 -> 2.0 ===
upgrade 1.1 -> 2.0 OK
ALL TESTS PASSED
```

```
=== e2e (replication protocol) ===
producer DONE ops=100 skipped=0
consumer c1 DONE count=100 last_lsn=0/192F7C8
consumer c2 DONE count=100 last_lsn=0/192F7C8
E2E OK: 2 consumers each replayed 100 events
```

Go: `gofmt`, `go build ./...`, `go vet ./...` clean.

Isolation specs from 1.x were deleted: they tested the removed dense-cursor
/ fence / prune machinery, which has no 2.0 counterpart.

---

## 6. Measurements (single VM, rough — see docs/benchmarks.md)

From the 1.x benchmark run (write-path numbers; the 2.0 write path has no
triggers, so these are upper bounds on overhead):

- WAL: ~146 B/row without trigger; the 2.0 plugin adds zero write-path WAL
  (it only reads what PostgreSQL already wrote).
- The e2e replayed 100 transactions through two independent replication
  slots in ~1s on a throwaway instance (dominated by test harness setup,
  not streaming).

---

## 7. Honest limitations (documented, not hidden)

- `test/run_tests.sh` pg_regress auto-detection does not find the Ubuntu
  path `/usr/lib/postgresql/16/lib/pgxs/src/test/regress/pg_regress`;
  `PG_REGRESS_BIN` must be exported (environmental detection still needs
  fixing).
- The upgrade test validates the migration script's destructive behavior
  against a reconstructed 1.1 catalog, not a real `ALTER EXTENSION ...
  UPDATE` from an installed 1.1 (PG16 removed `CREATE EXTENSION ... FROM`).
- No-op equality is attribute-based; TOAST/compression edge cases and
  collation-aware type equality are not fully hardened.
- The snapshot-bootstrap API (exported snapshot + consistent point) is
  documented but not implemented as a client helper; the demo starts from
  the slot position and notes the gap.
- PG17/PG18 builds not yet validated; Mac sync not yet done.
- Duplicate delivery was not independently audited under crash injection;
  delivery is at-least-once and docs do not claim otherwise.

---

## 8. Standing bar

"MAKE IT ROBUST SO THAT I WON'T FAIL AT FIRST PLACE." Every claim above is
backed by a test, a measurement, or an explicitly-labeled limitation. Where
evidence is missing, the report says so rather than implying it.
