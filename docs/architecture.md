# pg_replayfeed — Architecture & Design (2.0)

**Status:** design document for the WAL-native rewrite. This file is the
source of truth for the streaming correctness argument. `docs/semantics.md`
restates the guarantees in user-facing language.

## 1. Problem

PostgreSQL `LISTEN`/`NOTIFY` wakes a connected client but keeps no durable
history: a disconnected client cannot learn what it missed. Polling a table
for "what changed" is worse — it burns I/O on empty checks, delivers changes
late, and can never be exactly-once without application-level bookkeeping.

`pg_replayfeed` 2.0 streams changes **directly out of the binary Write-Ahead
Log** via PostgreSQL's logical decoding. There is no polling, no NOTIFY, no
trigger-written event table, no dense bigint cursor, no pruning. The WAL is
the log; the output plugin is the decoder; the replication protocol is the
transport.

```
[ Application Write ]
        │
        ▼ (SQL INSERT / UPDATE / DELETE / TRUNCATE)
┌──────────────────────────────────────────────┐
│ PostgreSQL Storage Engine                    │
│   • Writes standard heap tuple               │
│   • Appends binary record to Write-Ahead Log │
└──────────────────────┬───────────────────────┘
                       │ Binary WAL Record
                       ▼
┌──────────────────────────────────────────────┐
│ PostgreSQL Logical Decoding (ReorderBuffer)  │
│   • Reassembles transactions in commit order │
│   • Filters rolled-back transactions         │
└──────────────────────┬───────────────────────┘
                       │ Decoded Changes
                       ▼
┌──────────────────────────────────────────────┐
│ pg_replayfeed (C Output Plugin)              │
│   • Filters against feed registry            │
│   • Extracts primary keys & tenant keys      │
│   • Resolves key mutations & tenant moves    │
│   • Encodes compact, transaction-atomic JSON │
└──────────────────────┬───────────────────────┘
                       │ Replication Protocol (START_REPLICATION)
                       ▼
┌──────────────────────────────────────────────┐
│ Go Consumer (feedclient / cmd/consumer)      │
│   • Receives envelope with commit LSN        │
│   • Applies change atomically & idempotently │
│   • Acknowledges flush LSN back to Postgres  │
└──────────────────────────────────────────────┘
```

The consumer's cursor **is the transaction commit LSN**. No synthetic cursor
is allocated, so there is no allocation-order trap, no gap tracking, and no
per-feed writer serialization. PostgreSQL already solved commit ordering —
the LSN is it.

## 2. Core objects

- `replayfeed.feed` — one row per registered feed: immutable config
  (`name`, `source_table`, `key_column`, `tenant_column`). Config
  immutability is enforced by exposing no UPDATE path (only `create_feed` /
  `drop_feed`); changing key/tenant/source requires drop + recreate.
- **Replication slot** — one per independent consumer, created with the
  `pg_replayfeed` output plugin. The slot pins the WAL the consumer has not
  yet acknowledged. There is no retention interval and no pruning: the slot
  *is* the retention mechanism.
- **Output plugin** (`src/decode.c`) — registered via
  `_PG_output_plugin_init`, loaded by the walsender when a replication slot
  names it. Callbacks: `startup`, `begin`, `change`, `truncate`, `commit`,
  `shutdown`.

Registration (`replayfeed.create_feed`) sets `REPLICA IDENTITY FULL` on the
source table so UPDATE and DELETE always carry the old row. This is what
makes key-change detection and tenant-move detection possible without
triggers.

## 3. Wire format

One JSON envelope per relevant committed transaction, delivered as the
payload of a replication-protocol `XLogData` message:

```json
{
  "cursor": "0/16B1978",
  "committed_at_ms": 1727690096789,
  "changes": [
    {"feed": "orders", "op": "I", "key": "42", "tenant": "acme"},
    {"feed": "orders", "op": "U", "key": "43", "tenant": "acme"},
    {"feed": "orders", "op": "D", "key": "44", "tenant": "acme",
     "old_tenant": "globex"}
  ],
  "truncate": ["orders"]
}
```

- `cursor` — the transaction's commit LSN, as text. This is the consumer's
  checkpoint. It is strictly increasing per slot.
- `committed_at_ms` — commit timestamp, Unix epoch milliseconds. Informational
  only; never used for ordering.
- `changes` — key-only row changes in commit order. `op` is `I`/`U`/`D`.
  `key` is the key column rendered as text. `tenant` is the tenant column
  (or null when the feed has no tenant column).
- `truncate` — feed names truncated in this transaction, first-seen order.
  A consumer treats this as a resnapshot marker for that feed.

Rules:

- **Key-changing UPDATE becomes DELETE(old key) + INSERT(new key).** The
  consumer sees two changes: `{"op": "D", "key": "<old>"}` followed by
  `{"op": "I", "key": "<new>"}`. No entity ever "jumps" identities silently.
- **Tenant moves carry `old_tenant`.** An UPDATE that changes the tenant
  column emits the change with both `tenant` (new) and `old_tenant` (old),
  so the consumer can remove the key from the old tenant's view.
- **No-op UPDATE emits nothing.** When the old and new tuples are
  attribute-identical, the change is skipped. (This is why the demo
  leaderboard only redraws on real score changes.)
- **TRUNCATE produces a resnapshot marker**, not row events. There is no way
  to enumerate the deleted keys from WAL, so the honest signal is "this
  feed's contents are now unknown — resnapshot it."
- **Irrelevant transactions emit nothing.** A transaction that touches no
  feed table produces no envelope, though the slot's confirmed flush LSN may
  still advance past it.
- **Rolled-back transactions emit nothing.** Logical decoding only sees
  committed transactions. No compensation logic is needed or present.

## 4. Cursor semantics (the critical part)

### 4.1 Why the LSN works

The 1.x design allocated a synthetic bigint cursor per event inside the
writer's transaction, serialized per feed through a catalog row lock. That
solved commit ordering at the cost of write-path contention and a whole
subsystem (allocation, gap recycling, pruning, resnapshot sentinels).

The LSN needs none of that:

- PostgreSQL assigns every commit a unique, strictly increasing LSN.
- Logical decoding delivers transactions in LSN order per slot.
- The consumer persists the LSN **after** applying the envelope. On restart
  it resumes from the persisted LSN. Redelivery of the last envelope is
  possible (crash between apply and persist); the consumer must apply
  idempotently or tolerate re-application.

**At-least-once, ordered per slot.** That is the entire delivery contract.
There are no gaps to track because the WAL has no gaps; there is no
allocation order to reconcile because the consumer never allocates.

### 4.2 What we do NOT do

- No synthetic cursor allocation, no per-feed writer serialization.
- No polling (`events_after` is gone).
- No NOTIFY wake-ups (the replication protocol pushes).
- No trigger-written event table (the WAL is the table).
- No pruning or retention intervals (the slot pins WAL; drop the slot to
  release it).
- No background worker.
- No commit-timestamp ordering.

### 4.3 One slot per consumer

Replication slots are **not** shared between independent consumers. Each
consumer creates its own slot; each slot tracks its own confirmed flush
LSN. If ten consumers need the same feed, there are ten slots. This is
how PostgreSQL logical replication works, and we do not add a multiplexing
layer.

Consequence: WAL retention is bounded by the **slowest** slot. A consumer
that stops acknowledging pins WAL indefinitely. Monitor slot lag
(`pg_replication_slots.confirmed_flush_lsn` vs. current WAL insert
position); alert on it; drop dead slots. See `docs/semantics.md` §6.

## 5. Snapshot bootstrap

A fresh consumer needs initial state **and** a stream position that
corresponds to it, otherwise it will miss writes or double-apply them.

The correct bootstrap:

```sql
-- 1. Create the slot; note the consistent point.
SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pg_replayfeed');
--   -> (slot_name, consistent_point) e.g. consistent_point = '0/16A0000'

-- 2. In a REPEATABLE READ transaction at that LSN, read the source table.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT '<exported snapshot>';  -- if using exported snapshots
SELECT ... FROM orders;                          -- full state read
COMMIT;

-- 3. START_REPLICATION from the consistent point; apply envelopes with
--    cursor > snapshot LSN; persist each LSN after applying.
```

In practice the Go client (`e2e/feedclient`) starts streaming from a slot's
confirmed flush LSN, and the demo notes that a production consumer should
implement the exported-snapshot variant above. The invariant is the same as
the old fence protocol, with the LSN playing the role of the fenced cursor:
**the snapshot and the stream position must describe the same instant.**

## 6. Walsender execution context (implementation note)

The output plugin runs in two contexts:

- **SQL interface** (`pg_logical_slot_get_changes`): a regular backend with
  an active transaction and snapshot. SPI works directly.
- **Walsender** (`START_REPLICATION`): the decoding callbacks run without a
  snapshot pushed for SPI.

Two consequences, both load-bearing:

1. **`startup_cb` must not use SPI.** It runs before any transaction
   snapshot exists in the walsender. Option parsing only; catalog validation
   is deferred to `begin_cb`.
2. **`begin_cb` pushes a snapshot when none is active.** Before loading the
   feed registry via SPI, it checks `ActiveSnapshotSet()` and pushes the
   transaction snapshot if needed. Via the SQL interface a snapshot is
   already active, so nothing is pushed and the no-op tuple comparison
   (which is snapshot-sensitive) behaves identically in both paths.

Getting this wrong segfaults the walsender at `START_REPLICATION` (SPI
without a snapshot) or breaks the SQL-path tests (double-pushed snapshot
confusing tuple visibility). Both failure modes were hit during
development; the current code is the minimal correct form.

## 7. Security model

- `create_feed`/`drop_feed` are `SECURITY DEFINER` but check the
  **invoker's** `TRIGGER` privilege explicitly (three-argument
  `has_table_privilege` with the outer user id, so `SET ROLE` is honored).
  `EXECUTE` alone grants nothing.
- The output plugin reads `replayfeed.feed` via SPI as the walsender's user.
  Slot creation requires replication privilege; in practice the replication
  user is a dedicated role.
- Consumers authenticate as PostgreSQL replication clients. Tenant filtering
  (the `feed '<name>'` startup option) is a **filter**, not authentication —
  documented, not hidden.
- Details and threat model: `docs/security.md`.

## 8. HA / durability honesty

Streaming correctness (LSN ordering, at-least-once) holds on any single
primary. Across failover, `pg_replayfeed` inherits the deployment's
durability: with asynchronous replication, recently committed transactions
can be lost on promotion and their envelopes never stream; with synchronous
replication they survive. Slots do not survive failover unless the standby
was promoted with `pg_replication_slot` synchronization (PG 17+ slot
sync). We document this separation and do not claim zero-loss failover.

## 9. What this is not

No Kafka/Redis, no broker integration, no row bodies, no exactly-once
claims, no infinite history (WAL retention is bounded by disk and slot lag),
no cross-slot ordering, no multiplexing layer.

## 10. Test strategy

- `pg_regress` (via the SQL slot interface): basic CRUD envelopes,
  key-change DELETE+INSERT, tenant moves with `old_tenant`, no-op UPDATE
  suppression, TRUNCATE markers, multi-feed transactions, validation
  rejections, privilege boundaries, search_path attacks, upgrade 1.1→2.0.
- `e2e` (Go, via the real replication protocol): continuous producer +
  N independent consumers on independent slots; each consumer must replay
  every change exactly (count and final LSN verified).
- `make bench`: write throughput, WAL bytes per row, replay speed.

## 11. File map

```
src/pg_replayfeed.c   -- module entry point (output plugin + extension)
src/replayfeed.h      -- shared declarations
src/catalog.c         -- SPI helpers, feed registry load
src/feed.c            -- create_feed / drop_feed (REPLICA IDENTITY FULL)
src/decode.c          -- output plugin callbacks (startup/begin/change/truncate/commit)
src/extract.c         -- tuple -> key/tenant extraction, no-op comparison
src/encode.c          -- JSON envelope construction
sql/pg_replayfeed--2.0.sql       -- extension SQL (catalog, functions)
sql/pg_replayfeed--1.1--2.0.sql -- destructive upgrade (drops 1.x objects)
build/                -- object files (created by make; never in src/)
test/sql/ + test/expected/       -- pg_regress suites: basic, txn, retention, security
test/upgrade/                    -- 1.1 -> 2.0 upgrade verification
e2e/feedclient/                  -- Go replication-protocol client (no polling)
e2e/cmd/producer|consumer|loadtest -- demo producer, leaderboard consumer, load test
```
