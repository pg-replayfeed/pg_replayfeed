# pg_replayfeed — Semantics & Guarantees (2.0)

This document states what `pg_replayfeed` guarantees, what it does not, and
the exact contract a consumer must honor. The design-level argument lives in
`docs/architecture.md`; this is the user-facing restatement.

## 1. The cursor invariant

The consumer's cursor **is the transaction commit LSN** (`cursor` field of
each envelope, e.g. `"0/16B1978"`).

- LSNs are assigned by PostgreSQL: unique, strictly increasing, in commit
  order. No synthetic cursor is allocated, so there is no allocation-order
  trap and no per-feed writer serialization.
- Logical decoding delivers transactions in LSN order per slot.
- The consumer persists the LSN **after** applying the envelope, and resumes
  from the persisted LSN on restart.

What this rules out: the entire 1.x cursor subsystem (dense bigint cursors,
feed row locks, gap recycling, pruning watermarks, `RESNAPSHOT_REQUIRED`
sentinels). The WAL already provides a total commit order; we use it.

## 2. Delivery contract

**At-least-once, ordered per slot.**

- Each envelope is delivered at least once, in LSN order, to each slot's
  consumer.
- A crash between "apply envelope" and "persist LSN" causes redelivery of
  the last envelope. Consumers must apply idempotently (or tolerate
  re-application — for the leaderboard demo, re-applying a score change is
  naturally idempotent because the consumer re-reads the source table).
- The consumer acknowledges by advancing the slot's confirmed flush LSN
  (replication-protocol standby status updates). Unacknowledged data pins
  WAL.

## 3. Transaction atomicity

One envelope per committed transaction, containing **all** of that
transaction's feed changes (across every feed the slot streams). The consumer
applies the envelope atomically: either all its changes are visible
downstream or none are, and the LSN is persisted only after the whole
envelope is applied. A torn apply (some changes of a transaction visible,
others not) violates the contract.

## 4. Envelope rules

- **Key-changing UPDATE** becomes `DELETE(old key)` + `INSERT(new key)`.
  No entity ever silently changes identity.
- **Tenant moves** carry `old_tenant` alongside the new `tenant`.
- **No-op UPDATE** (old and new tuples attribute-identical) emits nothing.
- **TRUNCATE** emits a resnapshot marker (`"truncate": ["<feed>"]`), not row
  events. The consumer must resnapshot that feed: re-read the source table
  and resume streaming.
- **Irrelevant and rolled-back transactions** emit nothing.

## 5. Consumer contract

A correct consumer:

1. applies each envelope atomically (all changes or none);
2. persists the envelope's LSN **after** successful application, never before;
3. sends standby status updates so the slot advances (or WAL grows
   unboundedly);
4. never advances past a failed application — it retries the same envelope;
5. tolerates redelivery of the last envelope (idempotent apply);
6. on a `truncate` marker, resnapshots that feed and resumes from the
   marker's LSN.

The Go client (`e2e/feedclient`) implements 2–4; the demo consumer implements
1 via "re-read the source table on every batch" (which is trivially atomic
and idempotent for a leaderboard).

## 6. Slot operations

- **One slot per independent consumer.** Slots are not shared. Ten consumers
  of one feed means ten slots.
- **WAL retention is bounded by the slowest slot.** A consumer that stops
  acknowledging pins WAL indefinitely and can fill the disk. Monitor
  `pg_replication_slots`: alert when `confirmed_flush_lsn` falls behind the
  current WAL insert position; drop dead slots promptly.
- **Lost slot = resnapshot.** If a slot is dropped (or WAL is removed
  before the consumer reads it — `pg_wal` exhaustion, manual removal),
  the consumer cannot resume. It must create a new slot, bootstrap via the
  snapshot protocol (`docs/architecture.md` §5), and start streaming from
  the new consistent point. There is no "catch up from an arbitrary old LSN"
  — the WAL it needed is gone.
- **Restart** is the normal case: the slot survives, the consumer reconnects
  with its persisted LSN, the server replays from there. No bootstrap needed.

## 7. What is *not* guaranteed

- **No exactly-once delivery.** Redelivery after a crash between apply and
  persist is expected; consumers must be idempotent.
- **No infinite history.** WAL is finite; slow consumers pin it, and pinned
  WAL is an operational problem, not a feature.
- **No cross-slot ordering.** Each slot orders its own stream; two consumers
  on different slots see the same transactions in the same order, but there
  is no coordination between them.
- **No row bodies.** Envelopes are key-only: `(feed, op, key, tenant)`.
  Payload reconstruction is the consumer's job (usually: re-read the row
  from the source table).
- **No zero-loss failover.** See `docs/architecture.md` §8.
- **No production-readiness claim.** See `docs/benchmarks.md` for what was
  actually measured.
