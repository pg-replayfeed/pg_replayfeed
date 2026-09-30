# pg_replayfeed

A PostgreSQL logical-decoding output plugin that streams key-only change envelopes **directly out of the binary Write-Ahead Log (WAL)** over the PostgreSQL replication protocol. Register a table as a feed; every committed transaction touching it produces one clean JSON envelope; consumers stream using standard replication connections and checkpoint by commit Log Sequence Number (LSN).

**First-class Go & Node.js/TypeScript clients. Zero triggers on the write path. Zero NOTIFY. Zero polling. Zero table bloat.**

---

## Highlights

- **Direct Binary WAL Ingestion:** 0 write-path triggers, 0 extra WAL bytes generated, 0 auxiliary tables.
- **Transaction-Atomic Envelopes:** Exactly one JSON envelope per committed transaction, grouped in strict commit LSN order. Rolled back transactions never stream.
- **Payload-Size Independence:** Key-only change contract. Streaming wire size stays ~56 B/row whether the source table row is 50B, 1KB, 10KB, or 50KB+.
- **First-Class Client Libraries:** Production-ready client SDKs for both **Go (`client/go`)** and **Node.js / TypeScript (`client/nodejs`)**, featuring slot lifecycle management, automatic ACK, and standby heartbeats.
- **Durable LSN Resumption:** Standard PostgreSQL replication slots pin WAL on disk until confirmed by consumers. Automatic at-least-once crash recovery.

---

## Documentation Roadmap

- [**Architecture & Design (`docs/architecture.md`)**](docs/architecture.md): Complete systems design, WAL decoding mechanics, tuple extraction, snapshot bootstrap, and replication-protocol lifecycle.
- [**Benchmarks & Comparative Analysis (`docs/benchmarks.md`)**](docs/benchmarks.md): Comprehensive empirical benchmarks, latency percentiles (p50/p95/p99), sustained soak metrics, and head-to-head architectural comparisons against Supabase Realtime baselines.
- [**Semantics & Guarantees (`docs/semantics.md`)**](docs/semantics.md): Exact consistency models, ordering guarantees, failover durability, and consumer responsibilities.
- [**Security & Threat Model (`docs/security.md`)**](docs/security.md): Invoker privilege validation (`TRIGGER` permission checks), replication credentials, and attack mitigation.

---

## Envelope Specification

Each committed transaction touching one or more registered feeds produces exactly one JSON envelope:

```json
{
  "cursor": "0/16B1978",
  "committed_at_ms": 1727690096789,
  "changes": [
    {
      "feed": "orders",
      "op": "I",
      "key": "48921",
      "tenant": "acme"
    },
    {
      "feed": "orders",
      "op": "U",
      "key": "48920",
      "tenant": "acme",
      "old_tenant": "legacy_acme"
    }
  ],
  "truncate": []
}
```

- **`cursor`** *(string)*: Monotonically increasing transaction commit LSN (e.g. `0/16B1978`). Checkpointed by consumers upon durable processing.
- **`committed_at_ms`** *(integer)*: Millisecond Unix epoch timestamp of commit.
- **`changes`** *(array)*: Key mutations (`"I"` = Insert, `"U"` = Update, `"D"` = Delete). Key updates emit a `D(old)` + `I(new)` pair; tenant moves carry `old_tenant`; unchanged/no-op updates emit nothing.
- **`truncate`** *(array)*: Truncated feeds in this transaction (resnapshot marker).

---

## Quickstart & End-to-End Walkthrough

### 1. Build and Install

```bash
make clean && make
sudo make install
```

### 2. Configure PostgreSQL

Ensure `wal_level = logical` in `postgresql.conf`:

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

### 3. Register a Feed in SQL

Feeds are registered using `replayfeed.create_feed`:

```sql
CREATE EXTENSION pg_replayfeed;

-- Source table must have a primary key or single-column unique index
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    tenant text NOT NULL,
    total integer
);

-- Register feed: (feed_name, table_name, key_column, tenant_column DEFAULT NULL)
SELECT replayfeed.create_feed('orders', 'orders', 'id', 'tenant');
```

> **Note:** `create_feed` requires `TRIGGER` privilege on the source table and automatically configures `REPLICA IDENTITY FULL` so the output plugin can extract previous keys during updates and deletes.
> To remove a feed: `SELECT replayfeed.drop_feed('orders');`.
> To view active feeds: `SELECT * FROM replayfeed.feed;`.

### 4. Pure SQL Workflows (Zero Clients, Zero External Daemons)

You do **not** need to write application code or run an external daemon to take advantage of `pg_replayfeed`. PostgreSQL allows reading output plugins directly through standard SQL queries. This enables **in-database event processing, trigger-free audit trails, and scheduled batch pipelines** entirely within PostgreSQL.

#### A. Interactive Slot Management & Inspection

```sql
-- 1. Create a logical replication slot for SQL consumption
SELECT slot_name FROM pg_create_logical_replication_slot('sql_audit_slot', 'pg_replayfeed');

-- 2. Make changes to your feed table
INSERT INTO orders (id, tenant, total) VALUES (101, 'acme', 450);
UPDATE orders SET total = 500 WHERE id = 101;
DELETE FROM orders WHERE id = 101;

-- 3. PEEK changes without advancing the slot (idempotent / non-destructive)
SELECT * FROM pg_logical_slot_peek_changes('sql_audit_slot', NULL, NULL);

-- 4. Filter by specific feed in SQL
SELECT * FROM pg_logical_slot_peek_changes('sql_audit_slot', NULL, NULL, 'feed', 'orders');

-- 5. CONSUME changes and advance the replication slot (pruning WAL)
SELECT * FROM pg_logical_slot_get_changes('sql_audit_slot', NULL, NULL, 'feed', 'orders');
```

#### B. Unpack Envelopes into Relational Tables Directly in SQL

The JSON envelopes emitted by `pg_replayfeed` can be queried, filtered, and unpacked into relational columns using PostgreSQL's native JSON operators:

```sql
SELECT 
    s.lsn,
    s.xid,
    to_timestamp((d->>'committed_at_ms')::bigint / 1000.0) AS committed_at,
    d->>'cursor' AS cursor_lsn,
    ch->>'feed'   AS feed,
    ch->>'op'     AS operation,
    ch->>'key'    AS row_key,
    ch->>'tenant' AS tenant,
    ch->>'old_tenant' AS old_tenant
FROM pg_logical_slot_get_changes('sql_audit_slot', NULL, NULL) s,
     LATERAL json_array_elements((s.data::json)->'changes') AS ch;
```

#### C. Build a Zero-Trigger Audit Log / Event Table

Instead of attaching slow `AFTER INSERT OR UPDATE OR DELETE` triggers to your hot tables (which add locking, CPU overhead, and write-path latency), you can consume WAL asynchronously inside PostgreSQL:

```sql
-- Create an audit table
CREATE TABLE orders_audit_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lsn pg_lsn NOT NULL,
    committed_at timestamptz NOT NULL,
    operation text NOT NULL,
    order_id text NOT NULL,
    tenant text,
    consumed_at timestamptz DEFAULT now()
);

-- Ingest WAL changes directly into the audit table in one transaction:
WITH wal_batch AS (
    SELECT lsn, (data::json) AS env
    FROM pg_logical_slot_get_changes('sql_audit_slot', NULL, NULL, 'feed', 'orders')
),
unpacked AS (
    SELECT
        lsn::pg_lsn,
        to_timestamp((env->>'committed_at_ms')::bigint / 1000.0) AS committed_at,
        ch->>'op' AS op,
        ch->>'key' AS key,
        ch->>'tenant' AS tenant
    FROM wal_batch,
         LATERAL json_array_elements(env->'changes') AS ch
)
INSERT INTO orders_audit_log (lsn, committed_at, operation, order_id, tenant)
SELECT lsn, committed_at, op, key, tenant FROM unpacked;
```

> **Tip:** You can schedule this query to run every few seconds or minutes via `pg_cron` (`SELECT cron.schedule('drain_wal', '*/1 * * * *', 'SELECT drain_audit_batch()');`) for completely automated, triggerless event processing.

#### D. Clean Up Slots

```sql
-- View all active replication slots and their lag
SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots;

-- Drop the slot when done (releases pinned WAL files)
SELECT pg_drop_replication_slot('sql_audit_slot');
```

---

### 5. Stream via Go Client (`client/go`)

The official Go client (`github.com/pg-replayfeed/pg_replayfeed/client/go`) provides high-performance zero-allocation envelope decoding, automatic keepalive pings, and resilient slot management:

```bash
go get github.com/pg-replayfeed/pg_replayfeed/client/go
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	replayfeed "github.com/pg-replayfeed/pg_replayfeed/client/go"
)

func main() {
	ctx := context.Background()

	// 1. Initialize client (creates replication slot if missing)
	client, err := replayfeed.New(ctx, replayfeed.Config{
		ConnString: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
		SlotName:   "orders_consumer_slot",
		Feed:       "orders", // optional: stream only the 'orders' feed
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	fmt.Println("Streaming changes from pg_replayfeed...")

	// 2. Stream envelopes (automatically ACKs on nil return)
	err = client.Stream(ctx, func(env *replayfeed.Envelope, lsn replayfeed.LSN) error {
		fmt.Printf("Envelope @ LSN %s (%d changes):\n", env.Cursor, len(env.Changes))
		for _, ch := range env.Changes {
			fmt.Printf("  [%s] Op: %s Key: %s\n", ch.Feed, ch.Op, ch.Key)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

---

### 6. Stream via Node.js / TypeScript Client (`client/nodejs`)

The official Node.js / TypeScript client provides full typing, automatic standby heartbeats, automatic or manual acknowledgement, and both high-level stream and async iterator support:

```bash
npm install pg-replayfeed-client pg
```

```typescript
import { ReplayFeedClient, Envelope } from 'pg-replayfeed-client';

async function main() {
  const client = new ReplayFeedClient({
    connectionString: 'postgres://postgres:password@localhost:5432/mydb',
    slotName: 'orders_consumer_slot',
    feed: 'orders', // optional: stream only the 'orders' feed
  });

  console.log('Streaming changes from pg_replayfeed...');

  // Automatically ACKs the LSN when the handler resolves successfully
  await client.stream(async (envelope: Envelope, lsn: string) => {
    console.log(`Envelope @ LSN ${envelope.cursor} (committed at ${envelope.committed_at_ms}):`);

    for (const change of envelope.changes) {
      console.log(`  [${change.feed}] Op: ${change.op} Key: ${change.key} Tenant: ${change.tenant ?? 'none'}`);
    }
  });
}

main().catch(console.error);
```

Or using an async iterator:

```typescript
for await (const { envelope, lsn } of client) {
  processEnvelope(envelope);
  await client.ack(lsn);
}
```

---

### 7. Stream via CLI (`pg_recvlogical`)

```bash
# Create replication slot
pg_recvlogical -d mydb --slot orders_slot --create-slot -P pg_replayfeed

# Stream changes directly to stdout
pg_recvlogical -d mydb --slot orders_slot --start -o feed=orders -f -
```

---

## Testing & Demos (Go-Only)

All verification and demo tools are built in pure Go without third-party brokers:

```bash
# 1. Full pg_regress regression + destructive upgrade suite
make test

# 2. End-to-end verification (producer + 5 concurrent consumers with count verification)
E2E_OPS=1000 make e2e

# 3. Interactive demo (random-walk leaderboard + live terminal UI with crash resumption)
make producer   # Terminal 1: writes changes
make consumer   # Terminal 2: live updating leaderboard view

# 4. Sustained high-throughput load test
LOAD_WRITERS=8 LOAD_CONSUMERS=4 LOAD_DURATION=30s make loadtest
```

---

## Benchmarks & Performance Summary

Comprehensive performance reports and comparisons are documented in [**`docs/benchmarks.md`**](docs/benchmarks.md).

Quick summary of measured results on PostgreSQL 17:
- **Write Path Overhead:** 0 added triggers; 0 added WAL bytes (~146.7 B/row native WAL).
- **Stream Throughput:** >340,000 rows/sec bulk replay (>18 MB/s wire throughput).
- **Discrete Commit Rate:** >53,000 commits/sec under sustained concurrent load.
- **Latency Distribution:** Commit latency p50: **71 µs** (p99: 151 µs); End-to-end commit-to-consumer lag p50: **576 µs** (p99: 1.07 ms).

To execute benchmarks locally:
```bash
make bench              # Modular benchmark suite
make bench-dedicated    # Dedicated sustained concurrent soak matrix
```

---

## CI / CD Pipeline

The automated CI workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on push and PR:
- Builds across **PostgreSQL 16, 17, and 18** on `ubuntu-24.04`.
- Uses native Go (`actions/setup-go@v5`) with zero Node.js dependencies.
- Runs regression and upgrade tests (`bash test/run_tests.sh`).
- Runs deterministic end-to-end verification (`E2E_OPS=500 bash e2e/run_e2e.sh`).
- Executes modular and sustained benchmark matrices, publishing formatted markdown summaries to `$GITHUB_STEP_SUMMARY` and saving logs as artifacts.

---

## Upgrading from 1.x

**2.0 is a breaking, destructive upgrade.**
- The legacy TypeScript relay has been completely removed; consumers should use the official Go client (`client/go`), the Node.js client (`client/nodejs`), or any standard PostgreSQL replication-protocol client.
- Cursors are now transaction commit LSNs (e.g. `0/16B1978`).

---

## License

PostgreSQL License. See [LICENSE](LICENSE).
