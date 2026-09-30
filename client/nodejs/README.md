# pg_replayfeed Node.js / TypeScript Client (`client/nodejs`)

Official Node.js / TypeScript client for streaming transaction-atomic change envelopes from PostgreSQL via the `pg_replayfeed` logical-decoding output plugin.

## Installation

```bash
npm install pg-replayfeed-client pg
# or
pnpm add pg-replayfeed-client pg
# or
yarn add pg-replayfeed-client pg
```

## Features

- **Direct Logical Replication:** Connects with `replication=database` directly to the PostgreSQL replication engine.
- **Transaction-Atomic Envelopes:** Exactly one structured JSON envelope per committed transaction in strict commit order.
- **Zero Triggers, Zero NOTIFY:** Ingests changes directly from WAL with 0 write-path overhead.
- **Automatic Slot Management:** Automatically creates logical replication slots if missing, and resumes safely from the last confirmed flush LSN or saved cursor.
- **Automatic Keepalive & Standby Updates:** Handles server keepalive pings and idle status updates transparently.
- **High-Level Streaming & Async Iteration:** Supports `client.stream(handler)`, `client.on('envelope', ...)`, and `for await (const { envelope, lsn } of client)`.
- **Full TypeScript Support:** Complete typings for `Envelope`, `Change`, `Op`, and configuration.

## Quickstart

```typescript
import { ReplayFeedClient, Envelope } from 'pg-replayfeed-client';

async function main() {
  const client = new ReplayFeedClient({
    connectionString: 'postgres://postgres:password@localhost:5432/mydb',
    slotName: 'orders_consumer_slot',
    feed: 'orders', // Optional: stream only the 'orders' feed
  });

  console.log('Streaming changes from pg_replayfeed...');

  // Automatically ACKs the LSN when the handler resolves
  await client.stream(async (envelope: Envelope, lsn: string) => {
    console.log(`Envelope @ LSN ${envelope.cursor} (committed at ${envelope.committed_at_ms}):`);

    for (const change of envelope.changes) {
      console.log(`  [${change.feed}] Op=${change.op} Key=${change.key} Tenant=${change.tenant ?? 'none'}`);
    }

    if (envelope.truncate.length > 0) {
      console.log(`  Truncated feeds: ${envelope.truncate.join(', ')}`);
    }
  });
}

main().catch(console.error);
```

## Using Async Iterator

```typescript
import { ReplayFeedClient } from 'pg-replayfeed-client';

const client = new ReplayFeedClient({
  connectionString: 'postgres://postgres:password@localhost:5432/mydb',
  slotName: 'orders_consumer_slot',
});

for await (const { envelope, lsn } of client) {
  // 1. Process envelope durably
  await processEnvelope(envelope);

  // 2. Acknowledge LSN so PostgreSQL can advance the slot and prune WAL
  await client.ack(lsn);
}
```

## Using Event Emitter

```typescript
const client = new ReplayFeedClient({
  connectionString: 'postgres://postgres:password@localhost:5432/mydb',
  slotName: 'orders_consumer_slot',
});

client.on('envelope', async (envelope, lsn) => {
  console.log('Received envelope:', envelope);
  await client.ack(lsn);
});

client.on('error', (err) => {
  console.error('Replication stream error:', err);
});

await client.start();
```

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `connectionString` | `string` | `undefined` | PostgreSQL connection URI (`postgres://...`) |
| `host` | `string` | `'localhost'` | Database host |
| `port` | `number` | `5432` | Database port |
| `user` | `string` | `'postgres'` | Database user |
| `password` | `string` | `undefined` | Database password |
| `database` | `string` | `'postgres'` | Database name |
| `ssl` | `boolean \| object` | `undefined` | SSL options |
| `slotName` | `string` | `'replayfeed_slot'` | Logical replication slot name |
| `plugin` | `string` | `'pg_replayfeed'` | Output plugin name |
| `feed` | `string` | `undefined` | Feed name filter (omitted = all feeds) |
| `startLsn` | `string` | `undefined` | Starting LSN position (e.g. `'0/16B1978'`) |
| `createSlotIfMissing` | `boolean` | `true` | Create logical slot if it does not exist |
| `standbyIntervalMs` | `number` | `10000` | Idle heartbeat interval in milliseconds |
