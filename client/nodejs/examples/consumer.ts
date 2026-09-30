import { ReplayFeedClient, Envelope } from '../src';

async function main() {
  const client = new ReplayFeedClient({
    connectionString: process.env.DATABASE_URL || 'postgres://postgres@localhost:5432/mydb',
    slotName: 'nodejs_orders_consumer',
    feed: 'orders', // Optional: filter for feed "orders"
  });

  console.log('Connecting to PostgreSQL replication feed...');

  // 1. High-level streaming API (automatically ACKs on successful resolution)
  await client.stream(async (envelope: Envelope, lsn: string) => {
    console.log(`\nReceived Envelope @ LSN ${envelope.cursor} (commit timestamp: ${new Date(envelope.committed_at_ms).toISOString()}):`);

    for (const change of envelope.changes) {
      console.log(`  [${change.feed}] Op: ${change.op}, Key: ${change.key}, Tenant: ${change.tenant ?? 'none'}`);
    }

    if (envelope.truncate.length > 0) {
      console.log(`  Truncated feeds: ${envelope.truncate.join(', ')}`);
    }

    // Resolving here acknowledges the LSN and advances the slot
  });
}

// Alternatively: using async iterator
/*
async function runAsyncIterator() {
  const client = new ReplayFeedClient({
    connectionString: 'postgres://postgres@localhost:5432/mydb',
    slotName: 'orders_iter_slot',
  });

  for await (const { envelope, lsn } of client) {
    console.log(`Processing LSN ${lsn}...`);
    // Manually ack after durable storage
    await client.ack(lsn);
  }
}
*/

main().catch((err) => {
  console.error('Fatal streaming error:', err);
  process.exit(1);
});
