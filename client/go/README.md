# pg_replayfeed Go Client (`client/go`)

Official high-performance Go client for streaming transaction-atomic change envelopes from PostgreSQL via the `pg_replayfeed` logical-decoding output plugin.

## Installation

```bash
go get github.com/pg-replayfeed/pg_replayfeed/client/go
```

## Features

- **Replication Protocol Streaming:** Connects with `replication=database` directly to the PostgreSQL replication engine.
- **Transaction-Atomic Envelopes:** Receives exact, ordered JSON envelopes per committed transaction.
- **Fast Zero-Allocation Parser:** Direct byte scanning for standard envelopes, with fallback to standard `json.Unmarshal`.
- **Automatic Keepalive & Standby Updates:** Transparently responds to server keepalive pings and maintains standby liveness.
- **Resilient Slot Management:** Automatically creates logical replication slots if missing, and resumes safely from the last confirmed flush LSN or saved cursor.
- **Idiomatic Streaming Loop:** High-level `client.Stream(...)` helper with auto-acknowledgement on success.

## Quickstart

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

	// 1. Initialize client
	client, err := replayfeed.New(ctx, replayfeed.Config{
		ConnString: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
		SlotName:   "my_orders_slot",
		Feed:       "orders", // optional: stream only the 'orders' feed
	})
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close(ctx)

	// 2. Stream envelopes (automatically ACKs on nil return)
	err = client.Stream(ctx, func(env *replayfeed.Envelope, lsn replayfeed.LSN) error {
		fmt.Printf("Envelope cursor=%s committed_at=%d\n", env.Cursor, env.CommittedAtMs)

		for _, ch := range env.Changes {
			fmt.Printf("  [%s] Op: %s, Key: %s\n", ch.Feed, ch.Op, ch.Key)
		}

		return nil // Returning nil acknowledges the LSN to advance the replication slot
	})
	if err != nil {
		log.Fatalf("stream error: %v", err)
	}
}
```

## Manual Ingestion Loop (`Next` and `Ack`)

For advanced use cases requiring manual checkpointing or custom batching:

```go
for {
    env, lsn, err := client.Next(ctx)
    if err != nil {
        log.Fatalf("receive error: %v", err)
    }

    // Process envelope durably...
    processEnvelope(env)

    // Acknowledge LSN so PostgreSQL can advance the slot and prune WAL
    if err := client.Ack(ctx, lsn); err != nil {
        log.Fatalf("ack error: %v", err)
    }
}
```

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `ConnString` | `string` | `""` | PostgreSQL connection URI (`postgres://...`) |
| `Host` | `string` | `"localhost"` | Database host |
| `Port` | `int` | `5432` | Database port |
| `User` | `string` | `"postgres"` | Database user |
| `Password` | `string` | `""` | Database password |
| `Database` | `string` | `"postgres"` | Database name |
| `SocketDir` | `string` | `""` | Optional unix socket directory (overrides Host/Port) |
| `SlotName` | `string` | `"replayfeed_slot"` | Logical replication slot name |
| `Plugin` | `string` | `"pg_replayfeed"` | Output plugin name |
| `Feed` | `string` | `""` | Feed filter (empty = all feeds) |
| `StartLSN` | `pglogrepl.LSN` | `0` | Starting LSN position (0 = confirmed flush position) |
| `CreateSlotIfMissing` | `bool` | `true` | Create logical slot if it does not exist |
| `StandbyTimeout` | `time.Duration` | `10s` | Idle heartbeat interval |
