package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	replayfeed "github.com/pg-replayfeed/pg_replayfeed/client/go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Configure client
	cfg := replayfeed.Config{
		ConnString: "postgres://postgres@localhost:5432/mydb?sslmode=disable",
		SlotName:   "orders_consumer_slot",
		Feed:       "orders", // Optional: filter for feed "orders"
	}

	client, err := replayfeed.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close(ctx)

	fmt.Println("Streaming changes from pg_replayfeed...")

	// Stream envelopes with automatic ACK on successful processing
	err = client.Stream(ctx, func(env *replayfeed.Envelope, lsn replayfeed.LSN) error {
		fmt.Printf("Envelope @ LSN %s (%d changes, %d truncated):\n",
			env.Cursor, len(env.Changes), len(env.Truncate))

		for _, ch := range env.Changes {
			tenant := "-"
			if ch.Tenant != nil {
				tenant = *ch.Tenant
			}
			fmt.Printf("  [%s] Op=%s Key=%s Tenant=%s\n",
				ch.Feed, ch.Op, ch.Key, tenant)
		}

		return nil // Returning nil automatically ACKs the LSN to advance the replication slot
	})

	if err != nil && err != context.Canceled {
		log.Fatalf("stream error: %v", err)
	}
}
