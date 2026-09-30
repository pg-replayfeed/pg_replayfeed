// Command consumer streams a pg_replayfeed feed over the replication
// protocol. It opens its own logical slot, receives one JSON envelope per
// committed transaction, applies it, and acknowledges the commit LSN.
//
// Two modes:
//   - expect mode (--count-file): exits 0 once it has seen every change the
//     producer reported. count == expected proves nothing was missed or
//     duplicated (at-least-once delivery means the consumer must dedupe by
//     key, which the leaderboard does naturally).
//   - continuous mode (default): runs until SIGINT/SIGTERM, logging
//     throughput every --stats-interval, then prints a FINAL line.
//
// The cursor is the commit LSN (e.g. "0/16B1978"), persisted to --cursor-file
// and resumed on restart. Each consumer needs its own slot.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"e2e/feedclient"
)

// readExpect returns the producer's total once it has finished.
func readExpect(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func main() {
	sockDir := flag.String("sock", "", "postgres unix socket dir")
	port := flag.Int("port", 5432, "postgres port")
	user := flag.String("user", "postgres", "postgres user")
	db := flag.String("db", "e2edb", "database")
	feed := flag.String("feed", "e2e", "feed name (output plugin filter)")
	slot := flag.String("slot", "", "replication slot name (default: consumer-<id>)")
	id := flag.String("id", "c1", "consumer id (for logs)")
	countFile := flag.String("count-file", "", "expect mode: producer writes its total here when done; empty = continuous mode")
	cursorFile := flag.String("cursor-file", "", "persist the LSN cursor here; resume from it on restart")
	verbose := flag.Bool("verbose", false, "print every envelope as it arrives")
	leaderboard := flag.Bool("leaderboard", false, "live leaderboard view: redraw top scores from the source table whenever new events arrive")
	table := flag.String("table", "", "source table for --leaderboard")
	statsInterval := flag.Duration("stats-interval", 5*time.Second, "progress log interval in continuous mode")
	timeout := flag.Duration("timeout", 180*time.Second, "give up after this long (expect mode)")
	flag.Parse()

	if *sockDir == "" {
		log.Fatalf("consumer %s: --sock is required", *id)
	}
	if *slot == "" {
		*slot = "consumer_" + *id
	}
	continuous := *countFile == ""

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	qctx := context.Background()

	// Replication connection for the stream.
	repConn, err := feedclient.Connect(qctx, *sockDir, *port, *user, *db)
	if err != nil {
		log.Fatalf("consumer %s: %v", *id, err)
	}
	defer repConn.Close(qctx)

	// Plain connection for slot setup and leaderboard queries.
	queryConn, err := pgx.Connect(qctx,
		fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d", *user, *db, *sockDir, *port))
	if err != nil {
		log.Fatalf("consumer %s: query connect: %v", *id, err)
	}
	defer queryConn.Close(qctx)

	// Always ensure the replication slot exists before starting replication.
	slotLSN, err := feedclient.EnsureSlot(qctx, queryConn, *slot, "pg_replayfeed")
	if err != nil {
		log.Fatalf("consumer %s: ensure slot %q: %v", *id, *slot, err)
	}

	// Resume from the checkpointed LSN, if any.
	var startLSN pglogrepl.LSN
	if *cursorFile != "" {
		if b, err := os.ReadFile(*cursorFile); err == nil {
			if lsn, err := feedclient.ParseCursor(strings.TrimSpace(string(b))); err == nil {
				startLSN = lsn
				fmt.Printf("consumer %s: resuming from %s\n", *id, lsn)
			}
		}
	}
	if startLSN == 0 || startLSN < slotLSN {
		startLSN = slotLSN
		fmt.Printf("consumer %s: slot %s from %s\n", *id, *slot, startLSN)
	}

	client, err := feedclient.Start(qctx, repConn, *slot, "pg_replayfeed", *feed, startLSN)
	if err != nil {
		log.Fatalf("consumer %s: start: %v", *id, err)
	}
	defer client.Close(qctx)

	if *leaderboard {
		if *table == "" {
			log.Fatalf("consumer %s: --table is required with --leaderboard", *id)
		}
		drawLeaderboard(qctx, queryConn, *table, *id, startLSN.String(), 0)
	}

	var count atomic.Int64
	var lastLSN atomic.Pointer[string]
	initLSN := startLSN.String()
	lastLSN.Store(&initLSN)
	deadline := time.Now().Add(*timeout)
	start := time.Now()
	lastStats := start
	var lastCount int64

	if !continuous {
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if expected, ok := readExpect(*countFile); ok {
						c := count.Load()
						if c >= expected {
							if c == expected {
								curLSN := ""
								if p := lastLSN.Load(); p != nil {
									curLSN = *p
								}
								fmt.Printf("consumer %s DONE count=%d last_lsn=%s in %s\n",
									*id, c, curLSN, time.Since(start).Round(time.Millisecond))
								os.Exit(0)
							} else {
								log.Fatalf("consumer %s: expected %d changes, got count=%d (at-least-once: duplicates possible, investigate)",
									*id, expected, c)
							}
						}
					}
					if time.Now().After(deadline) {
						curLSN := ""
						if p := lastLSN.Load(); p != nil {
							curLSN = *p
						}
						log.Fatalf("consumer %s: timed out after %s with count=%d last_lsn=%s",
							*id, *timeout, count.Load(), curLSN)
					}
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			curLSN := ""
			if p := lastLSN.Load(); p != nil {
				curLSN = *p
			}
			fmt.Printf("consumer %s FINAL count=%d last_lsn=%s in %s\n",
				*id, count.Load(), curLSN, time.Since(start).Round(time.Millisecond))
			return
		default:
		}

		env, lsn, err := client.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				curLSN := ""
				if p := lastLSN.Load(); p != nil {
					curLSN = *p
				}
				fmt.Printf("consumer %s FINAL count=%d last_lsn=%s in %s\n",
					*id, count.Load(), curLSN, time.Since(start).Round(time.Millisecond))
				return
			}
			log.Fatalf("consumer %s: next: %v", *id, err)
		}

		n := int64(len(env.Changes) + len(env.Truncate))
		newCount := count.Add(n)
		lsnStr := lsn.String()
		lastLSN.Store(&lsnStr)
		if *verbose {
			fmt.Printf("consumer %s envelope cursor=%s changes=%d truncate=%d\n",
				*id, env.Cursor, len(env.Changes), len(env.Truncate))
			for _, c := range env.Changes {
				fmt.Printf("  %s %s key=%s tenant=%v old_tenant=%v\n",
					c.Feed, c.Op, c.Key, strOrNil(c.Tenant), strOrNil(c.OldTenant))
			}
		}

		// Apply the envelope atomically here (demo: just count), then
		// checkpoint the LSN so the slot can advance.
		if err := client.Ack(qctx, lsn); err != nil {
			log.Fatalf("consumer %s: ack: %v", *id, err)
		}
		if *cursorFile != "" {
			if err := os.WriteFile(*cursorFile, []byte(lsn.String()), 0644); err != nil {
				log.Printf("consumer %s: writing cursor file: %v", *id, err)
			}
		}
		if *leaderboard {
			drawLeaderboard(qctx, queryConn, *table, *id, lsn.String(), newCount)
		}

		if !continuous {
			if expected, ok := readExpect(*countFile); ok && newCount >= expected {
				if newCount == expected {
					fmt.Printf("consumer %s DONE count=%d last_lsn=%s in %s\n",
						*id, newCount, lsn, time.Since(start).Round(time.Millisecond))
					return
				}
				log.Fatalf("consumer %s: expected %d changes, got count=%d (at-least-once: duplicates possible, investigate)",
					*id, expected, newCount)
			}
			if time.Now().After(deadline) {
				log.Fatalf("consumer %s: timed out after %s with count=%d last_lsn=%s",
					*id, *timeout, newCount, lsn)
			}
		} else if !*leaderboard && time.Since(lastStats) >= *statsInterval {
			eps := float64(newCount-lastCount) / time.Since(lastStats).Seconds()
			fmt.Printf("consumer %s stats last_lsn=%s count=%d eps=%.0f\n",
				*id, lsn, newCount, eps)
			lastStats = time.Now()
			lastCount = newCount
		}
	}
}

func strOrNil(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}

// drawLeaderboard clears the screen and prints the current top-10 scores
// from the source table. The feed tells us *when* something changed; the
// table holds the current values.
func drawLeaderboard(ctx context.Context, conn *pgx.Conn, table, id, cursor string, count int64) {
	rows, err := conn.Query(ctx,
		fmt.Sprintf("SELECT id, COALESCE(total,0) FROM %s ORDER BY COALESCE(total,0) DESC, id LIMIT 10", table))
	if err != nil {
		log.Printf("consumer %s: leaderboard query: %v", id, err)
		return
	}
	defer rows.Close()
	fmt.Print("\033[2J\033[H")
	fmt.Printf("=== leaderboard @ %s (%d changes) ===\n", cursor, count)
	fmt.Printf("%-4s %-10s %s\n", "#", "player", "score")
	rank := 0
	for rows.Next() {
		var pid, total int64
		if err := rows.Scan(&pid, &total); err != nil {
			log.Printf("consumer %s: leaderboard scan: %v", id, err)
			return
		}
		rank++
		fmt.Printf("%-4d %-10s %d\n", rank, fmt.Sprintf("player-%d", pid), total)
	}
	if err := rows.Err(); err != nil {
		log.Printf("consumer %s: leaderboard rows: %v", id, err)
	}
}
