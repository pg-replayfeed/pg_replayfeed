// Command producer pushes DML operations into the source table, forever by
// default (or for exactly --ops operations).
//
// Two modes:
//
//	plain: odd iterations INSERT a fresh key, even iterations UPDATE a
//	    random existing key (--update-only makes every iteration an UPDATE
//	    once the first key exists).
//	leaderboard (--players N): keeps N players with random-walk scores. Each
//	    tick picks a random player and a new score; the UPDATE only runs
//	    when the score actually changed, so the feed carries exactly the
//	    real changes.
//
// Every committed statement writes exactly one feed event. Key allocation
// resumes from MAX(id), so restarting the producer never collides with
// existing primary keys. On SIGINT/SIGTERM it stops between statements and
// prints the total produced.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	sockDir := flag.String("sock", "", "postgres unix socket dir")
	port := flag.Int("port", 5432, "postgres port")
	user := flag.String("user", "postgres", "postgres user")
	db := flag.String("db", "e2edb", "database")
	table := flag.String("table", "e2e_orders", "source table")
	tenant := flag.String("tenant", "acme", "tenant key")
	ops := flag.Int("ops", 0, "total DML operations; 0 = run until interrupted")
	rate := flag.Int("rate", 2000, "target ops/sec in continuous mode; 0 = unthrottled")
	updateOnly := flag.Bool("update-only", false, "only UPDATE existing keys (inserts the first key when the table is empty)")
	players := flag.Int("players", 0, "leaderboard mode: keep N players with random-walk scores, updating only on change")
	countFile := flag.String("count-file", "", "write total produced events here when done (optional)")
	flag.Parse()

	if *sockDir == "" {
		log.Fatal("producer: --sock is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	qctx := context.Background()

	conn, err := pgx.Connect(qctx,
		fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d", *user, *db, *sockDir, *port))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(qctx)

	insertStmt := fmt.Sprintf("INSERT INTO %s(id, tenant, total) VALUES($1,$2,$3)", *table)
	updateStmt := fmt.Sprintf("UPDATE %s SET total=$2 WHERE id=$1 AND tenant=$3", *table)

	// Leaderboard mode: seed players 1..N (idempotent) and load scores.
	scores := make(map[int64]int64)
	if *players > 0 {
		for i := int64(1); i <= int64(*players); i++ {
			if _, err := conn.Exec(qctx,
				fmt.Sprintf("INSERT INTO %s(id, tenant, total) VALUES($1,$2,0) ON CONFLICT DO NOTHING", *table),
				i, *tenant); err != nil {
				log.Fatalf("producer: seeding player %d: %v", i, err)
			}
		}
		rows, err := conn.Query(qctx, fmt.Sprintf("SELECT id, COALESCE(total,0) FROM %s", *table))
		if err != nil {
			log.Fatalf("producer: loading scores: %v", err)
		}
		for rows.Next() {
			var id, total int64
			if err := rows.Scan(&id, &total); err != nil {
				rows.Close()
				log.Fatalf("producer: loading scores: %v", err)
			}
			scores[id] = total
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			log.Fatalf("producer: loading scores: %v", err)
		}
	}

	// Resume key allocation from the table so restarts never reuse an id.
	var maxKey int64
	if err := conn.QueryRow(qctx,
		fmt.Sprintf("SELECT COALESCE(MAX(id),0) FROM %s", *table)).Scan(&maxKey); err != nil {
		log.Fatalf("producer: reading max(id): %v", err)
	}
	nextID := maxKey + 1

	var throttle <-chan time.Time
	if *ops == 0 && *rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		throttle = ticker.C
	}

	var produced, skipped int64
	start := time.Now()
	lastReport := start
loop:
	for {
		if *ops > 0 && produced >= int64(*ops) {
			break
		}
		if throttle != nil {
			select {
			case <-ctx.Done():
				break loop
			case <-throttle:
			}
		} else {
			select {
			case <-ctx.Done():
				break loop
			default:
			}
		}
		if *players > 0 {
			// Leaderboard tick: only write when the score really changed.
			p := 1 + rand.Int63n(int64(*players))
			ns := scores[p] + rand.Int63n(7) - 2 // drift -2..+4
			if ns < 0 {
				ns = 0
			}
			if ns == scores[p] {
				skipped++
				continue
			}
			if _, err := conn.Exec(qctx, updateStmt, p, ns, *tenant); err != nil {
				log.Fatalf("producer: leaderboard update failed: %v", err)
			}
			scores[p] = ns
			produced++
		} else {
			produced++
			if nextID == 1 || (!*updateOnly && produced%2 == 1) {
				_, err = conn.Exec(qctx, insertStmt, nextID, *tenant, produced*10)
				nextID++
				maxKey = nextID - 1
			} else {
				_, err = conn.Exec(qctx, updateStmt, 1+rand.Int63n(maxKey), produced*100, *tenant)
			}
			if err != nil {
				log.Fatalf("producer: op %d failed: %v", produced, err)
			}
		}
		if time.Since(lastReport) >= 5*time.Second {
			fmt.Printf("producer: %d ops (%d skipped, no change) in %s\n",
				produced, skipped, time.Since(start).Round(time.Second))
			lastReport = time.Now()
		}
	}
	elapsed := time.Since(start).Round(time.Millisecond)

	if *countFile != "" {
		if err := os.WriteFile(*countFile, []byte(strconv.FormatInt(produced, 10)), 0644); err != nil {
			log.Fatalf("producer: writing count file: %v", err)
		}
	}
	fmt.Printf("producer DONE ops=%d skipped=%d in %s\n", produced, skipped, elapsed)
}
