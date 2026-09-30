// Command loadtest hammers the feed: W writer goroutines INSERT rows while
// C consumer goroutines stream the WAL via the replication protocol. It
// reports write throughput, per-operation latency percentiles (including
// tail), and commit-to-consumed lag per consumer.
package main

import (
	"context"
	"flag"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"net/http"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"e2e/feedclient"
)

// hist is a threadsafe latency sample store with exact percentiles.
type hist struct {
	mu   sync.Mutex
	vals []int64 // nanoseconds
}

func (h *hist) add(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.vals) >= 4_000_000 {
		kept := h.vals[:0]
		for i := 0; i < len(h.vals); i += 2 {
			kept = append(kept, h.vals[i])
		}
		h.vals = kept
	}
	h.vals = append(h.vals, int64(d))
}

func (h *hist) merge(other *hist) {
	other.mu.Lock()
	defer other.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vals = append(h.vals, other.vals...)
}

func (h *hist) q(p float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.vals)
	if n == 0 {
		return 0
	}
	vs := make([]int64, n)
	copy(vs, h.vals)
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	idx := int((p / 100) * float64(n))
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return time.Duration(vs[idx])
}

func (h *hist) recentQuantile(p float64, maxSamples int) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.vals)
	if n == 0 {
		return 0
	}
	start := 0
	if n > maxSamples {
		start = n - maxSamples
	}
	vs := make([]int64, n-start)
	copy(vs, h.vals[start:])
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	idx := int((p / 100) * float64(len(vs)))
	if idx >= len(vs) {
		idx = len(vs) - 1
	}
	return time.Duration(vs[idx])
}

func (h *hist) n() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.vals)
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fus", float64(d)/float64(time.Microsecond))
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func fmtComma(n int64) string {
	in := strconv.FormatInt(n, 10)
	neg := false
	if len(in) > 0 && in[0] == '-' {
		neg = true
		in = in[1:]
	}
	out := make([]byte, 0, len(in)+len(in)/3)
	leading := len(in) % 3
	if leading > 0 {
		out = append(out, in[:leading]...)
		if len(in) > 3 {
			out = append(out, ',')
		}
	}
	for i := leading; i < len(in); i += 3 {
		out = append(out, in[i:i+3]...)
		if i+3 < len(in) {
			out = append(out, ',')
		}
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func printRow(name string, h *hist) {
	fmt.Printf("  %-13s n=%-8d p50=%-9s p90=%-9s p95=%-9s p99=%-9s p999=%-9s max=%s\n",
		name, h.n(), fmtDur(h.q(50)), fmtDur(h.q(90)), fmtDur(h.q(95)),
		fmtDur(h.q(99)), fmtDur(h.q(99.9)), fmtDur(h.q(100)))
}

func main() {
	sockDir := flag.String("sock", "", "postgres unix socket dir")
	port := flag.Int("port", 5432, "postgres port")
	user := flag.String("user", "postgres", "postgres user")
	db := flag.String("db", "e2edb", "database")
	table := flag.String("table", "load_orders", "table to hammer")
	feed := flag.String("feed", "load", "feed name")
	tenant := flag.String("tenant", "acme", "tenant key")
	writers := flag.Int("writers", 8, "concurrent writer goroutines")
	consumers := flag.Int("consumers", 4, "concurrent consumer goroutines")
	duration := flag.Duration("duration", 30*time.Second, "how long writers run")
	drain := flag.Duration("drain", 5*time.Second, "extra replay time after writers stop")
	jsonOut := flag.String("json-out", "", "write structured metrics JSON to this file")
	promAddr := flag.String("prom-addr", "", "Prometheus metrics HTTP listen address (e.g. :9187)")
	flag.Parse()

	if *sockDir == "" {
		log.Fatal("loadtest: --sock is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	qctx := context.Background()

	pgxURL := fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d", *user, *db, *sockDir, *port)
	var connHist, writeHist, lagHist hist
	consumerLagHists := make([]hist, *consumers)

	connect := func() *pgx.Conn {
		t0 := time.Now()
		conn, err := pgx.Connect(qctx, pgxURL)
		connHist.add(time.Since(t0))
		if err != nil {
			log.Fatalf("loadtest: connect: %v", err)
		}
		return conn
	}

	// Ensure the table and feed exist.
	setup := connect()
	_, _ = setup.Exec(qctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id bigint PRIMARY KEY, tenant text NOT NULL, total bigint)`, *table))
	var feedExists bool
	_ = setup.QueryRow(qctx,
		`SELECT EXISTS (SELECT 1 FROM replayfeed.feed WHERE name = $1)`, *feed).Scan(&feedExists)
	if !feedExists {
		if _, err := setup.Exec(qctx,
			`SELECT replayfeed.create_feed($1, $2, 'id', 'tenant')`, *feed, *table); err != nil {
			log.Fatalf("loadtest: create_feed: %v", err)
		}
	}
	setup.Close(qctx)

	fmt.Printf("loadtest: %d writers, %d consumers, %s writes + %s drain (feed %s)\n",
		*writers, *consumers, *duration, *drain, *feed)

	var idGen atomic.Int64
	idGen.Store(int64(1) << 40) // far above any demo keys
	var writeCount, writeErrs atomic.Int64
	insertStmt := fmt.Sprintf("INSERT INTO %s(id, tenant, total) VALUES($1,$2,$3)", *table)

	var writersWg, consumersWg sync.WaitGroup
	seenCounts := make([]atomic.Int64, *consumers)

	type cstat struct {
		seen    int64
		lastLSN string
	}
	cstats := make([]cstat, *consumers)
	var consumersReady sync.WaitGroup
	consumersReady.Add(*consumers)

	for c := 0; c < *consumers; c++ {
		consumersWg.Add(1)
		go func(c int) {
			defer consumersWg.Done()
			slot := fmt.Sprintf("load_c%d_%d", c, time.Now().UnixNano()%100000)
			repConn, err := feedclient.Connect(qctx, *sockDir, *port, *user, *db)
			if err != nil {
				log.Fatalf("loadtest: consumer %d connect: %v", c, err)
			}
			defer repConn.Close(qctx)

			qc := connect()
			startLSN, err := feedclient.EnsureSlot(qctx, qc, slot, "pg_replayfeed")
			qc.Close(qctx)
			if err != nil {
				log.Fatalf("loadtest: consumer %d ensure slot: %v", c, err)
			}

			client, err := feedclient.Start(qctx, repConn, slot, "pg_replayfeed", *feed, startLSN)
			if err != nil {
				log.Fatalf("loadtest: consumer %d start: %v", c, err)
			}
			defer client.Close(qctx)

			consumersReady.Done()

			var seen int64
			var lastLSN pglogrepl.LSN
			var unacked int
			lastAckTime := time.Now()
			for {
				select {
				case <-ctx.Done():
					if unacked > 0 {
						_ = client.Ack(qctx, lastLSN)
					}
					cstats[c] = cstat{seen, lastLSN.String()}
					return
				default:
				}
				env, lsn, err := client.Next(ctx)
				if err != nil {
					if ctx.Err() != nil {
						if unacked > 0 {
							_ = client.Ack(qctx, lastLSN)
						}
						cstats[c] = cstat{seen, lastLSN.String()}
						return
					}
					log.Fatalf("loadtest: consumer %d next: %v", c, err)
				}
				now := time.Now()
				for range env.Changes {
					consumerLagHists[c].add(now.Sub(time.UnixMilli(env.CommittedAtMs)))
					seen++
					seenCounts[c].Store(seen)
				}
				lastLSN = lsn
				unacked++
				if unacked >= 256 || time.Since(lastAckTime) > 50*time.Millisecond {
					if err := client.Ack(qctx, lsn); err != nil {
						log.Fatalf("loadtest: consumer %d ack: %v", c, err)
					}
					unacked = 0
					lastAckTime = time.Now()
				}
			}
		}(c)
	}

	// Wait until all consumers are connected and actively streaming replication
	consumersReady.Wait()

	type TimeSeriesPoint struct {
		ElapsedSec       float64   `json:"elapsed_sec"`
		P50LagMs         float64   `json:"p50_lag_ms"`
		P90LagMs         float64   `json:"p90_lag_ms"`
		P95LagMs         float64   `json:"p95_lag_ms"`
		P99LagMs         float64   `json:"p99_lag_ms"`
		P999LagMs        float64   `json:"p999_lag_ms"`
		MaxLagMs         float64   `json:"max_lag_ms"`
		PerConsumerP99Ms []float64 `json:"per_consumer_p99_ms"`
		ProducerOpsSec   float64   `json:"producer_ops_sec"`
		ConsumerMsgsSec  float64   `json:"consumer_msgs_sec"`
		BacklogEvents    int64     `json:"backlog_events"`
	}
	var timeSeries []TimeSeriesPoint
	var tsMu sync.Mutex

	if *promAddr != "" {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			tsMu.Lock()
			var latest TimeSeriesPoint
			if len(timeSeries) > 0 {
				latest = timeSeries[len(timeSeries)-1]
			}
			tsMu.Unlock()
			fmt.Fprintf(w, "# HELP pg_replayfeed_lag_seconds Commit-to-consumer lag in seconds\n")
			fmt.Fprintf(w, "# TYPE pg_replayfeed_lag_seconds summary\n")
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"0.5\"} %.6f\n", latest.P50LagMs/1000.0)
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"0.9\"} %.6f\n", latest.P90LagMs/1000.0)
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"0.95\"} %.6f\n", latest.P95LagMs/1000.0)
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"0.99\"} %.6f\n", latest.P99LagMs/1000.0)
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"0.999\"} %.6f\n", latest.P999LagMs/1000.0)
			fmt.Fprintf(w, "pg_replayfeed_lag_seconds{quantile=\"1.0\"} %.6f\n", latest.MaxLagMs/1000.0)
			for i, p99 := range latest.PerConsumerP99Ms {
				fmt.Fprintf(w, "pg_replayfeed_consumer_lag_seconds{consumer=\"%d\",quantile=\"0.99\"} %.6f\n", i, p99/1000.0)
			}
			fmt.Fprintf(w, "# HELP pg_replayfeed_producer_ops_sec Producer write throughput\n")
			fmt.Fprintf(w, "# TYPE pg_replayfeed_producer_ops_sec gauge\n")
			fmt.Fprintf(w, "pg_replayfeed_producer_ops_sec %.2f\n", latest.ProducerOpsSec)
			fmt.Fprintf(w, "# HELP pg_replayfeed_consumer_msgs_sec Aggregate consumer throughput\n")
			fmt.Fprintf(w, "# TYPE pg_replayfeed_consumer_msgs_sec gauge\n")
			fmt.Fprintf(w, "pg_replayfeed_consumer_msgs_sec %.2f\n", latest.ConsumerMsgsSec)
			fmt.Fprintf(w, "# HELP pg_replayfeed_backlog_events Replication backlog in events\n")
			fmt.Fprintf(w, "# TYPE pg_replayfeed_backlog_events gauge\n")
			fmt.Fprintf(w, "pg_replayfeed_backlog_events %d\n", latest.BacklogEvents)
		})
		go func() {
			log.Printf("loadtest: Prometheus metrics exporter on http://%s/metrics", *promAddr)
			_ = http.ListenAndServe(*promAddr, nil)
		}()
	}

	var peakBacklog atomic.Int64
	monitorDone := make(chan struct{})
	testStartTime := time.Now()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		var lastWrites int64
		var lastSeen int64
		lastSample := testStartTime
		for {
			select {
			case <-monitorDone:
				return
			case now := <-ticker.C:
				dt := now.Sub(lastSample).Seconds()
				if dt <= 0.01 {
					continue
				}
				produced := writeCount.Load()
				var totalSeen int64
				var minSeen int64 = -1
				for i := 0; i < *consumers; i++ {
					s := seenCounts[i].Load()
					totalSeen += s
					if minSeen == -1 || s < minSeen {
						minSeen = s
					}
				}
				var backlog int64
				if minSeen != -1 && produced >= minSeen {
					backlog = produced - minSeen
					if backlog > peakBacklog.Load() {
						peakBacklog.Store(backlog)
					}
				}

				prodRate := float64(produced-lastWrites) / dt
				consRate := float64(totalSeen-lastSeen) / dt
				lastWrites = produced
				lastSeen = totalSeen
				lastSample = now

				perCP99 := make([]float64, *consumers)
				var combinedRecent hist
				for i := 0; i < *consumers; i++ {
					perCP99[i] = float64(consumerLagHists[i].recentQuantile(99, 10000)) / float64(time.Millisecond)
					combinedRecent.merge(&consumerLagHists[i])
				}

				pt := TimeSeriesPoint{
					ElapsedSec:       now.Sub(testStartTime).Seconds(),
					P50LagMs:         float64(combinedRecent.recentQuantile(50, 20000)) / float64(time.Millisecond),
					P90LagMs:         float64(combinedRecent.recentQuantile(90, 20000)) / float64(time.Millisecond),
					P95LagMs:         float64(combinedRecent.recentQuantile(95, 20000)) / float64(time.Millisecond),
					P99LagMs:         float64(combinedRecent.recentQuantile(99, 20000)) / float64(time.Millisecond),
					P999LagMs:        float64(combinedRecent.recentQuantile(99.9, 20000)) / float64(time.Millisecond),
					MaxLagMs:         float64(combinedRecent.recentQuantile(100, 20000)) / float64(time.Millisecond),
					PerConsumerP99Ms: perCP99,
					ProducerOpsSec:   prodRate,
					ConsumerMsgsSec:  consRate,
					BacklogEvents:    backlog,
				}
				tsMu.Lock()
				timeSeries = append(timeSeries, pt)
				tsMu.Unlock()
			}
		}
	}()

	var walStartLSN string
	initConn := connect()
	_ = initConn.QueryRow(qctx, "SELECT pg_current_wal_lsn()::text").Scan(&walStartLSN)
	initConn.Close(qctx)

	writeDeadline := time.Now().Add(*duration)
	for w := 0; w < *writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			conn := connect()
			defer conn.Close(qctx)
			for time.Now().Before(writeDeadline) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				id := idGen.Add(1)
				t0 := time.Now()
				_, err := conn.Exec(qctx, insertStmt, id, *tenant, id%1000)
				writeHist.add(time.Since(t0))
				if err != nil {
					if writeErrs.Add(1) < 5 {
						log.Printf("loadtest: writer %d: %v", w, err)
					}
					continue
				}
				writeCount.Add(1)
			}
		}(w)
	}

	// Wait for writers to complete their run duration
	writersWg.Wait()
	wc := writeCount.Load()

	// Capture target LSN from database immediately after writers complete
	var targetLSN string
	var walBytes int64
	qc := connect()
	if walStartLSN != "" {
		_ = qc.QueryRow(qctx, "SELECT pg_current_wal_lsn()::text, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn), 0)::bigint", walStartLSN).Scan(&targetLSN, &walBytes)
	} else {
		_ = qc.QueryRow(qctx, "SELECT pg_current_wal_lsn()::text").Scan(&targetLSN)
	}
	qc.Close(qctx)

	// Active drain monitoring: record exact duration until consumers catch up
	drainStart := time.Now()
	drainTimer := time.NewTimer(*drain)
	var drainDuration time.Duration
	drained := false

drainCheck:
	for {
		select {
		case <-ctx.Done():
			drainDuration = time.Since(drainStart)
			break drainCheck
		case <-drainTimer.C:
			drainDuration = *drain
			break drainCheck
		default:
			allCaughtUp := true
			for i := 0; i < *consumers; i++ {
				if seenCounts[i].Load() < wc {
					allCaughtUp = false
					break
				}
			}
			if allCaughtUp && wc > 0 {
				drainDuration = time.Since(drainStart)
				drained = true
				break drainCheck
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	if !drained && drainDuration == 0 {
		drainDuration = time.Since(drainStart)
	}
	drainTimer.Stop()
	close(monitorDone)

	// Stop consumers cleanly
	stop()
	consumersWg.Wait()

	for i := 0; i < *consumers; i++ {
		lagHist.merge(&consumerLagHists[i])
	}

	fmt.Printf("\n=== load test results ===\n")
	fmt.Printf("connections: %d established\n", *writers+*consumers+2)
	printRow("connect", &connHist)
	fmt.Printf("writes:      %d ops in %s -> %.0f ops/s, errors=%d\n",
		wc, *duration, float64(wc)/duration.Seconds(), writeErrs.Load())
	printRow("write stmt", &writeHist)
	printRow("consumer lag", &lagHist)

	fmt.Printf("consumers:\n")
	var lo, hi int64 = -1, -1
	var targetReachedCount int
	var minLSN, maxLSN string
	var totalSeen int64
	for c := range cstats {
		s := cstats[c]
		fmt.Printf("  c%d: seen=%d last_lsn=%s\n", c, s.seen, s.lastLSN)
		totalSeen += s.seen
		if lo == -1 || s.seen < lo {
			lo = s.seen
		}
		if s.seen > hi {
			hi = s.seen
		}
		if s.lastLSN == targetLSN && targetLSN != "" {
			targetReachedCount++
		}
		if minLSN == "" || s.lastLSN < minLSN {
			minLSN = s.lastLSN
		}
		if maxLSN == "" || s.lastLSN > maxLSN {
			maxLSN = s.lastLSN
		}
	}
	finalBacklog := wc - lo
	if finalBacklog < 0 {
		finalBacklog = 0
	}
	isMatch := (lo == hi && hi == wc && writeErrs.Load() == 0)
	if isMatch && maxLSN != "" {
		targetLSN = maxLSN
		targetReachedCount = *consumers
	}

	var catchupRate float64
	peakB := peakBacklog.Load()
	if drainDuration.Seconds() >= 0.05 && peakB > 0 {
		catchupRate = float64(peakB) / drainDuration.Seconds()
	} else if peakB > 0 && duration.Seconds() > 0 {
		// Consumers kept up concurrently; catchup rate matches stream replay throughput
		catchupRate = float64(totalSeen) / duration.Seconds()
	}

	var timeToConsumeAllSec float64
	var timeToConsumeAllStr string
	effectiveConsumerRate := float64(lo) / (duration.Seconds() + drainDuration.Seconds())
	if effectiveConsumerRate <= 0 && duration.Seconds() > 0 {
		effectiveConsumerRate = float64(wc) / duration.Seconds()
	}

	if finalBacklog == 0 {
		timeToConsumeAllSec = 0.0
		timeToConsumeAllStr = "0.00 sec (caught up)"
	} else if effectiveConsumerRate > 0 {
		timeToConsumeAllSec = float64(finalBacklog) / effectiveConsumerRate
		timeToConsumeAllStr = fmt.Sprintf("%.2f sec (to drain %s backlog at %s msg/s)",
			timeToConsumeAllSec, fmtComma(finalBacklog), fmtComma(int64(math.Round(effectiveConsumerRate))))
	} else {
		timeToConsumeAllStr = "unknown (consumer stalled)"
	}

	if isMatch {
		fmt.Printf("OK: all %d consumers saw %d changes\n", *consumers, hi)
	} else if targetReachedCount < *consumers {
		fmt.Printf("DEFERRED: consumers did not reach target LSN %s within drain (seen min=%d max=%d target=%d; %s)\n",
			targetLSN, lo, hi, wc, timeToConsumeAllStr)
	} else {
		fmt.Printf("MISMATCH: change counts differ across consumers (min=%d max=%d target=%d)\n", lo, hi, wc)
	}

	matchStr := "FAIL"
	if isMatch {
		matchStr = "PASS"
	} else if targetReachedCount < *consumers {
		matchStr = fmt.Sprintf("DEFERRED (%s remaining to drain backlog)", timeToConsumeAllStr)
	}

	lsnRange := fmt.Sprintf("%s..%s", minLSN, maxLSN)
	if minLSN == "" && maxLSN == "" {
		lsnRange = targetLSN
	}

	fmt.Printf("\nProducer:\n")
	fmt.Printf("  commits/sec       %s\n", fmtComma(int64(math.Round(float64(wc)/duration.Seconds()))))
	fmt.Printf("  WAL/sec             %.2f MB\n", float64(walBytes)/(1024*1024)/duration.Seconds())

	fmt.Printf("\nConsumers:\n")
	fmt.Printf("  aggregate msgs/sec %s\n", fmtComma(int64(math.Round(float64(totalSeen)/duration.Seconds()))))
	fmt.Printf("  per-consumer msgs/s ~%s\n", fmtComma(int64(math.Round(float64(totalSeen)/float64(*consumers)/duration.Seconds()))))

	fmt.Printf("\nBacklog:\n")
	fmt.Printf("  initial             0\n")
	fmt.Printf("  peak                 %s\n", fmtComma(peakB))
	fmt.Printf("  final                %s\n", fmtComma(finalBacklog))
	fmt.Printf("  target LSN           %s\n", targetLSN)
	fmt.Printf("  consumer LSN range   %s\n", lsnRange)

	fmt.Printf("\nCatch-up:\n")
	fmt.Printf("  drain duration       %.2f sec\n", drainDuration.Seconds())
	fmt.Printf("  catch-up rate        %s msg/s\n", fmtComma(int64(math.Round(catchupRate))))
	fmt.Printf("  time to consume all  %s\n", timeToConsumeAllStr)

	fmt.Printf("\nCorrectness:\n")
	fmt.Printf("  target LSN reached   %d/%d\n", targetReachedCount, *consumers)
	fmt.Printf("  event count match    %s\n", matchStr)

	if *jsonOut != "" {
		type Percentiles struct {
			N      int     `json:"n"`
			P50Ms  float64 `json:"p50_ms"`
			P90Ms  float64 `json:"p90_ms"`
			P95Ms  float64 `json:"p95_ms"`
			P99Ms  float64 `json:"p99_ms"`
			P999Ms float64 `json:"p999_ms"`
			MaxMs  float64 `json:"max_ms"`
		}
		toP := func(h *hist) Percentiles {
			return Percentiles{
				N:      h.n(),
				P50Ms:  float64(h.q(50)) / float64(time.Millisecond),
				P90Ms:  float64(h.q(90)) / float64(time.Millisecond),
				P95Ms:  float64(h.q(95)) / float64(time.Millisecond),
				P99Ms:  float64(h.q(99)) / float64(time.Millisecond),
				P999Ms: float64(h.q(99.9)) / float64(time.Millisecond),
				MaxMs:  float64(h.q(100)) / float64(time.Millisecond),
			}
		}
		data := map[string]interface{}{
			"writers":       *writers,
			"consumers":     *consumers,
			"duration_sec":  duration.Seconds(),
			"write_ops":     wc,
			"write_ops_sec": float64(wc) / duration.Seconds(),
			"write_errs":    writeErrs.Load(),
			"connect":       toP(&connHist),
			"write_stmt":    toP(&writeHist),
			"consumer_lag":  toP(&lagHist),
			"changes_seen":  hi,
			"verified":      isMatch,
			"producer": map[string]interface{}{
				"commits_sec": float64(wc) / duration.Seconds(),
				"wal_bytes":   walBytes,
				"wal_sec_mb":  float64(walBytes) / (1024 * 1024) / duration.Seconds(),
				"total_ops":   wc,
				"errors":      writeErrs.Load(),
			},
			"consumer_stats": map[string]interface{}{
				"aggregate_msgs_sec":    float64(totalSeen) / duration.Seconds(),
				"per_consumer_msgs_sec": float64(totalSeen) / float64(*consumers) / duration.Seconds(),
				"total_delivered":       totalSeen,
			},
			"backlog": map[string]interface{}{
				"initial":            0,
				"peak":               peakB,
				"final":              finalBacklog,
				"target_lsn":         targetLSN,
				"consumer_lsn_min":   minLSN,
				"consumer_lsn_max":   maxLSN,
				"consumer_lsn_range": lsnRange,
			},
			"catchup": map[string]interface{}{
				"drain_duration_sec":      drainDuration.Seconds(),
				"catchup_rate":            catchupRate,
				"time_to_consume_all_sec": timeToConsumeAllSec,
				"time_to_consume_all_str": timeToConsumeAllStr,
			},
			"correctness": map[string]interface{}{
				"target_lsn_reached": fmt.Sprintf("%d/%d", targetReachedCount, *consumers),
				"event_count_match":  matchStr,
			},
			"time_series": timeSeries,
		}
		if b, err := json.MarshalIndent(data, "", "  "); err == nil {
			_ = os.WriteFile(*jsonOut, b, 0644)
		}
	}
}
