// Package replayfeed provides a high-performance Go client for streaming
// change envelopes from PostgreSQL using the pg_replayfeed logical-decoding
// output plugin.
//
// pg_replayfeed streams transaction-atomic, key-only envelopes directly out
// of the PostgreSQL Write-Ahead Log (WAL) over the replication protocol.
// There are zero triggers on the write path, zero NOTIFY messages, zero polling,
// and zero auxiliary event tables.
package replayfeed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Op represents the type of mutation in a change record.
type Op string

const (
	OpInsert Op = "I"
	OpUpdate Op = "U"
	OpDelete Op = "D"
)

// LSN is a 64-bit integer representing a PostgreSQL Log Sequence Number.
type LSN = pglogrepl.LSN


// Change represents a single row key mutation within an envelope.
type Change struct {
	Feed         string  `json:"feed"`
	Op           Op      `json:"op"` // "I", "U", or "D"
	Key          string  `json:"key"`
	Tenant       *string `json:"tenant,omitempty"`
	OldTenant    *string `json:"old_tenant,omitempty"`
	TenantBuf    string  `json:"-"`
	OldTenantBuf string  `json:"-"`
}

// Envelope is the transaction-atomic change envelope emitted by pg_replayfeed.
// Each committed transaction touching one or more registered feeds emits exactly
// one envelope in strict commit order.
type Envelope struct {
	// Cursor is the transaction commit LSN (e.g. "0/16B1978").
	Cursor string `json:"cursor"`
	// CommittedAtMs is the Unix epoch timestamp in milliseconds when the transaction committed.
	CommittedAtMs int64 `json:"committed_at_ms"`
	// Changes is the list of key mutations in this transaction.
	Changes []Change `json:"changes"`
	// Truncate lists feeds that were truncated in this transaction (resnapshot marker).
	Truncate []string `json:"truncate"`
}

// Config specifies the connection and subscription options for Client.
type Config struct {
	// Connection string in standard PostgreSQL URI format (e.g. postgres://user:pass@host:5432/db).
	// If set, individual host/port/user fields below are overridden.
	ConnString string

	// Discrete connection parameters (used if ConnString is empty).
	Host      string
	Port      int
	User      string
	Password  string
	Database  string
	SocketDir string

	// SlotName is the PostgreSQL logical replication slot name.
	// If empty, defaults to "replayfeed_slot".
	SlotName string

	// Plugin is the output plugin name (defaults to "pg_replayfeed").
	Plugin string

	// Feed is the optional feed name filter. If non-empty, only changes
	// belonging to this feed are streamed. If empty, all feeds stream.
	Feed string

	// StartLSN is the optional initial LSN from which to begin streaming.
	// If 0, the slot's confirmed flush position (or creation point) is used.
	StartLSN pglogrepl.LSN

	// CreateSlotIfMissing automatically creates the logical slot if it does not exist.
	// Defaults to true.
	CreateSlotIfMissing bool

	// StandbyTimeout is the interval between periodic standby status updates
	// sent to PostgreSQL when idle. Defaults to 10 seconds.
	StandbyTimeout time.Duration
}

// Client connects to PostgreSQL over the replication protocol and streams
// pg_replayfeed envelopes.
type Client struct {
	cfg       Config
	repConn   *pgconn.PgConn
	slotLSN   pglogrepl.LSN
	lastLSN   pglogrepl.LSN
	standby   time.Time
	cachedEnv Envelope
}

// ParseLSN converts an LSN string (e.g. "0/16B1978") into a pglogrepl.LSN.
func ParseLSN(s string) (pglogrepl.LSN, error) {
	return pglogrepl.ParseLSN(strings.TrimSpace(s))
}

// FormatLSN converts a pglogrepl.LSN into its string representation (e.g. "0/16B1978").
func FormatLSN(lsn pglogrepl.LSN) string {
	return lsn.String()
}

// New connects to PostgreSQL, ensures the logical replication slot exists,
// starts streaming, and returns an initialized Client.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.SlotName == "" {
		cfg.SlotName = "replayfeed_slot"
	}
	if cfg.Plugin == "" {
		cfg.Plugin = "pg_replayfeed"
	}
	if cfg.StandbyTimeout <= 0 {
		cfg.StandbyTimeout = 10 * time.Second
	}

	queryConnStr, repConnStr, err := buildConnStrings(cfg)
	if err != nil {
		return nil, fmt.Errorf("replayfeed: build connection string: %w", err)
	}

	// 1. Ensure or check the slot via a standard query connection.
	startLSN := cfg.StartLSN
	if cfg.CreateSlotIfMissing || startLSN == 0 {
		qConn, err := pgx.Connect(ctx, queryConnStr)
		if err != nil {
			return nil, fmt.Errorf("replayfeed: admin connect: %w", err)
		}
		defer qConn.Close(ctx)

		slotLSN, err := EnsureSlot(ctx, qConn, cfg.SlotName, cfg.Plugin)
		if err != nil {
			return nil, fmt.Errorf("replayfeed: ensure slot: %w", err)
		}
		if startLSN == 0 || startLSN < slotLSN {
			startLSN = slotLSN
		}
	}

	// 2. Connect with replication=database.
	rConn, err := pgconn.Connect(ctx, repConnStr)
	if err != nil {
		return nil, fmt.Errorf("replayfeed: replication connect: %w", err)
	}

	// 3. START_REPLICATION
	opts := pglogrepl.StartReplicationOptions{PluginArgs: []string{}}
	if cfg.Feed != "" {
		opts.PluginArgs = append(opts.PluginArgs, fmt.Sprintf("feed '%s'", strings.ReplaceAll(cfg.Feed, "'", "''")))
	}

	if err := pglogrepl.StartReplication(ctx, rConn, cfg.SlotName, startLSN, opts); err != nil {
		_ = rConn.Close(ctx)
		return nil, fmt.Errorf("replayfeed: start replication: %w", err)
	}

	return &Client{
		cfg:     cfg,
		repConn: rConn,
		slotLSN: startLSN,
		lastLSN: startLSN,
		standby: time.Now(),
	}, nil
}

// EnsureSlot creates the replication slot if it does not already exist and
// returns the starting LSN (confirmed_flush_lsn or new consistent point).
func EnsureSlot(ctx context.Context, conn *pgx.Conn, slot, plugin string) (pglogrepl.LSN, error) {
	var lsnStr string
	err := conn.QueryRow(ctx,
		"SELECT pg_create_logical_replication_slot($1, $2)::text", slot, plugin).Scan(&lsnStr)
	if err == nil {
		// New slot created: "(slotname,0/1234)" -> extract LSN
		lsnStr = strings.Trim(lsnStr, "()")
		if i := strings.LastIndex(lsnStr, ","); i >= 0 {
			lsnStr = lsnStr[i+1:]
		}
		lsn, perr := pglogrepl.ParseLSN(lsnStr)
		if perr != nil {
			return 0, fmt.Errorf("parse new slot LSN %q: %w", lsnStr, perr)
		}
		return lsn, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "42710" {
		// 42710 = duplicate_object (slot already exists); anything else is fatal
		return 0, fmt.Errorf("create slot %q: %w", slot, err)
	}

	// Slot exists: retrieve its confirmed flush position
	var confirmed *string
	err = conn.QueryRow(ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&confirmed)
	if err != nil || confirmed == nil {
		return 0, fmt.Errorf("read confirmed LSN for slot %q: %w", slot, err)
	}

	lsn, err := pglogrepl.ParseLSN(*confirmed)
	if err != nil {
		return 0, fmt.Errorf("parse confirmed LSN %q: %w", *confirmed, err)
	}
	return lsn, nil
}

// DropSlot removes the logical replication slot.
func DropSlot(ctx context.Context, conn *pgx.Conn, slot string) error {
	_, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err != nil {
		return fmt.Errorf("drop slot %q: %w", slot, err)
	}
	return nil
}

// Next waits for and returns the next change envelope from PostgreSQL.
// It transparently handles server keepalive pings and maintains standby liveness.
//
// The returned LSN should be acknowledged via Ack(ctx, lsn) once the envelope
// has been durably applied.
func (c *Client) Next(ctx context.Context) (*Envelope, pglogrepl.LSN, error) {
	for {
		msg, err := c.repConn.ReceiveMessage(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("receive replication message: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					return nil, 0, fmt.Errorf("parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					if err := c.sendStatus(ctx, pkm.ServerWALEnd); err != nil {
						return nil, 0, err
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return nil, 0, fmt.Errorf("parse xlog: %w", err)
				}
				if !parseEnvelopeFast(xld.WALData, &c.cachedEnv) {
					if err := json.Unmarshal(xld.WALData, &c.cachedEnv); err != nil {
						return nil, 0, fmt.Errorf("decode envelope JSON: %w", err)
					}
				}
				lsn, err := pglogrepl.ParseLSN(c.cachedEnv.Cursor)
				if err != nil {
					return nil, 0, fmt.Errorf("parse cursor LSN %q: %w", c.cachedEnv.Cursor, err)
				}
				c.lastLSN = lsn
				return &c.cachedEnv, lsn, nil
			}
		}

		if time.Since(c.standby) > c.cfg.StandbyTimeout {
			if err := c.sendStatus(ctx, c.lastLSN); err != nil {
				return nil, 0, err
			}
		}
	}
}

// Ack sends a standby status update to PostgreSQL confirming that all WAL
// up to lsn has been flushed. This allows PostgreSQL to advance the replication
// slot and prune WAL files on disk.
func (c *Client) Ack(ctx context.Context, lsn pglogrepl.LSN) error {
	return c.sendStatus(ctx, lsn)
}

// Stream executes an ingestion loop, invoking handler for each received envelope.
// If handler returns nil, the envelope's commit LSN is automatically acknowledged.
// If handler returns an error, the loop halts and returns the error without acknowledging.
func (c *Client) Stream(ctx context.Context, handler func(env *Envelope, lsn pglogrepl.LSN) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		env, lsn, err := c.Next(ctx)
		if err != nil {
			return err
		}

		if err := handler(env, lsn); err != nil {
			return fmt.Errorf("handler failed on LSN %s: %w", lsn, err)
		}

		if err := c.Ack(ctx, lsn); err != nil {
			return fmt.Errorf("ack failed on LSN %s: %w", lsn, err)
		}
	}
}

// LastLSN returns the LSN of the most recently received envelope.
func (c *Client) LastLSN() pglogrepl.LSN {
	return c.lastLSN
}

// Close terminates the replication connection.
func (c *Client) Close(ctx context.Context) error {
	if c.repConn != nil {
		return c.repConn.Close(ctx)
	}
	return nil
}

func (c *Client) sendStatus(ctx context.Context, lsn pglogrepl.LSN) error {
	c.standby = time.Now()
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.repConn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			WALFlushPosition: lsn,
			WALApplyPosition: lsn,
			ClientTime:       c.standby,
		})
}

func buildConnStrings(cfg Config) (queryStr string, repStr string, err error) {
	if cfg.ConnString != "" {
		u, err := url.Parse(cfg.ConnString)
		if err != nil {
			return "", "", fmt.Errorf("invalid connection string: %w", err)
		}
		q := u.Query()
		q.Del("replication")
		u.RawQuery = q.Encode()
		queryStr = u.String()

		q.Set("replication", "database")
		u.RawQuery = q.Encode()
		repStr = u.String()
		return queryStr, repStr, nil
	}

	host := cfg.Host
	if cfg.SocketDir != "" {
		host = cfg.SocketDir
	}
	if host == "" {
		host = "localhost"
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	user := cfg.User
	if user == "" {
		user = "postgres"
	}
	db := cfg.Database
	if db == "" {
		db = "postgres"
	}

	var userPass string
	if cfg.Password != "" {
		userPass = fmt.Sprintf("%s:%s@", url.QueryEscape(user), url.QueryEscape(cfg.Password))
	} else {
		userPass = fmt.Sprintf("%s@", url.QueryEscape(user))
	}

	if cfg.SocketDir != "" {
		queryStr = fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d",
			url.QueryEscape(user), url.PathEscape(db), url.QueryEscape(cfg.SocketDir), port)
		repStr = fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d&replication=database",
			url.QueryEscape(user), url.PathEscape(db), url.QueryEscape(cfg.SocketDir), port)
	} else {
		queryStr = fmt.Sprintf("postgres://%s%s:%d/%s", userPass, host, port, url.PathEscape(db))
		repStr = fmt.Sprintf("postgres://%s%s:%d/%s?replication=database", userPass, host, port, url.PathEscape(db))
	}

	return queryStr, repStr, nil
}

// parseEnvelopeFast parses standard pg_replayfeed JSON directly without reflection.
func parseEnvelopeFast(data []byte, env *Envelope) bool {
	cIdx := bytes.Index(data, []byte(`"cursor":"`))
	if cIdx < 0 {
		return false
	}
	cStart := cIdx + 10
	cEnd := bytes.IndexByte(data[cStart:], '"')
	if cEnd < 0 {
		return false
	}
	env.Cursor = string(data[cStart : cStart+cEnd])

	msIdx := bytes.Index(data, []byte(`"committed_at_ms":`))
	if msIdx < 0 {
		return false
	}
	msStart := msIdx + 18
	var ms int64
	i := msStart
	for i < len(data) && data[i] >= '0' && data[i] <= '9' {
		ms = ms*10 + int64(data[i]-'0')
		i++
	}
	if i == msStart {
		return false
	}
	env.CommittedAtMs = ms

	chIdx := bytes.Index(data, []byte(`"changes":[`))
	if chIdx < 0 {
		return false
	}
	chStart := chIdx + 11
	chEnd := bytes.IndexByte(data[chStart:], ']')
	if chEnd < 0 {
		return false
	}
	chBytes := data[chStart : chStart+chEnd]

	nChanges := bytes.Count(chBytes, []byte{'{'})
	if cap(env.Changes) >= nChanges {
		env.Changes = env.Changes[:nChanges]
	} else {
		env.Changes = make([]Change, nChanges)
	}

	pos := 0
	for j := 0; j < nChanges; j++ {
		start := bytes.IndexByte(chBytes[pos:], '{')
		if start < 0 {
			break
		}
		start += pos
		end := bytes.IndexByte(chBytes[start:], '}')
		if end < 0 {
			break
		}
		end += start
		obj := chBytes[start+1 : end]
		pos = end + 1

		env.Changes[j].Tenant = nil
		env.Changes[j].OldTenant = nil

		if fIdx := bytes.Index(obj, []byte(`"feed":"`)); fIdx >= 0 {
			fStart := fIdx + 8
			if fEnd := bytes.IndexByte(obj[fStart:], '"'); fEnd >= 0 {
				env.Changes[j].Feed = string(obj[fStart : fStart+fEnd])
			}
		}
		if opIdx := bytes.Index(obj, []byte(`"op":"`)); opIdx >= 0 && opIdx+6 < len(obj) {
			env.Changes[j].Op = Op(obj[opIdx+6 : opIdx+7])
		}
		if kIdx := bytes.Index(obj, []byte(`"key":"`)); kIdx >= 0 {
			kStart := kIdx + 7
			if kEnd := bytes.IndexByte(obj[kStart:], '"'); kEnd >= 0 {
				env.Changes[j].Key = string(obj[kStart : kStart+kEnd])
			}
		}
		if tIdx := bytes.Index(obj, []byte(`"tenant":"`)); tIdx >= 0 {
			tStart := tIdx + 10
			if tEnd := bytes.IndexByte(obj[tStart:], '"'); tEnd >= 0 {
				env.Changes[j].TenantBuf = string(obj[tStart : tStart+tEnd])
				env.Changes[j].Tenant = &env.Changes[j].TenantBuf
			}
		}
		if otIdx := bytes.Index(obj, []byte(`"old_tenant":"`)); otIdx >= 0 {
			otStart := otIdx + 14
			if otEnd := bytes.IndexByte(obj[otStart:], '"'); otEnd >= 0 {
				env.Changes[j].OldTenantBuf = string(obj[otStart : otStart+otEnd])
				env.Changes[j].OldTenant = &env.Changes[j].OldTenantBuf
			}
		}
	}

	// Truncate array
	env.Truncate = env.Truncate[:0]
	trIdx := bytes.Index(data, []byte(`"truncate":[`))
	if trIdx >= 0 {
		trStart := trIdx + 12
		if trEnd := bytes.IndexByte(data[trStart:], ']'); trEnd >= 0 {
			trBytes := data[trStart : trStart+trEnd]
			for len(trBytes) > 0 {
				q1 := bytes.IndexByte(trBytes, '"')
				if q1 < 0 {
					break
				}
				q2 := bytes.IndexByte(trBytes[q1+1:], '"')
				if q2 < 0 {
					break
				}
				feedName := string(trBytes[q1+1 : q1+1+q2])
				env.Truncate = append(env.Truncate, feedName)
				trBytes = trBytes[q1+1+q2+1:]
			}
		}
	}

	return true
}

func parsePort(s string) int {
	p, err := strconv.Atoi(s)
	if err != nil {
		return 5432
	}
	return p
}
