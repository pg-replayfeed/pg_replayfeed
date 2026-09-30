// Package feedclient streams pg_replayfeed change envelopes over the
// PostgreSQL replication protocol. pg_replayfeed 2.0 is a logical-decoding
// output plugin: there is no polling and no NOTIFY. Consumers open a
// replication connection, START_REPLICATION on their slot, receive one JSON
// envelope per committed transaction, apply it atomically, and acknowledge
// the commit LSN so the slot can advance.
package feedclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Change is one row change inside an envelope.
type Change struct {
	Feed         string  `json:"feed"`
	Op           string  `json:"op"` // "I", "U" or "D"
	Key          string  `json:"key"`
	Tenant       *string `json:"tenant,omitempty"`
	OldTenant    *string `json:"old_tenant,omitempty"`
	TenantBuf    string  `json:"-"`
	OldTenantBuf string  `json:"-"`
}

// Envelope is the single message the output plugin emits per relevant
// committed transaction. Cursor is the commit LSN, e.g. "0/16B1978": the
// consumer checkpoints it only after the envelope is durably applied.
type Envelope struct {
	Cursor        string   `json:"cursor"`
	CommittedAtMs int64    `json:"committed_at_ms"`
	Changes       []Change `json:"changes"`
	Truncate      []string `json:"truncate"`
}

// Client is a replication-protocol consumer for one slot.
type Client struct {
	conn    *pgconn.PgConn
	slot    string
	plugin  string
	feed    string // empty = all feeds
	lastLSN pglogrepl.LSN
	standby time.Time
	env     Envelope
}

// Connect opens a replication connection over a unix socket directory.
// Uses replication=database (not replication=true): logical decoding
// requires a database connection.
func Connect(ctx context.Context, sockDir string, port int, user, db string) (*pgconn.PgConn, error) {
	url := fmt.Sprintf("postgres://%s@/%s?host=%s&port=%d&replication=database",
		user, db, sockDir, port)
	conn, err := pgconn.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("feedclient: connect: %w", err)
	}
	return conn, nil
}

// EnsureSlot creates the logical slot if it does not exist yet and returns
// the LSN to start streaming from: the slot's confirmed flush position for
// an existing slot, or the current WAL position for a new one.
func EnsureSlot(ctx context.Context, conn *pgx.Conn, slot, plugin string) (pglogrepl.LSN, error) {
	var lsnStr string
	err := conn.QueryRow(ctx,
		"SELECT pg_create_logical_replication_slot($1, $2)::text", slot, plugin).Scan(&lsnStr)
	if err == nil {
		// Created: "(slotname,0/1234)" -> parse the LSN part.
		lsnStr = strings.Trim(lsnStr, "()")
		if i := strings.LastIndex(lsnStr, ","); i >= 0 {
			lsnStr = lsnStr[i+1:]
		}
		lsn, perr := pglogrepl.ParseLSN(lsnStr)
		if perr != nil {
			return 0, fmt.Errorf("feedclient: parse new slot LSN %q: %w", lsnStr, perr)
		}
		return lsn, nil
	}
	// If the slot already exists (SQLSTATE 42710), resume from its confirmed
	// position. Any other error (wal_level, max slots, plugin load) is fatal:
	// return it instead of masking it with "no rows in result set".
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "42710" {
		return 0, fmt.Errorf("feedclient: create slot %q: %w", slot, err)
	}
	// Already exists: resume from its confirmed position.
	var confirmed *string
	err = conn.QueryRow(ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&confirmed)
	if err != nil || confirmed == nil {
		return 0, fmt.Errorf("feedclient: ensure slot %q: %w", slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(*confirmed)
	if err != nil {
		return 0, fmt.Errorf("feedclient: parse confirmed LSN %q: %w", *confirmed, err)
	}
	return lsn, nil
}

// Start begins streaming from the given LSN. The caller must have created
// (or ensured) the slot; startLSN is typically the last checkpointed cursor,
// or the slot's consistent point for a fresh consumer.
func Start(ctx context.Context, conn *pgconn.PgConn, slot, plugin, feed string, startLSN pglogrepl.LSN) (*Client, error) {
	opts := pglogrepl.StartReplicationOptions{PluginArgs: []string{}}
	if feed != "" {
		opts.PluginArgs = append(opts.PluginArgs, fmt.Sprintf("feed '%s'", strings.ReplaceAll(feed, "'", "''")))
	}
	if err := pglogrepl.StartReplication(ctx, conn, slot, startLSN, opts); err != nil {
		return nil, fmt.Errorf("feedclient: start replication: %w", err)
	}
	return &Client{conn: conn, slot: slot, plugin: plugin, feed: feed, lastLSN: startLSN}, nil
}

// Next blocks for the next envelope. It answers server keepalives and sends
// periodic standby status updates. The returned LSN is the envelope's commit
// LSN: acknowledge it via Ack only after the envelope is durably applied.
func (c *Client) Next(ctx context.Context) (*Envelope, pglogrepl.LSN, error) {
	for {
		msg, err := c.conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("feedclient: receive: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					return nil, 0, fmt.Errorf("feedclient: parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					if err := c.sendStatus(ctx, pkm.ServerWALEnd); err != nil {
						return nil, 0, err
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return nil, 0, fmt.Errorf("feedclient: parse xlog: %w", err)
				}
				if !parseEnvelopeFast(xld.WALData, &c.env) {
					if err := json.Unmarshal(xld.WALData, &c.env); err != nil {
						return nil, 0, fmt.Errorf("feedclient: decode envelope: %w", err)
					}
				}
				lsn, err := pglogrepl.ParseLSN(c.env.Cursor)
				if err != nil {
					return nil, 0, fmt.Errorf("feedclient: parse cursor %q: %w", c.env.Cursor, err)
				}
				c.lastLSN = lsn
				return &c.env, lsn, nil
			}
		case *pgproto3.NotificationResponse:
			// Not used by pg_replayfeed 2.0; ignore.
		}
		// Periodic status update so the server knows we are alive.
		if time.Since(c.standby) > 10*time.Second {
			if err := c.sendStatus(ctx, c.lastLSN); err != nil {
				return nil, 0, err
			}
		}
	}
}

// Ack tells the server the envelope at lsn is durably applied; the slot's
// confirmed position advances and WAL before it may be recycled.
func (c *Client) Ack(ctx context.Context, lsn pglogrepl.LSN) error {
	return c.sendStatus(ctx, lsn)
}

func (c *Client) sendStatus(ctx context.Context, lsn pglogrepl.LSN) error {
	c.standby = time.Now()
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			WALFlushPosition: lsn,
			WALApplyPosition: lsn,
			ClientTime:       c.standby,
		})
}

// Close terminates the replication connection.
func (c *Client) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// ParseCursor parses a "0/16B1978" cursor into an LSN.
func ParseCursor(s string) (pglogrepl.LSN, error) {
	return pglogrepl.ParseLSN(s)
}

// parseEnvelopeFast parses standard pg_replayfeed JSON without reflection or per-message allocs.
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
			env.Changes[j].Op = string(obj[opIdx+6 : opIdx+7])
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
	return true
}
