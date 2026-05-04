package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/orware/sluice/internal/ir"
)

// cdcChannelBuffer is the number of [ir.Change] events buffered before
// the pump goroutine blocks. Same value as the MySQL CDC reader.
const cdcChannelBuffer = 256

// keepaliveInterval is the cadence of [pglogrepl.SendStandbyStatusUpdate]
// calls when the server isn't actively sending. The Postgres default
// for [wal_sender_timeout] is 60s, so 10s leaves comfortable headroom
// while keeping slot advancement responsive.
const keepaliveInterval = 10 * time.Second

// defaultPublication and defaultSlot are the names sluice uses for its
// publication and replication slot when no override is supplied. They
// are deliberately short and namespaced so collision with other tools
// is unlikely.
const (
	defaultPublication = "sluice_pub"
	defaultSlot        = "sluice_slot"
)

// CDCReader streams Postgres logical-replication output as ir.Change
// events. It implements [ir.CDCReader].
//
// One reader → one [StreamChanges] call. Concurrent calls are not
// supported. The reader holds two distinct connections under the
// hood: a regular *sql.DB pool used for ordinary SQL (precondition
// queries, publication management) and a pgconn.PgConn opened in
// replication=database mode for the streaming protocol itself. The
// replication connection cannot be reused for normal queries — it's
// a one-way pipe once START_REPLICATION runs.
//
// The replication connection is owned exclusively by the pump
// goroutine; keepalive sends and message reads happen on the same
// goroutine, with the [pgconn.PgConn.ReceiveMessage] deadline
// driving the keepalive cadence (the canonical pglogrepl pattern).
// That keeps concurrent access off the connection without a mutex.
type CDCReader struct {
	// db is the standard pool used for precondition queries and
	// for the one-time CREATE PUBLICATION when the publication
	// is missing.
	db *sql.DB

	// schema is the Postgres namespace the reader is bound to.
	// Events for other schemas are dropped during dispatch.
	schema string

	// dsn is the underlying connection string the schema-DB was
	// opened with. Stashed so [StreamChanges] can re-open it in
	// replication mode.
	dsn string

	// publication is the PUBLICATION name to stream from. Created
	// on demand (FOR ALL TABLES) when missing.
	publication string

	// slotName is the logical-replication slot. Persistent across
	// reader restarts so resume works; not auto-dropped.
	slotName string

	// protoVersion is the pgoutput plugin protocol version. v2 is
	// available since PG 14 and is what we target.
	protoVersion int

	// replConn is the replication-mode pgconn opened by
	// StreamChanges. nil before the call, non-nil after, closed by
	// Close. Owned exclusively by the pump goroutine once started.
	replConn *pgconn.PgConn

	// streamerCancel cancels the pump goroutine. Stored so Close
	// can stop a stream when the caller's context isn't readily
	// available.
	streamerCancel context.CancelFunc

	// mu guards err. The pump writes; callers read via Err after
	// the channel closes.
	mu  sync.Mutex
	err error
}

// Close releases the schema-DB pool and stops the pump goroutine.
// Safe to call multiple times.
func (r *CDCReader) Close() error {
	if r.streamerCancel != nil {
		r.streamerCancel()
		r.streamerCancel = nil
	}
	// replConn is closed by the pump goroutine on its way out (it
	// owns the connection); we just wait for the cancellation to
	// propagate. In practice Close is called from a different
	// goroutine and the pump exit is asynchronous, so we rely on
	// the OS to clean up the socket if the pump hasn't returned by
	// the time the test process exits.
	if r.db != nil {
		err := r.db.Close()
		r.db = nil
		return err
	}
	return nil
}

// Err returns the error, if any, that terminated the most recent
// streaming session. Only valid after the change channel has closed.
func (r *CDCReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// StreamChanges starts streaming logical-replication output from the
// given position. Pass the zero [ir.Position{}] to stream "from now"
// — the reader creates the publication and slot if they don't exist
// and resumes from the slot's confirmed_flush_lsn.
//
// The channel is closed when ctx is cancelled, when a fatal error
// occurs (visible via [Err]), or when the upstream tears down the
// replication connection. Drain the channel or cancel ctx to avoid
// leaking the pump goroutine.
func (r *CDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	if r.replConn != nil {
		return nil, errors.New("postgres: StreamChanges already in progress; construct a new reader for a second stream")
	}

	if err := checkWALLevel(ctx, r.db); err != nil {
		return nil, err
	}

	if err := ensurePublication(ctx, r.db, r.publication); err != nil {
		return nil, err
	}

	conn, err := openReplicationConn(ctx, r.dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open replication connection: %w", err)
	}
	r.replConn = conn

	startLSN, err := r.resolveStartPosition(ctx, conn, from)
	if err != nil {
		_ = conn.Close(ctx)
		r.replConn = nil
		return nil, err
	}

	pluginArgs := []string{
		fmt.Sprintf("proto_version '%d'", r.protoVersion),
		fmt.Sprintf("publication_names '%s'", r.publication),
	}
	if err := pglogrepl.StartReplication(ctx, conn, r.slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		_ = conn.Close(ctx)
		r.replConn = nil
		return nil, fmt.Errorf("postgres: START_REPLICATION: %w", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel
	out := make(chan ir.Change, cdcChannelBuffer)
	go r.pump(loopCtx, conn, startLSN, out)
	return out, nil
}

// resolveStartPosition turns the caller's [ir.Position] into a
// concrete LSN. An empty position triggers slot creation if the slot
// doesn't exist yet, or resume from the slot's recorded position if
// it does. A non-empty position must reference an existing slot —
// silently re-creating one would skip changes between the recorded
// LSN and "now".
func (r *CDCReader) resolveStartPosition(ctx context.Context, conn *pgconn.PgConn, from ir.Position) (pglogrepl.LSN, error) {
	decoded, ok, err := decodePGPos(from)
	if err != nil {
		return 0, err
	}

	if ok {
		// Resume path: caller provided a {slot, lsn}. Verify slot
		// matches and exists, then start at the supplied LSN.
		if decoded.Slot != r.slotName {
			return 0, fmt.Errorf(
				"postgres: position references slot %q but reader is configured with slot %q",
				decoded.Slot, r.slotName)
		}
		exists, err := slotExists(ctx, r.db, r.slotName)
		if err != nil {
			return 0, err
		}
		if !exists {
			return 0, fmt.Errorf(
				"postgres: replication slot %q no longer exists; cannot resume from supplied LSN (start a fresh stream with empty position)",
				r.slotName)
		}
		lsn, err := pglogrepl.ParseLSN(decoded.LSN)
		if err != nil {
			return 0, fmt.Errorf("postgres: parse resume LSN: %w", err)
		}
		return lsn, nil
	}

	// "From now" path. Create the slot if it doesn't exist.
	exists, err := slotExists(ctx, r.db, r.slotName)
	if err != nil {
		return 0, err
	}
	if !exists {
		// CREATE_REPLICATION_SLOT runs on the replication connection
		// (it's a replication-protocol command), not on the *sql.DB.
		if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
			return 0, fmt.Errorf("postgres: create replication slot %q: %w", r.slotName, err)
		}
	}
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return 0, fmt.Errorf("postgres: IDENTIFY_SYSTEM: %w", err)
	}
	return sysident.XLogPos, nil
}

// pump is the event loop. Owns the replication connection from this
// point on: closes it on exit, and is the only goroutine that calls
// methods on it. The ReceiveMessage deadline drives the keepalive
// cadence — when it times out, we send a StandbyStatusUpdate and go
// back to receiving.
func (r *CDCReader) pump(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN, out chan<- ir.Change) {
	defer close(out)
	defer func() { _ = conn.Close(ctx) }()

	relations := map[uint32]*relationCacheEntry{}
	confirmedLSN := startLSN
	currentTxnLSN := startLSN
	var inStream bool // pgoutput v2 streaming-in-progress flag

	nextKeepalive := time.Now().Add(keepaliveInterval)

	for {
		// Send a keepalive when the deadline expires (or if the server
		// asked for an immediate reply on a previous keepalive, which
		// zeroes nextKeepalive).
		if time.Now().After(nextKeepalive) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: confirmedLSN,
			}); err != nil {
				r.setErr(fmt.Errorf("postgres: cdc: standby status update: %w", err))
				return
			}
			nextKeepalive = time.Now().Add(keepaliveInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextKeepalive)
		raw, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				// Deadline-driven timeout — the keepalive trigger.
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			r.setErr(fmt.Errorf("postgres: cdc: receive: %w", err))
			return
		}

		if errMsg, ok := raw.(*pgproto3.ErrorResponse); ok {
			r.setErr(fmt.Errorf("postgres: cdc: server error: %s", errMsg.Message))
			return
		}
		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok {
			// Unexpected message type — log-equivalent silent skip.
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				r.setErr(fmt.Errorf("postgres: cdc: parse keepalive: %w", err))
				return
			}
			if pkm.ReplyRequested {
				// Force the next loop iteration to send an update
				// before the deadline fires.
				nextKeepalive = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				r.setErr(fmt.Errorf("postgres: cdc: parse xlogdata: %w", err))
				return
			}
			if err := r.dispatchWAL(ctx, xld, relations, &currentTxnLSN, &confirmedLSN, &inStream, out); err != nil {
				r.setErr(err)
				return
			}
		}
	}
}

// dispatchWAL parses the WAL payload (a pgoutput message) and emits
// the corresponding [ir.Change] when the message is row-level. Begin
// and Commit messages bookend transactions and advance the resume
// position; Relation messages refresh the cache.
func (r *CDCReader) dispatchWAL(
	ctx context.Context,
	xld pglogrepl.XLogData,
	relations map[uint32]*relationCacheEntry,
	currentTxnLSN *pglogrepl.LSN,
	confirmedLSN *pglogrepl.LSN,
	inStream *bool,
	out chan<- ir.Change,
) error {
	logical, err := pglogrepl.ParseV2(xld.WALData, *inStream)
	if err != nil {
		return fmt.Errorf("postgres: cdc: parse logical message: %w", err)
	}

	switch m := logical.(type) {
	case *pglogrepl.RelationMessageV2:
		entry, err := buildRelationCacheEntry(m.RelationMessage)
		if err != nil {
			return fmt.Errorf("postgres: cdc: relation %s.%s: %w", m.Namespace, m.RelationName, err)
		}
		relations[m.RelationID] = entry
		return nil

	case *pglogrepl.RelationMessage:
		entry, err := buildRelationCacheEntry(*m)
		if err != nil {
			return fmt.Errorf("postgres: cdc: relation %s.%s: %w", m.Namespace, m.RelationName, err)
		}
		relations[m.RelationID] = entry
		return nil

	case *pglogrepl.BeginMessage:
		*currentTxnLSN = m.FinalLSN
		return nil

	case *pglogrepl.CommitMessage:
		*confirmedLSN = m.CommitLSN
		return nil

	case *pglogrepl.InsertMessageV2:
		return r.emitInsert(ctx, relations, m.RelationID, m.Tuple, *currentTxnLSN, out)
	case *pglogrepl.InsertMessage:
		return r.emitInsert(ctx, relations, m.RelationID, m.Tuple, *currentTxnLSN, out)

	case *pglogrepl.UpdateMessageV2:
		return r.emitUpdate(ctx, relations, m.RelationID, m.OldTuple, m.NewTuple, *currentTxnLSN, out)
	case *pglogrepl.UpdateMessage:
		return r.emitUpdate(ctx, relations, m.RelationID, m.OldTuple, m.NewTuple, *currentTxnLSN, out)

	case *pglogrepl.DeleteMessageV2:
		return r.emitDelete(ctx, relations, m.RelationID, m.OldTuple, *currentTxnLSN, out)
	case *pglogrepl.DeleteMessage:
		return r.emitDelete(ctx, relations, m.RelationID, m.OldTuple, *currentTxnLSN, out)

	case *pglogrepl.TruncateMessageV2:
		return r.emitTruncate(ctx, relations, m.RelationIDs, *currentTxnLSN, out)
	case *pglogrepl.TruncateMessage:
		return r.emitTruncate(ctx, relations, m.RelationIDs, *currentTxnLSN, out)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		return nil
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		return nil

	default:
		// TypeMessage, OriginMessage, LogicalDecodingMessage,
		// StreamCommit/Abort: not in v1 scope. Silent skip.
		return nil
	}
}

func (r *CDCReader) emitInsert(
	ctx context.Context,
	relations map[uint32]*relationCacheEntry,
	relID uint32,
	tuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
	out chan<- ir.Change,
) error {
	rel, ok := relations[relID]
	if !ok {
		return fmt.Errorf("postgres: cdc: insert for unknown relation OID %d", relID)
	}
	if rel.Schema != r.schema {
		return nil // out-of-scope schema; drop
	}
	row, err := decodeTuple(tuple, rel.Columns)
	if err != nil {
		return fmt.Errorf("postgres: cdc: decode insert for %s.%s: %w", rel.Schema, rel.Name, err)
	}
	pos, err := r.positionAt(lsn)
	if err != nil {
		return err
	}
	return send(ctx, out, ir.Insert{
		Position: pos,
		Schema:   rel.Schema,
		Table:    rel.Name,
		Row:      row,
	})
}

func (r *CDCReader) emitUpdate(
	ctx context.Context,
	relations map[uint32]*relationCacheEntry,
	relID uint32,
	oldTuple, newTuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
	out chan<- ir.Change,
) error {
	rel, ok := relations[relID]
	if !ok {
		return fmt.Errorf("postgres: cdc: update for unknown relation OID %d", relID)
	}
	if rel.Schema != r.schema {
		return nil
	}
	after, err := decodeTuple(newTuple, rel.Columns)
	if err != nil {
		return fmt.Errorf("postgres: cdc: decode update.after for %s.%s: %w", rel.Schema, rel.Name, err)
	}
	var before ir.Row
	if oldTuple != nil {
		before, err = decodeTuple(oldTuple, rel.Columns)
		if err != nil {
			return fmt.Errorf("postgres: cdc: decode update.before for %s.%s: %w", rel.Schema, rel.Name, err)
		}
	} else {
		before, err = synthesizeKeyOnlyBefore(rel, after)
		if err != nil {
			return fmt.Errorf("postgres: cdc: %w", err)
		}
	}
	pos, err := r.positionAt(lsn)
	if err != nil {
		return err
	}
	return send(ctx, out, ir.Update{
		Position: pos,
		Schema:   rel.Schema,
		Table:    rel.Name,
		Before:   before,
		After:    after,
	})
}

// synthesizeKeyOnlyBefore builds a key-only Before image from the
// after-tuple's identity-key columns. Used when pgoutput omits the
// old tuple from an UPDATE message — under REPLICA IDENTITY DEFAULT
// (and USING INDEX), the publisher omits OldTuple whenever the
// UPDATE didn't change any of the identity-key columns. The Before
// image is still required by the applier to construct a WHERE clause;
// without this helper the applier would emit
// "UPDATE t SET ... WHERE " with an empty predicate and Postgres
// rejects with "syntax error at end of input".
//
// The post-image identity values are correct as a Before substitute
// because, by construction, those columns are unchanged from the
// row's pre-image (otherwise pgoutput would have included OldTuple).
//
// Errors loudly when the relation has REPLICA IDENTITY NOTHING (no
// old tuple is ever emitted, regardless of column changes — UPDATEs
// cannot be replicated) or when the relation has no identity-key
// columns at all.
func synthesizeKeyOnlyBefore(rel *relationCacheEntry, after ir.Row) (ir.Row, error) {
	if rel.ReplicaIdentity == 'n' {
		return nil, fmt.Errorf(
			"update on %s.%s without identity: relation has REPLICA IDENTITY NOTHING; configure REPLICA IDENTITY DEFAULT, FULL, or USING INDEX before replicating UPDATEs",
			rel.Schema, rel.Name)
	}
	before := make(ir.Row, len(rel.Columns))
	keyCount := 0
	for _, col := range rel.Columns {
		if !col.KeyColumn {
			continue
		}
		v, ok := after[col.Name]
		if !ok {
			return nil, fmt.Errorf(
				"update on %s.%s: identity column %q missing from new tuple; cannot synthesize WHERE",
				rel.Schema, rel.Name, col.Name)
		}
		before[col.Name] = v
		keyCount++
	}
	if keyCount == 0 {
		return nil, fmt.Errorf(
			"update on %s.%s: relation has no identity-key columns (no PRIMARY KEY and no REPLICA IDENTITY index); cannot replicate UPDATE",
			rel.Schema, rel.Name)
	}
	return before, nil
}

func (r *CDCReader) emitDelete(
	ctx context.Context,
	relations map[uint32]*relationCacheEntry,
	relID uint32,
	oldTuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
	out chan<- ir.Change,
) error {
	rel, ok := relations[relID]
	if !ok {
		return fmt.Errorf("postgres: cdc: delete for unknown relation OID %d", relID)
	}
	if rel.Schema != r.schema {
		return nil
	}
	var before ir.Row
	if oldTuple != nil {
		var err error
		before, err = decodeTuple(oldTuple, rel.Columns)
		if err != nil {
			return fmt.Errorf("postgres: cdc: decode delete for %s.%s: %w", rel.Schema, rel.Name, err)
		}
	}
	pos, err := r.positionAt(lsn)
	if err != nil {
		return err
	}
	return send(ctx, out, ir.Delete{
		Position: pos,
		Schema:   rel.Schema,
		Table:    rel.Name,
		Before:   before,
	})
}

func (r *CDCReader) emitTruncate(
	ctx context.Context,
	relations map[uint32]*relationCacheEntry,
	relIDs []uint32,
	lsn pglogrepl.LSN,
	out chan<- ir.Change,
) error {
	pos, err := r.positionAt(lsn)
	if err != nil {
		return err
	}
	for _, id := range relIDs {
		rel, ok := relations[id]
		if !ok {
			return fmt.Errorf("postgres: cdc: truncate for unknown relation OID %d", id)
		}
		if rel.Schema != r.schema {
			continue
		}
		if err := send(ctx, out, ir.Truncate{
			Position: pos,
			Schema:   rel.Schema,
			Table:    rel.Name,
		}); err != nil {
			return err
		}
	}
	return nil
}

// positionAt is a thin wrapper over [encodePGPos] specialised to the
// reader's slot. Each emitted Change carries this so resume points
// at the start of the change's transaction.
func (r *CDCReader) positionAt(lsn pglogrepl.LSN) (ir.Position, error) {
	return encodePGPos(pgPos{Slot: r.slotName, LSN: lsn.String()})
}

// setErr stores the first streaming error; subsequent calls are
// no-ops so the originating cause isn't masked.
func (r *CDCReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// send pushes c onto out, honouring ctx cancellation.
func send(ctx context.Context, out chan<- ir.Change, c ir.Change) error {
	select {
	case out <- c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// buildRelationCacheEntry projects a [pglogrepl.RelationMessage] into
// the IR-typed cache entry. Errors when the relation contains a
// column whose OID isn't in the static OID-to-IR table — that's the
// loud-failure surface for unknown / custom types.
func buildRelationCacheEntry(m pglogrepl.RelationMessage) (*relationCacheEntry, error) {
	cols := make([]relationColumn, 0, len(m.Columns))
	for _, c := range m.Columns {
		t, err := oidToType(c.DataType, c.TypeModifier)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}
		cols = append(cols, relationColumn{
			Name:      c.Name,
			OID:       c.DataType,
			TypeMod:   c.TypeModifier,
			Type:      t,
			KeyColumn: c.Flags&1 != 0,
		})
	}
	return &relationCacheEntry{
		Schema:          m.Namespace,
		Name:            m.RelationName,
		ReplicaIdentity: m.ReplicaIdentity,
		Columns:         cols,
	}, nil
}

// decodeTuple turns a pgoutput TupleData (positional, parallel to the
// relation's column list) into an [ir.Row]. Unchanged TOAST columns
// (DataType byte 'u') are omitted from the row entirely; an absent
// key in [ir.Row] means "preserve the target's existing value", which
// is the right semantics for partial-row updates.
func decodeTuple(tuple *pglogrepl.TupleData, cols []relationColumn) (ir.Row, error) {
	if tuple == nil {
		return nil, nil
	}
	if int(tuple.ColumnNum) != len(cols) {
		return nil, fmt.Errorf(
			"tuple has %d columns; relation has %d", tuple.ColumnNum, len(cols))
	}
	row := make(ir.Row, len(cols))
	for i, col := range tuple.Columns {
		c := cols[i]
		switch col.DataType {
		case 'n':
			row[c.Name] = nil
		case 'u':
			// Unchanged TOAST: omit from row map.
			continue
		case 't':
			v, err := decodeValue(col.Data, c.Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", c.Name, err)
			}
			row[c.Name] = v
		case 'b':
			// Binary format. We request text via pgoutput defaults,
			// so this branch is defensive and surfaces loudly if it
			// ever gets hit.
			return nil, fmt.Errorf(
				"column %q: binary tuple format is not supported (request text via pgoutput defaults)",
				c.Name)
		default:
			return nil, fmt.Errorf("column %q: unknown tuple data type %q", c.Name, col.DataType)
		}
	}
	return row, nil
}

// checkWALLevel verifies the source has wal_level=logical configured
// before any replication command is issued. Surfacing this as a
// startup error matches the "Contain Postgres complexity" tenet —
// we name the GUC and what's required.
func checkWALLevel(ctx context.Context, db *sql.DB) error {
	var level string
	if err := db.QueryRowContext(ctx, "SHOW wal_level").Scan(&level); err != nil {
		return fmt.Errorf("postgres: read wal_level: %w", err)
	}
	if level != "logical" {
		return fmt.Errorf(
			"postgres: cdc: wal_level is %q; must be 'logical' for logical replication (set wal_level=logical in postgresql.conf and restart)",
			level)
	}
	return nil
}

// ensurePublication CREATEs the publication if it doesn't already
// exist. CREATE PUBLICATION ... FOR ALL TABLES is idempotent only by
// virtue of this check; running it twice is an error.
func ensurePublication(ctx context.Context, db *sql.DB, name string) error {
	var exists bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}
	if exists {
		return nil
	}
	createQuery := fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(name))
	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		return fmt.Errorf("postgres: create publication %q: %w", name, err)
	}
	return nil
}

// slotExists checks pg_replication_slots for the named slot.
func slotExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists bool
	const q = "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)"
	if err := db.QueryRowContext(ctx, q, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("postgres: check slot: %w", err)
	}
	return exists, nil
}

// openReplicationConn opens a pgconn.PgConn in replication=database
// mode. The caller-supplied DSN (which may already contain query
// parameters) is augmented with the replication parameter; existing
// values are preserved.
func openReplicationConn(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	withRepl, err := withReplicationParam(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: prepare replication DSN: %w", err)
	}
	conn, err := pgconn.Connect(ctx, withRepl)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// withReplicationParam ensures the URI carries `replication=database`.
// Both URI-style and KV-style DSNs are accepted; KV gets the param
// appended with a leading space, URI gets it merged into the query
// string preserving anything already there (sluice's `schema` param
// included — pgconn ignores unknown keys).
func withReplicationParam(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		q := u.Query()
		// schema is sluice-specific; pgconn doesn't recognise it and
		// will reject the connection if we leave it on. Strip it
		// here, the same way parseDSN strips it for the *sql.DB path.
		q.Del("schema")
		q.Set("replication", "database")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	// libpq KV form: same strip-schema dance, then append replication.
	out := []string{}
	for _, tok := range strings.Fields(dsn) {
		if strings.HasPrefix(strings.ToLower(tok), "schema=") {
			continue
		}
		if strings.HasPrefix(strings.ToLower(tok), "replication=") {
			continue
		}
		out = append(out, tok)
	}
	out = append(out, "replication=database")
	return strings.Join(out, " "), nil
}
