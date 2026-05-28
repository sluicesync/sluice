// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
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

	// appliedLSN is the slot-ack-after-apply tracker (Bug 15,
	// ADR-0020). Non-nil values come from the streamer wiring;
	// when nil, the keepalive routine reports the streamed LSN
	// (legacy v0.4.0 shape — preserved so non-streamer callers
	// like the cdc-snapshot test paths don't need to construct
	// a tracker).
	appliedLSN *lsnTracker

	// systemID and timeline pin the source's identity (ADR-0051,
	// research finding F5). Populated from IDENTIFY_SYSTEM at the
	// start of each StreamChanges call. Emitted on every change's
	// Position so a subsequent reconnect can compare the persisted
	// pin against what the new connection's IDENTIFY_SYSTEM reports.
	// After a source-side PITR or standby promotion the (sysid,
	// timeline) tuple changes; without this pin, sluice would
	// silently advance LSN values that live in a different timeline's
	// reference frame — silent-loss class.
	systemID string
	timeline int32

	// mu guards err. The pump writes; callers read via Err after
	// the channel closes.
	mu  sync.Mutex
	err error
}

// AttachLSNTracker installs an applied-LSN feedback channel from
// the [ChangeApplier]. The tracker's value is what the keepalive
// routine reports as confirmed_flush_lsn; until the applier reports
// its first commit, the reader falls back to startLSN so the slot
// stays alive on idle streams. See ADR-0020.
//
// Must be called before [StreamChanges]; calling it on a running
// reader is racy with the pump goroutine and will be ignored.
func (r *CDCReader) AttachLSNTracker(t *lsnTracker) {
	r.appliedLSN = t
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
// On cold-start the reader creates the slot lazily; if any setup step
// after slot creation (IDENTIFY_SYSTEM, START_REPLICATION) fails — or
// ctx is cancelled before the channel is returned — the freshly-
// created slot is auto-dropped before StreamChanges returns. Slots
// that already existed when StreamChanges was called are never
// auto-dropped: they may carry another caller's progress.
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

	// The streamer's coldStart calls Engine.EnsurePublication with
	// the scoped table list before this point (Bug 13, ADR-0021),
	// so this call is a no-op when the publication already exists
	// with the right scope. Pass nil tables: at this layer we
	// don't have the schema in hand, and the helper falls back to
	// FOR ALL TABLES only when the publication is genuinely
	// missing — i.e. a non-streamer caller never went through the
	// scoped-publication code path. That's the v0.4.0 shape and
	// remains correct for those callers.
	if err := ensurePublication(ctx, r.db, r.publication, r.schema, nil); err != nil {
		return nil, err
	}

	conn, err := openReplicationConn(ctx, r.dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open replication connection: %w", err)
	}
	r.replConn = conn

	// slotJustCreated tracks whether resolveStartPosition created a
	// fresh slot in this call. The deferred cleanup below drops it
	// only when both flags hold: we created it AND we're not handing
	// the caller a live channel. A pre-existing slot (someone else's
	// progress) is never touched.
	var slotJustCreated bool
	streamStarted := false
	defer func() {
		if streamStarted || !slotJustCreated {
			return
		}
		// Best-effort drop. The original error is the one the caller
		// cares about; a drop failure is logged via the error path's
		// returned error so it isn't silent, but never replaces the
		// primary cause.
		dropErr := dropReplicationSlot(ctx, conn, r.slotName)
		_ = conn.Close(ctx)
		r.replConn = nil
		if dropErr != nil {
			fmt.Fprintf(os.Stderr,
				"postgres: cdc: warning: failed to auto-drop freshly-created slot %q after setup error: %v\n",
				r.slotName, dropErr)
			return
		}
		fmt.Fprintf(os.Stderr,
			"postgres: cdc: auto-dropped freshly-created slot %q after setup error\n",
			r.slotName)
	}()

	startLSN, err := r.resolveStartPosition(ctx, conn, from, &slotJustCreated)
	if err != nil {
		if !slotJustCreated {
			_ = conn.Close(ctx)
			r.replConn = nil
		}
		return nil, err
	}

	pluginArgs := []string{
		fmt.Sprintf("proto_version '%d'", r.protoVersion),
		fmt.Sprintf("publication_names '%s'", r.publication),
	}
	if err := pglogrepl.StartReplication(ctx, conn, r.slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		if !slotJustCreated {
			_ = conn.Close(ctx)
			r.replConn = nil
		}
		return nil, fmt.Errorf("postgres: START_REPLICATION: %w", err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	r.streamerCancel = cancel
	out := make(chan ir.Change, cdcChannelBuffer)
	streamStarted = true // suppress the deferred slot-drop
	go r.pump(loopCtx, conn, startLSN, out)
	return out, nil
}

// resolveStartPosition turns the caller's [ir.Position] into a
// concrete LSN. An empty position triggers slot creation if the slot
// doesn't exist yet, or resume from the slot's recorded position if
// it does. A non-empty position must reference an existing slot —
// silently re-creating one would skip changes between the recorded
// LSN and "now".
//
// IDENTIFY_SYSTEM is invoked on BOTH paths (cold-start and resume):
// its (systemid, timeline) reply pins the source's identity so the
// reader can refuse loudly when a subsequent reconnect lands on a
// source whose identity has changed — post-PITR, post-promotion, or
// the operator pointed sluice at the wrong instance. Without the pin
// sluice would silently advance LSN values that live in a different
// timeline's reference frame (ADR-0051, research finding F5).
func (r *CDCReader) resolveStartPosition(
	ctx context.Context,
	conn *pgconn.PgConn,
	from ir.Position,
	slotJustCreated *bool,
) (pglogrepl.LSN, error) {
	decoded, ok, err := decodePGPos(from)
	if err != nil {
		return 0, err
	}

	// IDENTIFY_SYSTEM runs unconditionally so every StreamChanges call
	// captures the current source identity. Run BEFORE the slot
	// existence check on the resume path so a diverged source surfaces
	// the identity-mismatch error rather than a slot-missing error
	// (which would be misleading — the slot may genuinely exist on the
	// new source).
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return 0, fmt.Errorf("postgres: IDENTIFY_SYSTEM: %w", err)
	}
	r.systemID = sysident.SystemID
	r.timeline = sysident.Timeline

	if ok {
		// Resume path: caller provided a {slot, lsn[, sysid, timeline]}.
		// Verify slot matches and exists, then start at the supplied LSN.
		if decoded.Slot != r.slotName {
			return 0, fmt.Errorf(
				"postgres: position references slot %q but reader is configured with slot %q",
				decoded.Slot, r.slotName,
			)
		}
		// Source-identity pin check. If the persisted position carries a
		// (sysid, timeline) pair, the live source's reply MUST match
		// exactly — divergence indicates a PITR / promotion / wrong
		// instance, and any LSN comparison across the boundary is
		// silently meaningless (different timelines have independent
		// LSN reference frames). Refuse loudly, wrapping
		// ErrPositionInvalid so the pipeline orchestrator can route
		// the error through the ADR-0022 cold-start fall-through path
		// (the only recovery for a position that no longer points at
		// the same database).
		//
		// Positions persisted by pre-ADR-0051 sluice have empty
		// SystemID/Timeline and are accepted unchanged: a one-time INFO
		// log notes that the pin is being installed lazily. Subsequent
		// reconnects must match the now-pinned identity.
		if err := checkSourceIdentity(ctx, r.slotName, decoded.SystemID, decoded.Timeline, sysident.SystemID, sysident.Timeline); err != nil {
			return 0, err
		}
		info, err := slotInfo(ctx, r.db, r.slotName)
		if err != nil {
			return 0, err
		}
		if info == nil {
			// Wrap with [ir.ErrPositionInvalid] so the pipeline
			// orchestrator can detect via errors.Is and fall through
			// to cold-start (ADR-0022). The wrap message stays
			// engine-specific so operator-facing logs name the slot.
			return 0, fmt.Errorf(
				"postgres: replication slot %q no longer exists; cannot resume from supplied LSN: %w",
				r.slotName, ir.ErrPositionInvalid,
			)
		}
		if err := checkSlotUsable(info); err != nil {
			return 0, err
		}
		lsn, err := pglogrepl.ParseLSN(decoded.LSN)
		if err != nil {
			return 0, fmt.Errorf("postgres: parse resume LSN: %w", err)
		}
		return lsn, nil
	}

	// "From now" path. Create the slot if it doesn't exist; if it
	// already exists, validate its WAL status before reusing it.
	info, err := slotInfo(ctx, r.db, r.slotName)
	if err != nil {
		return 0, err
	}
	if info != nil {
		if err := checkSlotUsable(info); err != nil {
			return 0, err
		}
	} else {
		// CREATE_REPLICATION_SLOT runs on the replication connection
		// (it's a replication-protocol command), not on the *sql.DB.
		// The helper opts into FAILOVER on PG 17+ and warns on
		// PG ≤ 16 (see slot_create.go for the rationale).
		if _, _, err := createLogicalReplicationSlot(ctx, r.db, conn, r.slotName, false); err != nil {
			return 0, err
		}
		*slotJustCreated = true
	}
	return sysident.XLogPos, nil
}

// checkSlotUsable surfaces wal_status invalidation as a clear,
// actionable error. PostgreSQL drives slots through these states
// in pg_replication_slots.wal_status as the source generates WAL
// faster than the consumer advances:
//
//   - reserved   — slot has all required WAL on disk (healthy).
//   - extended   — slot is keeping more WAL than max_wal_size on disk
//     (healthy but should be watched; consumer is behind).
//   - unreserved — required WAL has been removed from pg_wal but
//     is still recoverable; slot is on the brink of
//     invalidation. Once a checkpoint runs, the state
//     transitions to "lost".
//   - lost       — required WAL is gone permanently. The slot still
//     exists but is unusable. The only recovery is to
//     drop and recreate, which forces a fresh snapshot.
//
// We refuse to start replication on "unreserved" or "lost" slots
// rather than letting START_REPLICATION fail mid-stream with the
// confusing "requested WAL segment has already been removed".
// The error names the slot, the wal_status, and the recovery path.
func checkSlotUsable(info *slotState) error {
	switch info.WALStatus {
	case "", "reserved", "extended":
		return nil
	case "unreserved":
		return fmt.Errorf(
			"postgres: replication slot %q has wal_status=%q — required WAL is on the brink of being lost; "+
				"resume immediately or recreate the slot. To recreate: `sluice slot drop %s --source-driver=postgres --source ...` then restart with empty position (forces a fresh snapshot)",
			info.SlotName, info.WALStatus, info.SlotName,
		)
	case "lost":
		return fmt.Errorf(
			"postgres: replication slot %q has wal_status=%q — required WAL has been permanently removed; "+
				"the slot must be dropped and recreated. To recover: `sluice slot drop %s --source-driver=postgres --source ...` then restart with empty position (forces a fresh snapshot). "+
				"To prevent recurrence, raise max_slot_wal_keep_size on the source — PlanetScale recommends > 4GB",
			info.SlotName, info.WALStatus, info.SlotName,
		)
	default:
		// Future PG versions could add states. Surface verbatim
		// rather than assume.
		return fmt.Errorf(
			"postgres: replication slot %q has unrecognised wal_status=%q; refusing to proceed",
			info.SlotName, info.WALStatus,
		)
	}
}

// checkSourceIdentity compares a persisted (systemid, timeline) pin
// against the live source's IDENTIFY_SYSTEM reply (ADR-0051, research
// finding F5). Returns:
//
//   - nil when the pin matches the live values, or when the persisted
//     position is from pre-ADR-0051 sluice (empty SystemID, zero
//     Timeline). The lazy-install case emits a one-time INFO log so
//     operators can see the pin going in.
//   - an error wrapping [ir.ErrPositionInvalid] when the pin diverges
//     from live. Wrapping the sentinel routes the error through the
//     ADR-0022 cold-start fall-through (the only recovery for a
//     position that no longer points at the same database — different
//     timelines have independent LSN reference frames, so cross-
//     timeline LSN comparisons are silently meaningless and the
//     persisted LSN cannot be resumed).
//
// The error message names both the OLD and NEW (systemid, timeline)
// pairs so operators have enough information to confirm whether the
// divergence matches their intended PITR or promotion event.
func checkSourceIdentity(ctx context.Context, slotName, persistedSysID string, persistedTimeline int32, liveSysID string, liveTimeline int32) error {
	if persistedSysID == "" && persistedTimeline == 0 {
		// Lazy-install case: pre-ADR-0051 position. Accept and log.
		slog.InfoContext(
			ctx, "postgres: cdc: source-identity pin installed lazily from IDENTIFY_SYSTEM (pre-ADR-0051 persisted position lacked it)",
			slog.String("slot", slotName),
			slog.String("systemid", liveSysID),
			slog.Int("timeline", int(liveTimeline)),
		)
		return nil
	}
	if persistedSysID == liveSysID && persistedTimeline == liveTimeline {
		return nil
	}
	return fmt.Errorf(
		"postgres: source identity has changed: persisted position pinned (systemid=%q, timeline=%d) but live source reports (systemid=%q, timeline=%d); "+
			"this indicates a source-side PITR, standby promotion, or that sluice is now pointed at a different instance — "+
			"the persisted LSN belongs to a different timeline's reference frame and is no longer valid. "+
			"To recover: confirm the change matches your intended PITR/promotion event, then drop the slot and persisted position so a fresh cold-start runs against the new source — "+
			"`sluice slot drop %s --source-driver=postgres --source ...` then restart with empty position (forces a fresh snapshot): %w",
		persistedSysID, persistedTimeline, liveSysID, liveTimeline,
		slotName, ir.ErrPositionInvalid,
	)
}

// pump is the event loop. Owns the replication connection from this
// point on: closes it on exit, and is the only goroutine that calls
// methods on it. The ReceiveMessage deadline drives the keepalive
// cadence — when it times out, we send a StandbyStatusUpdate and go
// back to receiving.
//
// Two LSNs are tracked side-by-side (Bug 15, ADR-0020):
//
//   - streamedLSN: the highest commit-LSN the pump has parsed off
//     the WAL stream. Advances as soon as a CommitMessage is seen.
//     Used internally for keepalive bookkeeping and as the fallback
//     ack value when no applier feedback is wired.
//   - appliedLSN (via r.appliedLSN tracker, when non-nil): the
//     highest LSN whose data has been committed to the target. The
//     applier's commit path reports this back; the keepalive
//     routine sends THIS value as WALWritePosition so the slot's
//     confirmed_flush_lsn never advances past durably-applied work.
//
// On streams without a tracker (tests, legacy non-streamer
// callers), the pump falls back to streamedLSN so the slot still
// gets keepalive activity. Production paths always wire a tracker
// via the [pipeline.Streamer].
func (r *CDCReader) pump(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN, out chan<- ir.Change) {
	defer close(out)
	defer func() { _ = conn.Close(ctx) }()

	relations := map[uint32]*relationCacheEntry{}
	// snapshotSig is the per-relation structural fingerprint of the
	// schema-history version last emitted as an [ir.SchemaSnapshot]
	// (ADR-0049 Chunk B3). Keyed by relation OID, parallel to
	// relations. Implements DP-1 sign-off point ii (true-delta only):
	// pgoutput re-sends a RelationMessage on reconnect / first-touch
	// *without* any DDL; a new schema-history version is written ONLY
	// when the projected (column-name, ordered-type) signature differs
	// from the one already snapshotted for that OID.
	snapshotSig := map[uint32]ir.SchemaSignature{}
	streamedLSN := startLSN
	currentTxnLSN := startLSN
	// currentTxnStartLSN is the WAL position of the BEGIN record for
	// the in-progress transaction. ADR-0036 (Path D Phase A) M1
	// instrumentation: a transaction whose BEGIN landed BEFORE
	// publication-add but commits AFTER would have its events filtered
	// by pgoutput's per-LSN catalog snapshot at decode time according
	// to the catalog state at the row record's LSN; the txn-start LSN
	// is one half of the diagnostic ordering. Distinct from
	// currentTxnLSN (which is the BeginMessage.FinalLSN, == the commit
	// LSN preview emitted by pgoutput).
	currentTxnStartLSN := startLSN
	// firstSeenRelLSN remembers the WAL LSN of the very first row event
	// observed for each relation during this pump's lifetime. ADR-0036
	// M3 instrumentation: lets the diagnostic test compare against the
	// publication-add LSN to see how long pgoutput's per-relation
	// catalog snapshot lagged the catalog change.
	firstSeenRelLSN := map[uint32]pglogrepl.LSN{}
	var inStream bool // pgoutput v2 streaming-in-progress flag

	nextKeepalive := time.Now().Add(keepaliveInterval)

	for {
		// Send a keepalive when the deadline expires (or if the server
		// asked for an immediate reply on a previous keepalive, which
		// zeroes nextKeepalive).
		if time.Now().After(nextKeepalive) {
			ack := r.ackLSN(streamedLSN, startLSN)
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: ack,
				WALFlushPosition: ack,
				WALApplyPosition: ack,
			}); err != nil {
				r.setErr(classifyReaderError(fmt.Errorf("postgres: cdc: standby status update: %w", err)))
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
			r.setErr(classifyReaderError(fmt.Errorf("postgres: cdc: receive: %w", err)))
			return
		}

		if errMsg, ok := raw.(*pgproto3.ErrorResponse); ok {
			r.setErr(classifyReaderError(fmt.Errorf("postgres: cdc: server error: %s", errMsg.Message)))
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
			if err := r.dispatchWAL(ctx, xld, relations, snapshotSig, &currentTxnLSN, &currentTxnStartLSN, &streamedLSN, &inStream, firstSeenRelLSN, out); err != nil {
				r.setErr(err)
				return
			}
		}
	}
}

// dispatchWAL parses the WAL payload (a pgoutput message) and emits
// the corresponding [ir.Change] when the message is row-level. Begin
// and Commit messages bookend transactions and advance the streamed-
// LSN bookkeeping; Relation messages refresh the cache.
//
// streamedLSN tracks the highest commit-LSN parsed off the wire and
// is updated on each CommitMessage. It is NOT what the slot ack uses
// when an applier feedback tracker is wired — the keepalive routine
// reads from r.appliedLSN to honour the slot-ack-after-apply rule
// (Bug 15, ADR-0020).
func (r *CDCReader) dispatchWAL(
	ctx context.Context,
	xld pglogrepl.XLogData,
	relations map[uint32]*relationCacheEntry,
	snapshotSig map[uint32]ir.SchemaSignature,
	currentTxnLSN *pglogrepl.LSN,
	currentTxnStartLSN *pglogrepl.LSN,
	streamedLSN *pglogrepl.LSN,
	inStream *bool,
	firstSeenRelLSN map[uint32]pglogrepl.LSN,
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
		if err := r.resolveIdentityKeyCols(ctx, entry); err != nil {
			return fmt.Errorf("postgres: cdc: relation %s.%s: %w", m.Namespace, m.RelationName, err)
		}
		relations[m.RelationID] = entry
		// ADR-0036 M3: log RelationMessage arrivals so the diagnostic
		// run can correlate them with the publication-add LSN.
		slog.DebugContext(
			ctx, "cdc.diag: relation message",
			slog.String("phase", "relation"),
			slog.String("schema", entry.Schema),
			slog.String("relation", entry.Name),
			slog.Uint64("rel_oid", uint64(m.RelationID)),
			slog.String("wal_start", xld.WALStart.String()),
			slog.String("server_wal_end", xld.ServerWALEnd.String()),
		)
		return r.maybeSnapshotSchema(ctx, entry, m.RelationID, xld.WALStart, snapshotSig, out)

	case *pglogrepl.RelationMessage:
		entry, err := buildRelationCacheEntry(*m)
		if err != nil {
			return fmt.Errorf("postgres: cdc: relation %s.%s: %w", m.Namespace, m.RelationName, err)
		}
		if err := r.resolveIdentityKeyCols(ctx, entry); err != nil {
			return fmt.Errorf("postgres: cdc: relation %s.%s: %w", m.Namespace, m.RelationName, err)
		}
		relations[m.RelationID] = entry
		slog.DebugContext(
			ctx, "cdc.diag: relation message",
			slog.String("phase", "relation"),
			slog.String("schema", entry.Schema),
			slog.String("relation", entry.Name),
			slog.Uint64("rel_oid", uint64(m.RelationID)),
			slog.String("wal_start", xld.WALStart.String()),
			slog.String("server_wal_end", xld.ServerWALEnd.String()),
		)
		return r.maybeSnapshotSchema(ctx, entry, m.RelationID, xld.WALStart, snapshotSig, out)

	case *pglogrepl.BeginMessage:
		*currentTxnLSN = m.FinalLSN
		*currentTxnStartLSN = xld.WALStart
		// ADR-0036 M1: log txn-start (WAL position of the BEGIN record)
		// alongside the txn's final commit LSN. Lets the diagnostic test
		// detect transactions that straddle the publication-add LSN
		// (txn_start < LSN_pubadd < commit).
		slog.DebugContext(
			ctx, "cdc.diag: txn begin",
			slog.String("phase", "begin"),
			slog.String("txn_start_lsn", xld.WALStart.String()),
			slog.String("txn_commit_lsn", m.FinalLSN.String()),
			slog.Uint64("xid", uint64(m.Xid)),
		)
		// Surface the source-tx boundary to the applier so the
		// batched path can flush in-flight non-tx-aware batches and
		// open a fresh target transaction aligned to this source
		// transaction. Per-change appliers treat the event as a
		// no-op. See ADR-0027.
		pos, err := r.positionAt(m.FinalLSN)
		if err != nil {
			return err
		}
		return send(ctx, out, ir.TxBegin{Position: pos})

	case *pglogrepl.CommitMessage:
		*streamedLSN = m.CommitLSN
		slog.DebugContext(
			ctx, "cdc.diag: txn commit",
			slog.String("phase", "commit"),
			slog.String("txn_start_lsn", currentTxnStartLSN.String()),
			slog.String("txn_commit_lsn", m.CommitLSN.String()),
			slog.String("wal_start", xld.WALStart.String()),
		)
		// Source-tx commit boundary: flush whatever the applier has
		// in flight as one target transaction. The empty-source-tx
		// case (BEGIN immediately followed by COMMIT with no row
		// events) is harmless — the applier's flush path skips when
		// no rows have accumulated. See ADR-0027.
		pos, err := r.positionAt(m.CommitLSN)
		if err != nil {
			return err
		}
		return send(ctx, out, ir.TxCommit{Position: pos})

	case *pglogrepl.InsertMessageV2:
		r.diagRowEvent(ctx, "insert", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitInsert(ctx, relations, m.RelationID, m.Tuple, *currentTxnLSN, out)
	case *pglogrepl.InsertMessage:
		r.diagRowEvent(ctx, "insert", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitInsert(ctx, relations, m.RelationID, m.Tuple, *currentTxnLSN, out)

	case *pglogrepl.UpdateMessageV2:
		r.diagRowEvent(ctx, "update", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitUpdate(ctx, relations, m.RelationID, m.OldTuple, m.NewTuple, *currentTxnLSN, out)
	case *pglogrepl.UpdateMessage:
		r.diagRowEvent(ctx, "update", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitUpdate(ctx, relations, m.RelationID, m.OldTuple, m.NewTuple, *currentTxnLSN, out)

	case *pglogrepl.DeleteMessageV2:
		r.diagRowEvent(ctx, "delete", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitDelete(ctx, relations, m.RelationID, m.OldTuple, *currentTxnLSN, out)
	case *pglogrepl.DeleteMessage:
		r.diagRowEvent(ctx, "delete", relations, m.RelationID, xld, *currentTxnStartLSN, *currentTxnLSN, firstSeenRelLSN)
		return r.emitDelete(ctx, relations, m.RelationID, m.OldTuple, *currentTxnLSN, out)

	case *pglogrepl.TruncateMessageV2:
		return r.emitTruncate(ctx, relations, m.RelationIDs, *currentTxnLSN, out)
	case *pglogrepl.TruncateMessage:
		return r.emitTruncate(ctx, relations, m.RelationIDs, *currentTxnLSN, out)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		// pgoutput v2 streaming-in-progress: large source
		// transactions arrive in chunks separated by StreamStart /
		// StreamStop pairs (ADR-0027). Treat each chunk as its own
		// boundary for applier batching purposes — the alternative
		// (buffer the whole transaction in memory) would defeat the
		// streaming protocol's purpose. The trade-off is documented
		// in the ADR: a huge source transaction produces multiple
		// target transactions on the receiver, which is still
		// correct under ADR-0010 idempotency.
		pos, err := r.positionAt(*currentTxnLSN)
		if err != nil {
			return err
		}
		return send(ctx, out, ir.TxBegin{Position: pos})
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		pos, err := r.positionAt(*currentTxnLSN)
		if err != nil {
			return err
		}
		return send(ctx, out, ir.TxCommit{Position: pos})

	case *pglogrepl.StreamAbortMessageV2:
		// ADR-0055: refuse loudly. sluice runs pgoutput with
		// proto_version=2 but does NOT pass streaming='on' (the v2
		// parser accepts streaming chunks; the publisher only EMITS
		// them when streaming is explicitly enabled). So a StreamAbort
		// SHOULD NEVER fire on a sluice stream. If it does, two
		// possibilities exist:
		//   (a) PG-side config drift enabled streaming externally; or
		//   (b) a future sluice change wired streaming='on' without
		//       teaching this dispatch path how to roll back the
		//       chunks emitted before the abort.
		// In either case ADR-0027 has already committed each
		// pre-abort chunk as its own target transaction (StreamStart
		// → TxBegin, StreamStop → TxCommit). The source transaction
		// rolled back; the target's chunks did not. Silently skipping
		// the abort would leave a target with rows the source no
		// longer has — silent-loss class. Refuse loudly per the
		// loud-failure tenet so the operator can drop the slot,
		// re-snapshot, and reconcile.
		return fmt.Errorf("postgres: cdc: pgoutput StreamAbortMessageV2 received "+
			"(xid=%d sub_xid=%d) but sluice does not enable streaming "+
			"(proto_version=2 without streaming='on'). This message indicates "+
			"a source-side rolled-back transaction whose pre-abort chunks may "+
			"have already been committed on the target — data divergence is "+
			"possible. Either: (a) PG config drift enabled streaming externally; "+
			"(b) a future sluice change enabled it without wiring StreamAbort "+
			"rollback. Refusing loudly per the loud-failure tenet. To recover, "+
			"drop the slot, re-snapshot. See ADR-0055",
			m.Xid, m.SubXid)

	default:
		// TypeMessage, OriginMessage, LogicalDecodingMessage,
		// StreamCommitMessageV2: not in v1 scope. Silent skip is safe
		// for these — they carry no row data sluice would otherwise
		// have committed (StreamCommit's per-chunk semantics are
		// already covered by the StreamStop → TxCommit handler above;
		// the others are diagnostic / origin-routing). StreamAbort is
		// the one streaming-related message we cannot silently skip
		// — see its dedicated case immediately above (ADR-0055).
		return nil
	}
}

// diagRowEvent emits ADR-0036 (Path D Phase A) DEBUG-level diagnostic
// log lines for every row event that pgoutput delivers to the pump.
// Captures four of the load-bearing facts the diagnostic test needs to
// attribute v0.24.0 mid-stream live add-table loss:
//
//   - the relation OID + schema-qualified name (so the test can scope
//     to events on the new table);
//   - the WAL LSN of the row record itself (xld.WALStart);
//   - the txn-start and txn-commit LSNs (M1: long-txn-across-pubadd);
//   - whether this is the first event observed for this relation since
//     the pump started (M3: pgoutput catalog-snapshot lag — the gap
//     between LSN_pubadd and the first event delivered for the new
//     relation tells us if pgoutput's per-LSN catalog snapshot lagged
//     the catalog change in any visible way).
//
// All log lines carry the `cdc.diag` slug so the diagnostic test can
// grep them out of the captured log stream cleanly.
func (r *CDCReader) diagRowEvent(
	ctx context.Context,
	op string,
	relations map[uint32]*relationCacheEntry,
	relID uint32,
	xld pglogrepl.XLogData,
	txnStartLSN, txnCommitLSN pglogrepl.LSN,
	firstSeenRelLSN map[uint32]pglogrepl.LSN,
) {
	rel, ok := relations[relID]
	if !ok {
		slog.DebugContext(
			ctx, "cdc.diag: row event (unknown relation)",
			slog.String("phase", "row"),
			slog.String("op", op),
			slog.Uint64("rel_oid", uint64(relID)),
			slog.String("wal_start", xld.WALStart.String()),
			slog.String("txn_start_lsn", txnStartLSN.String()),
			slog.String("txn_commit_lsn", txnCommitLSN.String()),
		)
		return
	}
	firstSeen := false
	if _, seen := firstSeenRelLSN[relID]; !seen {
		firstSeenRelLSN[relID] = xld.WALStart
		firstSeen = true
	}
	slog.DebugContext(
		ctx, "cdc.diag: row event",
		slog.String("phase", "row"),
		slog.String("op", op),
		slog.String("schema", rel.Schema),
		slog.String("relation", rel.Name),
		slog.Uint64("rel_oid", uint64(relID)),
		slog.String("wal_start", xld.WALStart.String()),
		slog.String("server_wal_end", xld.ServerWALEnd.String()),
		slog.String("txn_start_lsn", txnStartLSN.String()),
		slog.String("txn_commit_lsn", txnCommitLSN.String()),
		slog.Bool("first_seen_for_rel", firstSeen),
	)
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
		decoded, err := decodeTuple(oldTuple, rel.Columns)
		if err != nil {
			return fmt.Errorf("postgres: cdc: decode update.before for %s.%s: %w", rel.Schema, rel.Name, err)
		}
		// Narrow Before to the relation's resolved identity-key columns
		// (rel.IdentityKeyCols) before it becomes the applier's UPDATE
		// WHERE source — the exact symmetry the DELETE path has via
		// filterBeforeToKeyCols. Under REPLICA IDENTITY FULL the OldTuple
		// carries every column with real data, including rich types
		// (jsonb / timestamptz / bytea / high-precision numeric) that do
		// NOT `=`-match the stored target value after the pgoutput
		// decode→rebind round-trip; a WHERE over those columns matches
		// zero rows, which ADR-0010's resume-idempotent zero-rows-ok
		// behaviour silently absorbs — silent UPDATE loss (Bug 92).
		// Crucially the narrowing keys off IdentityKeyCols (resolved to
		// the TRUE PRIMARY KEY under FULL by resolveIdentityKeyCols), NOT
		// the pgoutput per-column wire key flag — which under FULL is set
		// on EVERY column and so would narrow to nothing. The result is a
		// key-only WHERE (WHERE id = $N). Under DEFAULT/USING INDEX
		// IdentityKeyCols is the wire-flagged replica-identity set, so the
		// pre-Bug-92 correct behaviour is preserved.
		before, err = filterBeforeToKeyCols(rel, decoded)
		if err != nil {
			return fmt.Errorf("postgres: cdc: %w", err)
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
// UPDATE didn't change any of the identity-key columns. (Under FULL
// the OldTuple is always present, so this path is not reached for FULL
// relations.) The Before image is still required by the applier to
// construct a WHERE clause; without this helper the applier would emit
// "UPDATE t SET ... WHERE " with an empty predicate and Postgres
// rejects with "syntax error at end of input".
//
// The post-image identity values are correct as a Before substitute
// because, by construction, those columns are unchanged from the
// row's pre-image (otherwise pgoutput would have included OldTuple).
//
// The identity columns come from rel.IdentityKeyCols (the resolved
// replica-identity set — see [CDCReader.resolveIdentityKeyCols]), not
// the raw per-column wire flag, so this stays consistent with the
// [filterBeforeToKeyCols] narrowing.
//
// Errors loudly when the relation has REPLICA IDENTITY NOTHING (no
// old tuple is ever emitted, regardless of column changes — UPDATEs
// cannot be replicated) or when the relation has no identity-key
// columns at all.
func synthesizeKeyOnlyBefore(rel *relationCacheEntry, after ir.Row) (ir.Row, error) {
	if rel.ReplicaIdentity == 'n' {
		return nil, fmt.Errorf(
			"update on %s.%s without identity: relation has REPLICA IDENTITY NOTHING; configure REPLICA IDENTITY DEFAULT, FULL, or USING INDEX before replicating UPDATEs",
			rel.Schema, rel.Name,
		)
	}
	if len(rel.IdentityKeyCols) == 0 {
		return nil, fmt.Errorf(
			"update on %s.%s: relation has no identity-key columns (no PRIMARY KEY and no REPLICA IDENTITY index); cannot replicate UPDATE",
			rel.Schema, rel.Name,
		)
	}
	before := make(ir.Row, len(rel.IdentityKeyCols))
	for _, name := range rel.IdentityKeyCols {
		v, ok := after[name]
		if !ok {
			return nil, fmt.Errorf(
				"update on %s.%s: identity column %q missing from new tuple; cannot synthesize WHERE",
				rel.Schema, rel.Name, name,
			)
		}
		before[name] = v
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
	if oldTuple == nil {
		// pgoutput emits no OldTuple at all only under REPLICA IDENTITY
		// NOTHING, which is unreplicatable for DELETE: the applier has
		// no way to identify the row. Surface this loudly so the
		// operator fixes the source rather than silently losing rows
		// (the original Bug 8 surface — see filterBeforeToKeyCols).
		return fmt.Errorf(
			"postgres: cdc: delete on %s.%s without identity: relation has REPLICA IDENTITY NOTHING; configure REPLICA IDENTITY DEFAULT, FULL, or USING INDEX before replicating DELETEs",
			rel.Schema, rel.Name,
		)
	}
	decoded, err := decodeTuple(oldTuple, rel.Columns)
	if err != nil {
		return fmt.Errorf("postgres: cdc: decode delete for %s.%s: %w", rel.Schema, rel.Name, err)
	}
	before, err := filterBeforeToKeyCols(rel, decoded)
	if err != nil {
		return fmt.Errorf("postgres: cdc: %w", err)
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

// filterBeforeToKeyCols narrows the decoded OldTuple of a DELETE or
// UPDATE event down to its identity-key columns, so the Before image
// the applier turns into a WHERE clause uses only the replica-identity
// predicates. The narrowing is load-bearing for silent-data-loss
// prevention on BOTH paths, and the protocol details driving it are
// asymmetric enough to be worth spelling out:
//
// DELETE (Bug 8): under REPLICA IDENTITY DEFAULT (and USING INDEX),
// pgoutput's DeleteMessage carries a 'K' OldTuple with ColumnNum equal
// to the relation's full column count, but only the identity-key
// columns hold actual data — non-key columns are sent as 'n' (null)
// markers. [decodeTuple] faithfully translates 'n' into a
// present-but-nil entry in the row map. The applier's
// [buildWhereClause] then emits "non_key_col IS NULL" for those
// entries, predicates that fail to match real rows whose non-key
// columns hold non-null values. The DELETE matches zero rows, ADR-0010
// absorbs the miss for resume idempotency, and the position advances —
// silent data divergence.
//
// UPDATE/DELETE (Bug 92): under REPLICA IDENTITY FULL the OldTuple
// carries EVERY column with real data ('t'), not null markers — AND
// pgoutput sets the per-column wire key flag on EVERY column (the whole
// row is "the identity"). The first Bug-92 fix attempt narrowed via
// relationColumn.KeyColumn (the wire flag) and so was a silent no-op
// under FULL: every column was flagged, nothing got dropped, and the
// WHERE still spanned rich types (jsonb / timestamptz / bytea /
// high-precision numeric) whose decoded→rebound text does NOT `=`-match
// the value already stored on the target. The statement matched zero
// rows, ADR-0010 absorbed the miss, and the new value was silently
// dropped. (DELETEs appeared to "land" pre-fix only because the test
// tables that exercised FULL+DELETE carried no rich-typed columns — a
// rich-typed DELETE under FULL drops just the same.)
//
// The correct narrowing therefore keys off rel.IdentityKeyCols, which
// is resolved per-relation by [CDCReader.resolveIdentityKeyCols]:
//   - DEFAULT / USING INDEX → the wire-flagged replica-identity columns;
//   - FULL → the table's TRUE PRIMARY KEY (queried via pg_index, NOT the
//     all-set wire flags). The WHERE becomes key-only (WHERE id = $N),
//     robust to round-trip representation drift in non-key columns.
//
// Edge cases:
//
//   - REPLICA IDENTITY NOTHING is rejected upstream in [emitDelete] and
//     [synthesizeKeyOnlyBefore] (no usable OldTuple), so this helper is
//     never reached with a NOTHING relation's tuple.
//   - A relation with no key columns at all (no PK and no REPLICA
//     IDENTITY index) under REPLICA IDENTITY FULL is unusual but
//     legitimate: the operator deliberately set FULL knowing there was
//     no PK. resolveIdentityKeyCols leaves IdentityKeyCols empty, and we
//     honour that by falling back to the full decoded row — anything
//     else would silently lose DELETEs/UPDATEs on PK-less FULL tables,
//     the very class of bug this helper exists to prevent. (FULL with no
//     key is the one case where rich-type round-trip drift can still
//     bite, but there is no narrower identity to fall back to; the
//     alternative — dropping the row — is strictly worse.)
func filterBeforeToKeyCols(rel *relationCacheEntry, decoded ir.Row) (ir.Row, error) {
	if len(rel.IdentityKeyCols) == 0 {
		// No resolvable identity (PK-less FULL, or a hand-built unit
		// fixture with no flagged columns): the only usable identity is
		// "every column", which is exactly what `decoded` already holds.
		// Hand it back verbatim.
		return decoded, nil
	}
	before := make(ir.Row, len(rel.IdentityKeyCols))
	for _, name := range rel.IdentityKeyCols {
		v, ok := decoded[name]
		if !ok {
			return nil, fmt.Errorf(
				"%s.%s: identity column %q missing from old tuple; refusing to emit a partial WHERE",
				rel.Schema, rel.Name, name,
			)
		}
		before[name] = v
	}
	return before, nil
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
//
// The reader's pinned (systemID, timeline) — captured from
// IDENTIFY_SYSTEM at stream-start — is propagated onto every emitted
// position so that on a subsequent reconnect, [resolveStartPosition]
// can compare the persisted pin against the new connection's
// IDENTIFY_SYSTEM reply and refuse loudly on divergence
// (ADR-0051, research finding F5).
func (r *CDCReader) positionAt(lsn pglogrepl.LSN) (ir.Position, error) {
	return encodePGPos(pgPos{
		Slot:     r.slotName,
		LSN:      lsn.String(),
		SystemID: r.systemID,
		Timeline: r.timeline,
	})
}

// ackLSN picks the LSN to advertise to the upstream slot. When an
// applier feedback tracker is wired, its value wins; until the
// applier reports its first commit, anchor at startLSN so the slot
// can't advance past the position the stream resumed from. Without a
// tracker (legacy/test paths), report streamedLSN — equivalent to
// the v0.4.0 behaviour, which is correct when no async-batched
// apply layer is buffering ahead of the durable target write.
//
// Bug 15 (post-v0.5.0): the pre-fix branch on `applied == 0` returned
// streamedLSN, which advances as the pump parses CommitMessages off
// the WAL stream — well before the applier has durably committed. On
// warm-resume against a fresh tracker (applied=0 always at startup,
// the tracker doesn't restore from persisted state), a keepalive
// firing in the window between stream-start and first-apply would
// ack confirmed_flush_lsn past the position. A subsequent crash or
// `sync stop` mid-batch then permanently lost the events between
// persisted_position and confirmed_flush_lsn.
//
// startLSN is the LSN the pump started streaming from (cold-start:
// snapshot LSN; warm-resume: persisted_position's LSN). It's the
// safe floor: the slot already had events past startLSN durably-
// applied at startup, and the applier's first commit will report a
// higher value via the tracker.
func (r *CDCReader) ackLSN(streamedLSN, startLSN pglogrepl.LSN) pglogrepl.LSN {
	if r.appliedLSN == nil {
		return streamedLSN
	}
	applied := r.appliedLSN.LoadApplied()
	if applied == 0 {
		return startLSN
	}
	return applied
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

// maybeSnapshotSchema is the ADR-0049 Chunk B3 boundary path. On
// every pgoutput RelationMessage it projects the just-built,
// already-IR-typed relationCacheEntry into an [ir.Table] (the
// in-stream position-anchored snapshot — pgoutput's RelationMessage
// IS the position-anchored metadata; no re-introspection, locked
// decision #2) and emits an [ir.SchemaSnapshot] iff the projected
// (column-name, ordered-type) signature differs from the one already
// snapshotted for this relation OID (locked DP-1 sign-off point ii:
// true-delta only — pgoutput re-sends Relation on reconnect /
// first-touch *without* any DDL; a naive "a Relation arrived" trigger
// would bloat the history with no-op versions and break DP-2's
// retention ∝ DDL-count assumption).
//
// The anchor is the RelationMessage's OWN WAL position (xld.WALStart,
// passed as relLSN) — captured at detection, BEFORE the first
// post-DDL row's LSN (locked decision #4c: a replayed event between
// the Relation and the first post-DDL row must resolve to the
// post-DDL schema; the Relation always precedes its rows in WAL so
// WALStart ≤ every subsequent row LSN and the PG LSN-≤ orderer
// resolves correctly).
//
// Out-of-scope schemas are skipped, mirroring the emit-side
// rel.Schema != r.schema gate: the applier hosts schema-history rows
// only for the bound schema's tables; a version for a relation whose
// rows are never applied would be dead weight.
//
// A loud floor is preserved: this path only ADDS a durable version
// write ahead of the relation's rows; the existing "insert/update/
// delete for unknown relation OID" hard errors in emit* are
// untouched. A send failure propagates (fatal/loud, decision #4b).
func (r *CDCReader) maybeSnapshotSchema(
	ctx context.Context,
	rel *relationCacheEntry,
	relID uint32,
	relLSN pglogrepl.LSN,
	snapshotSig map[uint32]ir.SchemaSignature,
	out chan<- ir.Change,
) error {
	if rel.Schema != r.schema {
		return nil // out-of-scope schema; no schema-history row to host
	}
	tbl := projectRelation(rel)
	sig := ir.SchemaSignatureOf(tbl)
	if prev, ok := snapshotSig[relID]; ok && prev.Equal(sig) {
		// No-op Relation re-emit (reconnect / first-touch with no
		// DDL): not a true delta — do NOT write a new version.
		return nil
	}
	pos, err := r.positionAt(relLSN)
	if err != nil {
		return err
	}
	if err := send(ctx, out, ir.SchemaSnapshot{
		Position: pos,
		Schema:   rel.Schema,
		Table:    rel.Name,
		IR:       tbl,
	}); err != nil {
		return err
	}
	snapshotSig[relID] = sig
	return nil
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

// resolveIdentityKeyCols populates entry.IdentityKeyCols — the column
// set the Before image must narrow to before it becomes an
// UPDATE/DELETE WHERE clause. Called once per RelationMessage (a fresh
// one is emitted on schema change), so it is off the per-row hot path.
//
// The resolution is asymmetric by replica identity, and that asymmetry
// IS the Bug 92 fix:
//
//   - DEFAULT ('d') / USING INDEX ('i'): the pgoutput per-column key
//     flag (relationColumn.KeyColumn) faithfully marks the replica-
//     identity index columns. Use them directly — no DB round-trip.
//
//   - FULL ('f'): pgoutput sets the key flag on EVERY column (the whole
//     row is "the identity"), so the wire flags are useless for
//     narrowing — trusting them keeps every column in the WHERE,
//     including rich types whose decoded→rebound text fails to `=`-match
//     the stored target value, the statement matches zero rows, and
//     ADR-0010's resume-idempotent zero-rows-ok behaviour silently
//     swallows the loss (Bug 92, a CRITICAL silent UPDATE/DELETE-loss
//     class). Under FULL we therefore IGNORE the wire flags and resolve
//     the table's TRUE PRIMARY KEY via pg_index. If the table has no PK,
//     IdentityKeyCols is left empty and filterBeforeToKeyCols falls back
//     to the full row (the only identity available on a PK-less FULL
//     table — anything narrower would be guessing).
//
//   - NOTHING ('n'): never reaches here with a usable tuple (emitDelete
//     and synthesizeKeyOnlyBefore reject first).
//
// A nil r.db (hand-built reader in non-streaming unit paths) skips the
// FULL PK lookup and falls through to the wire flags; the integration
// path always has a live pool.
func (r *CDCReader) resolveIdentityKeyCols(ctx context.Context, entry *relationCacheEntry) error {
	if entry.ReplicaIdentity == 'f' && r.db != nil {
		pkCols, err := primaryKeyColumnsFromCatalog(ctx, r.db, entry.Schema, entry.Name)
		if err != nil {
			return fmt.Errorf("resolve primary key for REPLICA IDENTITY FULL narrowing: %w", err)
		}
		// pkCols may be empty (FULL table with no PK) — that's a valid
		// state; the full-row fallback in filterBeforeToKeyCols handles
		// it. Store whatever we found; emptiness is meaningful.
		entry.IdentityKeyCols = pkCols
		return nil
	}
	// DEFAULT / USING INDEX (and the nil-db unit fallback): the wire key
	// flags are the replica-identity columns. Project them in column
	// order so the WHERE is deterministic.
	for _, col := range entry.Columns {
		if col.KeyColumn {
			entry.IdentityKeyCols = append(entry.IdentityKeyCols, col.Name)
		}
	}
	return nil
}

// primaryKeyColumnsFromCatalog returns the ordered PRIMARY KEY column
// names for schema.table by querying the live catalog (pg_index), or an
// empty slice if the table has no PRIMARY KEY. The ordering follows the
// index's column order (pg_index.indkey), which is the order an
// idempotent WHERE clause should use. Used only on the REPLICA IDENTITY
// FULL narrowing path (Bug 92), where pgoutput's wire key flags are
// all-set and cannot identify the real PK.
//
// Distinct from the IR-based [primaryKeyColumns] in row_writer_batch.go:
// that one extracts the PK from an already-introspected *ir.Table; this
// one runs a fresh catalog query because the CDC reader has only the
// pgoutput RelationMessage (which doesn't distinguish the PK under FULL)
// plus a live *sql.DB pool.
func primaryKeyColumnsFromCatalog(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	const q = `
		SELECT a.attname
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		JOIN   pg_attribute  a  ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  ix.indisprimary
		  AND  n.nspname  = $1
		  AND  cl.relname = $2
		  AND  cl.relkind = 'r'
		ORDER  BY u.ord`
	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
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
			"tuple has %d columns; relation has %d", tuple.ColumnNum, len(cols),
		)
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
				c.Name,
			)
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
			level,
		)
	}
	return nil
}

// ensurePublication CREATEs the publication if it doesn't already
// exist, or ALTERs an existing publication's table set when one of
// the call sites supplies an explicit list (Bug 13, ADR-0021).
//
// Three cases:
//
//   - tables == nil: legacy "FOR ALL TABLES" shape. The caller
//     hasn't told us which tables to scope to — typically a non-
//     streamer test path or a code path that doesn't yet have the
//     schema in hand. CREATE FOR ALL TABLES if missing; leave any
//     pre-existing publication alone.
//   - tables non-nil and missing: CREATE PUBLICATION … FOR TABLE
//     <list> with each name qualified by schema. The publication
//     is scoped to just those tables so a CREATE TABLE on the
//     source mid-stream stays out of the WAL stream and the
//     applier never sees events for a non-existent target table.
//   - tables non-nil and the publication already exists: ALTER
//     PUBLICATION … SET TABLE <list>. This handles the migration
//     path from a v0.4.0-or-earlier "FOR ALL TABLES" publication
//     to a scoped one. ALTER ... SET TABLE replaces the entire
//     table set atomically.
//
// The schema-qualification matters because a publication's table
// references resolve in the session's search_path; quoting and
// schema-qualifying both the relation and identifiers keeps the
// behaviour robust against unusual search_path settings.
func ensurePublication(ctx context.Context, db *sql.DB, name, schema string, tables []string) error {
	var exists, allTables bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), " +
		"COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}

	if !exists {
		var createQuery string
		if len(tables) == 0 {
			createQuery = fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(name))
		} else {
			createQuery = fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`,
				quoteIdent(name), formatPublicationTableList(schema, tables))
		}
		if _, err := db.ExecContext(ctx, createQuery); err != nil {
			return fmt.Errorf("postgres: create publication %q: %w", name, err)
		}
		return nil
	}

	// Publication exists. If the caller supplied an explicit table
	// list, sync the scope (ALTER … SET TABLE replaces the whole
	// list atomically; safe to run repeatedly). If the existing
	// publication is FOR ALL TABLES and the caller wants a scoped
	// list, ALTER ... SET TABLE on a FOR-ALL-TABLES publication
	// errors with "publication ... is defined as FOR ALL TABLES";
	// in that case we drop and recreate. The drop is safe because
	// the publication is metadata only — slots reference WAL by
	// LSN, not by publication name binding.
	if len(tables) == 0 {
		// No-op: caller hasn't supplied a scope; respect whatever
		// the publication currently is.
		return nil
	}
	if allTables {
		// Migrate: drop the FOR ALL TABLES publication and recreate
		// scoped. ALTER cannot demote FOR ALL TABLES → FOR TABLE
		// directly.
		dropQuery := fmt.Sprintf(`DROP PUBLICATION %s`, quoteIdent(name))
		if _, err := db.ExecContext(ctx, dropQuery); err != nil {
			return fmt.Errorf("postgres: drop FOR-ALL-TABLES publication %q for migration: %w", name, err)
		}
		createQuery := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`,
			quoteIdent(name), formatPublicationTableList(schema, tables))
		if _, err := db.ExecContext(ctx, createQuery); err != nil {
			return fmt.Errorf("postgres: re-create publication %q with scoped tables: %w", name, err)
		}
		return nil
	}
	alterQuery := fmt.Sprintf(`ALTER PUBLICATION %s SET TABLE %s`,
		quoteIdent(name), formatPublicationTableList(schema, tables))
	if _, err := db.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("postgres: alter publication %q tables: %w", name, err)
	}
	return nil
}

// addTablesToPublication issues `ALTER PUBLICATION ... ADD TABLE
// <list>` so the named tables join the publication scope without
// disturbing the existing scope. Used by the mid-stream add-table
// flow where ensurePublication's `SET TABLE` semantics would replace
// the entire list and silently drop tables that were already in
// scope.
//
// Refuses (with a clear error) when the publication is FOR ALL
// TABLES — adding a specific table to a FOR ALL TABLES publication
// is meaningless and almost always indicates an operator
// misconfiguration. The publication must already exist.
//
// Tables already in the publication are skipped so the call is
// idempotent on a partial-add re-run. Schema-qualifies each table.
func addTablesToPublication(ctx context.Context, db *sql.DB, name, schema string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	var exists, allTables bool
	const checkQuery = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1), " +
		"COALESCE((SELECT puballtables FROM pg_publication WHERE pubname = $1), false)"
	if err := db.QueryRowContext(ctx, checkQuery, name).Scan(&exists, &allTables); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}
	if !exists {
		return fmt.Errorf("postgres: add tables to publication %q: publication does not exist (mid-stream add-table requires an active stream's publication)", name)
	}
	if allTables {
		return fmt.Errorf("postgres: add tables to publication %q: publication is FOR ALL TABLES; nothing to add (the new table is already implicitly in scope)", name)
	}

	// Look up the current member set so we can skip duplicates and
	// emit a clean ALTER even if some of the supplied tables are
	// already in the publication. pg_publication_rel + pg_class +
	// pg_namespace gives us schema-qualified names that match
	// formatPublicationTableList's quoting.
	const memberQuery = `
		SELECT n.nspname, c.relname
		FROM   pg_publication p
		JOIN   pg_publication_rel pr ON pr.prpubid = p.oid
		JOIN   pg_class c            ON c.oid     = pr.prrelid
		JOIN   pg_namespace n        ON n.oid     = c.relnamespace
		WHERE  p.pubname = $1`
	rows, err := db.QueryContext(ctx, memberQuery, name)
	if err != nil {
		return fmt.Errorf("postgres: list publication members: %w", err)
	}
	defer func() { _ = rows.Close() }()

	existing := make(map[string]struct{})
	for rows.Next() {
		var nsp, rel string
		if err := rows.Scan(&nsp, &rel); err != nil {
			return fmt.Errorf("postgres: scan publication member: %w", err)
		}
		existing[nsp+"."+rel] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: list publication members: %w", err)
	}

	toAdd := make([]string, 0, len(tables))
	for _, t := range tables {
		key := schema + "." + t
		if schema == "" {
			// Match the unqualified shape used by formatPublicationTableList
			// for empty-schema callers.
			key = "." + t
		}
		if _, ok := existing[key]; ok {
			continue
		}
		toAdd = append(toAdd, t)
	}
	if len(toAdd) == 0 {
		return nil
	}

	alterQuery := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s`,
		quoteIdent(name), formatPublicationTableList(schema, toAdd))
	if _, err := db.ExecContext(ctx, alterQuery); err != nil {
		return fmt.Errorf("postgres: alter publication %q add tables: %w", name, err)
	}
	return nil
}

// formatPublicationTableList renders a comma-separated list of
// schema-qualified, double-quoted table identifiers for use after
// `FOR TABLE` / `SET TABLE`.
func formatPublicationTableList(schema string, tables []string) string {
	parts := make([]string, len(tables))
	for i, t := range tables {
		if schema == "" {
			parts[i] = quoteIdent(t)
			continue
		}
		parts[i] = quoteIdent(schema) + "." + quoteIdent(t)
	}
	return strings.Join(parts, ", ")
}

// slotState carries the bits of pg_replication_slots the CDC reader
// uses for cold-start validation. WALStatus drives the can-we-resume?
// decision; see checkSlotUsable for the state-transition table.
type slotState struct {
	SlotName  string
	WALStatus string
}

// slotInfo returns the slot's state, or nil when no row exists. The
// "row missing" case is split out from the error path because the
// cold-start code branches on existence — a missing slot is normal
// (we'll create one), but an errored query is fatal.
//
// wal_status was added in PG 13. On older servers, the column would
// be absent and this query would error; sluice's Engine.Capabilities
// lists pgoutput-v2 (PG 14+) as the baseline, so this is safe.
func slotInfo(ctx context.Context, db *sql.DB, name string) (*slotState, error) {
	const q = `SELECT slot_name, COALESCE(wal_status, '') FROM pg_replication_slots WHERE slot_name = $1`
	row := db.QueryRowContext(ctx, q, name)
	var s slotState
	if err := row.Scan(&s.SlotName, &s.WALStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres: check slot: %w", err)
	}
	return &s, nil
}

// dropReplicationSlot drops the named slot via the replication
// protocol. Used by the cold-start cleanup path to remove a slot we
// just created when a later setup step fails.
func dropReplicationSlot(ctx context.Context, conn *pgconn.PgConn, name string) error {
	return pglogrepl.DropReplicationSlot(ctx, conn, name, pglogrepl.DropReplicationSlotOptions{})
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
