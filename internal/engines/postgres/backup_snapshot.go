// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// backupSnapshotSlotPrefix is the prefix the backup-anchor temporary
// slot is named with. Each call appends a Unix-nanosecond timestamp so
// concurrent backups against the same source don't fight for the same
// slot name. The slot is protocol-TEMPORARY (Bug 137) — the server
// drops it when the creating replication conn closes, including on
// hard process death — so the timestamp is purely for
// collision-avoidance during the run. The timestamp doubles as the
// age signal the resume-time orphan sweep uses to clean up persistent
// anchors leaked by pre-fix binaries (see backup_anchor_sweep.go).
const backupSnapshotSlotPrefix = "sluice_backup_anchor_"

// OpenBackupSnapshot implements [ir.BackupSnapshotOpener]. It captures
// a consistent Postgres snapshot anchored at a logical-replication
// slot's `consistent_point` LSN, returning a snapshot-pinned RowReader
// the full-backup orchestrator drives the table sweep against.
//
// Two anchor shapes, selected by opts.PersistChainSlot:
//
//   - Default (false): the anchor slot is protocol-TEMPORARY (named
//     with a timestamp prefix; the server drops it when the creating
//     replication conn closes — graceful Close AND hard process death
//     both qualify, which is the Bug 137 fix: a SIGKILLed backup can
//     no longer leak a persistent slot that pins WAL forever) —
//     distinct from the chain-handoff slot recorded in the manifest's
//     EndPosition. The exported snapshot only needs the replication
//     conn alive until the SQL conn below has run SET TRANSACTION
//     SNAPSHOT; after that the pinned tx stands alone, and nothing
//     ever consumes the anchor slot — so tying the slot's life to the
//     session costs nothing. The
//     chain-handoff slot is the operator's responsibility to maintain
//     (created via `sluice sync start` or manually, BEFORE this
//     backup — a slot created after it cannot serve the WAL in
//     between; see [Engine.PreflightChainResume]).
//   - --chain-slot (true): the PERSISTENT chain slot itself (named
//     opts.SlotName) is created and used as the anchor, so its
//     consistent point IS the recorded EndPosition and `backup
//     incremental` chains with zero gap by construction. The slot is
//     kept only when the orchestrator calls [ir.BackupSnapshot.Commit]
//     — since task #42 (ADR-0085) that happens once the run's
//     in-progress manifest durably records the anchor, so an
//     interrupted-but-resumable run keeps the slot for resume adoption;
//     a run that fails before that point Closes uncommitted and drops
//     it. The publication the CDC reader decodes through is
//     ensured here too — pgoutput evaluates publication membership
//     with a HISTORIC catalog snapshot, so a publication created
//     after the anchor cannot decode the chain's first window.
//
// Caller closes the returned snapshot to release the snapshot tx, the
// pinned SQL conn(s), the slot-creation replication conn, the anchor
// slot (unless committed), and the underlying DB pool.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn string, opts ir.BackupSnapshotOptions) (*ir.BackupSnapshot, error) {
	chainSlotName := opts.SlotName
	if chainSlotName == "" {
		chainSlotName = defaultSlot
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := checkWALLevel(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Resolve the anchor slot. Default shape: a fresh timestamped
	// protocol-TEMPORARY anchor, auto-dropped when its replication
	// conn closes. --chain-slot shape: the persistent chain slot
	// itself.
	anchorSlot := fmt.Sprintf("%s%d", backupSnapshotSlotPrefix, time.Now().UnixNano())
	anchorIsTemporary := !opts.PersistChainSlot
	if opts.PersistChainSlot {
		anchorSlot = chainSlotName
	}

	// dropAnchorBestEffort cleans up a half-created PERSISTENT anchor
	// on the open-failure paths below. A temporary anchor needs no SQL
	// drop — the closeReplConnGraceful that always follows releases it
	// server-side (and a cross-session pg_drop_replication_slot on a
	// temporary slot would fail anyway: temporary slots stay owned by
	// their creating session for their whole life).
	dropAnchorBestEffort := func() {
		if anchorIsTemporary {
			return
		}
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
	}

	// A pre-existing slot at the anchor name is refused loudly. For
	// the timestamped default this is near-impossible and indicates a
	// stale leak; for --chain-slot it is the load-bearing guard: an
	// existing slot's consistent point is NOT this backup's anchor, so
	// silently reusing it would record a position the slot may not be
	// able to serve gap-free.
	info, err := slotInfo(ctx, db, anchorSlot)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if info != nil {
		_ = db.Close()
		if opts.PersistChainSlot {
			// Recovery wording (task #42, ADR-0085): a crashed
			// --chain-slot backup's slot is the WAL-retention guarantee
			// for a sound resume — re-running the same backup ADOPTS it
			// (the resume opens a temporary anchor, so it never reaches
			// this refusal). The pre-fix message advised drop+retry as
			// crash recovery, which released the gap-covering WAL and
			// funneled straight into the silent chain gap.
			return nil, fmt.Errorf(
				"postgres: backup snapshot: --chain-slot: replication slot %q already exists. "+
					"It may belong to a running `sluice sync` (which already retains WAL for chaining — omit --chain-slot and chain off its position), "+
					"an interrupted --chain-slot backup (re-run the SAME `backup full` command against the same destination — resume adopts the slot and its anchor; "+
					"do NOT drop the slot to recover, that releases the WAL the resume needs), "+
					"or another consumer (pass a different --slot-name). "+
					"Only for a deliberate fresh start: drop it via `sluice slot drop %s` and pass --force-overwrite",
				anchorSlot, anchorSlot,
			)
		}
		return nil, fmt.Errorf(
			"postgres: backup snapshot: anchor slot %q already exists; this should be impossible (timestamped name) — drop manually and retry",
			anchorSlot,
		)
	}

	// --chain-slot: ensure the publication the chain's incrementals
	// will decode through exists BEFORE the slot is created. pgoutput
	// resolves publication membership with a historic catalog snapshot
	// at each WAL record's LSN, so the publication must predate the
	// anchor or the chain's first window cannot be decoded (loud
	// "publication does not exist" at incremental time — observed live
	// in the 2026-06-10 backup benchmark). FOR ALL TABLES matches the
	// CDC reader's own no-scope ensure and is superset-safe.
	if opts.PersistChainSlot {
		if err := ensureAllTablesPublication(ctx, db, defaultPublication); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("postgres: backup snapshot: --chain-slot: ensure publication: %w", err)
		}
	}

	// Open a replication connection dedicated to slot creation. We
	// keep it alive for the lifetime of the BackupSnapshot so the
	// exported snapshot stays valid through the row sweep. Once we
	// drop the slot in Close the conn is released too.
	replConn, err := openReplicationConn(ctx, cfg.dsn)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: open replication conn: %w", err)
	}

	// EXPORT_SNAPSHOT under createLogicalReplicationSlot's PG-version-
	// adaptive helper. Default shape: protocol-TEMPORARY (Bug 137) —
	// the slot's only job is to pin the exported snapshot for this
	// run, and the server auto-drops it with the replication conn, so
	// a hard-killed run leaves nothing behind. --chain-slot shape:
	// persistent + FAILOVER true on PG 17+ — that slot is intended to
	// live across failovers, so FAILOVER is exactly right there (and
	// the server refuses TEMPORARY+FAILOVER combined anyway).
	consistentPoint, snapshotName, err := createLogicalReplicationSlot(ctx, db, replConn, anchorSlot, slotCreateOptions{
		exportSnapshot: true,
		temporary:      anchorIsTemporary,
	})
	if err != nil {
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: %w", err)
	}
	if snapshotName == "" {
		// Drop the slot we just made so we don't leak it.
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, errors.New("postgres: backup snapshot: server returned empty snapshot_name; expected EXPORT_SNAPSHOT to populate it")
	}

	// Pin a regular SQL connection and import the exported snapshot.
	// SET TRANSACTION SNAPSHOT must be the first statement after
	// BEGIN — the docs are explicit about this.
	conn, err := db.Conn(ctx)
	if err != nil {
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: BEGIN: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", quoteSnapshotName(snapshotName))); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: SET TRANSACTION SNAPSHOT: %w", err)
	}

	// Encode the position with the CHAIN-HANDOFF slot name (not the
	// anchor slot) so a Phase 3 incremental against this manifest
	// opens CDC against the slot the operator manages. On the
	// --chain-slot shape the two are the same slot, so the recorded
	// name is right either way. The recorded LSN is the snapshot's
	// consistent_point: every write before that LSN is captured by the
	// row sweep; every write after it is captured by the chain's next
	// link's CDC stream from this LSN forward.
	position, err := encodePGPos(pgPos{Slot: chainSlotName, LSN: consistentPoint})
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: encode position: %w", err)
	}

	if !opts.PersistChainSlot {
		// The chain prerequisites (standing slot + publication) are NOT
		// provisioned on this shape — say so once, at the moment the
		// operator can still act on it, instead of letting the first
		// `backup incremental` discover it the hard way.
		slog.InfoContext(
			ctx, "backup: snapshot anchor slot is temporary; to chain incrementals off this backup, the chain slot must retain WAL from this point",
			slog.String("chain_slot", chainSlotName),
			slog.String("hint", "re-run with --chain-slot to provision it at the anchor, or run continuous `sluice sync start`"),
		)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.schema,
		closer: nil, // BackupSnapshot owns the lifecycle
	}

	closed := false
	committed := false
	closeFn := func() error {
		if closed {
			return nil
		}
		closed = true
		// Order matters: commit (or rollback) the snapshot tx → drop
		// the anchor slot via SQL → close the replication conn → close
		// the DB pool. The anchor slot drop must happen on the regular
		// SQL conn (not the replication conn) because the replication
		// conn is in REPLICATION mode and pg_drop_replication_slot()
		// is a regular SQL function. context.Background so a cancelled
		// parent ctx doesn't prevent cleanup.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if anchorIsTemporary {
			// Protocol-TEMPORARY anchor (Bug 137): the server drops the
			// slot itself when replConn closes below — including on hard
			// process death, which is the entire point. No explicit SQL
			// drop is attempted: a temporary slot stays owned (active)
			// by its creating session for its whole life, so a
			// cross-session pg_drop_replication_slot here would fail
			// with "replication slot is active for PID …" rather than
			// drop anything.
		} else if committed {
			// --chain-slot run that completed: the chain slot is now a
			// durable resource anchored at this backup's EndPosition —
			// keeping it is the entire point. Skip the drop.
			slog.InfoContext(
				context.Background(), "postgres: backup snapshot: chain slot persisted at the backup's anchor position",
				slog.String("slot", anchorSlot),
				slog.String("consistent_point", consistentPoint),
				slog.String("note", "the slot retains WAL until the next `backup incremental` consumes it; drop via `sluice slot drop` if you abandon the chain"),
			)
		} else if _, err := db.ExecContext(context.Background(), "SELECT pg_drop_replication_slot($1)", anchorSlot); err != nil {
			// Slot drop failure is logged but doesn't escalate — the
			// backup itself is durable, and a leaked anchor slot is
			// recoverable via `sluice slot drop` or manual SQL.
			slog.WarnContext(
				context.Background(), "postgres: backup snapshot: drop anchor slot failed; manual cleanup may be required",
				slog.String("slot", anchorSlot),
				slog.String("err", err.Error()),
			)
			if !isSlotAlreadyGoneErr(err) && firstErr == nil {
				firstErr = err
			}
		}
		if err := replConn.Close(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	snap := &ir.BackupSnapshot{
		Position:     position,
		Rows:         rowReader,
		CloseFn:      closeFn,
		SnapshotName: snapshotName,
	}
	if opts.PersistChainSlot {
		snap.CommitFn = func(context.Context) error {
			committed = true
			return nil
		}
	}
	return snap, nil
}

// isSlotAlreadyGoneErr reports whether err is a "slot does not exist"
// error from pg_drop_replication_slot. The drop call uses an
// idempotent intent — finding the slot already gone (manual drop,
// automatic cleanup on connection drop, etc.) is success, not failure.
func isSlotAlreadyGoneErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not exist")
}

// avoid an unused-import warning when sql is referenced indirectly.
var _ = sql.ErrNoRows
