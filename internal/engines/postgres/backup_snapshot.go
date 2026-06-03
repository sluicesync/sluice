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
// slot name. The slot is dropped on the snapshot's Close, so the
// timestamp is purely for collision-avoidance during the run.
const backupSnapshotSlotPrefix = "sluice_backup_anchor_"

// OpenBackupSnapshot implements [ir.BackupSnapshotOpener]. It captures
// a consistent Postgres snapshot anchored at a logical-replication
// slot's `consistent_point` LSN, returning a snapshot-pinned RowReader
// the full-backup orchestrator drives the table sweep against.
//
// The slot used to anchor the snapshot is a TEMPORARY-shape (named
// with a timestamp prefix and dropped on Close) — distinct from the
// chain-handoff slot recorded in the manifest's EndPosition. The
// chain-handoff slot is the operator's responsibility to maintain
// (created via `sluice sync start` or manually); this anchor slot
// only exists for the duration of the backup run.
//
// Caller closes the returned snapshot to release the snapshot tx, the
// pinned SQL conn(s), the slot-creation replication conn, the anchor
// slot (DROP), and the underlying DB pool.
//
// chainSlotName is the slot name to record on the returned Position so
// a Phase 3 incremental chained off this manifest opens CDC against
// the right slot. Empty falls back to [defaultSlot]. The chain slot
// need not exist at backup time — Phase 3.3's `--position-from-manifest`
// pre-flights slot state before resuming CDC.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn, chainSlotName string) (*ir.BackupSnapshot, error) {
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

	// Generate a fresh anchor-slot name. The orchestrator drops it on
	// Close; the timestamp suffix protects against collisions when two
	// backups race against the same source.
	anchorSlot := fmt.Sprintf("%s%d", backupSnapshotSlotPrefix, time.Now().UnixNano())

	// A pre-existing anchor slot at this exact name is implausible
	// (timestamped) but the failure mode would silently inherit a
	// stale consistent_point — refuse explicitly if one is found.
	info, err := slotInfo(ctx, db, anchorSlot)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if info != nil {
		_ = db.Close()
		return nil, fmt.Errorf(
			"postgres: backup snapshot: anchor slot %q already exists; this should be impossible (timestamped name) — drop manually and retry",
			anchorSlot,
		)
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
	// adaptive helper (FAILOVER true on PG 17+). We don't strictly need
	// FAILOVER on the anchor slot since it lives only for the duration
	// of the backup, but the helper is the single source of truth and
	// the cost on PG ≤ 16 is one stderr warning per slot name (which
	// our timestamp suffix uniques anyway, so each backup run gets one
	// warning — acceptable noise for the benefit of unified slot-
	// creation code).
	consistentPoint, snapshotName, err := createLogicalReplicationSlot(ctx, db, replConn, anchorSlot, true)
	if err != nil {
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: %w", err)
	}
	if snapshotName == "" {
		// Drop the slot we just made so we don't leak it.
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, errors.New("postgres: backup snapshot: server returned empty snapshot_name; expected EXPORT_SNAPSHOT to populate it")
	}

	// Pin a regular SQL connection and import the exported snapshot.
	// SET TRANSACTION SNAPSHOT must be the first statement after
	// BEGIN — the docs are explicit about this.
	conn, err := db.Conn(ctx)
	if err != nil {
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: BEGIN: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", quoteSnapshotName(snapshotName))); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: SET TRANSACTION SNAPSHOT: %w", err)
	}

	// Encode the position with the CHAIN-HANDOFF slot name (not the
	// anchor slot) so a Phase 3 incremental against this manifest
	// opens CDC against the slot the operator manages, even though
	// the anchor slot is long gone. The recorded LSN is the snapshot's
	// consistent_point: every write before that LSN is captured by the
	// row sweep; every write after it is captured by the chain's next
	// link's CDC stream from this LSN forward.
	position, err := encodePGPos(pgPos{Slot: chainSlotName, LSN: consistentPoint})
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_, _ = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", anchorSlot)
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: encode position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.schema,
		closer: nil, // BackupSnapshot owns the lifecycle
	}

	closed := false
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
		if _, err := db.ExecContext(context.Background(), "SELECT pg_drop_replication_slot($1)", anchorSlot); err != nil {
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

	return &ir.BackupSnapshot{
		Position: position,
		Rows:     rowReader,
		CloseFn:  closeFn,
	}, nil
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
