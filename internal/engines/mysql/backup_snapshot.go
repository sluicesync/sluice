// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// OpenBackupSnapshot implements [ir.BackupSnapshotOpener]. It captures
// a consistent MySQL snapshot via `START TRANSACTION WITH CONSISTENT
// SNAPSHOT` on a single pinned connection, returning a snapshot-pinned
// RowReader the full-backup orchestrator drives the table sweep
// against.
//
// Because MySQL's REPEATABLE READ snapshot is per-session and not
// shareable across connections (ADR-0019), all table reads run on
// this one connection sequentially. The trade-off is documented in
// the v0.18.0 release notes: MySQL backups under heavy parallelism
// configurations don't get parallel reads (PG does), but the cross-
// table consistency property holds and the EndPosition is anchored
// at the snapshot's start point — closing the during-backup write-
// window gap.
//
// slotName is accepted for [ir.BackupSnapshotOpener] interface
// uniformity and ignored — MySQL has no slot concept on the source
// side. The captured EndPosition is `@@global.gtid_executed` (when
// GTID mode is on) or `(file, pos)` (when off), captured INSIDE the
// snapshot transaction so the recorded position refers to the
// snapshot's logical clock.
//
// Caller closes the returned snapshot to commit the snapshot tx,
// release the pinned conn, and close the underlying DB pool.
//
// PlanetScale flavor (v0.44.0, GitHub issue #16): delegates to
// [openBackupSnapshotVStream] which rides VStream COPY mode for
// both data and position capture. The pinned-conn + binlog-position
// path below stays for vanilla MySQL only — applying it to a Vitess
// source produces a binlog-shape position the VStream-based
// continuous-sync path can't decode, breaking incremental + stream-
// run chain-resume entirely. The pre-v0.44.0 PS-MySQL backup-via-
// pinned-conn path "worked" against single-shard keyspaces only in
// the data-read sense — operators couldn't actually chain
// incrementals onto those backups because the encoded position
// shape was wrong.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn, slotName string) (*ir.BackupSnapshot, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: backup snapshot not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor == FlavorPlanetScale {
		return e.openBackupSnapshotVStream(ctx, dsn)
	}
	_ = slotName // accepted for interface uniformity; ignored on the binlog-snapshot path
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: pin conn: %w", err)
	}

	// REPEATABLE READ + WITH CONSISTENT SNAPSHOT in two statements is
	// the canonical InnoDB snapshot capture (mirrors
	// [openBinlogSnapshotStream]). The session isolation is set
	// explicitly first so the behaviour doesn't depend on the
	// server's tx_isolation default.
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: set isolation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: start tx: %w", err)
	}

	// Capture the position INSIDE the snapshot tx so it refers to the
	// snapshot's logical clock (the same shape openBinlogSnapshotStream
	// uses). Prefer GTID when gtid_mode is on; fall back to file/pos.
	position, err := captureBackupPositionInTx(ctx, conn)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: capture position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.DBName,
		closer: nil, // BackupSnapshot owns the lifecycle
	}

	closed := false
	closeFn := func() error {
		if closed {
			return nil
		}
		closed = true
		// Order matters: COMMIT (releases the read view), close the
		// pinned conn back to the pool, close the underlying DB pool.
		// context.Background so a cancelled parent ctx doesn't prevent
		// cleanup.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
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

// captureBackupPositionInTx queries the source-side cursor against the
// pinned snapshot connection and encodes it the same way the standalone
// CDC reader emits via [encodeBinlogPos]. The function is the conn-
// scoped sibling of [SchemaReader.CaptureBackupPosition].
func captureBackupPositionInTx(ctx context.Context, conn *sql.Conn) (ir.Position, error) {
	useGTID, err := gtidModeOnConn(ctx, conn)
	if err != nil {
		return ir.Position{}, fmt.Errorf("detect gtid mode: %w", err)
	}
	if useGTID {
		set, err := executedGTIDSetConn(ctx, conn)
		if err != nil {
			return ir.Position{}, fmt.Errorf("read @@gtid_executed: %w", err)
		}
		return encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
	}
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		return ir.Position{}, fmt.Errorf("master status: %w", err)
	}
	return encodeBinlogPos(binlogPos{Mode: positionModeFilePos, File: file, Pos: pos})
}

// gtidModeOnConn is the *sql.Conn variant of gtidModeOn (cdc_reader.go).
// Used so the snapshot's gtid_mode probe runs INSIDE the snapshot tx —
// reading the SHOW VARIABLES result outside the tx would lose the
// snapshot read-view alignment.
func gtidModeOnConn(ctx context.Context, conn *sql.Conn) (bool, error) {
	var name, value string
	err := conn.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'gtid_mode'").Scan(&name, &value)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "ON" || value == "ON_PERMISSIVE", nil
}

// executedGTIDSetConn is the *sql.Conn variant of executedGTIDSet.
func executedGTIDSetConn(ctx context.Context, conn *sql.Conn) (string, error) {
	var set string
	err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&set)
	if err != nil {
		return "", err
	}
	return set, nil
}
