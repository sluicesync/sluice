// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Plain-SQL snapshot export for the migrate path (perf research delta
// 1). The sync cold-start pins its parallel readers to the snapshot a
// replication slot exports (CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT,
// cdc_snapshot.go); backup anchors on a slot the same way
// (backup_snapshot.go). `sluice migrate` needs the SAME shareable-
// snapshot property with NONE of the replication prerequisites — no
// wal_level=logical, no REPLICATION privilege, no slot to leak — so
// this file exports one the plain-SQL way: a pinned connection runs
// BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY, then
// pg_export_snapshot(); the returned name is importable by
// [SnapshotImporter] readers for as long as that transaction stays
// open. The exporting connection doubles as the migrate orchestrator's
// primary RowReader (the same conn-doubles-as-reader shape
// OpenBackupSnapshot uses).
//
// pg_export_snapshot() cannot run during recovery, so a hot-standby
// source returns a loud error here and the orchestrator falls back to
// the documented independent-per-connection readers.

package postgres

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ExportSnapshot implements [ir.SnapshotExporter]. The returned
// snapshot's Rows is a pinned single-conn reader (snapshotPinned, with
// the off-snapshot estimator DSN threaded exactly like the importer
// readers mint theirs), its Name feeds [Engine.OpenSnapshotImporter]
// imports, and its Release/Close closures implement the
// [ir.ExportedSnapshot] lifecycle: Release COMMITs the exporting
// transaction (read-only, so commit == rollback semantically) and
// leaves the conn usable in autocommit mode; Close returns the conn and
// closes the pool. Both are idempotent.
func (e Engine) ExportSnapshot(ctx context.Context, dsn string) (*ir.ExportedSnapshot, error) {
	cfg, err := e.parseDSN(dsn)
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
		return nil, fmt.Errorf("postgres: export snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("postgres: export snapshot: BEGIN: %w", err)
	}
	var name string
	if err := conn.QueryRowContext(ctx, "SELECT pg_export_snapshot()").Scan(&name); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("postgres: export snapshot: pg_export_snapshot: %w", err)
	}
	if name == "" {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, errors.New("postgres: export snapshot: server returned an empty snapshot name")
	}

	// The exporting conn IS the primary reader: snapshotPinned so the
	// CountRows overlapping-query deadlock guard engages, and the
	// estimator DSN threaded so the pre-stream chunk decision probes
	// reltuples on a FRESH conn, never the pinned one (ADR-0079 v1.1).
	// estimatorExactCount keeps migrate's ADR-0042 N1 chunk-decision
	// contract: on the never-ANALYZEd reltuples sentinel the estimate
	// resolves via an exact COUNT(*) on that same fresh off-snapshot
	// conn (a size DECISION needs no snapshot consistency — the chunk
	// BOUNDS and row streams stay pinned), so a freshly-loaded source
	// still parallelizes exactly as it did when the migrate primary was
	// a non-pinned *sql.DB reader. closer stays nil — the
	// ExportedSnapshot closures own the lifecycle, exactly like the
	// backup snapshot's reader.
	rows := &RowReader{
		q:                   conn,
		schema:              cfg.schema,
		closer:              nil,
		snapshotPinned:      true,
		estimatorDSN:        cfg.dsn,
		estimatorAppID:      cfg.appID,
		estimatorExactCount: true,
	}

	released := false
	closed := false
	releaseFn := func() error {
		if released || closed {
			return nil
		}
		released = true
		// context.Background so a cancelled parent ctx can't strand the
		// exporting transaction open (the vacuum pin Release exists to
		// drop). Post-COMMIT the conn serves autocommit queries — Rows
		// keeps working with fresh per-query views.
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
			return fmt.Errorf("postgres: export snapshot: release COMMIT: %w", err)
		}
		return nil
	}
	closeFn := func() error {
		if closed {
			return nil
		}
		var firstErr error
		if err := releaseFn(); err != nil {
			firstErr = err
		}
		closed = true
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	return &ir.ExportedSnapshot{
		Name:      name,
		Rows:      rows,
		ReleaseFn: releaseFn,
		CloseFn:   closeFn,
	}, nil
}
