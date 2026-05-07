// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Snapshot-import support for the parallel within-table bulk-copy
// path (v0.5.0). The orchestrator's parallel-copy phase needs N
// reader connections that all observe the same consistent source view;
// PG's `SET TRANSACTION SNAPSHOT '<name>'` provides that out-of-the-
// box, but only for as long as the exporting transaction (held open
// by [OpenSnapshotStream]) is still alive.
//
// This file implements the [ir.SnapshotImporter] surface — given a
// snapshot name from a previously-exported snapshot, it returns N
// pinned-and-imported [RowReader] values. Each reader holds its own
// *sql.Conn and its own REPEATABLE READ transaction; closing each
// reader rolls back its tx and returns the connection to the pool.
//
// The MySQL engine deliberately does not implement this interface —
// MySQL's REPEATABLE READ snapshot is per-session with no shareable
// name, so N readers would necessarily see N independent snapshots.
// ADR-0019 documents the asymmetry.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// SnapshotImporter is the engine-level surface for [ir.SnapshotImporter].
// It is constructed via [Engine.OpenSnapshotImporter]; one importer
// owns one *sql.DB pool and produces N pinned readers via
// [SnapshotImporter.ImportSnapshot].
type SnapshotImporter struct {
	db     *sql.DB
	schema string
}

// Close releases the underlying connection pool. Callers should close
// the importer after every reader returned by ImportSnapshot has been
// closed; closing the importer does not roll back any open
// reader transactions.
func (s *SnapshotImporter) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// ImportSnapshot returns n pinned RowReaders, each tied to a separate
// *sql.Conn that has imported snapshotName. Each reader runs its read
// queries inside a long-lived REPEATABLE READ READ ONLY transaction;
// closing the reader rolls the tx back and returns the connection to
// the importer's pool.
//
// Failure on the kth import closes the previously-imported readers
// before returning, so callers don't need a partial-cleanup branch.
func (s *SnapshotImporter) ImportSnapshot(ctx context.Context, snapshotName string, n int) ([]ir.RowReader, error) {
	if s == nil {
		return nil, errors.New("postgres: SnapshotImporter: nil receiver")
	}
	if snapshotName == "" {
		return nil, errors.New("postgres: ImportSnapshot: empty snapshotName")
	}
	if n <= 0 {
		return nil, errors.New("postgres: ImportSnapshot: n must be > 0")
	}

	readers := make([]ir.RowReader, 0, n)
	cleanup := func() {
		for _, r := range readers {
			if c, ok := r.(closer); ok {
				_ = c.Close()
			}
		}
	}

	for i := 0; i < n; i++ {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("postgres: ImportSnapshot conn %d: %w", i, err)
		}
		// SET TRANSACTION SNAPSHOT must be the first statement after
		// BEGIN — the docs are explicit. We use the same shape as the
		// snapshot-stream import in cdc_snapshot.go.
		if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
			_ = conn.Close()
			cleanup()
			return nil, fmt.Errorf("postgres: ImportSnapshot BEGIN %d: %w", i, err)
		}
		stmt := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", quoteSnapshotName(snapshotName))
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			_ = conn.Close()
			cleanup()
			return nil, fmt.Errorf("postgres: ImportSnapshot SET %d: %w", i, err)
		}
		// Wrap the pinned conn into a RowReader. The closer is a small
		// adapter that ROLLBACKs the snapshot tx then returns the conn
		// to the pool.
		rdr := &RowReader{
			q:      conn,
			schema: s.schema,
			closer: snapshotConnCloser{conn: conn},
		}
		readers = append(readers, rdr)
	}
	return readers, nil
}

// snapshotConnCloser ROLLBACKs the open REPEATABLE READ tx and then
// closes the *sql.Conn (returning it to the pool). The two-step
// shape mirrors what cdc_snapshot.go does on its single pinned conn.
type snapshotConnCloser struct {
	conn *sql.Conn
}

func (c snapshotConnCloser) Close() error {
	// ROLLBACK on a context.Background so a cancelled parent ctx
	// doesn't prevent the cleanup statement from running.
	_, _ = c.conn.ExecContext(context.Background(), "ROLLBACK")
	return c.conn.Close()
}

// OpenSnapshotImporter returns a [SnapshotImporter] bound to the
// database identified by dsn. Implements [ir.SnapshotImporterOpener];
// the orchestrator type-asserts on this method when wiring N parallel
// readers to a single exported snapshot.
func (Engine) OpenSnapshotImporter(ctx context.Context, dsn string) (ir.SnapshotImporter, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SnapshotImporter{db: db, schema: cfg.schema}, nil
}
