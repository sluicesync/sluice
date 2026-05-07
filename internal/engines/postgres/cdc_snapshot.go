// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/orware/sluice/internal/ir"
)

// OpenSnapshotStream opens a consistent Postgres snapshot via the
// logical-replication slot's atomic CREATE_REPLICATION_SLOT EXPORT_
// SNAPSHOT mechanism, and returns a paired RowReader (pinned to a
// transaction that imports the exported snapshot) and CDCReader
// (configured to start from the slot's consistent_point LSN).
//
// Caller closes the returned stream to release the snapshot tx,
// the pinned SQL connection, the replication connection that
// created the slot, and the underlying DB pool. The CDC reader's
// own resources are closed too — including any replication
// connection it opens during StreamChanges.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	return e.OpenSnapshotStreamWithSlot(ctx, dsn, defaultSlot)
}

// OpenSnapshotStreamWithSlot satisfies [ir.SnapshotStreamWithSlotOpener].
// Empty slotName falls back to the default `sluice_slot`. Same code
// path as the default constructor so the slot-creation, snapshot
// import, and CDC-handoff logic stay in one place.
func (e Engine) OpenSnapshotStreamWithSlot(ctx context.Context, dsn, slotName string) (*ir.SnapshotStream, error) {
	if slotName == "" {
		slotName = defaultSlot
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

	// Publication must exist BEFORE the replication slot is created.
	// Logical replication slots pin a catalog snapshot at their
	// consistent_point; a publication created after the slot isn't
	// visible to the slot's view of the catalog and pgoutput will
	// stream errors with "publication does not exist". Order
	// matters here even though it didn't for the standalone CDC
	// reader (which creates the slot fresh per StreamChanges call).
	// Pass nil tables: snapshot stream construction doesn't have
	// the table list in hand. The streamer's coldStart calls
	// EnsurePublication explicitly with the scoped table list
	// before this point (Bug 13, ADR-0021), so when the publication
	// already exists with the right scope this call is a no-op.
	// Falls back to FOR ALL TABLES on a fresh setup with no prior
	// EnsurePublication call (test paths, direct API consumers).
	if err := ensurePublication(ctx, db, defaultPublication, cfg.schema, nil); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Slot must NOT already exist — a pre-existing slot has its own
	// consistent_point and we'd silently inherit it instead of
	// capturing a fresh snapshot. Refuse explicitly so the operator
	// reckons with the leftover.
	info, err := slotInfo(ctx, db, slotName)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if info != nil {
		_ = db.Close()
		return nil, fmt.Errorf(
			"postgres: snapshot: replication slot %q already exists; drop it before starting a snapshot stream (manual cleanup avoids accidentally inheriting a stale consistent_point)",
			slotName)
	}

	// Open a replication connection dedicated to slot creation. We
	// keep it alive for the lifetime of the SnapshotStream so the
	// exported snapshot stays valid through the bulk-copy phase. The
	// CDCReader opens its OWN replication connection later (during
	// StreamChanges) for the actual streaming.
	replConn, err := openReplicationConn(ctx, cfg.dsn)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: open replication conn: %w", err)
	}

	// EXPORT_SNAPSHOT is the default for non-temporary slots, but
	// stating it explicitly documents intent. The helper layers
	// FAILOVER true on PG 17+ (see slot_create.go) so the slot
	// survives Patroni / sync_replication_slots failover events.
	consistentPoint, snapshotName, err := createLogicalReplicationSlot(ctx, db, replConn, slotName, true)
	if err != nil {
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: %w", err)
	}
	if snapshotName == "" {
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, errors.New("postgres: snapshot: server returned empty snapshot_name; expected EXPORT_SNAPSHOT to populate it")
	}

	// Pin a regular SQL connection and import the exported snapshot.
	// SET TRANSACTION SNAPSHOT must be the first statement after
	// BEGIN — the docs are explicit about this.
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: BEGIN: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", quoteSnapshotName(snapshotName))); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: SET TRANSACTION SNAPSHOT: %w", err)
	}

	// Build the CDC reader with no pre-opened connection. It will
	// open its own replication conn on StreamChanges and use the
	// slot we just created. The slot existence check inside
	// resolveStartPosition will see the slot we created here and
	// resume from the supplied position. Pass the same slot name so
	// the CDC reader picks up the slot we just created (not the
	// hard-coded default).
	cdcReader, err := e.OpenCDCReaderWithSlot(ctx, dsn, slotName)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: build cdc reader: %w", err)
	}

	position, err := encodePGPos(pgPos{Slot: slotName, LSN: consistentPoint})
	if err != nil {
		_ = cdcReader.(closer).Close()
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = replConn.Close(ctx)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: snapshot: encode position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.schema,
		closer: nil, // SnapshotStream owns the lifecycle
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rowReader,
		Changes:  cdcReader,
	}

	// rowsReleased is the idempotency guard for ReleaseRowsFn /
	// CloseFn. Once the snapshot tx is committed and the import-side
	// connections are closed, both must be skipped on the second
	// caller (e.g. ReleaseRows then Close, or Close-only without
	// ReleaseRows). Captured by closure; no surrounding mutex
	// because the orchestrator never calls these concurrently —
	// bulk-copy is single-goroutine for the snapshot tx, and the
	// streamer's defer chain serialises Close with everything else.
	rowsReleased := false
	releaseRows := func() error {
		if rowsReleased {
			return nil
		}
		rowsReleased = true
		// Commit (don't ROLLBACK) so the snapshot tx exits cleanly —
		// nothing was written, but COMMIT is the project convention
		// and matches what CloseFn used to do. Order: commit the
		// snapshot tx → close the pinned SQL conn → close the
		// slot-creation replication conn (the slot itself stays
		// alive on the server; the replication-protocol conn used
		// to create it is no longer needed once all importers have
		// imported).
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := replConn.Close(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	stream.ReleaseRowsFn = releaseRows
	stream.CloseFn = func() error {
		// Order matters: stop CDC first (releases its own repl conn),
		// then release the import-side resources if not already
		// released, then close the schema DB pool.
		var firstErr error
		if c, ok := cdcReader.(closer); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := releaseRows(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return stream, nil
}

// closer is the local view of io.Closer for the CDC reader cleanup
// path. Avoids importing io for this single use.
type closer interface{ Close() error }

// Compile-time guard: pgconn is referenced indirectly via
// openReplicationConn's return type but never named directly in this
// file's body, so without this line goimports / the unused-import
// check would drop the package. The guard pins it.
var _ = pgconn.Connect

// quoteSnapshotName escapes a Postgres snapshot identifier for use
// in a SET TRANSACTION SNAPSHOT statement. Snapshot names returned
// by CREATE_REPLICATION_SLOT have the form `<xid>-<numeric>-<numeric>`
// with no embedded quotes, but defending against the format ever
// changing is cheap.
func quoteSnapshotName(s string) string {
	out := make([]byte, 0, len(s))
	for _, b := range []byte(s) {
		if b == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, b)
	}
	return string(out)
}
