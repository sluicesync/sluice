// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
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
	return e.openSnapshotStreamShared(ctx, dsn, slotName, nil)
}

// OpenMultiDatabaseSnapshotStream implements
// [ir.MultiDatabaseSnapshotOpener] (ADR-0075 Phase 2b): it opens the
// SINGLE consistent snapshot spanning all selected schemas that a
// multi-schema `sync start` cold-start needs.
//
// Postgres makes this the easy, natural case: a logical replication slot
// is DATABASE-WIDE, and its CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT
// returns an exported snapshot at a single consistent LSN that already
// spans EVERY schema in the database. So this is the ordinary slot-based
// snapshot path with exactly two differences from
// [Engine.OpenSnapshotStream]:
//
//   - the publication is forced to FOR ALL TABLES (the slot is DB-wide;
//     the reader's inScope filter is the selection boundary — see
//     [ensureAllTablesPublication]); and
//   - the returned RowReader has [RowReader.qualifyBySchema] set, so its
//     single pinned exported-snapshot connection reads schema."table"
//     across every selected schema (the orchestrator stamps Table.Schema
//     via [ir.MultiDatabaseScoper] before bulk-copy).
//
// The single LSN captured at the slot's consistent_point is the gapless
// handoff for every selected schema's CDC — one slot, one LSN, no N-slot
// coordination. Unlike MySQL there is no FLUSH TABLES WITH READ LOCK dance:
// the slot's exported snapshot already IS the consistent boundary.
//
// schemas is the concrete, already-resolved selected set (the orchestrator
// applied the include/exclude globs and excluded system schemas); it must
// be non-empty. It is accepted for symmetry with the IR surface and to log
// the spanned set — the slot's snapshot spans the whole database by
// construction, and the reader-side inScope filter (wired by the
// orchestrator via [CDCReader.SetCDCDatabaseScope]) does the selection.
func (e Engine) OpenMultiDatabaseSnapshotStream(ctx context.Context, dsn string, schemas []string) (*ir.SnapshotStream, error) {
	if len(schemas) == 0 {
		return nil, errors.New("postgres: multi-schema snapshot: no schemas selected")
	}
	slog.InfoContext(ctx, "postgres: opening single spanning consistent snapshot across selected schemas",
		slog.Int("schema_count", len(schemas)),
		slog.Any("schemas", schemas))
	return e.openSnapshotStreamShared(ctx, dsn, defaultSlot, schemas)
}

// openSnapshotStreamShared is the shared body of the single-schema and
// multi-schema (ADR-0075 Phase 2b) slot-based snapshot openers. The two
// differ in exactly two parameters, both driven by `spanning`:
//
//   - the publication is forced to FOR ALL TABLES when spanning (the slot
//     is DB-wide; the reader's inScope filter is the selection boundary)
//     vs. the scope-respecting [ensurePublication] in single-schema mode
//     (byte-identical back-compat).
//   - the returned RowReader sets [RowReader.qualifyBySchema] = spanning so
//     the one pinned exported-snapshot connection reads schema."table"
//     across N schemas.
//
// Everything else — the ONE exported snapshot, the ONE consistent_point
// LSN, the ONE pinned import transaction, the CDC handoff, and the
// release/close lifecycle — is identical. That identity is the point: the
// multi-schema snapshot is the SAME single-slot / single-LSN capture, just
// spanning N schemas (which a PG slot does by construction).
//
// spanSchemas is nil/empty in single-schema mode and the resolved selected
// set in multi-schema mode (used only to log + drive the FOR ALL TABLES /
// qualifyBySchema toggles).
func (e Engine) openSnapshotStreamShared(ctx context.Context, dsn, slotName string, spanSchemas []string) (*ir.SnapshotStream, error) {
	spanning := len(spanSchemas) > 0
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
	//
	// Multi-schema (ADR-0075 Phase 2b): force FOR ALL TABLES — the slot
	// is database-wide and the reader's inScope filter does the schema
	// selection, so a per-schema scoped publication would WRONGLY drop the
	// other selected schemas' WAL. Single-schema: pass nil tables — the
	// streamer's coldStart calls EnsurePublication explicitly with the
	// scoped table list before this point (Bug 13, ADR-0021), so when the
	// publication already exists with the right scope this call is a no-op,
	// falling back to FOR ALL TABLES only on a fresh setup with no prior
	// EnsurePublication call (test paths, direct API consumers).
	if spanning {
		if err := ensureAllTablesPublication(ctx, db, defaultPublication); err != nil {
			_ = db.Close()
			return nil, err
		}
	} else {
		if err := ensurePublication(ctx, db, defaultPublication, cfg.schema, nil); err != nil {
			_ = db.Close()
			return nil, err
		}
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
			slotName,
		)
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
		// Multi-schema spanning snapshot (ADR-0075 Phase 2b): qualify each
		// SELECT by the table's own Schema (source schema) so this single
		// pinned exported-snapshot connection reads across N schemas. In the
		// single-schema path this is false and the SELECT qualifies by
		// cfg.schema exactly as before — byte-identical back-compat.
		qualifyBySchema: spanning,
		closer:          nil, // SnapshotStream owns the lifecycle
		// estimatorDSN lets EstimateRowCount open a fresh off-snapshot conn
		// for the pre-stream within-table chunk decision (ADR-0079 v1.1).
		// This reader IS the chunk-0 decision reader on the sync fast path
		// (primaryRows = stream.Rows), and it is pinned (closer == nil), so
		// without the DSN it would report "no estimate" and single-stream.
		// cfg.dsn is the driver-ready DSN (schema stripped) — reltuples is
		// snapshot-insensitive, so the off-conn read is correct.
		estimatorDSN: cfg.dsn,
	}

	stream := &ir.SnapshotStream{
		Position: position,
		// The exported snapshot name is SHAREABLE: any other connection
		// can `SET TRANSACTION SNAPSHOT '<name>'` to observe this exact
		// consistent_point view. Surfacing it (ADR-0079) lets the sync
		// cold-start mint N parallel readers all pinned to this one
		// snapshot via [Engine.OpenSnapshotImporter], so the fast
		// cross-table/within-table copy machinery runs gap-free. The
		// snapshotName is valid only while the slot-creation tx
		// (replConn) stays open — which it does until ReleaseRows.
		SnapshotName: snapshotName,
		Rows:         rowReader,
		Changes:      cdcReader,
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
