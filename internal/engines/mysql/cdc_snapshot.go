// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// OpenSnapshotStream opens a consistent source snapshot and returns
// a paired RowReader and CDCReader whose start position is the
// snapshot's logical capture point. The concrete mechanism depends
// on the engine's flavor:
//
//   - FlavorVanilla → REPEATABLE READ + WITH CONSISTENT SNAPSHOT
//     pinned to a captured binlog position. Bulk-copy and CDC use
//     separate connections (binlog dump speaks a different protocol).
//   - FlavorPlanetScale → VStream's built-in COPY mode. A single
//     gRPC stream produces both the COPY-phase rows and the
//     post-COPY change events; the seam is the global
//     COPY_COMPLETED event, at which point the captured VGTID is
//     the resume position.
//
// Flavors declaring [ir.CDCNone] return [ErrNotImplemented]; check
// the engine's [ir.Capabilities.CDC] before requesting a snapshot
// stream. Caller closes the returned stream to release all
// connections / transactions.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: snapshot+CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor == FlavorPlanetScale {
		return e.openVStreamSnapshotStream(ctx, dsn)
	}
	// FlavorVanilla and any future binlog-based flavor land here.
	return e.openBinlogSnapshotStream(ctx, dsn)
}

// OpenSnapshotStreamForTables is the optional [ir.TableScopedSnapshotOpener]
// surface: it opens a snapshot whose COPY is scoped to the unqualified
// table names in tables (an empty slice means "all tables", identical to
// [Engine.OpenSnapshotStream]).
//
// Only the PlanetScale (VStream) flavor over-streams by default — vtgate's
// COPY copies every table the filter rules match, so a large unrelated
// table in the same keyspace gets streamed and buffered even when only a
// small table is in scope (the ADR-0071 multi-table-interleaving buffer
// overflow). Scoping the VStream COPY filter to the allowlist makes vtgate
// scan only those tables. The vanilla/binlog flavor's snapshot RowReader
// already reads per-table, so it never over-streams; there the scope is a
// no-op and we delegate to the plain binlog snapshot open.
func (e Engine) OpenSnapshotStreamForTables(ctx context.Context, dsn string, tables []string) (*ir.SnapshotStream, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: snapshot+CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor == FlavorPlanetScale {
		return e.openVStreamSnapshotStreamFrom(ctx, dsn, nil, tables)
	}
	// Vanilla/binlog flavor: its snapshot RowReader already reads per-table,
	// so it never over-streams; the table scope is a no-op there.
	return e.openBinlogSnapshotStream(ctx, dsn)
}

// OpenSnapshotStreamFromPosition resumes an INTERRUPTED cold-start COPY
// (v0.99.8) by seeding the bulk snapshot stream from a persisted position
// that carries Vitess's per-shard TablePKs cursor. This is the optional
// [ir.SnapshotStreamResumer] surface: the pipeline routes a process-
// restart resume here (instead of the plain CDC reader) when the
// persisted position carries a mid-COPY cursor, so vtgate's re-emitted
// COPY-tail rows flow through the batched bulk-COPY writer rather than the
// per-row CDC apply path (~4000 rows/sec vs ~10 rows/sec — the
// silent-degrade this fixes). The COPY continues from the cursor (no full
// re-copy) and transitions to CDC on completion exactly as a fresh
// cold-start does.
//
// Only the PlanetScale (VStream) flavor implements a meaningful resume:
// the binlog-snapshot flavor has no mid-COPY cursor (its snapshot is a
// single REPEATABLE-READ transaction, all-or-nothing), so a position
// reaching here for that flavor is a pure-CDC position the plain CDC
// warm-resume path already handles — we refuse loudly rather than silently
// re-copy from row 0.
//
// from MUST carry a TablePKs cursor (PositionCarriesCopyCursor true); the
// pipeline gates on that before calling. A cursor-less position is
// rejected loudly: seeding a bulk snapshot from an empty-TablePKs Gtid
// would make vtgate restart the whole COPY from row 0 (silent full
// re-copy of a partially-populated target), which is exactly the
// silent-loss class the loud-failure tenet forbids.
// tables scopes the resumed COPY filter exactly as
// [Engine.OpenSnapshotStreamForTables] scopes a fresh one: empty/nil keeps
// the whole-keyspace COPY; a non-empty allowlist restricts vtgate's COPY to
// those unqualified table names.
func (e Engine) OpenSnapshotStreamFromPosition(ctx context.Context, dsn string, from ir.Position, tables []string) (*ir.SnapshotStream, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: snapshot+CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor != FlavorPlanetScale {
		return nil, fmt.Errorf(
			"%s: resumable cold-start COPY is only implemented for the planetscale (VStream) flavor: %w",
			e.Name(), ErrNotImplemented,
		)
	}
	start, ok, err := decodeVStreamPos(from)
	if err != nil {
		return nil, fmt.Errorf("mysql/vstream: snapshot resume: decode position: %w", err)
	}
	if !ok {
		return nil, errors.New("mysql/vstream: snapshot resume: empty position has no COPY cursor to resume from")
	}
	if !anyTablePKsPresent(start) {
		// A cursor-less position must NOT seed a bulk snapshot: vtgate
		// would restart the COPY from row 0 against the partially-copied
		// target. The pipeline only routes here for cursor-carrying
		// positions, so reaching this branch is a contract violation —
		// fail loudly rather than silently full-re-copy.
		return nil, errors.New(
			"mysql/vstream: snapshot resume: position carries no TablePKs cursor; refusing to re-copy from row 0 " +
				"(the cursor-less warm-resume belongs on the plain CDC path)",
		)
	}
	// Scope the resumed COPY to the same table allowlist a fresh cold-start
	// would use. There is nothing to reconcile by hand: Vitess's resume
	// cursor (TablePKs) is PER-TABLE, so passing the current allowlist into
	// the resumed COPY is correct in every case — a table that has a cursor
	// entry resumes from it, an allowlisted table with no cursor entry
	// starts fresh, and a table dropped from the allowlist simply stops
	// being copied. Empty/nil tables keeps the whole-keyspace COPY.
	if len(tables) > 0 {
		slog.InfoContext(ctx, "mysql/vstream: snapshot resume: scoping resumed COPY to included tables",
			slog.Int("table_count", len(tables)))
	}
	return e.openVStreamSnapshotStreamFrom(ctx, dsn, start, tables)
}

// PositionCarriesCopyCursor reports whether a persisted position carries a
// mid-COPY resume cursor (Vitess per-shard TablePKs) — i.e. it was written
// while an INTERRUPTED cold-start COPY was still in flight (v0.99.8). The
// pipeline uses this engine-agnostic discriminator to decide whether a
// process-restart resume must route through the bulk snapshot resume path
// ([OpenSnapshotStreamFromPosition]) rather than the plain CDC warm-resume
// path. A pure-CDC position (completed cold-start, or a non-VStream
// engine's position) returns false and stays on the fast plain-CDC path.
//
// A position that fails to decode returns false (the plain CDC path's own
// decoder will surface the decode error loudly) — this discriminator is a
// routing hint, not a validation gate.
func (e Engine) PositionCarriesCopyCursor(from ir.Position) bool {
	if e.Flavor != FlavorPlanetScale {
		return false
	}
	start, ok, err := decodeVStreamPos(from)
	if err != nil || !ok {
		return false
	}
	return anyTablePKsPresent(start)
}

// openBinlogSnapshotStream is the FlavorVanilla path of
// [Engine.OpenSnapshotStream]. Lifted out of OpenSnapshotStream so
// the flavor dispatch stays readable.
func (e Engine) openBinlogSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Pin a single connection. All snapshot-pinned reads will run on
	// this conn; the snapshot transaction is bound to it.
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: pin conn: %w", err)
	}

	// REPEATABLE READ + WITH CONSISTENT SNAPSHOT in a single statement
	// is the canonical InnoDB snapshot capture. The session isolation
	// is set explicitly first so the behaviour doesn't depend on the
	// server's tx_isolation default.
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: set isolation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: start tx: %w", err)
	}

	// Capture the position INSIDE the same transaction so it is
	// guaranteed to refer to the snapshot's logical clock. SHOW BINARY
	// LOG STATUS is the 8.4+ spelling; SHOW MASTER STATUS is the
	// pre-8.4 fallback. Same shape as the standalone CDC reader.
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: capture position: %w", err)
	}

	// Bind the handoff position to the source instance (Track 1c
	// node-replace floor). @@server_uuid is a global, not
	// tx-scoped, so reading it on the snapshot conn is fine. A
	// failed lookup is non-fatal: the position is persisted without
	// the uuid and the resume path's identity check degrades to the
	// filename-only behaviour (no regression). Stamping it HERE
	// means even a cold-start that hasn't yet streamed a single CDC
	// event persists an instance-bound EndPosition.
	var serverUUID string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&serverUUID); err != nil {
		serverUUID = ""
	}

	// The CDC reader uses an entirely separate connection and protocol
	// (binlog dump). Construct it with the same DSN so it parses the
	// host/port/credentials itself.
	cdcReader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: build cdc reader: %w", err)
	}

	position, err := encodeBinlogPos(binlogPos{
		Mode:       positionModeFilePos,
		File:       file,
		Pos:        pos,
		ServerUUID: serverUUID,
	})
	if err != nil {
		_ = cdcReader.(closer).Close()
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: encode position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.DBName,
		// Snapshot mode: SnapshotStream.Close handles cleanup.
		closer: nil,
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rowReader,
		Changes:  cdcReader,
	}

	// rowsReleased is the idempotency guard for ReleaseRowsFn / CloseFn.
	// Once COMMIT has run and the import-side conn+pool are closed,
	// neither must repeat. Captured by closure; no surrounding mutex
	// because the orchestrator never calls these concurrently
	// (bulk-copy is single-goroutine for the snapshot tx, and the
	// streamer's defer chain serialises Close with everything else).
	// Mirrors the PG engine's shape in postgres/cdc_snapshot.go.
	rowsReleased := false
	releaseRows := func() error {
		if rowsReleased {
			return nil
		}
		rowsReleased = true
		// COMMIT (not ROLLBACK): nothing was written on this tx, but
		// COMMIT is the project convention for a clean exit from a
		// REPEATABLE-READ + WITH CONSISTENT SNAPSHOT tx. Order:
		// commit the snapshot tx → close the pinned conn → close the
		// schema-side DB pool. Until this runs, MySQL keeps the
		// MDL_SHARED_READ acquired by START TRANSACTION WITH
		// CONSISTENT SNAPSHOT alive (dur=TRANSACTION), which blocks
		// any operator ALTER on the snapshotted source tables — even
		// ALGORITHM=INSTANT, which still needs a brief MDL upgrade.
		// Documented at length on the deferred Chunk E pin
		// (task #28) and the IR-side SnapshotStream contract; see
		// ir/snapshot.go's "Optional early release" docstring.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
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
	stream.ReleaseRowsFn = releaseRows
	stream.CloseFn = func() error {
		// Order: stop the CDC reader first (joins the engine-side
		// streaming goroutine deterministically; relying on ctx-cancel
		// alone left a goroutine racing slog.Default under -race),
		// then release the import-side resources (idempotent — fires
		// the COMMIT + closes only if ReleaseRowsFn hasn't already).
		var firstErr error
		if c, ok := cdcReader.(closer); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := releaseRows(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return stream, nil
}

// closer is the local view of io.Closer for the CDC reader cleanup
// path. Avoids importing io for this single use.
type closer interface{ Close() error }

// snapshotMasterStatus is the single-conn variant of the standalone
// CDC reader's masterStatus helper. We can't reuse that one directly
// because it operates on *sql.DB; here we need to run on the pinned
// *sql.Conn so the position is captured inside the snapshot tx.
func snapshotMasterStatus(ctx context.Context, conn *sql.Conn) (file string, pos uint32, err error) {
	for _, q := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		file, pos, err = scanMasterStatusOnConn(ctx, conn, q)
		if err == nil {
			return file, pos, nil
		}
	}
	return "", 0, errors.New("mysql: snapshot: SHOW BINARY LOG STATUS / SHOW MASTER STATUS both failed (binlog disabled?)")
}

// scanMasterStatusOnConn mirrors scanMasterStatus from cdc_reader.go,
// adapted for *sql.Conn. The query may return additional columns
// after (file, position) which we discard.
func scanMasterStatusOnConn(ctx context.Context, conn *sql.Conn, q string) (file string, pos uint32, err error) {
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, errors.New("master status returned no rows")
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	dest := make([]any, len(cols))
	holders := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return "", 0, err
	}
	f, ok := scanString(dest[0])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected file type %T", dest[0])
	}
	p, ok := scanUint32(dest[1])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected position type %T", dest[1])
	}
	return f, p, nil
}
