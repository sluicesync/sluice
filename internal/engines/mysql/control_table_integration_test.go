//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the per-target sluice_cdc_state control
// table. Two flavors:
//
//   - Schema migration: a v0.2.x-shape table without the new
//     stop_requested_at column should pick up the column on the
//     next EnsureControlTable call, without losing existing rows.
//   - RequestStop / ReadStopRequested: the column round-trips an
//     UPDATE through ReadStopRequested, and an unknown stream id
//     surfaces errStreamNotFound.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEnsureControlTable_AddsStopRequestedColumn verifies the
// migration path for v0.2.x deployments. We hand-create a control
// table with the pre-v0.3 column set, insert a row, then call
// EnsureControlTable and confirm:
//
//   - The new stop_requested_at column exists.
//   - The previously inserted row is still there with its old
//     position intact.
//
// The detect-then-ALTER path is exercised here (vs. the simpler
// CREATE TABLE path) — that's the codepath we ship for MySQL 8.x
// versions older than 8.0.29 that lack ADD COLUMN IF NOT EXISTS.
func TestEnsureControlTable_AddsStopRequestedColumn(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-v0.3 shape: no stop_requested_at column.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id       VARCHAR(255) NOT NULL,"+
		"  source_position TEXT         NOT NULL,"+
		"  updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('legacy-stream', 'legacy-token');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = 'sluice_cdc_state'
		  AND  COLUMN_NAME  = 'stop_requested_at'
	`).Scan(&n); err != nil {
		t.Fatalf("column lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("stop_requested_at column missing after EnsureControlTable; got count %d", n)
	}

	// Existing row should be preserved with its old token.
	var token string
	var stopReq sql.NullTime
	if err := db.QueryRowContext(
		ctx,
		"SELECT source_position, stop_requested_at FROM `sluice_cdc_state` WHERE stream_id = ?",
		"legacy-stream",
	).Scan(&token, &stopReq); err != nil {
		t.Fatalf("legacy row select: %v", err)
	}
	if token != "legacy-token" {
		t.Errorf("legacy token = %q; want %q", token, "legacy-token")
	}
	if stopReq.Valid {
		t.Errorf("stop_requested_at on freshly-migrated row should be NULL; got %v", stopReq.Time)
	}

	// Calling EnsureControlTable again is still a no-op.
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

// TestRequestStop_RoundTrips covers the happy path: write a stream
// position via writePositionTx, RequestStop, then observe the flag
// via ReadStopRequested.
func TestRequestStop_RoundTrips(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Seed a row by writing a position. Reuses the engine's
	// writePositionTx via a manual tx — the applier's normal Apply
	// path goes through this too.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writePositionTx(ctx, tx, "", "test-stream", "tok", "", "", "", 0, upsertRowAlias); err != nil {
		t.Fatalf("writePositionTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Initially: no stop request.
	myApplier := applier.(*ChangeApplier)
	stop, err := myApplier.ReadStopRequested(ctx, "test-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested(initial): %v", err)
	}
	if stop {
		t.Fatal("ReadStopRequested(initial) = true; want false")
	}

	if err := applier.RequestStop(ctx, "test-stream"); err != nil {
		t.Fatalf("RequestStop: %v", err)
	}

	stop, err = myApplier.ReadStopRequested(ctx, "test-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested(after stop): %v", err)
	}
	if !stop {
		t.Fatal("ReadStopRequested(after stop) = false; want true")
	}

	// Idempotent: second RequestStop should not error.
	if err := applier.RequestStop(ctx, "test-stream"); err != nil {
		t.Errorf("second RequestStop: %v", err)
	}
}

// TestRequestStop_UnknownStreamReturnsSentinel verifies the
// errStreamNotFound branch — operators sometimes typo the stream
// ID; the CLI surfaces a friendly message based on this sentinel.
func TestRequestStop_UnknownStreamReturnsSentinel(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	err = applier.RequestStop(ctx, "no-such-stream")
	if err == nil {
		t.Fatal("RequestStop on unknown stream returned nil; want errStreamNotFound")
	}
	if !errors.Is(err, errStreamNotFound) {
		t.Errorf("RequestStop unknown: err = %v; want errors.Is errStreamNotFound", err)
	}
}

// TestEnsureControlTable_AddsCrossEngineParityColumns verifies the
// v0.32.2 migration path that closes OBS-1. Pre-v0.32.2 deployments
// had a control table with stream_id / source_position / updated_at /
// stop_requested_at / live_added_tables only; the slot_name /
// source_dsn_fingerprint / target_schema columns lived on the PG
// side but never made it to the MySQL writer. EnsureControlTable
// now backfills them via the detect-then-ALTER path (portable to
// MySQL 8.0.x versions older than 8.0.29 that lack ADD COLUMN IF
// NOT EXISTS), preserves existing rows, and starts the new columns
// NULL.
//
// **ADR-0049 Chunk A invariant (Chunk E regression-pin):** the
// additive sluice_cdc_schema_history table introduced by Chunk A
// must not perturb this existing additive-migration chain. The
// EnsureControlTable call this test exercises is the same call
// that, post-Chunk-A, also ensures sluice_cdc_schema_history (via
// ensureSchemaHistoryTable, see schema_history.go). If a future
// change broke the additive contract — e.g. dropping pre-existing
// cdc_state rows on schema-history ensure — this test's "legacy
// row preserved" assertion still catches it. The
// TestEnsureSchemaHistoryTable_AdditiveToCDCState test in
// schema_history_integration_test.go pins the same invariant from
// the schema-history side; both must stay green.
func TestEnsureControlTable_AddsCrossEngineParityColumns(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-v0.32.2 shape: stop_requested_at + live_added_tables, but
	// none of the cross-engine parity columns. Includes a seeded row
	// so the migration's row-preservation property has something to
	// assert against.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id         VARCHAR(255) NOT NULL,"+
		"  source_position   TEXT         NOT NULL,"+
		"  updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  stop_requested_at TIMESTAMP    NULL,"+
		"  live_added_tables TEXT         NULL,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('legacy-stream', 'legacy-token');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, col := range []string{"slot_name", "source_dsn_fingerprint", "target_schema"} {
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM   information_schema.COLUMNS
			WHERE  TABLE_SCHEMA = DATABASE()
			  AND  TABLE_NAME   = 'sluice_cdc_state'
			  AND  COLUMN_NAME  = ?
		`, col).Scan(&n); err != nil {
			t.Fatalf("column %s lookup: %v", col, err)
		}
		if n != 1 {
			t.Errorf("%s column missing after EnsureControlTable; count = %d", col, n)
		}
	}

	// Existing row should be preserved with its original token; the
	// new columns start NULL on the freshly-migrated row.
	var (
		token       string
		slot        sql.NullString
		fingerprint sql.NullString
		tsch        sql.NullString
	)
	if err := db.QueryRowContext(
		ctx,
		"SELECT source_position, slot_name, source_dsn_fingerprint, target_schema FROM `sluice_cdc_state` WHERE stream_id = ?",
		"legacy-stream",
	).Scan(&token, &slot, &fingerprint, &tsch); err != nil {
		t.Fatalf("legacy row select: %v", err)
	}
	if token != "legacy-token" {
		t.Errorf("legacy token = %q; want %q", token, "legacy-token")
	}
	if slot.Valid {
		t.Errorf("slot_name on freshly-migrated row should be NULL; got %q", slot.String)
	}
	if fingerprint.Valid {
		t.Errorf("source_dsn_fingerprint on freshly-migrated row should be NULL; got %q", fingerprint.String)
	}
	if tsch.Valid {
		t.Errorf("target_schema on freshly-migrated row should be NULL; got %q", tsch.String)
	}

	// Calling EnsureControlTable again is still a no-op (idempotent).
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

// TestWritePositionTx_RowsAppliedAccumulatesAndBackCompat pins the ADR-0156
// phase 2 rows_applied counter end-to-end against a real MySQL:
//
//   - a LEGACY control table (no rows_applied column) picks the column up on
//     EnsureControlTable, and the existing row backfills to 0 (NOT NULL
//     DEFAULT 0 — an honest cumulative start);
//   - each writePositionTx ADDS its delta (COALESCE(existing,0) + delta), so
//     successive writes ACCUMULATE (never replace); a delta-0 write is a no-op
//     on the count;
//   - listStreams round-trips the cumulative value via StreamStatus.RowsApplied.
func TestWritePositionTx_RowsAppliedAccumulatesAndBackCompat(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Legacy shape: no rows_applied column, one pre-existing row.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id         VARCHAR(255) NOT NULL,"+
		"  source_position   TEXT         NOT NULL,"+
		"  updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  stop_requested_at TIMESTAMP    NULL,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('legacy-stream', 'legacy-token');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rowsFor := func(streamID string) int64 {
		t.Helper()
		streams, err := applier.ListStreams(ctx)
		if err != nil {
			t.Fatalf("ListStreams: %v", err)
		}
		for _, s := range streams {
			if s.StreamID == streamID {
				return s.RowsApplied
			}
		}
		t.Fatalf("stream %q not found in ListStreams", streamID)
		return 0
	}
	if got := rowsFor("legacy-stream"); got != 0 {
		t.Fatalf("legacy row rows_applied = %d; want 0 (backfilled)", got)
	}

	write := func(streamID, token string, delta int64) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := writePositionTx(ctx, tx, "", streamID, token, "", "", "", delta, upsertRowAlias); err != nil {
			t.Fatalf("writePositionTx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	write("legacy-stream", "tok-1", 3)
	write("legacy-stream", "tok-2", 2)
	write("legacy-stream", "tok-3", 0)
	if got := rowsFor("legacy-stream"); got != 5 {
		t.Fatalf("legacy-stream rows_applied = %d; want 5 (3+2+0 accumulated)", got)
	}

	write("fresh-stream", "f-1", 4)
	write("fresh-stream", "f-2", 4)
	if got := rowsFor("fresh-stream"); got != 8 {
		t.Fatalf("fresh-stream rows_applied = %d; want 8", got)
	}
	if got := rowsFor("legacy-stream"); got != 5 {
		t.Fatalf("legacy-stream rows_applied drifted to %d; want 5 (streams independent)", got)
	}
}

// TestControlKeyspace_SidecarRoutesControlTables pins the sidecar-keyspace
// prototype (--control-keyspace) on a real server. On a SHARDED PlanetScale/
// Vitess target the control tables can't live in the sharded data keyspace (no
// vindex); the operator points --control-keyspace at a separate UNSHARDED
// keyspace. Vanilla MySQL has a flat namespace, so a "keyspace" is just a
// database — that lets us prove, without a Vitess cluster, that:
//
//   - EnsureControlTable creates all THREE control tables in the sidecar
//     database, NOT in the connection's default database.
//   - A position write (which rides the atomicity-critical writePositionTx
//     three-part `ks`.`table`.column UPSERT) and RequestStop / ReadStopRequested
//     round-trip through the sidecar-qualified statements.
//
// The live sharded-cluster validation is run separately by the operator; this
// same-engine test guards the SQL-shape / routing contract the prototype rests
// on so a malformed qualified statement can't slip through.
func TestControlKeyspace_SidecarRoutesControlTables(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const sidecar = "sluice_ctl_sidecar"
	// The operator pre-creates the unsharded sidecar keyspace; here that is a
	// database. DROP first so the shared container stays deterministic across
	// reruns; drop again on cleanup so we don't leak it to sibling tests.
	applyMySQLApplier(t, dsn, "DROP DATABASE IF EXISTS `"+sidecar+"`; CREATE DATABASE `"+sidecar+"`;")
	t.Cleanup(func() { applyMySQLApplier(t, dsn, "DROP DATABASE IF EXISTS `"+sidecar+"`;") })

	engIR, err := (Engine{Flavor: FlavorVanilla}).WithControlKeyspace(sidecar)
	if err != nil {
		t.Fatalf("WithControlKeyspace: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := engIR.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Seed a position via WritePosition — exercises the sidecar-qualified
	// writePositionTx UPSERT (the three-part column reference).
	pw, ok := applier.(interface {
		WritePosition(ctx context.Context, streamID string, pos ir.Position) error
	})
	if !ok {
		t.Fatal("ChangeApplier does not implement ir.PositionWriter")
	}
	if err := pw.WritePosition(ctx, "sidecar-stream", ir.Position{Engine: engineNameMySQL, Token: "tok1"}); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}

	myApplier := applier.(*ChangeApplier)
	pos, found, err := myApplier.ReadPosition(ctx, "sidecar-stream")
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !found || pos.Token != "tok1" {
		t.Fatalf("ReadPosition = (%+v, %v); want token tok1, found true", pos, found)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Every control table must live in the sidecar database and NOT in the
	// connection's default database (target_db).
	for _, tbl := range []string{
		"sluice_cdc_state",
		"sluice_cdc_schema_history",
		"sluice_shard_consolidation_lease",
	} {
		var inSidecar int
		if err := db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			sidecar, tbl,
		).Scan(&inSidecar); err != nil {
			t.Fatalf("sidecar lookup %s: %v", tbl, err)
		}
		if inSidecar != 1 {
			t.Errorf("%s not created in sidecar keyspace %q; count = %d", tbl, sidecar, inSidecar)
		}

		var inDefault int
		if err := db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
			tbl,
		).Scan(&inDefault); err != nil {
			t.Fatalf("default-db lookup %s: %v", tbl, err)
		}
		if inDefault != 0 {
			t.Errorf("%s leaked into the default database; count = %d (want 0)", tbl, inDefault)
		}
	}

	// RequestStop / ReadStopRequested round-trip through the sidecar table.
	if err := applier.RequestStop(ctx, "sidecar-stream"); err != nil {
		t.Fatalf("RequestStop: %v", err)
	}
	stop, err := myApplier.ReadStopRequested(ctx, "sidecar-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested: %v", err)
	}
	if !stop {
		t.Fatal("ReadStopRequested(after stop) = false; want true")
	}
}

// TestWritePositionTx_SlotNameRoundTrip pins the OBS-1 fix:
// SetSlotName followed by a position-write upserts the slot_name
// column; ListStreams returns the recorded value via StreamStatus.
// Empty SetSlotName preserves the previously-recorded value (the
// COALESCE branch in writePositionTx) — important so a chain-handoff
// WritePosition that lacks streamer context doesn't clobber the
// streamer's recorded slot.
func TestWritePositionTx_SlotNameRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	myApplier := applier.(*ChangeApplier)
	myApplier.SetSlotName("sluice_shard_a")

	// Seed a row via WritePosition (Apply's per-change path is the
	// other writer; both share writePositionTx so either suffices).
	pw, ok := applier.(interface {
		WritePosition(ctx context.Context, streamID string, pos ir.Position) error
	})
	if !ok {
		t.Fatal("ChangeApplier does not implement ir.PositionWriter")
	}
	if err := pw.WritePosition(ctx, "shard-stream", ir.Position{Engine: engineNameMySQL, Token: "tok1"}); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}

	statuses, err := applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	var got ir.StreamStatus
	for _, s := range statuses {
		if s.StreamID == "shard-stream" {
			got = s
			break
		}
	}
	if got.StreamID == "" {
		t.Fatalf("ListStreams did not return shard-stream; got %+v", statuses)
	}
	if got.SlotName != "sluice_shard_a" {
		t.Errorf("StreamStatus.SlotName = %q; want %q", got.SlotName, "sluice_shard_a")
	}

	// Empty SetSlotName preserves the previously-recorded value: the
	// position-write's COALESCE keeps slot_name pointing at the
	// streamer's recorded slot.
	myApplier.SetSlotName("")
	if err := pw.WritePosition(ctx, "shard-stream", ir.Position{Engine: engineNameMySQL, Token: "tok2"}); err != nil {
		t.Fatalf("WritePosition (preserve): %v", err)
	}
	statuses, err = applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("ListStreams (after preserve): %v", err)
	}
	for _, s := range statuses {
		if s.StreamID == "shard-stream" && s.SlotName != "sluice_shard_a" {
			t.Errorf("after empty SetSlotName, SlotName = %q; want preserved %q", s.SlotName, "sluice_shard_a")
		}
	}
}
