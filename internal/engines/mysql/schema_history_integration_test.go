//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0049 sluice_cdc_schema_history
// control table (Chunk A). Boots a MySQL container and asserts:
//
//   - ensureSchemaHistoryTable is additive: a target that already has
//     sluice_cdc_state data keeps it intact, and a second ensure call
//     is a no-op.
//   - writeSchemaVersion → resolveSchemaVersion round-trips an
//     ir.Table through the backup tagged-union codec, selecting the
//     correct version per the GTID partial order.
//   - A position below the retention floor surfaces a loud
//     ir.ErrPositionInvalid (→ ADR-0022 cold-start).

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestEnsureSchemaHistoryTable_AdditiveToCDCState(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-create sluice_cdc_state with a row, as a live stream would.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id       VARCHAR(255) NOT NULL,"+
		"  source_position TEXT         NOT NULL,"+
		"  updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('live-stream', 'tok-1');")

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

	// The schema-history table now exists...
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sluice_cdc_schema_history'`).Scan(&n); err != nil {
		t.Fatalf("table lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("sluice_cdc_schema_history missing after EnsureControlTable; count=%d", n)
	}

	// ...and the pre-existing cdc-state row is untouched.
	var tok string
	if err := db.QueryRowContext(ctx,
		"SELECT source_position FROM `sluice_cdc_state` WHERE stream_id = ?", "live-stream").Scan(&tok); err != nil {
		t.Fatalf("cdc-state select: %v", err)
	}
	if tok != "tok-1" {
		t.Errorf("cdc-state row mutated: token = %q; want tok-1", tok)
	}

	// Second ensure is a no-op (idempotent).
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

func TestSchemaHistory_WriteResolveRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	const u = "11111111-1111-1111-1111-111111111111"
	anchorOld := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`}
	anchorNew := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-20"}`}

	tblOld := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	tblNew := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}

	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorOld, tblOld); err != nil {
		t.Fatalf("writeSchemaVersion old: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new: %v", err)
	}
	// Idempotent re-write of the same anchor.
	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new (idempotent): %v", err)
	}

	// Event at GTID 1-15 → between old and new → resolves to the OLD
	// (pre-ALTER) schema, the position-anchored-correctness property.
	pBetween := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-15"}`}
	got, err := resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pBetween)
	if err != nil {
		t.Fatalf("resolve between: %v", err)
	}
	if len(got.Columns) != 1 {
		t.Errorf("between-resolve should be the 1-column pre-ALTER schema; got %d cols", len(got.Columns))
	}

	// Event at GTID 1-25 → at-or-after new → resolves to the NEW schema.
	pAfter := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-25"}`}
	got, err = resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pAfter)
	if err != nil {
		t.Fatalf("resolve after: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Errorf("after-resolve should be the 2-column post-ALTER schema; got %d cols", len(got.Columns))
	}

	// Event at GTID 1-5 → before the oldest retained anchor → loud
	// ErrPositionInvalid (DP-2 floor → ADR-0022 cold-start).
	pBelow := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-5"}`}
	if _, err := resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pBelow); !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor resolve must wrap ir.ErrPositionInvalid; got %v", err)
	}
}

// TestSchemaHistory_VersionAndPosition_SameTxAtomicity is the
// locked-decision-#4a integration pin: the schema-version write and
// the ADR-0007 position write MUST be the same target transaction, so
// a failure in the version write rolls back the position write (a
// cross-tx crash that persists a position whose schema version isn't
// durable causes a spurious ADR-0022 cold-start). We inject a
// version-write failure (the schema-history table is absent) inside a
// tx that has already written the position, then assert the position
// row never landed after the rollback.
//
// **ADR-0049 #4a invariant (Chunk E regression-pin):** the
// schema-history write rides the SAME target tx as the ADR-0007
// position write. This test IS the direct extension of the ADR-0007
// position-and-data atomicity contract into the schema-history
// realm — pre-Chunk-B, a position write that committed without a
// version write was structurally impossible (no version write
// existed); post-Chunk-B, the same property must hold by sharing
// the tx, NOT by serial writes. Any change that introduces a
// separate tx for the version write (e.g. write-then-commit-tx,
// then start-tx for position) silently breaks this invariant and
// regresses the spurious-cold-start class.
func TestSchemaHistory_VersionAndPosition_SameTxAtomicity(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureControlTable(ctx, db); err != nil {
		t.Fatalf("ensureControlTable: %v", err)
	}
	// Deliberately do NOT create sluice_cdc_schema_history so the
	// version write inside the tx fails (table doesn't exist).

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	// Position write succeeds first (mirrors applyOne ordering: data
	// + version on the tx, then writePositionTx).
	if err := writePositionTx(ctx, tx, "atomic-stream", "tok-after-ddl", "", "", ""); err != nil {
		_ = tx.Rollback()
		t.Fatalf("writePositionTx: %v", err)
	}
	// Version write on the SAME tx fails (no schema-history table).
	anchor := ir.Position{Engine: engineNameMySQL, Token: "tok-after-ddl"}
	verr := writeSchemaVersion(ctx, tx, "atomic-stream", "", "users", anchor,
		&ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}})
	if verr == nil {
		_ = tx.Rollback()
		t.Fatal("expected version write to fail (schema-history table absent), got nil")
	}
	// #4b: the failure is fatal/loud — the caller rolls back the
	// WHOLE tx, so the position write must NOT be durable.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	_, ok, err := readPosition(ctx, db, "atomic-stream")
	if err != nil {
		t.Fatalf("readPosition: %v", err)
	}
	if ok {
		t.Fatal("position row IS present after a version-write failure + rollback — " +
			"version and position are NOT in the same tx (#4a violated; spurious-cold-start class)")
	}
}

// TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine is the
// headline Bug 78 regression pin (v0.70.1 hotfix).
//
// Repro shape: a cross-engine chain-restore (MySQL src → PG dst →
// resume) lands a row in PG's sluice_cdc_schema_history whose
// anchor_position is a MySQL GTID token; on the symmetric direction
// (PG src → MySQL dst → resume), MySQL's schema-history table holds a
// row whose anchor_position is a PG LSN token. Pre-fix, the load path
// stamped engineNameMySQL on every Engine field regardless of the
// token's true origin, and the cross-engine PositionOrderer strict
// engine-tag decode then rejected it (`decode cdc position: engine =
// "postgres"; want "mysql"`), blocking the entire restore-then-resume
// operator path.
//
// This pin writes a "postgres"-engine anchor through MySQL's
// writeSchemaVersion and asserts loadRetainedSchemaVersions returns a
// RetainedSchemaVersion whose Anchor.Engine is "postgres" (NOT
// "mysql"). Catches any regression that re-hardcodes the applier's own
// engine on the load path.
func TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// A cross-engine chain-restore would persist a PG-shape anchor on
	// MySQL's side. The token is opaque to the MySQL applier; what
	// matters is the Engine tag survives the write/load round-trip.
	crossEngineAnchor := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"s1","lsn":"0/2000000"}`,
	}
	tbl := &ir.Table{Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, "sluice_chain_restore", "src_app", "widgets", crossEngineAnchor, tbl); err != nil {
		t.Fatalf("writeSchemaVersion cross-engine: %v", err)
	}

	versions, err := loadRetainedSchemaVersions(ctx, db, "sluice_chain_restore", "src_app", "widgets")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("want 1 retained version; got %d", len(versions))
	}
	if got := versions[0].Anchor.Engine; got != "postgres" {
		t.Fatalf("Bug 78 regression: Anchor.Engine = %q; want %q (cross-engine source-engine identity lost on load path)",
			got, "postgres")
	}
	if got, want := versions[0].Anchor.Token, crossEngineAnchor.Token; got != want {
		t.Errorf("Anchor.Token round-trip: got %q; want %q", got, want)
	}
}

// TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine pins the
// pre-fix behaviour for legacy rows persisted by v0.70.0 (before the
// source_engine column existed). A NULL source_engine in storage MUST
// fall back to the applier's own engine name (engineNameMySQL on
// MySQL) — that's the pre-fix shape, which is correct for same-engine
// streams (target == source). Pin-the-class: ensures the fallback
// branch in loadRetainedSchemaVersions stays load-bearing and a future
// "drop the fallback because the new write path always populates it"
// refactor doesn't silently regress legacy-row resume.
func TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Bypass writeSchemaVersion entirely to simulate a v0.70.0 row:
	// source_engine NULL. raw SQL keeps the test independent of any
	// future change to writeSchemaVersion's column list.
	const tok = `{"mode":"gtid","gtid_set":"22222222-2222-2222-2222-222222222222:1-50"}`
	tbl := &ir.Table{Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	payload, err := ir.MarshalTable(tbl)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	vk := ir.SchemaVersionKey("legacy-stream", "src_app", "widgets", tok)
	const insertRaw = "INSERT INTO `" + schemaHistoryTableName + "` " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
		"VALUES (?, ?, ?, ?, ?, ?, NULL)"
	if _, err := db.ExecContext(ctx, insertRaw, vk, "legacy-stream", "src_app", "widgets", tok, string(payload)); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	versions, err := loadRetainedSchemaVersions(ctx, db, "legacy-stream", "src_app", "widgets")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("want 1 retained version; got %d", len(versions))
	}
	if got := versions[0].Anchor.Engine; got != engineNameMySQL {
		t.Fatalf("legacy-NULL fallback regression: Anchor.Engine = %q; want %q (the pre-fix behaviour for same-engine streams)",
			got, engineNameMySQL)
	}
}

// TestEnsureSchemaHistoryTable_UpgradeAddsSourceEngineColumn pins the
// v0.70.0 → v0.70.1 upgrade path: a pre-existing schema-history table
// without the source_engine column must pick the column up via
// ensureSchemaHistoryTable's detect-then-ALTER migration, with
// existing rows preserved intact (source_engine NULL on them — the
// load path falls back to the applier's own engine name per
// TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine).
func TestEnsureSchemaHistoryTable_UpgradeAddsSourceEngineColumn(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Pre-create the v0.70.0-shape schema-history table WITHOUT the
	// new column, and seed a row as a live v0.70.0 deployment would.
	const v0700DDL = "CREATE TABLE `" + schemaHistoryTableName + "` (" +
		"version_key     CHAR(64)     NOT NULL," +
		"stream_id       VARCHAR(255) NOT NULL," +
		"schema_name     VARCHAR(255) NOT NULL," +
		"table_name      VARCHAR(255) NOT NULL," +
		"anchor_position LONGTEXT     NOT NULL," +
		"ir_schema_json  LONGTEXT     NOT NULL," +
		"created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"PRIMARY KEY (version_key)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	if _, err := db.ExecContext(ctx, v0700DDL); err != nil {
		t.Fatalf("seed v0.70.0 table: %v", err)
	}
	const tok = `{"mode":"gtid","gtid_set":"33333333-3333-3333-3333-333333333333:1-1"}`
	vk := ir.SchemaVersionKey("upgrade-stream", "src_app", "widgets", tok)
	tbl := &ir.Table{Name: "widgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	payload, err := ir.MarshalTable(tbl)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	const seedQ = "INSERT INTO `" + schemaHistoryTableName + "` " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json) " +
		"VALUES (?, ?, ?, ?, ?, ?)"
	if _, err := db.ExecContext(ctx, seedQ, vk, "upgrade-stream", "src_app", "widgets", tok, string(payload)); err != nil {
		t.Fatalf("seed v0.70.0 row: %v", err)
	}

	// Run the v0.70.1 ensureSchemaHistoryTable: must add the column,
	// not error, and not touch the existing row.
	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable (upgrade): %v", err)
	}

	// The column now exists.
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = 'source_engine'`, schemaHistoryTableName).Scan(&n); err != nil {
		t.Fatalf("column lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("source_engine column missing after upgrade ensure; count=%d", n)
	}

	// The existing row is intact + has NULL source_engine.
	var (
		gotTok        string
		gotPayload    string
		gotSrcEngNull sql.NullString
	)
	if err := db.QueryRowContext(
		ctx,
		"SELECT anchor_position, ir_schema_json, source_engine FROM `"+schemaHistoryTableName+"` WHERE version_key = ?", vk,
	).Scan(&gotTok, &gotPayload, &gotSrcEngNull); err != nil {
		t.Fatalf("row select: %v", err)
	}
	if gotTok != tok {
		t.Errorf("anchor_position mutated by upgrade: got %q; want %q", gotTok, tok)
	}
	if gotPayload != string(payload) {
		t.Errorf("ir_schema_json mutated by upgrade")
	}
	if gotSrcEngNull.Valid {
		t.Errorf("source_engine on legacy row should be NULL after upgrade; got %q", gotSrcEngNull.String)
	}

	// Second ensure call is a no-op (detect-then-ALTER skips when
	// column already exists).
	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable (idempotent): %v", err)
	}
}
