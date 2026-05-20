//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0049 sluice_cdc_schema_history
// control table (Chunk A), Postgres side. Mirrors the MySQL test:
// additive to sluice_cdc_state, idempotent ensure, write→resolve
// round-trip with the LSN total order, and the below-floor loud
// ir.ErrPositionInvalid.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestEnsureSchemaHistoryTable_AdditiveToCDCState(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position)
		VALUES ('live-stream', 'tok-1');
	`)

	eng := Engine{}
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

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'sluice_cdc_schema_history'`).Scan(&n); err != nil {
		t.Fatalf("table lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("sluice_cdc_schema_history missing after EnsureControlTable; count=%d", n)
	}

	var tok string
	if err := db.QueryRowContext(ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`, "live-stream").Scan(&tok); err != nil {
		t.Fatalf("cdc-state select: %v", err)
	}
	if tok != "tok-1" {
		t.Errorf("cdc-state row mutated: token = %q; want tok-1", tok)
	}

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

func TestSchemaHistory_WriteResolveRoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchorOld := mkPos("0/1000000")
	anchorNew := mkPos("0/2000000")

	tblOld := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	tblNew := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}

	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorOld, tblOld); err != nil {
		t.Fatalf("writeSchemaVersion old: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new: %v", err)
	}
	// Idempotent re-write.
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new (idempotent): %v", err)
	}

	// LSN between old and new → resolves to OLD (pre-ALTER) schema.
	got, err := resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/1500000"))
	if err != nil {
		t.Fatalf("resolve between: %v", err)
	}
	if len(got.Columns) != 1 {
		t.Errorf("between-resolve should be 1-column pre-ALTER; got %d", len(got.Columns))
	}

	// LSN at-or-after new → NEW schema.
	got, err = resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/2500000"))
	if err != nil {
		t.Fatalf("resolve after: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Errorf("after-resolve should be 2-column post-ALTER; got %d", len(got.Columns))
	}

	// LSN before the oldest retained anchor → loud ir.ErrPositionInvalid.
	if _, err := resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/500000")); !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor resolve must wrap ir.ErrPositionInvalid; got %v", err)
	}
}

// TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine is the
// headline Bug 78 regression pin (v0.70.1 hotfix).
//
// Repro shape: a cross-engine chain-restore (MySQL src → PG dst →
// resume) lands a row in PG's sluice_cdc_schema_history whose
// anchor_position is a MySQL GTID token. Pre-fix, the load path
// stamped engineNamePostgres on every Engine field regardless of the
// token's true origin, and the cross-engine PositionOrderer strict
// engine-tag decode then rejected it (`decode cdc position: engine =
// "mysql"; want "postgres"`), blocking the entire restore-then-resume
// operator path.
//
// This pin writes a "mysql"-engine anchor through PG's
// writeSchemaVersion and asserts loadRetainedSchemaVersions returns a
// RetainedSchemaVersion whose Anchor.Engine is "mysql" (NOT
// "postgres"). Catches any regression that re-hardcodes the applier's
// own engine on the load path.
func TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// A cross-engine chain-restore (the v0.70.0 release notes' Scenario
	// 2: MySQL src → PG dst → resume) lands a MySQL GTID token in PG's
	// schema-history. The token is opaque to the PG applier; what
	// matters is the Engine tag survives the write/load round-trip.
	const u = "26d1668c-1234-5678-9abc-def012345678"
	crossEngineAnchor := ir.Position{
		Engine: "mysql",
		Token:  `{"mode":"gtid","gtid_set":"` + u + `:1-48"}`,
	}
	tbl := &ir.Table{Schema: schema, Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, schema, "sluice_chain_restore", "src_app", "widgets", crossEngineAnchor, tbl); err != nil {
		t.Fatalf("writeSchemaVersion cross-engine: %v", err)
	}

	versions, err := loadRetainedSchemaVersions(ctx, db, schema, "sluice_chain_restore", "src_app", "widgets")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("want 1 retained version; got %d", len(versions))
	}
	if got := versions[0].Anchor.Engine; got != "mysql" {
		t.Fatalf("Bug 78 regression: Anchor.Engine = %q; want %q (cross-engine source-engine identity lost on load path)",
			got, "mysql")
	}
	if got, want := versions[0].Anchor.Token, crossEngineAnchor.Token; got != want {
		t.Errorf("Anchor.Token round-trip: got %q; want %q", got, want)
	}
}

// TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine pins the
// pre-fix behaviour for legacy rows persisted by v0.70.0 (before the
// source_engine column existed). A NULL source_engine in storage MUST
// fall back to the applier's own engine name (engineNamePostgres on
// PG) — that's the pre-fix shape, which is correct for same-engine
// streams (target == source). Pin-the-class: ensures the fallback
// branch in loadRetainedSchemaVersions stays load-bearing and a future
// "drop the fallback because the new write path always populates it"
// refactor doesn't silently regress legacy-row resume.
func TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Bypass writeSchemaVersion entirely to simulate a v0.70.0 row:
	// source_engine NULL. Raw SQL keeps the test independent of any
	// future change to writeSchemaVersion's column list.
	tok, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/5000000"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	tbl := &ir.Table{Schema: schema, Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	payload, err := ir.MarshalTable(tbl)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	vk := ir.SchemaVersionKey("legacy-stream", "src_app", "widgets", tok.Token)
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)
	insertRaw := "INSERT INTO " + tableRef + " " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
		"VALUES ($1, $2, $3, $4, $5, $6, NULL)"
	if _, err := db.ExecContext(ctx, insertRaw, vk, "legacy-stream", "src_app", "widgets", tok.Token, string(payload)); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	versions, err := loadRetainedSchemaVersions(ctx, db, schema, "legacy-stream", "src_app", "widgets")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("want 1 retained version; got %d", len(versions))
	}
	if got := versions[0].Anchor.Engine; got != engineNamePostgres {
		t.Fatalf("legacy-NULL fallback regression: Anchor.Engine = %q; want %q (the pre-fix behaviour for same-engine streams)",
			got, engineNamePostgres)
	}
}

// TestEnsureSchemaHistoryTable_UpgradeAddsSourceEngineColumn pins the
// v0.70.0 → v0.70.1 upgrade path: a pre-existing schema-history table
// without the source_engine column must pick the column up via
// ensureSchemaHistoryTable's ADD COLUMN IF NOT EXISTS migration, with
// existing rows preserved intact (source_engine NULL on them — the
// load path falls back to the applier's own engine name per
// TestSchemaHistory_LoadFallsBackForLegacyNullSourceEngine).
func TestEnsureSchemaHistoryTable_UpgradeAddsSourceEngineColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)

	// Pre-create the v0.70.0-shape schema-history table WITHOUT the
	// new column, and seed a row as a live v0.70.0 deployment would.
	v0700DDL := `
		CREATE TABLE ` + tableRef + ` (
			version_key     CHAR(64)     NOT NULL,
			stream_id       VARCHAR(255) NOT NULL,
			schema_name     VARCHAR(255) NOT NULL,
			table_name      VARCHAR(255) NOT NULL,
			anchor_position TEXT         NOT NULL,
			ir_schema_json  TEXT         NOT NULL,
			created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (version_key)
		)`
	if _, err := db.ExecContext(ctx, v0700DDL); err != nil {
		t.Fatalf("seed v0.70.0 table: %v", err)
	}
	tok, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/4000000"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	vk := ir.SchemaVersionKey("upgrade-stream", "src_app", "widgets", tok.Token)
	tbl := &ir.Table{Schema: schema, Name: "widgets", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	payload, err := ir.MarshalTable(tbl)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	seedQ := "INSERT INTO " + tableRef + " " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json) " +
		"VALUES ($1, $2, $3, $4, $5, $6)"
	if _, err := db.ExecContext(ctx, seedQ, vk, "upgrade-stream", "src_app", "widgets", tok.Token, string(payload)); err != nil {
		t.Fatalf("seed v0.70.0 row: %v", err)
	}

	// Run the v0.70.1 ensureSchemaHistoryTable: must add the column,
	// not error, and not touch the existing row.
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable (upgrade): %v", err)
	}

	// The column now exists.
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1
		  AND table_name   = $2
		  AND column_name  = 'source_engine'`, schema, schemaHistoryTableName).Scan(&n); err != nil {
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
		"SELECT anchor_position, ir_schema_json, source_engine FROM "+tableRef+" WHERE version_key = $1", vk,
	).Scan(&gotTok, &gotPayload, &gotSrcEngNull); err != nil {
		t.Fatalf("row select: %v", err)
	}
	if gotTok != tok.Token {
		t.Errorf("anchor_position mutated by upgrade: got %q; want %q", gotTok, tok.Token)
	}
	if gotPayload != string(payload) {
		t.Errorf("ir_schema_json mutated by upgrade")
	}
	if gotSrcEngNull.Valid {
		t.Errorf("source_engine on legacy row should be NULL after upgrade; got %q", gotSrcEngNull.String)
	}

	// Second ensure call is a no-op (ADD COLUMN IF NOT EXISTS is a
	// safe re-run).
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable (idempotent): %v", err)
	}
}
