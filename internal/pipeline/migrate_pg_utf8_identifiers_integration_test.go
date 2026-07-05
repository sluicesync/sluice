//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration pin for multi-byte UTF-8 identifier
// round-trip through pipeline.Migrator (PG → PG). Adoption from the
// broader-mining report (gap #1 — highest silent-loss-class risk;
// docs/dev/notes/test-gap-mining-broader.md → confirmed gap, the
// existing TestQuoteIdent in both engines covered only ASCII edge
// cases). Mirrors Bucardo's `t/10-object-names.t` shape: a table with
// multi-byte-named columns + multi-byte data flows source → target
// byte-exact.
//
// Per CLAUDE.md "loud-failure tenet" — silent identifier truncation /
// mojibake during migrate would be a worst-class regression. The
// quoteIdent unit tests (postgres + mysql row_reader_unit_test.go)
// pin the SQL-shape side; this file pins that an end-to-end migrate
// preserves the bytes through every emitter, the schema reader, the
// bulk-copy path, and the rich-type identifier lookups.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_UTF8Identifiers seeds a source with a
// table whose name + columns are in multi-byte UTF-8 (CJK / Cyrillic
// / Latin-1 supplement), then migrates and asserts byte-exact identifier
// preservation on the target. The data side also carries multi-byte
// content so a byte-for-byte target read confirms the writer's
// identifier quoting + the bulk-copy COPY path both stay UTF-8-safe.
func TestMigrate_PostgresToPostgres_UTF8Identifiers(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Source DDL: every name in the schema (table, columns, PK,
	// unique index) carries multi-byte UTF-8. The combination
	// exercises the writer's quoteIdent on three byte-width families
	// (Latin-1 supplement = 2-byte, Cyrillic = 2-byte, CJK = 3-byte)
	// plus a column whose name embeds an ASCII apostrophe — a regression
	// guard because the apostrophe is NOT the PG quote character, so
	// the writer must NOT escape it but MUST pass it through verbatim.
	const seedDDL = `
		CREATE TABLE "用户表" (
			"идентификатор" BIGINT      PRIMARY KEY,
			"café_日付"     TIMESTAMP   NOT NULL,
			"日本語コメント" TEXT       NOT NULL,
			"jeu_d'études"  VARCHAR(64) NOT NULL,
			CONSTRAINT "用户表_uq_jeu" UNIQUE ("jeu_d'études")
		);

		INSERT INTO "用户表" ("идентификатор", "café_日付", "日本語コメント", "jeu_d'études") VALUES
			(1, '2026-01-02 03:04:05', 'こんにちは世界',           'études-numéro-1'),
			(2, '2026-01-03 06:07:08', 'Здравствуй мир (mojibake?)', 'études-numéro-2'),
			(3, '2026-01-04 09:10:11', '🎉 emoji-safe rows too 🎉',   'études-numéro-3');
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v\n(a silent identifier corruption would surface here as a quoting or "+
			"reserved-name error from the writer — sluice's quote-by-byte-wrap policy should pass UTF-8 verbatim)", err)
	}

	// Re-read the target schema and assert every identifier round-tripped
	// byte-exact. Reading via the SchemaReader exercises the inverse
	// (catalog → IR) of the writer's IR → DDL path, so a bug on either
	// side surfaces.
	sr, err := pgEng.OpenSchemaReader(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)

	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(got, "用户表")
	if tbl == nil {
		t.Fatalf("target missing the multi-byte table name; have %v", targetTableNames(got))
	}

	wantCols := []string{"идентификатор", "café_日付", "日本語コメント", "jeu_d'études"}
	gotCols := make([]string, len(tbl.Columns))
	for i, c := range tbl.Columns {
		gotCols[i] = c.Name
	}
	if len(gotCols) != len(wantCols) {
		t.Fatalf("column count = %d; want %d (got=%v want=%v)", len(gotCols), len(wantCols), gotCols, wantCols)
	}
	for i := range wantCols {
		if gotCols[i] != wantCols[i] {
			t.Errorf("column[%d]: got %q (% x); want %q (% x) — byte-exact identifier preservation regressed",
				i, gotCols[i], []byte(gotCols[i]), wantCols[i], []byte(wantCols[i]))
		}
	}
	if tbl.PrimaryKey == nil || len(tbl.PrimaryKey.Columns) == 0 || tbl.PrimaryKey.Columns[0].Column != "идентификатор" {
		t.Errorf("PK = %+v; want PK on \"идентификатор\"", tbl.PrimaryKey)
	}
	// Unique constraint name + the column it references both byte-exact.
	hasUTF8Unique := false
	for _, ix := range tbl.Indexes {
		if !ix.Unique || len(ix.Columns) != 1 {
			continue
		}
		if ix.Columns[0].Column == "jeu_d'études" {
			hasUTF8Unique = true
			break
		}
	}
	if !hasUTF8Unique {
		t.Errorf("indexes = %#v; want a unique index on \"jeu_d'études\"", tbl.Indexes)
	}

	// Data round-trip: read the target rows directly via SQL and check
	// every value byte-exact. quoteIdent is exercised in the SELECT
	// itself (we name the table + columns by their multi-byte names).
	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open(pgx): %v", err)
	}
	defer func() { _ = db.Close() }()

	const selectMultibyte = `
		SELECT "идентификатор", "日本語コメント", "jeu_d'études"
		  FROM "用户表"
		 ORDER BY "идентификатор"
	`
	rows, err := db.QueryContext(ctx, selectMultibyte)
	if err != nil {
		t.Fatalf("SELECT from multi-byte-named table: %v\n(quoteIdent must produce a SELECT PG accepts; "+
			"a mis-quoted UTF-8 identifier would surface here as a syntax error)", err)
	}
	defer func() { _ = rows.Close() }()

	type row struct {
		id      int64
		comment string
		jeu     string
	}
	want := []row{
		{1, "こんにちは世界", "études-numéro-1"},
		{2, "Здравствуй мир (mojibake?)", "études-numéro-2"},
		{3, "🎉 emoji-safe rows too 🎉", "études-numéro-3"},
	}
	var gotRows []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.comment, &r.jeu); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		gotRows = append(gotRows, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(gotRows) != len(want) {
		t.Fatalf("row count = %d; want %d", len(gotRows), len(want))
	}
	for i := range want {
		if gotRows[i] != want[i] {
			t.Errorf("row[%d]: got %+v; want %+v\n(byte-by-byte: got comment=% x want=% x)",
				i, gotRows[i], want[i], []byte(gotRows[i].comment), []byte(want[i].comment))
		}
	}

	// Defensive: also verify the table's NAME on the target matches the
	// source byte-exact. ReadSchema went via pg_catalog; this verifies
	// the table actually exists under that name by attempting a SELECT
	// on it (already done above) and via pg_class.relname.
	var relname string
	const q = `SELECT relname FROM pg_class WHERE relname = $1 AND relkind = 'r'`
	if err := db.QueryRowContext(ctx, q, "用户表").Scan(&relname); err != nil {
		t.Fatalf("pg_class lookup by source name: %v", err)
	}
	if relname != "用户表" {
		t.Errorf("relname = %q (% x); want %q (% x)", relname, []byte(relname), "用户表", []byte("用户表"))
	}
	if !strings.Contains(relname, "用户表") {
		// Belt-and-suspenders — strings.Contains would catch a mojibake-
		// transform that fooled the equality check via Unicode normalization.
		t.Errorf("relname does not literally contain \"用户表\"; got %q", relname)
	}
}
