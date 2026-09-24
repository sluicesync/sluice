//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cold-start `migrate` half of GC-36 item 4: a DEFAULT on a column the
// MySQL-family target stores as TEXT / BLOB / JSON / GEOMETRY lands on the
// target, carrying the source's value.
//
// History: v0.32.2 made the MySQL writer DROP every such DEFAULT with a
// WARN, because MySQL rejects a plain literal there (Error 1101) and a PG
// `jsonb NOT NULL DEFAULT '{}'::jsonb` column had killed CREATE TABLE. The
// test this file replaces pinned that drop. The drop was a metadata loss on
// migrate (later inserts that omit the column got NULL, or failed on NOT
// NULL) and silent corruption on a forwarded ADD COLUMN; MySQL 8.0.13+ and
// MariaDB both accept the parenthesised expression form the writer now
// emits. The forward half is graded by the pre-existing-row gate
// (streamer_schema_forward_preexisting_defaults_integration_test.go).
//
// # The independent expected value
//
// The SOURCE's own default: a row inserted on the source naming only its
// key is filled by the source server, and a row inserted the same way on
// the target after the migrate is filled by the target server. Each cell
// compares the two in its family's canonical text. Neither side is read
// through sluice, and the target catalog is never consulted.

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// lobDefaultCell is one column: its source definition and the canonical
// text expression each side renders it in.
type lobDefaultCell struct {
	col, def         string
	srcExpr, tgtExpr string
}

// lobDefaultLane is one cold-start migrate pair.
type lobDefaultLane struct {
	sourceEngine, targetEngine string
	sourceDSN, targetDSN       string
	src, tgt                   fdDialect
	cells                      []lobDefaultCell
}

// runLOBDefaultLane creates `lobd` on the source with every cell, inserts
// row 1 naming only the key, migrates, inserts row 2 the same way on the
// target, and requires target row 2 — and the copied row 1 — to read back
// the source's row 1 in every cell.
func runLOBDefaultLane(t *testing.T, lane lobDefaultLane) {
	t.Helper()
	ddl := "CREATE TABLE lobd (id BIGINT NOT NULL PRIMARY KEY"
	for _, c := range lane.cells {
		ddl += ", " + fdQuote(lane.src, c.col) + " " + c.def
	}
	ddl += ")"
	fdExec(t, lane.src, lane.sourceDSN, ddl)
	fdExec(t, lane.src, lane.sourceDSN, "INSERT INTO lobd (id) VALUES (1)")

	srcEng, ok := engines.Get(lane.sourceEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.sourceEngine)
	}
	tgtEng, ok := engines.Get(lane.targetEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.targetEngine)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mig := &Migrator{Source: srcEng, Target: tgtEng, SourceDSN: lane.sourceDSN, TargetDSN: lane.targetDSN}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	fdExec(t, lane.tgt, lane.targetDSN, "INSERT INTO lobd (id) VALUES (2)")

	srcDB, err := sql.Open(lane.src.driver(), lane.sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	tgtDB, err := sql.Open(lane.tgt.driver(), lane.targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	read := func(db *sql.DB, expr string, id int) string {
		var v sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT "+expr+" FROM lobd WHERE id = "+strconv.Itoa(id)).Scan(&v); err != nil {
			t.Fatalf("read %s id=%d: %v", expr, id, err)
		}
		if !v.Valid {
			return fdNull
		}
		return v.String
	}
	for _, c := range lane.cells {
		want := read(srcDB, c.srcExpr, 1)
		if want == fdNull {
			t.Fatalf("%s: the source's own default is NULL — the cell grades nothing", c.col)
		}
		if got := read(tgtDB, c.tgtExpr, 2); got != want {
			t.Errorf("%s %s: a target INSERT omitting the column holds [%s]; the source default is [%s]", c.col, c.def, got, want)
		}
		if got := read(tgtDB, c.tgtExpr, 1); got != want {
			t.Errorf("%s %s: the copied row holds [%s]; the source row holds [%s]", c.col, c.def, got, want)
		}
		t.Logf("%-10s %-44s source default [%s]", c.col, c.def, want)
	}
}

// TestMigrate_LOBDefaultsLandOnTheTarget_PostgresToMySQL covers every
// Postgres family that lands on a MySQL large-object column: text (incl.
// the wide-VARCHAR TEXT tier and the backslash wart), bytea, jsonb and
// the carried array element families.
func TestMigrate_LOBDefaultsLandOnTheTarget_PostgresToMySQL(t *testing.T) {
	src, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, tgt, myCleanup := startMySQL(t)
	defer myCleanup()
	text := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdPG, fdText, col), fdCanonExpr(fdMySQL, fdText, col)}
	}
	bin := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdPG, fdBinary, col), fdCanonExpr(fdMySQL, fdBinary, col)}
	}
	arr := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdPG, fdArray, col), fdCanonExpr(fdMySQL, fdArray, col)}
	}
	runLOBDefaultLane(t, lobDefaultLane{
		sourceEngine: "postgres", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdMySQL,
		cells: []lobDefaultCell{
			text("t_text", "TEXT DEFAULT 'it''s'"),
			text("t_nn", "TEXT NOT NULL DEFAULT ''"),
			text("t_bslash", `TEXT DEFAULT 'a\b'`),
			text("t_unicode", "TEXT DEFAULT 'héllo ✓ 日本'"),
			text("t_wide", "VARCHAR(20000) DEFAULT 'wide'"),
			bin("b_bytea", `BYTEA DEFAULT '\x00ab00'`),
			bin("b_text", "BYTEA DEFAULT 'abc'"),
			text("j_empty", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
			text("j_bslash", `JSONB DEFAULT '{"k": "x\\y", "n": [1, 2]}'`),
			arr("a_int", "INTEGER[] DEFAULT '{1,-2}'"),
			arr("a_text", `TEXT[] DEFAULT '{a,"b c",NULL}'`),
			arr("a_bool", "BOOLEAN[] DEFAULT '{t,f}'"),
		},
	})
}

// TestMigrate_LOBDefaultsLandOnTheTarget_MySQLToMySQL covers MySQL's own
// expression defaults on every large-object family.
func TestMigrate_LOBDefaultsLandOnTheTarget_MySQLToMySQL(t *testing.T) {
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()
	text := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdMySQL, fdText, col), fdCanonExpr(fdMySQL, fdText, col)}
	}
	runLOBDefaultLane(t, lobDefaultLane{
		sourceEngine: "mysql", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		cells: []lobDefaultCell{
			text("t_expr", "TEXT DEFAULT ('txt')"),
			// ASCII on purpose: MySQL's information_schema.COLUMN_DEFAULT
			// double-encodes non-ASCII in an EXPRESSION default (é reads back
			// as C383C2A9; SHOW CREATE TABLE has it right, measured on
			// 8.0.46), so the MySQL reader hands every family — VARCHAR
			// included — a mojibake default. That is a reader defect of its
			// own, reported with GC-36 item 4 rather than folded into it.
			text("t_long", "LONGTEXT NOT NULL DEFAULT ('hello')"),
			{"b_expr", "BLOB DEFAULT (0x00FF)", fdCanonExpr(fdMySQL, fdBinary, "b_expr"), fdCanonExpr(fdMySQL, fdBinary, "b_expr")},
			text("j_expr", "JSON DEFAULT (JSON_OBJECT('a', 1))"),
			{"g_expr", "GEOMETRY DEFAULT (ST_GeomFromText('POINT(1 2)'))", "ST_AsText(`g_expr`)", "ST_AsText(`g_expr`)"},
		},
	})
}

// TestMigrate_LOBDefaultsLandOnTheTarget_MariaDBToMariaDB covers the
// literal defaults MariaDB (unlike MySQL) takes on TEXT, BLOB and JSON,
// including a backslash in each and a BLOB literal, which the MariaDB
// reader carries as hex bytes.
func TestMigrate_LOBDefaultsLandOnTheTarget_MariaDBToMariaDB(t *testing.T) {
	src, tgt, cleanup := startMariaDB(t)
	defer cleanup()
	text := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdMySQL, fdText, col), fdCanonExpr(fdMySQL, fdText, col)}
	}
	bin := func(col, def string) lobDefaultCell {
		return lobDefaultCell{col, def, fdCanonExpr(fdMySQL, fdBinary, col), fdCanonExpr(fdMySQL, fdBinary, col)}
	}
	runLOBDefaultLane(t, lobDefaultLane{
		sourceEngine: "mariadb", targetEngine: "mariadb",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		cells: []lobDefaultCell{
			text("t_text", "TEXT DEFAULT 'txt'"),
			text("t_bslash", `TEXT DEFAULT 'a\\b'`),
			bin("b_blob", `BLOB DEFAULT 'ab\\c'`),
			text("j_json", `JSON DEFAULT '{"a": "x\\\\y"}'`),
		},
	})
}
