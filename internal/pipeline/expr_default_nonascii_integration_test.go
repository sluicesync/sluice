//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 288 (GC-37 (a)): an EXPRESSION DEFAULT holding any non-ASCII
// character reached every target garbled. MySQL's information_schema
// renders a stored expression's bytes as Latin-1 re-encoded as UTF-8, so
// `DEFAULT ('é')` reads back as `_utf8mb4\'Ã©\'`, and sluice created the
// target's DEFAULT from that at exit 0 — `é` became `Ã©` on migrate, sync
// cold start, restore and a forwarded ADD COLUMN alike, MySQL and Postgres
// targets both. MariaDB's information_schema is faithful for the Basic
// Multilingual Plane but writes one '?' per byte for a character beyond it.
// The readers now recover the text against SHOW CREATE TABLE
// (internal/engines/mysql expr_text_recovery.go).
//
// # The independent expected value
//
// The SOURCE server's own fill, exactly as the GC-37 (h) literal-default
// lanes in text_default_supplementary_chars_integration_test.go grade it:
// on migrate a row inserted on each side naming only its key; on a
// forwarded ADD COLUMN, with the backfill suppressed so the target keeps
// what its own DEFAULT filled, the source rows that existed at the ALTER
// against the target's same rows. Neither side is read through sluice,
// and no catalog is consulted.

package pipeline

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// exprDefaultShapes is the expression-default matrix: plain ASCII (the
// control that must not change), each UTF-8 length — two bytes (é, ß),
// three (中), four (😀) — a genuine '?', an escaped apostrophe beside a
// non-ASCII character, and a TEXT column. MySQL accepts `DEFAULT (expr)`;
// MariaDB stores a bare string expression as a quoted LITERAL (the GC-37
// (h) path, already graded), so its shapes wrap each value in a function
// to stay expressions. withJSON adds a JSON default, which a forward
// refuses by design (json_object is not on ADR-0058 §2a's deterministic
// allowlist) and so rides only the migrate lanes.
func exprDefaultShapes(mariadb, withJSON bool) [][2]string {
	wrap := func(lit string) string {
		if mariadb {
			return "concat(" + lit + ",'')"
		}
		return "(" + lit + ")"
	}
	shapes := [][2]string{
		{"x_ascii", "VARCHAR(20) DEFAULT " + wrap("'abc'")},
		{"x_e", "VARCHAR(20) DEFAULT " + wrap("'é'")},
		{"x_mixed", "VARCHAR(20) DEFAULT (concat('ß','中'))"},
		{"x_emoji", "VARCHAR(20) DEFAULT " + wrap("'😀x'")},
		{"x_emojibmp", "VARCHAR(20) DEFAULT " + wrap("'é😀中'")},
		{"x_q", "VARCHAR(20) DEFAULT " + wrap("'?'")},
		{"x_quote", "VARCHAR(20) DEFAULT " + wrap("'it''s é'")},
		{"x_text", "TEXT DEFAULT " + wrap("'中é😀'")},
	}
	if withJSON {
		shapes = append(shapes, [2]string{"x_json", "JSON DEFAULT (json_object('k','é😀'))"})
	}
	return shapes
}

func exprDefaultMigrateLane(t *testing.T, sourceEngine, targetEngine, src, tgt string, tgtDialect fdDialect) {
	t.Helper()
	var cells []lobDefaultCell
	for _, sh := range exprDefaultShapes(sourceEngine == "mariadb", sourceEngine != "mariadb") {
		cells = append(cells, lobDefaultCell{
			col: sh[0], def: sh[1],
			srcExpr: fdCanonExpr(fdMySQL, fdText, sh[0]),
			tgtExpr: fdCanonExpr(tgtDialect, fdText, sh[0]),
		})
	}
	runLOBDefaultLane(t, lobDefaultLane{
		sourceEngine: sourceEngine, targetEngine: targetEngine,
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: tgtDialect,
		cells: cells,
	})
}

func TestMigrate_NonASCIIExpressionDefaults_MySQLToMySQL(t *testing.T) {
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()
	exprDefaultMigrateLane(t, "mysql", "mysql", src, tgt, fdMySQL)
}

func TestMigrate_NonASCIIExpressionDefaults_MySQLToPostgres(t *testing.T) {
	src, _, myCleanup := startMySQL(t)
	defer myCleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()
	exprDefaultMigrateLane(t, "mysql", "postgres", src, tgt, fdPG)
}

func TestMigrate_NonASCIIExpressionDefaults_MariaDBToMariaDB(t *testing.T) {
	src, tgt, cleanup := startMariaDB(t)
	defer cleanup()
	exprDefaultMigrateLane(t, "mariadb", "mariadb", src, tgt, fdMySQL)
}

func exprDefaultForwardShapes(mariadb bool) []fdShape {
	var out []fdShape
	for _, sh := range exprDefaultShapes(mariadb, false) {
		out = append(out, fdShape{col: sh[0], def: sh[1], fam: fdText})
	}
	return out
}

// The forwarded-ADD-COLUMN lanes run with the backfill suppressed, for the
// reason text_default_supplementary_chars_integration_test.go gives: the
// backfill would repair the pre-existing rows from the source and hide the
// forwarded DEFAULT, which is what later inserts on the target get.

func TestStreamer_AddColumnForward_NonASCIIExpressionDefaults_MySQLToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->mysql non-ascii expression", sourceEngine: "mysql", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes: exprDefaultForwardShapes(false), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}

func TestStreamer_AddColumnForward_NonASCIIExpressionDefaults_MySQLToPostgres(t *testing.T) {
	src, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->postgres non-ascii expression", sourceEngine: "mysql", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
		shapes: exprDefaultForwardShapes(false), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}

func TestStreamer_AddColumnForward_NonASCIIExpressionDefaults_MariaDBToMariaDB(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	tgtServer, tgtCleanup := startMariaDBBinlog(t)
	defer tgtCleanup()
	tgt := fdFreshDB(t, fdMySQL, tgtServer, "target_db")
	runForwardedDefaultLane(t, fdLane{
		name: "mariadb->mariadb non-ascii expression", sourceEngine: "mariadb", targetEngine: "mariadb",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes: exprDefaultForwardShapes(true), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}

// latin1SessionExec runs statements on ONE connection whose session charset
// is latin1, so the literals in them are stored as latin1 bytes — the table
// a client configured for latin1 creates.
func latin1SessionExec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, q := range append([]string{"SET NAMES latin1"}, stmts...) {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// exprLatin1Lane is the pre-tag review's F1/F3 pin through migrate: a table
// created from a latin1 session, whose expression literals are stored as
// latin1 bytes — C3A9 (the value Ã©) and E9 (é) — in a DEFAULT, a STORED
// generated column and a CHECK. The first cut of the Bug 288 recovery read
// C3A9 as é (silently wrong on every surface) and refused E9 outright.
//
// Independent expected value: the source server's own fill of a row naming
// only its key, against the target's own fill of the same; and for the
// CHECK, the target must refuse the value the source refuses and accept one
// it accepts. Nothing is read through sluice or any catalog.
func exprLatin1Lane(t *testing.T, targetEngine, src, tgt string, tgtDialect fdDialect) {
	t.Helper()
	latin1SessionExec(t, src, "CREATE TABLE lx (id BIGINT NOT NULL PRIMARY KEY, "+
		"a VARCHAR(20) DEFAULT ('\xc3\xa9'), e VARCHAR(20) DEFAULT ('caf\xe9'), "+
		"g VARCHAR(20) GENERATED ALWAYS AS (concat('\xc3\xa9', e)) STORED, "+
		"CONSTRAINT lx_ck CHECK (a <> 'x\xc3\xa9')) DEFAULT CHARSET utf8mb4")
	fdExec(t, fdMySQL, src, "INSERT INTO lx (id) VALUES (1)")
	srcEng, _ := engines.Get("mysql")
	tgtEng, _ := engines.Get(targetEngine)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := (&Migrator{Source: srcEng, Target: tgtEng, SourceDSN: src, TargetDSN: tgt}).Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	fdExec(t, tgtDialect, tgt, "INSERT INTO lx (id) VALUES (2)")
	read := func(d fdDialect, dsn string, id int) []string {
		t.Helper()
		db, err := sql.Open(d.driver(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		var out []string
		for _, col := range []string{"a", "e", "g"} {
			var v sql.NullString
			q := "SELECT " + fdCanonExpr(d, fdText, col) + " FROM lx WHERE id = " + strconv.Itoa(id)
			if err := db.QueryRowContext(ctx, q).Scan(&v); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			out = append(out, col+"="+v.String)
		}
		return out
	}
	srcVals, tgtVals := read(fdMySQL, src, 1), read(tgtDialect, tgt, 2)
	if strings.Join(srcVals, ",") != "a=Ã©,e=café,g=Ã©café" {
		t.Fatalf("the source evaluates %v; the fixture expects a=Ã©, e=café, g=Ã©café (fix the fixture)", srcVals)
	}
	if strings.Join(tgtVals, ",") != strings.Join(srcVals, ",") {
		t.Errorf("the target's own fill %v differs from the source's %v", tgtVals, srcVals)
	}
	tgtDB, err := sql.Open(tgtDialect.driver(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tgtDB.Close() }()
	if _, err := tgtDB.ExecContext(ctx, "INSERT INTO lx (id, a) VALUES (3, 'xÃ©')"); err == nil {
		t.Error("the target accepted a = 'xÃ©', which the source's CHECK refuses")
	}
	if _, err := tgtDB.ExecContext(ctx, "INSERT INTO lx (id, a) VALUES (4, 'xé')"); err != nil {
		t.Errorf("the target refused a = 'xé', which the source's CHECK accepts: %v", err)
	}
}

func TestMigrate_Latin1SessionExpressions_MySQLToMySQL(t *testing.T) {
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()
	exprLatin1Lane(t, "mysql", src, tgt, fdMySQL)
}

func TestMigrate_Latin1SessionExpressions_MySQLToPostgres(t *testing.T) {
	src, _, myCleanup := startMySQL(t)
	defer myCleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()
	exprLatin1Lane(t, "postgres", src, tgt, fdPG)
}
