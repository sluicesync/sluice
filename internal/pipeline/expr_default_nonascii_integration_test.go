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

import "testing"

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
