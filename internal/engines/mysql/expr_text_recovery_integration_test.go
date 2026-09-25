//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// readIRTableViaEngine is [readTableViaEngine] returning the whole table, so
// CHECK constraints and functional indexes can be graded too.
func readIRTableViaEngine(t *testing.T, engineName, dsn, table string) *ir.Table {
	t.Helper()
	tbl, err := readIRTableViaEngineErr(engineName, dsn, table)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func readIRTableViaEngineErr(engineName, dsn, table string) (*ir.Table, error) {
	eng, ok := engines.Get(engineName)
	if !ok {
		return nil, errNotRegistered(engineName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == table {
			return tbl, nil
		}
	}
	return nil, errNotRegistered("table " + table)
}

type errNotRegistered string

func (e errNotRegistered) Error() string { return string(e) + " not found" }

// exprTextWant is one graded object: what the IR must carry, EXACTLY.
type exprTextWant struct {
	what, got, want string
}

// TestExprText_NonASCII_OnARealServer is the live Bug 288 pin, on MySQL and
// every supported MariaDB LTS line, across all four catalog expression
// surfaces — an expression DEFAULT, a VIRTUAL and a STORED generated column,
// a CHECK and (MySQL only; MariaDB has none) a multi-part functional UNIQUE
// key — each holding two-, three- and four-byte characters. MySQL's
// information_schema widens every stored byte (é read back as Ã©) and
// MariaDB's writes '?' per byte for the four-byte one, and sluice carried
// either into every target. For each server:
//
//   - the catalog really does not render the text faithfully (anti-vacuity,
//     per surface: a server that stopped would leave this proving nothing);
//   - the server's own evaluation of the default and generated columns —
//     an INSERT read back through the row path, not through any catalog —
//     equals the declared text (the independent expected value);
//   - the cold-start seed (ReadSchema) carries EXACTLY the declared text on
//     every surface, and the CDC boundary projection on the default and the
//     generated columns;
//   - on MariaDB: a column with a genuine '?' DEFAULT and an inline CHECK
//     holding an emoji resolves each to its own clause (F5); the GC-2 door
//     sees a default change at a lost character; and its all-tables
//     baseline is not broken by a VIEW over the table (F4).
func TestExprText_NonASCII_OnARealServer(t *testing.T) {
	type server struct {
		name, engine string
		flavor       Flavor
		dsn          func(t *testing.T) (dsn, schema string)
	}
	servers := []server{{"mysql", "mysql", FlavorVanilla, func(t *testing.T) (string, string) {
		dsn, _ := startMySQL(t)
		return dsn, "sluice_test"
	}}}
	for _, image := range mariadbLTSImages() {
		servers = append(servers, server{image, "mariadb", FlavorMariaDB, func(t *testing.T) (string, string) {
			return newMariaDB(t, image, "bug288_expr_text"), "bug288_expr_text"
		}})
	}
	for _, sv := range servers {
		t.Run(sv.name, func(t *testing.T) {
			dsn, schema := sv.dsn(t)
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			exec := func(q string) {
				t.Helper()
				if _, err := db.ExecContext(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			mariadb := sv.flavor == FlavorMariaDB
			ddl := "CREATE TABLE xr (id INT PRIMARY KEY, " +
				"d VARCHAR(40) DEFAULT (concat('é😀','中')), " +
				"g VARCHAR(60) AS (concat('ß😀', id)) VIRTUAL, " +
				"s VARCHAR(60) AS (concat('中😀', id)) STORED, " +
				"b VARCHAR(40), " +
				"CONSTRAINT xr_chk CHECK (b <> 'é😀')"
			if mariadb {
				ddl += ", q VARCHAR(40) DEFAULT concat('?','') CHECK (q <> concat('😀',''))" +
					", w VARCHAR(40) DEFAULT concat('????','') CHECK (w <> concat('😀',''))"
			} else {
				ddl += ", UNIQUE KEY xr_ux ((concat(b,'中')),(concat(b,'😀')))"
			}
			exec(ddl + ")")

			// Anti-vacuity: information_schema must not render any surface
			// faithfully, or recovery is not what this exercises.
			var def, gen, clause string
			if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'xr' AND column_name = 'd'`, schema).Scan(&def); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT generation_expression FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'xr' AND column_name = 'g'`, schema).Scan(&gen); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT check_clause FROM information_schema.check_constraints
				WHERE constraint_schema = ? AND constraint_name = 'xr_chk'`, schema).Scan(&clause); err != nil {
				t.Fatal(err)
			}
			for what, text := range map[string]string{"default": def, "generated": gen, "check": clause} {
				if strings.Contains(text, "😀") {
					t.Fatalf("%s: information_schema renders the %s faithfully (%q); re-measure the premise", sv.name, what, text)
				}
			}

			// The independent expected value: the server's own evaluation.
			exec("INSERT INTO xr (id, b) VALUES (1, 'x')")
			var gotD, gotG, gotS string
			if err := db.QueryRowContext(ctx, "SELECT d, g, s FROM xr WHERE id = 1").Scan(&gotD, &gotG, &gotS); err != nil {
				t.Fatal(err)
			}
			if gotD != "é😀中" || gotG != "ß😀1" || gotS != "中😀1" {
				t.Fatalf("the server evaluates d=%q g=%q s=%q; the fixture declares é😀中 / ß😀1 / 中😀1 (fix the fixture)", gotD, gotG, gotS)
			}

			defaultText := func(v ir.DefaultValue) string {
				if e, ok := v.(ir.DefaultExpression); ok {
					return e.Expr
				}
				return ""
			}
			seed := readIRTableViaEngine(t, sv.engine, dsn, "xr")
			cols := map[string]*ir.Column{}
			for _, c := range seed.Columns {
				cols[c.Name] = c
			}
			checks := map[string]string{}
			for _, c := range seed.CheckConstraints {
				checks[c.Name] = c.Expr
			}
			// The portable spelling each flavor's read yields for the DECLARED
			// text: introducers and identifier backticks stripped; MySQL keeps
			// a CHECK's outer parentheses, MariaDB prints none.
			wants := []exprTextWant{
				{"seed default", defaultText(cols["d"].Default), "concat('é😀','中')"},
				{"seed virtual generated", cols["g"].GeneratedExpr, "concat('ß😀',id)"},
				{"seed stored generated", cols["s"].GeneratedExpr, "concat('中😀',id)"},
			}
			if mariadb {
				wants = append(
					wants,
					exprTextWant{"seed check", checks["xr_chk"], "b <> 'é😀'"},
					exprTextWant{"seed genuine-'?' default", defaultText(cols["q"].Default), "concat('?','')"},
					exprTextWant{"seed inline check", checks["q"], "q <> concat('😀','')"},
					exprTextWant{"seed genuine-'????' default beside an emoji CHECK", defaultText(cols["w"].Default), "concat('????','')"},
					exprTextWant{"seed emoji inline check beside a '????' default", checks["w"], "w <> concat('😀','')"},
				)
			} else {
				wants = append(wants, exprTextWant{"seed check", checks["xr_chk"], "(b <> 'é😀')"})
				var parts []string
				for _, idx := range seed.Indexes {
					if idx.Name == "xr_ux" {
						for _, c := range idx.Columns {
							parts = append(parts, c.Expression)
						}
					}
				}
				wants = append(wants, exprTextWant{"seed functional key parts", strings.Join(parts, " | "), "concat(b,'中') | concat(b,'😀')"})
			}
			tbl, err := loadTableSchema(ctx, db, schema, "xr", sv.flavor)
			if err != nil {
				t.Fatalf("loadTableSchema: %v", err)
			}
			for _, c := range projectTableIR(tbl).Columns {
				switch c.Name {
				case "d":
					wants = append(wants, exprTextWant{"projection default", defaultText(c.Default), "concat('é😀','中')"})
				case "g":
					wants = append(wants, exprTextWant{"projection virtual generated", c.GeneratedExpr, "concat('ß😀',id)"})
				case "s":
					wants = append(wants, exprTextWant{"projection stored generated", c.GeneratedExpr, "concat('中😀',id)"})
				}
			}
			for _, w := range wants {
				if w.got != w.want {
					t.Errorf("%s = %q; want exactly %q", w.what, w.got, w.want)
				}
			}

			if mariadb {
				read := func() *mysqlTableFacts {
					t.Helper()
					facts, err := readTableFacts(ctx, db, sv.flavor, schema, "xr", nil)
					if err != nil {
						t.Fatal(err)
					}
					return facts[qualifiedName(schema, "xr")]
				}
				prior := read()
				exec("ALTER TABLE xr ALTER COLUMN d SET DEFAULT (concat('é😁','中'))")
				deltas := strings.Join(diffTableFacts(prior, read(), nil), "; ")
				if !strings.Contains(deltas, "😁") {
					t.Errorf("a default change at a lost character: deltas = %q; want it seen", deltas)
				}

				// F4: a view over the table reports the base column's lossy
				// default in information_schema.columns, and SHOW CREATE TABLE
				// on a view returns four columns — the all-tables baseline must
				// read base tables only.
				exec("CREATE VIEW xr_v AS SELECT * FROM xr")
				all, err := readTableFacts(ctx, db, sv.flavor, "", "", nil)
				if err != nil {
					t.Fatalf("all-tables baseline with a view over an emoji-default table: %v", err)
				}
				if _, isThere := all[qualifiedName(schema, "xr_v")]; isThere {
					t.Error("the baseline fingerprinted a VIEW")
				}
				if _, isThere := all[qualifiedName(schema, "xr")]; !isThere {
					t.Error("the baseline lost the base table")
				}
			}

			// A table dropped between the catalog read and its SHOW CREATE:
			// the door's all-tables read skips it (onGone); every other reader
			// keeps the failure.
			var gone []string
			p := []pendingExprText{{table: "bug288_gone", kind: exprSiteDefault, name: "d", catalog: def, set: func(string) {
				t.Error("a gone table's expression was set")
			}}}
			if err := recoverExprTexts(ctx, db, schema, sv.flavor, p, func(tb string) { gone = append(gone, tb) }); err != nil || len(gone) != 1 {
				t.Errorf("onGone: err = %v, gone = %v; want nil and [bug288_gone]", err, gone)
			}
			if err := recoverExprTexts(ctx, db, schema, sv.flavor, p, nil); err == nil {
				t.Error("with no onGone a missing table did not fail the read")
			}
		})
	}
}

// TestExprText_NonUTF8LiteralCharsets_MySQL pins the introducer-aware MySQL
// arm (the pre-tag review's F1–F3) on a real server. MySQL stores each
// string literal of an expression in its introducer's charset, and
// information_schema widens every stored byte regardless of which. A table
// created from a latin1 session stores `_latin1'<C3><A9>'` — the VALUE Ã©,
// which the first cut of the recovery read as é — and `_latin1'caf<E9>'`,
// which it refused. Both must come back as their latin1 value; a gbk
// literal, whose bytes C3A9 are 茅, must refuse; and a latin1 byte in the
// cp1252 range must refuse. The independent expected value is the server's
// own evaluation (INSERT naming only the key, read back through the row
// path).
func TestExprText_NonUTF8LiteralCharsets_MySQL(t *testing.T) {
	dsn, _ := startMySQL(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// createIn runs one CREATE on a connection whose session charset is cs,
	// so its literals are stored in that charset (raw bytes, as a client in
	// that charset would send them).
	createIn := func(cs, ddl string) {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		for _, q := range []string{"SET NAMES " + cs, ddl, "SET NAMES utf8mb4"} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	createIn("latin1", "CREATE TABLE l1 (id INT PRIMARY KEY, "+
		"a VARCHAR(20) DEFAULT ('\xc3\xa9'), e VARCHAR(20) DEFAULT ('caf\xe9'), "+
		"g VARCHAR(20) AS (concat('\xc3\xa9', id)), "+
		"CONSTRAINT l1_ck CHECK (a <> 'x\xc3\xa9')) DEFAULT CHARSET utf8mb4")
	if _, err := db.ExecContext(ctx, "INSERT INTO l1 (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var a, e, g string
	if err := db.QueryRowContext(ctx, "SELECT a, e, g FROM l1 WHERE id = 1").Scan(&a, &e, &g); err != nil {
		t.Fatal(err)
	}
	if a != "Ã©" || e != "café" || g != "Ã©1" {
		t.Fatalf("the server evaluates a=%q e=%q g=%q; the fixture expects Ã© / café / Ã©1 (fix the fixture)", a, e, g)
	}
	seed := readIRTableViaEngine(t, "mysql", dsn, "l1")
	cols := map[string]*ir.Column{}
	for _, c := range seed.Columns {
		cols[c.Name] = c
	}
	var chk string
	for _, c := range seed.CheckConstraints {
		if c.Name == "l1_ck" {
			chk = c.Expr
		}
	}
	for _, w := range []exprTextWant{
		{"latin1 C3A9 default", exprDefaultText(cols["a"].Default), "'Ã©'"},
		{"latin1 E9 default", exprDefaultText(cols["e"].Default), "'café'"},
		{"latin1 generated", cols["g"].GeneratedExpr, "concat('Ã©',id)"},
		{"latin1 check", chk, "(a <> 'xÃ©')"},
	} {
		if w.got != w.want {
			t.Errorf("%s = %q; want exactly %q", w.what, w.got, w.want)
		}
	}

	// A latin1 byte MySQL reads as cp1252 (0x80 is €) refuses.
	createIn("latin1", "CREATE TABLE l2 (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT ('\x80')) DEFAULT CHARSET utf8mb4")
	if _, err := readIRTableViaEngineErr("mysql", dsn, "l2"); err == nil || !strings.Contains(err.Error(), "cp1252") {
		t.Errorf("latin1 0x80 default: err = %v; want the cp1252 refusal", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE l2"); err != nil {
		t.Fatal(err)
	}

	// A gbk literal refuses: its bytes C3A9 are 茅, happen to be valid UTF-8,
	// and SHOW CREATE agrees with the widening undo — only the introducer
	// tells them apart (F2).
	createIn("gbk", "CREATE TABLE k1 (id INT PRIMARY KEY, a VARCHAR(20) DEFAULT ('\xc3\xa9')) DEFAULT CHARSET utf8mb4")
	var k string
	if _, err := db.ExecContext(ctx, "INSERT INTO k1 (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT a FROM k1 WHERE id = 1").Scan(&k); err != nil || k != "茅" {
		t.Fatalf("the server evaluates k1.a = %q (%v); the fixture expects 茅", k, err)
	}
	if _, err := readIRTableViaEngineErr("mysql", dsn, "k1"); err == nil || !strings.Contains(err.Error(), "character set gbk") {
		t.Errorf("gbk default: err = %v; want the named refusal", err)
	}

	// F6: the TARGET-side applier reads the catalog only for column types,
	// so a target table whose expression the recovery would refuse must not
	// stop a stream applying to it.
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	if cols, err := applier.(*ChangeApplier).colTypesFor(ctx, nil, "sluice_test", "k1"); err != nil || cols["a"] == nil {
		t.Errorf("target column types of a gbk-default table: cols = %v, err = %v; want the columns and no refusal", cols, err)
	}
}

func exprDefaultText(v ir.DefaultValue) string {
	if e, ok := v.(ir.DefaultExpression); ok {
		return e.Expr
	}
	return ""
}

// TestUnforwardedDoor_MySQLExpressionText_ValueLevel pins the MySQL arm of
// the unforwarded-schema-change door against its expression texts, on a
// real server. MySQL re-spells a CHECK's literals on ANY later ALTER TABLE —
// a latin1-session CHECK reads `_latin1'…'` before an unrelated ADD COLUMN
// and `_utf8mb4'…'` after (measured, for a non-ASCII AND a pure-ASCII
// literal) — so the door, comparing the raw widened text, halted the stream
// on a change nobody made and displayed the clause as mojibake an operator
// could copy onto the target. It now compares and displays the value-level
// text ([mysqlDoorExprText]). The three cells:
//
//	(a) latin1 CHECKs + an unrelated ADD COLUMN from a utf8mb4 session → no delta;
//	(b) a genuine CHECK and expression-DEFAULT change é → ê → deltas that show
//	    é and ê as themselves (exact substrings, no Ã©);
//	(c) a latin1 CHECK's VALUE changed → a delta.
//
// The independent expected value is the DDL each cell ran: whether it
// changed a value, and to what.
func TestUnforwardedDoor_MySQLExpressionText_ValueLevel(t *testing.T) {
	dsn, _ := startMySQL(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	inSession := func(cs string, stmts ...string) {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		// Restore utf8mb4 before the connection returns to the pool, or the
		// catalog reads below would be transcoded by a latin1 session.
		for _, q := range append(append([]string{"SET NAMES " + cs}, stmts...), "SET NAMES utf8mb4") {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	read := func(table string) *mysqlTableFacts {
		t.Helper()
		facts, err := readTableFacts(ctx, db, FlavorVanilla, "sluice_test", table, nil)
		if err != nil {
			t.Fatal(err)
		}
		return facts[qualifiedName("sluice_test", table)]
	}
	step := func(table, cs string, ddl ...string) string {
		t.Helper()
		prior := read(table)
		inSession(cs, ddl...)
		return strings.Join(diffTableFacts(prior, read(table), nil), "; ")
	}
	checkClause := func(name string) string {
		t.Helper()
		var c string
		if err := db.QueryRowContext(ctx, `SELECT check_clause FROM information_schema.check_constraints
			WHERE constraint_schema = 'sluice_test' AND constraint_name = ?`, name).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}

	// (a) latin1 CHECKs — non-ASCII and pure ASCII — survive an unrelated
	// ADD COLUMN from a utf8mb4 session.
	inSession("latin1", "CREATE TABLE da (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), "+
		"CONSTRAINT da_ck CHECK (a <> 'x\xc3\xa9'), CONSTRAINT da_ck2 CHECK (b <> 'plain')) DEFAULT CHARSET utf8mb4")
	before := checkClause("da_ck2")
	if deltas := step("da", "utf8mb4", "ALTER TABLE da ADD COLUMN z INT"); deltas != "" {
		t.Errorf("(a) an unrelated ADD COLUMN produced deltas %q; want none", deltas)
	}
	if after := checkClause("da_ck2"); before == after {
		t.Fatalf("(a) MySQL did not re-spell the CHECK (%q both times); the cell no longer exercises the premise — re-measure", before)
	}

	// (b) a genuine change é → ê, shown as itself.
	if _, err := db.ExecContext(ctx, "CREATE TABLE db2 (id INT PRIMARY KEY, a VARCHAR(20) DEFAULT (concat('xé','')), "+
		"CONSTRAINT db2_ck CHECK (a <> 'yé'))"); err != nil {
		t.Fatal(err)
	}
	deltas := step("db2", "utf8mb4",
		"ALTER TABLE db2 DROP CHECK db2_ck", "ALTER TABLE db2 ADD CONSTRAINT db2_ck CHECK (a <> 'yê')",
		"ALTER TABLE db2 ALTER COLUMN a SET DEFAULT (concat('xê',''))")
	for _, want := range []string{
		"CHECK ((a <> 'yé')) -> CHECK ((a <> 'yê'))",
		"SET DEFAULT (concat('xê','')) (was (concat('xé','')))",
	} {
		if !strings.Contains(deltas, want) {
			t.Errorf("(b) deltas %q do not show %q", deltas, want)
		}
	}
	if strings.Contains(deltas, "Ã") {
		t.Errorf("(b) deltas %q carry mojibake", deltas)
	}

	// (c) a latin1 CHECK's value changed is still a delta, shown as its
	// latin1 value.
	deltas = step("da", "latin1", "ALTER TABLE da DROP CHECK da_ck",
		"ALTER TABLE da ADD CONSTRAINT da_ck CHECK (a <> 'y\xc3\xa9')")
	if !strings.Contains(deltas, "(a <> 'xÃ©')") || !strings.Contains(deltas, "(a <> 'yÃ©')") {
		t.Errorf("(c) a latin1 CHECK value change: deltas %q; want xÃ© → yÃ© shown", deltas)
	}
}
