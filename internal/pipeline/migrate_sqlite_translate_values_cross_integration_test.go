//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// src==dst VALUE-ground-truthed pins for the SQLite→canonical expression
// translator (ADR-0133 follow-up). Unlike a string-equality unit test, these
// seed each construct as a STORED generated column in SQLite (so SQLite
// COMPUTES the value), migrate into a real PG/MySQL target (which RE-COMPUTES
// the translated expression), and assert the target's value EQUALS SQLite's —
// the Bug-74 discipline: prove equivalence on the real target, per operand
// shape, not just that a representative string emitted.
//
// The companion half asserts every EXCLUDED construct now REFUSES LOUDLY on
// the gencol AND CHECK path (a migrate that carries it aborts non-zero) rather
// than emitting a syntactically-valid-but-divergent body the target would
// silently accept.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// portableDDL is the BOTH-target table exercising the constructs the shrunk
// allowlist KEEPS, each as a STORED generated column. s1='café' (multibyte)
// pins length→PG LENGTH / MySQL CHAR_LENGTH (character count, 4). substr uses a
// literal start≥1 (the only portable shape).
const portableDDL = `CREATE TABLE portable (
	id INTEGER PRIMARY KEY,
	s1 TEXT, s2 TEXT, s3 TEXT, na INTEGER,
	g_concat   TEXT    GENERATED ALWAYS AS (s2 || '-' || s1) STORED,
	g_coalesce TEXT    GENERATED ALWAYS AS (coalesce(s3, 'def')) STORED,
	g_abs      INTEGER GENERATED ALWAYS AS (abs(na)) STORED,
	g_substr   TEXT    GENERATED ALWAYS AS (substr(s2, 2, 3)) STORED,
	g_length   INTEGER GENERATED ALWAYS AS (length(s1)) STORED
);
INSERT INTO portable (id, s1, s2, s3, na) VALUES (1, 'café', 'abcdef', NULL, -5);`

// portableCols is the ordered gencol list read back on each side (integers cast
// to text so the comparison is driver-neutral).
var portableSelect = struct{ sqlite, pg, mysql string }{
	sqlite: `SELECT g_concat, g_coalesce, CAST(g_abs AS TEXT), g_substr, CAST(g_length AS TEXT) FROM portable ORDER BY id`,
	pg:     `SELECT g_concat, g_coalesce, g_abs::text, g_substr, g_length::text FROM portable ORDER BY id`,
	mysql:  `SELECT g_concat, g_coalesce, CAST(g_abs AS CHAR), g_substr, CAST(g_length AS CHAR) FROM portable ORDER BY id`,
}

// TestMigrate_SQLiteTranslateValues_ToPostgres seeds the portable table PLUS a
// PG-only table (integer `/` incl. a negative, and cast AS numeric) and asserts
// every target-recomputed gencol equals SQLite's computed value.
func TestMigrate_SQLiteTranslateValues_ToPostgres(t *testing.T) {
	pgExtra := `CREATE TABLE pg_only (
		id INTEGER PRIMARY KEY, na INTEGER, nb INTEGER, x REAL,
		g_div  INTEGER GENERATED ALWAYS AS (na / nb) STORED,
		g_cast NUMERIC GENERATED ALWAYS AS (cast(x AS numeric)) STORED
	);
	INSERT INTO pg_only (id, na, nb, x) VALUES (1, -7, 2, 2.5), (2, 7, 2, 2.5);
	CREATE TABLE pg_bs (
		id INTEGER PRIMARY KEY, a TEXT,
		g_bs TEXT GENERATED ALWAYS AS (a || ' C:\temp\') STORED
	);
	INSERT INTO pg_bs (id, a) VALUES (1, 'x');`

	src := seedSQLiteText(t, portableDDL+"\n"+pgExtra)
	srcDB := openSQLite(t, src)
	defer func() { _ = srcDB.Close() }()

	_, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	migrateSQLite(t, "postgres", src, pgTarget)

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()

	// portable gencols: src == dst.
	assertRowsEqual(t, "portable",
		readRowsAsStrings(t, srcDB, portableSelect.sqlite, 5),
		readRowsAsStrings(t, pg, portableSelect.pg, 5))

	// PG `/` integer division (incl. negative → -3, matches SQLite toward-zero).
	assertRowsEqual(t, "pg_only.g_div",
		readRowsAsStrings(t, srcDB, `SELECT CAST(g_div AS TEXT) FROM pg_only ORDER BY id`, 1),
		readRowsAsStrings(t, pg, `SELECT g_div::text FROM pg_only ORDER BY id`, 1))

	// cast AS numeric (2.5 stays 2.5 — PG NUMERIC is faithful). Float compare.
	var srcCast, pgCast float64
	if err := srcDB.QueryRow(`SELECT g_cast FROM pg_only WHERE id = 1`).Scan(&srcCast); err != nil {
		t.Fatalf("sqlite g_cast: %v", err)
	}
	if err := pg.QueryRow(`SELECT g_cast FROM pg_only WHERE id = 1`).Scan(&pgCast); err != nil {
		t.Fatalf("pg g_cast: %v", err)
	}
	if srcCast != pgCast || pgCast != 2.5 {
		t.Errorf("g_cast src=%v dst=%v; want both 2.5", srcCast, pgCast)
	}

	// SEC-1 asymmetry, value-ground-truthed: a backslash-bearing string
	// literal (interior AND trailing backslash) is PORTABLE to PG — the
	// target-recomputed gencol must equal SQLite's byte-for-byte (`x C:\temp\`),
	// proving standard_conforming_strings treats \ literally on the real
	// target. The MySQL half REFUSES the same class — see
	// TestMigrate_SQLiteBackslashLiteral_RefusedToMySQL.
	assertRowsEqual(t, "pg_bs.g_bs",
		readRowsAsStrings(t, srcDB, `SELECT g_bs FROM pg_bs ORDER BY id`, 1),
		readRowsAsStrings(t, pg, `SELECT g_bs FROM pg_bs ORDER BY id`, 1))
}

// TestMigrate_SQLiteTranslateValues_ToMySQL seeds the portable table PLUS a
// MySQL-only table (min/max, incl. a NULL argument → NULL, proving LEAST/
// GREATEST propagate NULL like SQLite) and asserts src == dst.
func TestMigrate_SQLiteTranslateValues_ToMySQL(t *testing.T) {
	myExtra := `CREATE TABLE my_only (
		id INTEGER PRIMARY KEY, na INTEGER, nb INTEGER,
		g_min INTEGER GENERATED ALWAYS AS (min(na, nb)) STORED,
		g_max INTEGER GENERATED ALWAYS AS (max(na, nb)) STORED
	);
	INSERT INTO my_only (id, na, nb) VALUES (1, 3, 7), (2, 5, NULL);`

	src := seedSQLiteText(t, portableDDL+"\n"+myExtra)
	srcDB := openSQLite(t, src)
	defer func() { _ = srcDB.Close() }()

	_, myTarget, cleanup := startMySQL(t)
	defer cleanup()
	migrateSQLite(t, "mysql", src, myTarget)

	my, err := sql.Open("mysql", myTarget)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = my.Close() }()

	assertRowsEqual(t, "portable",
		readRowsAsStrings(t, srcDB, portableSelect.sqlite, 5),
		readRowsAsStrings(t, my, portableSelect.mysql, 5))

	// min/max incl. the NULL-propagation row (row 2 → <NULL>/<NULL>).
	assertRowsEqual(t, "my_only.min_max",
		readRowsAsStrings(t, srcDB, `SELECT CAST(g_min AS TEXT), CAST(g_max AS TEXT) FROM my_only ORDER BY id`, 2),
		readRowsAsStrings(t, my, `SELECT CAST(g_min AS CHAR), CAST(g_max AS CHAR) FROM my_only ORDER BY id`, 2))
}

// excludedGenColBodies are the shrunk-out constructs; each must REFUSE LOUDLY
// when carried as a generated column (BOTH targets).
var excludedGenColBodies = []string{
	"na % nb",            // % diverges on non-integers
	"cast(x AS integer)", // truncate vs round
	"substr(a, -3, 2)",   // negative start counts from end in SQLite only
	"round(x)",           // half-away vs half-even
	"upper(a)",           // ASCII vs Unicode case fold
}

// excludedCheckBodies are the same class carried as a CHECK; the seed row is
// chosen to SATISFY each so SQLite accepts the insert (na=7,nb=2,x=2.9,a='abcdef').
var excludedCheckBodies = []string{
	"na % nb = 1",
	"cast(x AS integer) = 2",
	"substr(a, -3, 2) = 'de'",
	"round(x) = 3",
	"upper(a) = 'ABCDEF'",
}

// TestMigrate_SQLiteNonPortable_RefusedToPostgres asserts every excluded
// construct REFUSES loudly on the gencol AND CHECK path into Postgres.
func TestMigrate_SQLiteNonPortable_RefusedToPostgres(t *testing.T) {
	runRefusalSuite(t, "postgres", startPostgres, excludedGenColBodies, excludedCheckBodies)
}

// TestMigrate_SQLiteNonPortable_RefusedToMySQL adds the MySQL-only exclusions
// (`a / b` decimal-division divergence and cast AS numeric rounding).
func TestMigrate_SQLiteNonPortable_RefusedToMySQL(t *testing.T) {
	genBodies := append([]string{"na / nb", "cast(x AS numeric)"}, excludedGenColBodies...)
	chkBodies := append([]string{"na / nb = 3", "cast(x AS numeric) = 2.9"}, excludedCheckBodies...)
	runRefusalSuite(t, "mysql", startMySQL, genBodies, chkBodies)
}

// TestMigrate_SQLiteBackslashLiteral_RefusedToMySQL pins SEC-1 end-to-end: a
// SQLite source whose generated column / CHECK / DEFAULT expression carries a
// backslash inside a string literal must abort LOUDLY on a MySQL target with
// an error NAMING the backslash — never emit (MySQL's default sql_mode would
// silently reinterpret the backslash as an escape, and a literal ending in \
// swallows its closing quote, shifting expression text into string position).
func TestMigrate_SQLiteBackslashLiteral_RefusedToMySQL(t *testing.T) {
	_, target, cleanup := startMySQL(t)
	defer cleanup()

	// Generated column: plain interior backslash.
	assertNamedRefusal(t, target, "gencol", "bs_gen", `a || 'C:\temp'`, "backslash", seedSQLiteGenCol)
	// CHECK: the trailing-\ quote-swallow shape (satisfied by the seed row).
	assertNamedRefusal(t, target, "CHECK", "bs_chk", `a <> 'x\'`, "backslash", seedSQLiteCheck)
	// DEFAULT expression: the verbatim-carry boundary that never routes
	// through the translator. The body must NOT begin with a quote: PRAGMA
	// table_info strips the outer parens, and the SQLite reader's
	// parseDefault classifies a quote-delimited dflt_value as a
	// DefaultLiteral, which takes the quoteSQLString path instead of the
	// expression carry this test pins.
	assertNamedRefusal(t, target, "DEFAULT", "bs_def", `(coalesce(NULL, 'C:\temp'))`, "backslash", seedSQLiteDefaultExpr)
}

// TestMigrate_SQLiteDoubleQuoted_RefusedToMySQL pins SEC-1 review gap 1
// end-to-end: a "…" double-quoted token in a gencol / CHECK — an identifier
// or the double-quoted-string misfeature to SQLite, but a STRING LITERAL
// with escape semantics to MySQL's default sql_mode — must abort LOUDLY on a
// MySQL target with an error naming the double-quoted token. (No DEFAULT
// cell: SQLite itself rejects a double-quoted token in DEFAULT position. The
// index cell WARN-skips rather than aborting and is pinned at unit level in
// the mysql package.)
func TestMigrate_SQLiteDoubleQuoted_RefusedToMySQL(t *testing.T) {
	_, target, cleanup := startMySQL(t)
	defer cleanup()

	// Generated column: misfeature string with interior backslash.
	assertNamedRefusal(t, target, "gencol", "dq_gen", `a || "C:\temp"`, "double-quoted", seedSQLiteGenCol)
	// CHECK: the trailing-backslash quote-swallow shape, one quote flavor
	// over (satisfied by the seed row: 'abcdef' <> the string "x\").
	assertNamedRefusal(t, target, "CHECK", "dq_chk", `a <> "x\"`, "double-quoted", seedSQLiteCheck)
}

// assertNamedRefusal runs a SQLite→MySQL migrate that must abort with an
// error containing wantToken. Each migrate gets an explicit MigrationID: the
// auto-derived id hashes DSN host info, which is identical across sqlite
// temp-file sources, so a second run against a shared target would otherwise
// fail on the partial-migration guard instead of the named refusal.
func assertNamedRefusal(t *testing.T, target, kind, table, body, wantToken string, seed func(*testing.T, string, string) string) {
	t.Helper()
	src := seed(t, table, body)
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	targetEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	mig := &Migrator{Source: sqliteEng, Target: targetEng, SourceDSN: src, TargetDSN: target, MigrationID: table}
	err := mig.Run(ctx2min(t))
	if err == nil {
		t.Errorf("%s %q migrated to mysql without error; want a LOUD refusal naming %q", kind, body, wantToken)
		return
	}
	if !strings.Contains(err.Error(), wantToken) {
		t.Errorf("%s refusal = %v; want the error to NAME %q", kind, err, wantToken)
	}
}

// TestMigrate_SQLiteBackslashDefaultLiteral_ToMySQL ground-truths SEC-1
// review gap 2 on the DefaultLiteral route: an UNPARENTHESIZED SQLite string
// default takes the DefaultLiteral path (the reader's parseDefault strips
// the quotes to the raw value — probed: PRAGMA reports `'C:\temp'`, decoded
// to one backslash), and the MySQL writer's quoteSQLString must re-escape it
// so the STORED default round-trips byte-identically — pre-fix `'C:\temp'`
// emitted undoubled and MySQL silently decoded it to "C:<TAB>emp", and the
// trailing-\ shape swallowed the closing quote. Both columns ride a varchar
// type-override: SQLite's affinity mapping lands declared text at ir.Text,
// and MySQL forbids DEFAULT on TEXT (the writer warn-suppresses it), so
// without the override there is no target DEFAULT to pin.
func TestMigrate_SQLiteBackslashDefaultLiteral_ToMySQL(t *testing.T) {
	_, target, cleanup := startMySQL(t)
	defer cleanup()

	src := seedSQLiteText(t, `CREATE TABLE bs_lit (
		id INTEGER PRIMARY KEY,
		d TEXT NOT NULL DEFAULT 'C:\temp',
		w TEXT NOT NULL DEFAULT 'trail\'
	);
	INSERT INTO bs_lit (id) VALUES (1);`)
	srcDB := openSQLite(t, src)
	defer func() { _ = srcDB.Close() }()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	targetEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	mig := &Migrator{
		Source: sqliteEng, Target: targetEng, SourceDSN: src, TargetDSN: target,
		MigrationID: "bs_lit",
		Mappings: []config.Mapping{
			{Table: "bs_lit", Column: "d", TargetType: "varchar"},
			{Table: "bs_lit", Column: "w", TargetType: "varchar"},
		},
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→MySQL backslash DefaultLiteral): %v", err)
	}

	my, err := sql.Open("mysql", target)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = my.Close() }()

	// The copied row (SQLite applied the defaults at seed time) and a
	// TARGET-side DEFAULT-applied row must both equal SQLite's values.
	srcRows := readRowsAsStrings(t, srcDB, `SELECT d, w FROM bs_lit WHERE id = 1`, 2)
	assertRowsEqual(t, "bs_lit copied row",
		srcRows,
		readRowsAsStrings(t, my, `SELECT d, w FROM bs_lit WHERE id = 1`, 2))
	if _, err := my.ExecContext(ctx2min(t), `INSERT INTO bs_lit (id) VALUES (2)`); err != nil {
		t.Fatalf("insert DEFAULT-applied row on target: %v", err)
	}
	assertRowsEqual(t, "bs_lit target DEFAULT-applied row",
		srcRows,
		readRowsAsStrings(t, my, `SELECT d, w FROM bs_lit WHERE id = 2`, 2))
	if srcRows[0][0] != `C:\temp` || srcRows[0][1] != `trail\` {
		t.Errorf("source default values = %v; want [C:\\temp trail\\] (seed anchor)", srcRows[0])
	}

	// Stored-metadata anchor: information_schema reports the DECODED value,
	// so the target's COLUMN_DEFAULT must carry exactly one backslash.
	var dDflt, wDflt sql.NullString
	if err := my.QueryRowContext(ctx2min(t), `SELECT
		(SELECT COLUMN_DEFAULT FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'bs_lit' AND column_name = 'd'),
		(SELECT COLUMN_DEFAULT FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'bs_lit' AND column_name = 'w')`).Scan(&dDflt, &wDflt); err != nil {
		t.Fatalf("read target COLUMN_DEFAULTs: %v", err)
	}
	if dDflt.String != `C:\temp` || wDflt.String != `trail\` {
		t.Errorf("target COLUMN_DEFAULTs = (%q, %q); want (C:\\temp, trail\\) — the stored default must be byte-identical", dDflt.String, wDflt.String)
	}
}

// runRefusalSuite reuses ONE target container and attempts a migrate per
// excluded body (unique table each), asserting every one aborts non-zero.
func runRefusalSuite(t *testing.T, targetName string, start func(*testing.T) (string, string, func()), genBodies, chkBodies []string) {
	_, target, cleanup := start(t)
	defer cleanup()

	for i, body := range genBodies {
		src := seedSQLiteGenCol(t, fmt.Sprintf("gr_%d", i), body)
		if err := runSQLiteMigrate(t, targetName, src, target); err == nil {
			t.Errorf("gencol %q migrated to %s without error; want a LOUD refusal", body, targetName)
		}
	}
	for i, body := range chkBodies {
		src := seedSQLiteCheck(t, fmt.Sprintf("cr_%d", i), body)
		if err := runSQLiteMigrate(t, targetName, src, target); err == nil {
			t.Errorf("CHECK %q migrated to %s without error; want a LOUD refusal", body, targetName)
		}
	}
}

// ---- helpers ----

func openSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", path, err)
	}
	return db
}

// seedSQLiteText writes a multi-statement SQLite script to a temp .db file.
func seedSQLiteText(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "translate.db")
	db := openSQLite(t, path)
	defer func() { _ = db.Close() }()
	for _, s := range splitSQLStatements(script) {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// seedSQLiteGenCol writes a one-table source whose sole generated column uses
// body. Generic input columns cover every excluded body's references.
func seedSQLiteGenCol(t *testing.T, table, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), table+".db")
	db := openSQLite(t, path)
	defer func() { _ = db.Close() }()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE %s (
			id INTEGER PRIMARY KEY, a TEXT, x REAL, na INTEGER, nb INTEGER,
			g TEXT GENERATED ALWAYS AS (%s) STORED)`, table, body),
		fmt.Sprintf(`INSERT INTO %s (id, a, x, na, nb) VALUES (1, 'abcdef', 2.9, 7, 2)`, table),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed gencol exec: %v", err)
		}
	}
	return path
}

// seedSQLiteCheck writes a one-table source carrying a CHECK using body. The
// seed row satisfies the check so SQLite accepts the insert.
func seedSQLiteCheck(t *testing.T, table, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), table+".db")
	db := openSQLite(t, path)
	defer func() { _ = db.Close() }()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE %s (
			id INTEGER PRIMARY KEY, a TEXT, x REAL, na INTEGER, nb INTEGER,
			CONSTRAINT ck CHECK (%s))`, table, body),
		fmt.Sprintf(`INSERT INTO %s (id, a, x, na, nb) VALUES (1, 'abcdef', 2.9, 7, 2)`, table),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed check exec: %v", err)
		}
	}
	return path
}

// seedSQLiteDefaultExpr writes a one-table source whose sole non-key column
// carries an EXPRESSION default (parenthesised, so the SQLite reader
// classifies it DefaultExpression dialect "sqlite", not DefaultLiteral).
func seedSQLiteDefaultExpr(t *testing.T, table, expr string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), table+".db")
	db := openSQLite(t, path)
	defer func() { _ = db.Close() }()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE %s (id INTEGER PRIMARY KEY, d VARCHAR(50) DEFAULT %s)`, table, expr),
		fmt.Sprintf(`INSERT INTO %s (id) VALUES (1)`, table),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed default exec: %v", err)
		}
	}
	return path
}

// migrateSQLite runs a migrate and fails the test on error (for the value pins).
func migrateSQLite(t *testing.T, targetName, src, target string) {
	t.Helper()
	if err := runSQLiteMigrate(t, targetName, src, target); err != nil {
		t.Fatalf("Migrator.Run (SQLite→%s): %v", targetName, err)
	}
}

// runSQLiteMigrate runs a migrate and RETURNS its error (for the refusal pins).
func runSQLiteMigrate(t *testing.T, targetName, src, target string) error {
	t.Helper()
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	targetEng, ok := engines.Get(targetName)
	if !ok {
		t.Fatalf("%s engine not registered", targetName)
	}
	mig := &Migrator{Source: sqliteEng, Target: targetEng, SourceDSN: src, TargetDSN: target}
	return mig.Run(ctx2min(t))
}

// readRowsAsStrings reads every row of a query into []string, rendering NULL as
// the sentinel "<NULL>" so a NULL result compares equal across engines.
func readRowsAsStrings(t *testing.T, db *sql.DB, query string, ncols int) [][]string {
	t.Helper()
	rows, err := db.QueryContext(ctx2min(t), query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	var out [][]string
	for rows.Next() {
		cells := make([]sql.NullString, ncols)
		ptrs := make([]any, ncols)
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		row := make([]string, ncols)
		for i, c := range cells {
			if c.Valid {
				row[i] = c.String
			} else {
				row[i] = "<NULL>"
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err %q: %v", query, err)
	}
	return out
}

// assertRowsEqual compares two string matrices cell-by-cell (src vs dst).
func assertRowsEqual(t *testing.T, label string, src, dst [][]string) {
	t.Helper()
	if len(src) != len(dst) {
		t.Fatalf("%s: src has %d rows, dst has %d", label, len(src), len(dst))
	}
	for r := range src {
		if len(src[r]) != len(dst[r]) {
			t.Fatalf("%s row %d: src %d cols, dst %d cols", label, r, len(src[r]), len(dst[r]))
		}
		for c := range src[r] {
			if src[r][c] != dst[r][c] {
				t.Errorf("%s row %d col %d: src=%q dst=%q (target must recompute SQLite's value)",
					label, r, c, src[r][c], dst[r][c])
			}
		}
	}
}

// splitSQLStatements splits a `;`-terminated multi-statement script. The seed
// scripts here contain no `;` inside string literals, so a plain split is safe.
func splitSQLStatements(script string) []string {
	var out []string
	for _, s := range strings.Split(script, ";") {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
