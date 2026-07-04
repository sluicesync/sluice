//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SEC-1b reverse-direction ground truth: a MySQL SOURCE carrying
// backslash-bearing DDL string text (column DEFAULT literals incl. the
// trailing-\ shape, ENUM/SET labels, column/table COMMENTs) migrated to a PG
// and a SQLite TARGET must land those bytes VERBATIM — never silently
// re-escaped, decoded, or corrupted.
//
// Phase-A finding (audited CLEAN): the MySQL reader hands the writer RAW value
// bytes (COLUMN_DEFAULT / COLUMN_COMMENT / TABLE_COMMENT arrive decoded from
// information_schema; parseEnumOrSet decodes the escaped ENUM/SET COLUMN_TYPE —
// both ground-truthed on real MySQL 8.0). The PG and SQLite writers'
// quoteSQLString then quote with interior-single-quote doubling ONLY, which is
// exactly right for THEIR dialects: PG pins standard_conforming_strings=on (so
// a backslash is an ordinary literal character) and SQLite has no backslash
// escapes at all — so neither target needs (nor may do) backslash doubling.
// The direction is therefore correct by construction. These pins ground-truth
// it on the real targets so a future writer/reader change can't regress it
// silently.

package pipeline

import (
	"database/sql"
	"path/filepath"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// TestMigrate_MySQLToPostgres_BackslashDDLReverse pins the MySQL→PG label/
// default/comment sinks. Value bytes intended (default sql_mode → doubled
// backslash in the DDL literal is ONE stored backslash):
//
//	d_plain    = C:\temp        d_trail    = trail\
//	d_doubled  = a\\b           d_quoteadj = q\'a
//	role (ENUM) label a\b,  x\y   default a\b
//	tags (SET) label p\q,  r\     default p\q
//	column comment path\note,  table comment tbl\c
func TestMigrate_MySQLToPostgres_BackslashDDLReverse(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE bs_rev (
			id         INT NOT NULL AUTO_INCREMENT,
			d_plain    VARCHAR(32) NOT NULL DEFAULT 'C:\\temp' COMMENT 'path\\note',
			d_trail    VARCHAR(32) NOT NULL DEFAULT 'trail\\',
			d_doubled  VARCHAR(32) NOT NULL DEFAULT 'a\\\\b',
			d_quoteadj VARCHAR(32) NOT NULL DEFAULT 'q\\''a',
			role       ENUM('a\\b','x\\y') NOT NULL DEFAULT 'a\\b',
			tags       SET('p\\q','r\\')   NOT NULL DEFAULT 'p\\q',
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='tbl\\c';
		INSERT INTO bs_rev (id) VALUES (1);
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx := ctx2min(t)
	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (MySQL→PG backslash DDL): %v", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pg.Close() }()

	// The COPIED row (explicit values) carries the source bytes.
	assertPGRow := func(label string, id int) {
		t.Helper()
		var dp, dt, dd, dq, role string
		if err := pg.QueryRowContext(
			ctx,
			`SELECT d_plain, d_trail, d_doubled, d_quoteadj, role FROM bs_rev WHERE id = $1`, id,
		).Scan(&dp, &dt, &dd, &dq, &role); err != nil {
			t.Fatalf("%s: read row %d: %v", label, id, err)
		}
		if dp != `C:\temp` || dt != `trail\` || dd != `a\\b` || dq != `q\'a` || role != `a\b` {
			t.Errorf("%s row %d = (d_plain=%q d_trail=%q d_doubled=%q d_quoteadj=%q role=%q); want (C:\\temp, trail\\, a\\\\b, q\\'a, a\\b)",
				label, id, dp, dt, dd, dq, role)
		}
	}
	assertPGRow("copied row", 1)

	// The emitted DEFAULT clauses themselves must be faithful: a
	// DEFAULT-applied row (no explicit values) recovers the same bytes.
	if _, err := pg.ExecContext(ctx, `INSERT INTO bs_rev (id) VALUES (2)`); err != nil {
		t.Fatalf("insert DEFAULT-applied row on pg: %v", err)
	}
	assertPGRow("DEFAULT-applied row", 2)

	// ENUM LABEL sink: the PG enum type carries the exact backslash-bearing
	// labels (CREATE TYPE ... AS ENUM ('a\b','x\y')). Read them from pg_enum.
	labels := map[string]bool{}
	rows, err := pg.QueryContext(ctx, `
		SELECT e.enumlabel
		FROM pg_enum e
		JOIN pg_type t ON t.oid = e.enumtypid
		WHERE t.typname LIKE 'bs_rev_role%'`)
	if err != nil {
		t.Fatalf("read pg_enum labels: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			t.Fatalf("scan enum label: %v", err)
		}
		labels[l] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("enum label rows: %v", err)
	}
	if !labels[`a\b`] || !labels[`x\y`] {
		t.Errorf("PG enum labels = %v; want a\\b and x\\y present (backslash-bearing labels must round-trip)", labels)
	}

	// SET-default sink: the DEFAULT-applied row's tags is the TEXT[] literal
	// setDefaultToArrayLiteral emitted via quoteSQLString ({p\q}). Read it as
	// text; the member bytes must be faithful.
	var tags string
	if err := pg.QueryRowContext(ctx, `SELECT array_to_string(tags, ',') FROM bs_rev WHERE id = 2`).Scan(&tags); err != nil {
		t.Fatalf("read tags: %v", err)
	}
	if tags != `p\q` {
		t.Errorf("PG tags = %q; want p\\q (SET member backslash must round-trip)", tags)
	}

	// COMMENT sink: column + table comments carry the backslash bytes.
	var colComment, tblComment string
	if err := pg.QueryRowContext(
		ctx,
		`SELECT col_description('bs_rev'::regclass, attnum) FROM pg_attribute
		 WHERE attrelid = 'bs_rev'::regclass AND attname = 'd_plain'`,
	).Scan(&colComment); err != nil {
		t.Fatalf("read column comment: %v", err)
	}
	if colComment != `path\note` {
		t.Errorf("PG column comment = %q; want path\\note", colComment)
	}
	if err := pg.QueryRowContext(ctx,
		`SELECT obj_description('bs_rev'::regclass)`).Scan(&tblComment); err != nil {
		t.Fatalf("read table comment: %v", err)
	}
	if tblComment != `tbl\c` {
		t.Errorf("PG table comment = %q; want tbl\\c", tblComment)
	}
}

// TestMigrate_MySQLToSQLite_BackslashDDLReverse pins the MySQL→SQLite
// DefaultLiteral / SET-default sinks. SQLite drops comments and maps
// ENUM/SET → TEXT, so the DDL sink that survives is the DEFAULT clause; the
// enum/set LABELS ride the value path (asserted via the copied row).
func TestMigrate_MySQLToSQLite_BackslashDDLReverse(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const seedDDL = `
		CREATE TABLE bs_rev (
			id         INTEGER NOT NULL AUTO_INCREMENT,
			d_plain    VARCHAR(32) NOT NULL DEFAULT 'C:\\temp',
			d_trail    VARCHAR(32) NOT NULL DEFAULT 'trail\\',
			d_doubled  VARCHAR(32) NOT NULL DEFAULT 'a\\\\b',
			d_quoteadj VARCHAR(32) NOT NULL DEFAULT 'q\\''a',
			role       ENUM('a\\b','x\\y') NOT NULL DEFAULT 'a\\b',
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO bs_rev (id) VALUES (1);
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	ctx := ctx2min(t)
	dst := filepath.Join(t.TempDir(), "bs_rev.db")
	mig := &Migrator{Source: mysqlEng, Target: sqliteEng, SourceDSN: mysqlSource, TargetDSN: dst}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (MySQL→SQLite backslash DDL): %v", err)
	}

	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = db.Close() }()

	assertRow := func(label string, id int) {
		t.Helper()
		var dp, dt, dd, dq, role string
		if err := db.QueryRowContext(
			ctx,
			`SELECT d_plain, d_trail, d_doubled, d_quoteadj, role FROM bs_rev WHERE id = ?`, id,
		).Scan(&dp, &dt, &dd, &dq, &role); err != nil {
			t.Fatalf("%s: read row %d: %v", label, id, err)
		}
		if dp != `C:\temp` || dt != `trail\` || dd != `a\\b` || dq != `q\'a` || role != `a\b` {
			t.Errorf("%s row %d = (d_plain=%q d_trail=%q d_doubled=%q d_quoteadj=%q role=%q); want (C:\\temp, trail\\, a\\\\b, q\\'a, a\\b)",
				label, id, dp, dt, dd, dq, role)
		}
	}
	// Copied row (value path).
	assertRow("copied row", 1)
	// DEFAULT-applied row (DDL DEFAULT-clause path — role has a TEXT default too).
	if _, err := db.ExecContext(ctx, `INSERT INTO bs_rev (id) VALUES (2)`); err != nil {
		t.Fatalf("insert DEFAULT-applied row on sqlite: %v", err)
	}
	assertRow("DEFAULT-applied row", 2)
}
