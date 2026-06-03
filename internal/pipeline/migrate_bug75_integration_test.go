//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for Bug 75 (SILENT bit/varbit corruption):
//
// Phase-A ground truth (instrumented vs real PG + MySQL):
//   - PG `bit`/`varbit` under pgx stdlib mode comes back as the
//     canonical '0'/'1' TEXT ("10101010"). The old reader did
//     []byte(text) → the ASCII bytes of the digits; the writer's
//     bitBytesMySQLToPG then kept only the trailing byte, so every
//     distinct value collapsed to its last digit's ASCII (10101010 &
//     11110000 → 0x30 = "00110000"). Silent, exit 0, irreversible.
//   - MySQL BIT(N) round-trips faithfully from ceil(N/8) big-endian
//     bytes; the digit string is loud-rejected by INSERT and silently
//     wrong via LOAD DATA — hence the bit-string IR contract plus the
//     LOAD DATA CONV(...,2,10) SET expression.
//
// Fix: ir.Bit is carried as the engine-neutral '0'/'1' bit-string
// (internal/ir/bit.go). These pins ground-truth EXACT src==dst in all
// three directions a bit column can travel, including NULL preserved
// as NULL and DISTINCT values staying DISTINCT (the irreversibility
// signature).

package pipeline

import (
	"database/sql"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_Bug75BitVarbit pins faithful PG→PG
// bit/varbit round-trip. The ::text oracle renders the bit string;
// distinct values must stay distinct (pre-fix they collapsed).
func TestMigrate_PostgresToPostgres_Bug75BitVarbit(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE bt (id int PRIMARY KEY, b8 bit(8), b9 bit(9), vb bit varying(16));
		INSERT INTO bt VALUES
		  (1, B'10101010', B'101010101', B'1100'),
		  (2, B'11110000', B'000011110', B'1'),
		  (3, NULL, NULL, NULL);
	`)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→PG bit/varbit must round-trip): %v", err)
	}

	dstDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	type row struct {
		id         int
		b8, b9, vb sql.NullString
	}
	want := []row{
		{1, ns("10101010"), ns("101010101"), ns("1100")},
		{2, ns("11110000"), ns("000011110"), ns("1")},
		{3, sql.NullString{}, sql.NullString{}, sql.NullString{}},
	}
	rows, err := dstDB.Query(`SELECT id, b8::text, b9::text, vb::text FROM bt ORDER BY id`)
	if err != nil {
		t.Fatalf("query pg target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.b8, &r.b9, &r.vb); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("row[%d]: got %+v; want %+v (bit/varbit must be faithful, distinct, NULL→NULL)", i, g, want[i])
		}
	}
}

// TestMigrate_PostgresToMySQL_Bug75BitVarbit pins the cross-engine
// direction (the LOAD DATA path). MySQL renders BIT via BIN(col+0);
// distinct PG values must remain distinct on MySQL (pre-fix all
// collapsed to 11111111).
func TestMigrate_PostgresToMySQL_Bug75BitVarbit(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE bt (id int PRIMARY KEY, b8 bit(8), vb bit varying(16));
		INSERT INTO bt VALUES
		  (1, B'10101010', B'1100'),
		  (2, B'11110000', B'1'),
		  (3, NULL, NULL);
	`)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→MySQL bit/varbit): %v", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	type row struct {
		id     int
		b8, vb sql.NullString
	}
	// BIN() drops leading zeros; values chosen so the rendering is
	// unambiguous and, crucially, DISTINCT per source row.
	want := []row{
		{1, ns("10101010"), ns("1100")},
		{2, ns("11110000"), ns("1")},
		{3, sql.NullString{}, sql.NullString{}},
	}
	rows, err := mysqlDB.Query(`SELECT id,
		CASE WHEN b8 IS NULL THEN NULL ELSE BIN(b8+0) END,
		CASE WHEN vb IS NULL THEN NULL ELSE BIN(vb+0) END
		FROM bt ORDER BY id`)
	if err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.b8, &r.vb); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("row[%d]: got %+v; want %+v (PG→MySQL bit must be faithful + distinct, NULL→NULL)", i, g, want[i])
		}
	}
}

// TestMigrate_MySQLToPostgres_Bug75Bit pins the reverse direction
// (MySQL BIT source → PG bit target) — the Bug 62 surface must still
// round-trip under the new bit-string IR contract.
func TestMigrate_MySQLToPostgres_Bug75Bit(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE bt (
		  id int NOT NULL PRIMARY KEY,
		  b8 bit(8),
		  b9 bit(9),
		  vb bit(16)
		) ENGINE=InnoDB;
		INSERT INTO bt VALUES
		  (1, b'10101010', b'101010101', b'1100'),
		  (2, b'11110000', b'000011110', b'1'),
		  (3, NULL, NULL, NULL);
	`)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (MySQL→PG bit, Bug 62 surface): %v", err)
	}

	pgDB, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	type row struct {
		id         int
		b8, b9, vb sql.NullString
	}
	// MySQL BIT(16) for value 1100b = 12 stores as 16 bits → PG bit(16)
	// renders zero-padded to its declared width.
	want := []row{
		{1, ns("10101010"), ns("101010101"), ns("0000000000001100")},
		{2, ns("11110000"), ns("000011110"), ns("0000000000000001")},
		{3, sql.NullString{}, sql.NullString{}, sql.NullString{}},
	}
	rows, err := pgDB.Query(`SELECT id, b8::text, b9::text, vb::text FROM bt ORDER BY id`)
	if err != nil {
		t.Fatalf("query pg target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.b8, &r.b9, &r.vb); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("row[%d]: got %+v; want %+v (MySQL→PG bit must round-trip; Bug 62 no-regression)", i, g, want[i])
		}
	}
}

func ns(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
