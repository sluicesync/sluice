//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration regressions for the v0.65.1 DDL-emit correctness fixes:
//
//   - Bug 61: the PG schema-reader's cast-stripper truncated a
//     multi-argument function-call DEFAULT at an inner per-arg
//     `::text` cast (PG injects those into information_schema's
//     column_default), corrupting the IR → SQLSTATE 42601 on the
//     target. Same-engine PG → PG; blast radius is CORE functions
//     (left/round/coalesce/digest) plus pgcrypto's canonical
//     crypt(…, gen_salt(…)) shape.
//
//   - Bug 62: MySQL BIT(N>1) was mis-mapped to Varbinary and its
//     `b'…'` default was decimal-stringified ('165'). The fix adds
//     ir.Bit so BIT(N) round-trips MySQL ↔ PG with a real bit-literal
//     default. Exercised on BOTH MySQL → MySQL (Error 1067 pre-fix)
//     and MySQL → PG (silent default corruption pre-fix). This is the
//     coverage gap that let Bug 62 through d89debc's bit(1)-only test.
//
// Both use the stock mysql:8.0 / postgres:16 images CI pre-pulls; the
// pgcrypto case uses the contrib bundle already present in postgres:16
// (same as the uuid-ossp / pgcrypto Tier-3 suite — no special image).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_Bug61_PG_MultiArgFunctionDefaults pins Bug 61: a PG → PG
// migration of columns whose DEFAULT is a function call with ≥2
// comma-separated args. Pre-fix the cast-stripper truncated the
// expression at the first inner `::text` cast and the next column line
// was consumed → CREATE TABLE failed with SQLSTATE 42601. Post-fix the
// whole expression survives the read boundary and the migrate
// succeeds.
//
// Cases cover the BUG-CATALOG's required shapes: core left/round/
// coalesce, a string-literal-with-comma coalesce, and the nested
// pgcrypto crypt(…, gen_salt('bf')) headline shape.
func TestMigrate_Bug61_PG_MultiArgFunctionDefaults(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresWithQuotedExtension(t, "pgcrypto", true)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE multiarg_defaults (
			id        int PRIMARY KEY,
			prefix    text NOT NULL DEFAULT left('hello', 3),
			rounded   numeric(6,2) NOT NULL DEFAULT round(1.2345, 2),
			coalesced text NOT NULL DEFAULT coalesce(NULL, 'fallback'),
			sep       text NOT NULL DEFAULT coalesce(NULL, ', '),
			token     text NOT NULL DEFAULT crypt('seedpw', gen_salt('bf')),
			tail      int NOT NULL
		);
		INSERT INTO multiarg_defaults (id, tail) VALUES (1, 10), (2, 20), (3, 30);
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
		// pgcrypto is enabled on both sides; the flag opts the crypt()/
		// gen_salt() default through the ADR-0032/0044 passthrough.
		EnabledPGExtensions: []string{"pgcrypto"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run = %v; want SUCCESS (Bug 61: multi-arg function defaults must not truncate)", err)
	}

	tgt, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// All 3 rows copied.
	var n int
	if err := tgt.QueryRowContext(ctx, "SELECT COUNT(*) FROM multiarg_defaults").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Errorf("target row count = %d; want 3", n)
	}

	// The DEFAULTs survived intact: a bare INSERT (only the NOT NULL
	// non-defaulted columns supplied) must apply every default cleanly.
	if _, err := tgt.ExecContext(ctx,
		"INSERT INTO multiarg_defaults (id, tail) VALUES (99, 1)"); err != nil {
		t.Fatalf("insert relying on defaults = %v; want success (defaults must be valid SQL)", err)
	}
	var prefix, coalesced, sep string
	var rounded float64
	if err := tgt.QueryRowContext(ctx,
		"SELECT prefix, rounded, coalesced, sep FROM multiarg_defaults WHERE id=99").
		Scan(&prefix, &rounded, &coalesced, &sep); err != nil {
		t.Fatalf("read defaulted row: %v", err)
	}
	if prefix != "hel" {
		t.Errorf("prefix default = %q; want %q (left('hello',3))", prefix, "hel")
	}
	if rounded != 1.23 {
		t.Errorf("rounded default = %v; want 1.23 (round(1.2345,2))", rounded)
	}
	if coalesced != "fallback" {
		t.Errorf("coalesced default = %q; want %q", coalesced, "fallback")
	}
	if sep != ", " {
		t.Errorf("sep default = %q; want %q (string literal with comma must not split)", sep, ", ")
	}
}

// TestMigrate_Bug62_MySQLToMySQL_BitDefaults pins Bug 62 same-engine:
// BIT(N>1) must land as BIT(N) (not VARBINARY(1)) with a real bit
// literal default (not the decimal string '165'). Pre-fix this failed
// at `create tables` with MySQL Error 1067. bit(1) is included to
// confirm d89debc's #4 boolean path is NOT regressed.
func TestMigrate_Bug62_MySQLToMySQL_BitDefaults(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE bit_defaults (
			id     INT PRIMARY KEY,
			flags  BIT(8)  NOT NULL DEFAULT b'10100101',
			onebit BIT(1)  NOT NULL DEFAULT b'1',
			wide   BIT(16) NOT NULL DEFAULT b'1111000011110000'
		);
		INSERT INTO bit_defaults (id) VALUES (1), (2);
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run = %v; want SUCCESS (Bug 62: BIT(N) must not mis-map / Error 1067)", err)
	}

	tgt, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// Column types preserved as BIT(N), not VARBINARY.
	typeCases := []struct {
		col, want string
	}{
		{"flags", "bit(8)"},
		{"onebit", "tinyint(1)"}, // bit(1) → ir.Boolean → TINYINT(1) (catalog #4, unchanged)
		{"wide", "bit(16)"},
	}
	for _, tc := range typeCases {
		var colType string
		if err := tgt.QueryRowContext(ctx, `
			SELECT COLUMN_TYPE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'bit_defaults'
			  AND COLUMN_NAME = ?`, tc.col).Scan(&colType); err != nil {
			t.Fatalf("read column type %q: %v", tc.col, err)
		}
		if colType != tc.want {
			t.Errorf("column %q type = %q; want %q", tc.col, colType, tc.want)
		}
	}

	// 2 rows copied; the bit VALUES round-trip (must not regress).
	var got8, got16 uint64
	if err := tgt.QueryRowContext(ctx,
		"SELECT flags+0, wide+0 FROM bit_defaults WHERE id=1").Scan(&got8, &got16); err != nil {
		t.Fatalf("read bit values: %v", err)
	}
	if got8 != 0xA5 {
		t.Errorf("flags value = %d; want %d (0xA5)", got8, 0xA5)
	}
	if got16 != 0xF0F0 {
		t.Errorf("wide value = %d; want %d (0xF0F0)", got16, 0xF0F0)
	}

	// The DEFAULT survived as a real bit literal: a bare INSERT must
	// apply b'10100101' = 0xA5, not the decimal string '165'.
	if _, err := tgt.ExecContext(ctx, "INSERT INTO bit_defaults (id) VALUES (5)"); err != nil {
		t.Fatalf("insert relying on bit default = %v; want success (Bug 62: default must be a valid bit literal)", err)
	}
	var dflt8, dflt16 uint64
	var dflt1 int
	if err := tgt.QueryRowContext(ctx,
		"SELECT flags+0, onebit+0, wide+0 FROM bit_defaults WHERE id=5").
		Scan(&dflt8, &dflt1, &dflt16); err != nil {
		t.Fatalf("read defaulted bit row: %v", err)
	}
	if dflt8 != 0xA5 {
		t.Errorf("flags DEFAULT = %d; want %d (b'10100101'; NOT decimal-string corrupted)", dflt8, 0xA5)
	}
	if dflt1 != 1 {
		t.Errorf("onebit DEFAULT = %d; want 1 (bit(1)→bool path, catalog #4 not regressed)", dflt1)
	}
	if dflt16 != 0xF0F0 {
		t.Errorf("wide DEFAULT = %d; want %d (b'1111000011110000')", dflt16, 0xF0F0)
	}
}

// TestMigrate_Bug62_MySQLToPG_BitDefaults pins Bug 62 cross-engine:
// MySQL BIT(N>1) → PG bit(N) with a B'…' default. Pre-fix the column
// landed as BYTEA with the literal decimal string '165', so a bare
// INSERT silently stored \x313635 instead of the bit value 0xA5
// (migrate SUCCEEDED but the default was corrupted). This direction is
// the half d89debc's bit(1)-only test never exercised.
func TestMigrate_Bug62_MySQLToPG_BitDefaults(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE bit_defaults (
			id     INT PRIMARY KEY,
			flags  BIT(8)  NOT NULL DEFAULT b'10100101',
			onebit BIT(1)  NOT NULL DEFAULT b'1',
			wide   BIT(16) NOT NULL DEFAULT b'1111000011110000'
		);
		INSERT INTO bit_defaults (id) VALUES (1), (2);
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
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run = %v; want SUCCESS (Bug 62 cross-engine: BIT(N) → bit(N))", err)
	}

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// Column types landed as bit / boolean, not bytea.
	typeCases := []struct {
		col, want string
	}{
		{"flags", "bit"},
		{"onebit", "boolean"}, // bit(1) → ir.Boolean → BOOLEAN (catalog #4, unchanged)
		{"wide", "bit"},
	}
	for _, tc := range typeCases {
		var dataType string
		if err := tgt.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_name = 'bit_defaults' AND column_name = $1`, tc.col).Scan(&dataType); err != nil {
			t.Fatalf("read column type %q: %v", tc.col, err)
		}
		if dataType != tc.want {
			t.Errorf("column %q data_type = %q; want %q", tc.col, dataType, tc.want)
		}
	}

	// Bit VALUES round-trip on the copied rows (must not regress; the
	// row-data path was already correct pre-fix).
	var v8, v16 string
	if err := tgt.QueryRowContext(ctx,
		"SELECT flags::text, wide::text FROM bit_defaults WHERE id=1").Scan(&v8, &v16); err != nil {
		t.Fatalf("read bit values: %v", err)
	}
	if v8 != "10100101" {
		t.Errorf("flags value = %q; want %q (0xA5)", v8, "10100101")
	}
	if v16 != "1111000011110000" {
		t.Errorf("wide value = %q; want %q (0xF0F0)", v16, "1111000011110000")
	}

	// The DEFAULT is a real bit literal B'10100101', NOT '165'::bytea.
	// A bare INSERT must store 0xA5, not the ASCII bytes of "165".
	if _, err := tgt.ExecContext(ctx, "INSERT INTO bit_defaults (id) VALUES (5)"); err != nil {
		t.Fatalf("insert relying on bit default = %v; want success", err)
	}
	var d8, d16 string
	var d1 bool
	if err := tgt.QueryRowContext(ctx,
		"SELECT flags::text, onebit, wide::text FROM bit_defaults WHERE id=5").
		Scan(&d8, &d1, &d16); err != nil {
		t.Fatalf("read defaulted bit row: %v", err)
	}
	if d8 != "10100101" {
		t.Errorf("flags DEFAULT = %q; want %q (B'10100101'; pre-fix this was \\x313635)", d8, "10100101")
	}
	if !d1 {
		t.Errorf("onebit DEFAULT = %v; want true (bit(1)→bool path, catalog #4 not regressed)", d1)
	}
	if d16 != "1111000011110000" {
		t.Errorf("wide DEFAULT = %q; want %q (B'1111000011110000')", d16, "1111000011110000")
	}
}
