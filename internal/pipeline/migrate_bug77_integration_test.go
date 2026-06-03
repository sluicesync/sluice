//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for Bug 77 (PG→MySQL BIT/varbit loud 1264/22003
// mid-INSERT for multi-byte values).
//
// Phase-A ground truth (instrumented vs real MySQL 8.0):
//   - sluice emits `bit varying(N)` / `bit(N)` as MySQL `BIT(N)` and
//     the IR carries the canonical '0'/'1' bit-string (Bug 75).
//   - The Bug 75 fix bound that value to the BIT column as the
//     ceil(N/8) big-endian []byte. go-sql-driver sends a []byte param
//     as a binary string; MySQL's string→BIT coercion stores some
//     byte patterns correctly (e.g. 0x02 0x93 → 659) but raises
//     `1264 (22003) Out of range value` for others (e.g. 0x2D 0x32 →
//     a 14-bit value that fits BIT(16)) — a loud failure mid-copy.
//   - Binding the unsigned integer value instead round-trips for
//     every width 2..64 incl. high-bit-set and all-ones (probe2).
//
// The Bug 75 PG→MySQL pin passed review only because every pinned
// value was ≤1 byte / small — the "pin the class, not the
// representative" gap. This pin exercises the *class*: 1-byte,
// 2-byte, high-bit-set, wide bit varying, fixed bit(N>8), the exact
// fuzz-found failing values, NULL, and src-distinctness.

package pipeline

import (
	"database/sql"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToMySQL_Bug77BitClass pins the full bit-width
// class PG→MySQL. Every value must land faithfully (BIN(col+0) is the
// MySQL-side canonical render); distinct sources stay distinct;
// NULL→NULL. Pre-fix, rows 2/3/6 raised 1264/22003 mid-INSERT.
func TestMigrate_PostgresToMySQL_Bug77BitClass(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE bt (
		  id  int PRIMARY KEY,
		  vb  bit varying(16),
		  b9  bit(9),
		  b16 bit(16),
		  b64 bit(64)
		);
		-- Fixed bit(N) requires EXACTLY N bits in PG; only bit
		-- varying(16) may be shorter (the variable-length class).
		INSERT INTO bt VALUES
		  -- row 1: small / 1-byte (the Bug 75 representative class)
		  (1, B'1100',             B'000000001', B'0000000000001100',
		      B'0000000000000000000000000000000000000000000000000000000000000001'),
		  -- row 2: the exact fuzz-found 14-bit failing value (0x2D32)
		  (2, B'10110100110010',   B'101010101', B'0010110100110110',
		      B'0000000000000000000000000000000000000000000000001111111100000000'),
		  -- row 3: all-ones high-bit-set — pre-fix 22003 across widths
		  (3, B'1111111111111111', B'111111111', B'1111111111111111',
		      B'1111111111111111111111111111111111111111111111111111111111111111'),
		  -- row 4: empty varbit + all-zero fixed widths
		  (4, B'',                 B'000000000', B'0000000000000000',
		      B'0000000000000000000000000000000000000000000000000000000000000000'),
		  -- row 5: distinct mid value, must not collide with row 2
		  (5, B'10110100110011',   B'010101010', B'1000000000000001',
		      B'0000000000000000000000000000000000000000000000001010101010101010'),
		  -- row 6: NULL preserved as NULL
		  (6, NULL,                NULL,         NULL,                NULL);
	`)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→MySQL bit class, Bug 77): %v", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	type row struct {
		id               int
		vb, b9, b16, b64 sql.NullString
	}
	// BIN() drops leading zeros; an empty bit varying value reads back
	// as 0 (BIN(0)='0'), matching the documented zero-extend semantics.
	want := []row{
		{1, ns("1100"), ns("1"), ns("1100"), ns("1")},
		{2, ns("10110100110010"), ns("101010101"), ns("10110100110110"), ns("1111111100000000")},
		{
			3, ns("1111111111111111"), ns("111111111"), ns("1111111111111111"),
			ns("1111111111111111111111111111111111111111111111111111111111111111"),
		},
		{4, ns("0"), ns("0"), ns("0"), ns("0")},
		{5, ns("10110100110011"), ns("10101010"), ns("1000000000000001"), ns("1010101010101010")},
		{6, sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{}},
	}
	rows, err := mysqlDB.Query(`SELECT id,
		CASE WHEN vb  IS NULL THEN NULL ELSE BIN(vb+0)  END,
		CASE WHEN b9  IS NULL THEN NULL ELSE BIN(b9+0)  END,
		CASE WHEN b16 IS NULL THEN NULL ELSE BIN(b16+0) END,
		CASE WHEN b64 IS NULL THEN NULL ELSE BIN(b64+0) END
		FROM bt ORDER BY id`)
	if err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.vb, &r.b9, &r.b16, &r.b64); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("row[%d]: got %+v; want %+v (PG→MySQL bit class must be faithful + distinct, NULL→NULL)", i, g, want[i])
		}
	}
}
