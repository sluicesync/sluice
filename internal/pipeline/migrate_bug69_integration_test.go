//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 69 (an unconstrained PostgreSQL `numeric`
// column — declared `numeric` with NO precision/scale, arbitrary
// precision — is mis-emitted on cross-engine migrate):
//
//   - PG→PG: pre-fix rendered NUMERIC(0,0) → PG rejects CREATE TABLE
//     with SQLSTATE 22023 (loud, exit 1, no partial table).
//   - PG→MySQL: pre-fix rendered DECIMAL(0,0) → SILENT decimal-precision
//     data loss: exit 0, no WARN at any level, 3.14159 → 3.
//
// Root cause: PG information_schema reports numeric_precision /
// numeric_scale as NULL for arbitrary-precision numeric; the reader /
// IR collapsed NULL→0; both target emitters then rendered (0,0).
//
// The fix models the unconstrained case distinctly
// (ir.Decimal{Unconstrained: true}); PG-target emits bare NUMERIC,
// MySQL-target emits the widest representable DECIMAL(65,30) plus a
// loud, operator-actionable advisory at `migrate` preflight (and
// `schema preview`) mirroring the bigint-unsigned precedent.
//
// This is the verbatim BUG-CATALOG section 69 minimal repro: an
// unconstrained `numeric` and a `numeric[]` column holding 3.14159,
// 9999.999, and a high-precision value, plus a guard that a
// CONSTRAINED numeric(15,2) still round-trips byte-identically.

package pipeline

import (
	"database/sql"
	"log/slog"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/engines"

	// Both engines must be registered for engines.Get to find them.
	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// bug69SeedDDL is the canonical BUG-CATALOG section 69 repro: an
// unconstrained `numeric`, an unconstrained `numeric[]`, and a bounded
// `numeric(15,2)` regression guard, with values exercising fractional
// precision (3.14159), rounding pressure (9999.999), and a
// high-precision value that DECIMAL(65,30) preserves but DECIMAL(0,0)
// would have destroyed.
const bug69SeedDDL = `
	CREATE TABLE measures (
	  id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	  amount    numeric,            -- UNCONSTRAINED — the Bug 69 column
	  readings  numeric[],          -- unconstrained array element
	  bounded   numeric(15,2),      -- CONSTRAINED — regression guard
	  label     text
	);
	INSERT INTO measures (amount, readings, bounded, label) VALUES
	  (3.14159,                          ARRAY[3.14159, 2.71828], 12345.67, 'r1'),
	  (9999.999,                         ARRAY[9999.999],         0.01,     'r2'),
	  (12345678901234567890.1234567890,  NULL,                    42.00,    'r3');
`

// TestMigrate_PostgresToPostgres_Bug69UnconstrainedNumeric pins the
// PG→PG closure: pre-fix the CREATE TABLE failed with 22023 because the
// unconstrained numeric rendered NUMERIC(0,0). Post-fix it emits bare
// NUMERIC, migrate exits 0, and every value is ground-truthed EXACT on
// the PG target (arbitrary precision preserved, including the
// high-precision r3 value and the numeric[] cells). The bounded
// numeric(15,2) must be byte-identical.
func TestMigrate_PostgresToPostgres_Bug69UnconstrainedNumeric(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, bug69SeedDDL)

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
	if err := mig.Run(ctx2min(t)); err != nil {
		// Pre-fix this failed with SQLSTATE 22023 (NUMERIC(0,0)).
		t.Fatalf("Migrator.Run (PG→PG unconstrained numeric must migrate, no 22023): %v", err)
	}

	pgDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	// The unconstrained column must be bare `numeric` on the target:
	// information_schema reports NULL precision for arbitrary precision.
	var numPrec sql.NullInt64
	if err := pgDB.QueryRow(`
		SELECT numeric_precision FROM information_schema.columns
		WHERE table_name = 'measures' AND column_name = 'amount'`).Scan(&numPrec); err != nil {
		t.Fatalf("read measures.amount numeric_precision: %v", err)
	}
	if numPrec.Valid {
		t.Errorf("measures.amount numeric_precision = %d; want NULL (bare NUMERIC, arbitrary precision)", numPrec.Int64)
	}
	// The bounded column must keep its declared precision/scale.
	var bPrec, bScale sql.NullInt64
	if err := pgDB.QueryRow(`
		SELECT numeric_precision, numeric_scale FROM information_schema.columns
		WHERE table_name = 'measures' AND column_name = 'bounded'`).Scan(&bPrec, &bScale); err != nil {
		t.Fatalf("read measures.bounded precision/scale: %v", err)
	}
	if bPrec.Int64 != 15 || bScale.Int64 != 2 {
		t.Errorf("measures.bounded = numeric(%d,%d); want numeric(15,2) (constrained must be unchanged)", bPrec.Int64, bScale.Int64)
	}

	type row struct {
		id       int
		amount   string
		readings sql.NullString
		bounded  string
		label    string
	}
	want := []row{
		{1, "3.14159", sql.NullString{String: "{3.14159,2.71828}", Valid: true}, "12345.67", "r1"},
		{2, "9999.999", sql.NullString{String: "{9999.999}", Valid: true}, "0.01", "r2"},
		{3, "12345678901234567890.1234567890", sql.NullString{Valid: false}, "42.00", "r3"},
	}
	rows, err := pgDB.Query(`SELECT id, amount::text, readings::text, bounded::text, label
		FROM measures ORDER BY id`)
	if err != nil {
		t.Fatalf("query pg target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.amount, &r.readings, &r.bounded, &r.label); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.amount != w.amount {
			t.Errorf("row[%d] amount: got %q; want %q (arbitrary precision must be EXACT)", i, g.amount, w.amount)
		}
		if g.readings != w.readings {
			t.Errorf("row[%d] readings: got %+v; want %+v", i, g.readings, w.readings)
		}
		if g.bounded != w.bounded {
			t.Errorf("row[%d] bounded: got %q; want %q (constrained numeric(15,2) must round-trip)", i, g.bounded, w.bounded)
		}
	}
}

// TestMigrate_PostgresToMySQL_Bug69UnconstrainedNumeric pins the
// PG→MySQL closure: pre-fix exit 0 with DECIMAL(0,0) silently truncating
// 3.14159→3 and 9999.999→10000, no WARN. Post-fix the unconstrained
// numeric lands as DECIMAL(65,30), the values are preserved on the
// MySQL target (NOT truncated to integer), and the loud advisory FIRES
// at migrate preflight (asserted from the captured slog stream). The
// `numeric[]` lands as MySQL JSON (lossless). The bounded numeric(15,2)
// is byte-identical.
func TestMigrate_PostgresToMySQL_Bug69UnconstrainedNumeric(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug69SeedDDL)

	// Capture the slog stream so the loud advisory can be asserted —
	// the Bug 69 silent-loss class is closed only if the operator is
	// loudly told. Same lockedBuffer + JSONHandler pattern the diagnose
	// and broker integration tests use.
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(prevDefault)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→MySQL unconstrained numeric must migrate): %v", err)
	}

	// The loud advisory MUST have fired — this is the load-bearing
	// assertion that closes the silent-loss class.
	logs := string(logBuf.Bytes())
	for _, want := range []string{
		"migrate",            // preflight contextID
		"DECIMAL(65,30)",     // the target type named
		"measures.amount",    // the affected column named
		"Migration proceeds", // it's a NOTICE, not a refusal
		"--type-override",    // the escape hatch
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("migrate log missing Bug 69 advisory fragment %q\n--- captured logs ---\n%s", want, logs)
		}
	}

	// The unconstrained column must be DECIMAL(65,30) on MySQL.
	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	var colType string
	if err := mysqlDB.QueryRow(`
		SELECT COLUMN_TYPE FROM information_schema.columns
		WHERE table_name = 'measures' AND column_name = 'amount'`).Scan(&colType); err != nil {
		t.Fatalf("read measures.amount COLUMN_TYPE: %v", err)
	}
	if !strings.EqualFold(colType, "decimal(65,30)") {
		t.Errorf("measures.amount COLUMN_TYPE = %q; want decimal(65,30) (Bug 69 widest-fit)", colType)
	}

	type row struct {
		id      int
		amount  string
		bounded string
		label   string
	}
	// DECIMAL(65,30) right-pads the scale; the load-bearing assertion is
	// that the integer + fractional digits are PRESERVED, not truncated
	// to 3 / 10000 as the pre-fix DECIMAL(0,0) did.
	want := []row{
		{1, "3.141590000000000000000000000000", "12345.67", "r1"},
		{2, "9999.999000000000000000000000000000", "0.01", "r2"},
		{3, "12345678901234567890.123456789000000000000000000000", "42.00", "r3"},
	}
	rows, err := mysqlDB.Query(`SELECT id, amount, bounded, label FROM measures ORDER BY id`)
	if err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.amount, &r.bounded, &r.label); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.amount != w.amount {
			t.Errorf("row[%d] amount: got %q; want %q (pre-fix Bug 69 silently truncated to integer)", i, g.amount, w.amount)
		}
		if g.bounded != w.bounded {
			t.Errorf("row[%d] bounded: got %q; want %q (constrained numeric(15,2) must round-trip)", i, g.bounded, w.bounded)
		}
	}
}
