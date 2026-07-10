//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for [RowWriter.UpdateFloatColumnsByPK] (ir.FloatRepairWriter)
// — the target half of the VStream-COPY FLOAT display-rounding repair
// (roadmap open-bug 2026-07-09). Against a real MySQL container it seeds
// each row with a DELIBERATELY WRONG (display-rounded stand-in) FLOAT
// value, runs the PK-keyed repair with the EXACT float32 torture values,
// and reads the stored value back through the ADR-0153 `(col * 1E0)`
// projection (so the read itself is not the thing that rounds). The pin is
// float32-EXACT across the magnitude family — single-precision powers of
// two, float32-max, negative fractional, a >6-significant-digit value,
// −0.0 sign, a NULL leg — plus a composite-PK shape and a
// zero-rows-affected (row-absent) no-op.
//
// The value-SHAPING half (float64 → the interpolated/bound literal MySQL
// stores) is the SAME buildSetClause → prepareApplierValue path the CDC
// applier uses, already pinned byte-exact by the ADR-0153 interpolation
// matrix; this test pins the UPDATE-by-PK plumbing on top of it.

package mysql

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowWriter_UpdateFloatColumnsByPK_Float32Exact(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE frepair (
			id  BIGINT NOT NULL,
			fl  FLOAT  NULL,
			dbl DOUBLE NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	// The magnitude family. Each is a distinct float32-exactness challenge;
	// index+1 is the row id. A nil entry is the NULL leg.
	vals := []any{
		float64(float32(8388608)),     // 2^23 — mysqld text prints 8388610
		float64(math.MaxFloat32),      // float32 max
		float64(float32(-123456.789)), // negative fractional — prints -123457
		float64(float32(0.1)),         // >6 significant digits after widening
		math.Copysign(0, -1),          // −0.0 (sign must survive)
		nil,                           // NULL leg
	}

	// Seed every row with a WRONG placeholder (the "display-rounded" stand-in
	// the COPY would have landed) so a passing read-back proves the repair
	// UPDATE actually ran — not that the seed happened to be right.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for i := range vals {
		if _, err := db.ExecContext(ctx, "INSERT INTO frepair (id, fl, dbl) VALUES (?, 1, 1)", int64(i+1)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	table := &ir.Table{
		Name: "frepair",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			// A FLOAT source mapped to a DOUBLE target — pins the batched
			// derived-column (`? AS dbl`) → DOUBLE-target coercion (the suite
			// had left this column declared-but-unrepaired).
			{Name: "dbl", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	// PlanetScale flavor ⇒ interpolated (text) UPDATE literals — the path
	// ADR-0153's `-0`-string + float rendering had to get right.
	rwGeneric, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	fw, ok := rwGeneric.(ir.FloatRepairWriter)
	if !ok {
		t.Fatal("mysql RowWriter does not implement ir.FloatRepairWriter")
	}

	rows := make(chan ir.Row, len(vals))
	for i, v := range vals {
		rows <- ir.Row{"id": int64(i + 1), "fl": v, "dbl": v}
	}
	close(rows)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"id"}, rows); err != nil {
		t.Fatalf("UpdateFloatColumnsByPK: %v", err)
	}

	// Read back through the ADR-0153 projection so the READ doesn't round.
	for i, want := range vals {
		var got, dbl sql.NullFloat64
		if err := db.QueryRowContext(ctx, "SELECT (fl * 1E0), dbl FROM frepair WHERE id = ?", int64(i+1)).Scan(&got, &dbl); err != nil {
			t.Fatalf("read row %d: %v", i+1, err)
		}
		if want == nil {
			if got.Valid || dbl.Valid {
				t.Errorf("row %d: NULL leg landed non-NULL fl=%v dbl=%v", i+1, got, dbl)
			}
			continue
		}
		wf := want.(float64)
		if !got.Valid || !dbl.Valid {
			t.Errorf("row %d: got NULL; want %v", i+1, wf)
			continue
		}
		if math.Float32bits(float32(got.Float64)) != math.Float32bits(float32(wf)) {
			t.Errorf("row %d: FLOAT repair not float32-exact: got %v (bits %x), want %v (bits %x)",
				i+1, got.Float64, math.Float32bits(float32(got.Float64)), wf, math.Float32bits(float32(wf)))
		}
		// DOUBLE target stores the float64 exactly — the batched coercion must
		// not perturb it.
		if math.Float64bits(dbl.Float64) != math.Float64bits(wf) {
			t.Errorf("row %d: DOUBLE repair not float64-exact: got %v (bits %x), want %v (bits %x)",
				i+1, dbl.Float64, math.Float64bits(dbl.Float64), wf, math.Float64bits(wf))
		}
		// −0.0 sign must survive the repair.
		if wf == 0 && math.Signbit(wf) && (!math.Signbit(got.Float64) || !math.Signbit(dbl.Float64)) {
			t.Errorf("row %d: repair lost the −0.0 sign: fl=%v dbl=%v", i+1, got.Float64, dbl.Float64)
		}
	}

	// Row-absent no-op: repairing a PK not on the target is a
	// zero-rows-affected no-op, not an error.
	orphan := make(chan ir.Row, 1)
	orphan <- ir.Row{"id": int64(9999), "fl": float64(float32(42.5))}
	close(orphan)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"id"}, orphan); err != nil {
		t.Errorf("repair of an absent row must be a no-op, got: %v", err)
	}
}

// TestRowWriter_UpdateFloatColumnsByPK_CompositePK pins the WHERE builder
// on a composite primary key, and that a FLOAT column that is a PK member
// is left in the WHERE (never SET) while the non-PK FLOAT is repaired.
func TestRowWriter_UpdateFloatColumnsByPK_CompositePK(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE frepair_c (
			a INT   NOT NULL,
			b INT   NOT NULL,
			fl FLOAT NULL,
			PRIMARY KEY (a, b)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "INSERT INTO frepair_c (a, b, fl) VALUES (1, 2, 1), (1, 3, 1)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	table := &ir.Table{
		Name: "frepair_c",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Integer{Width: 32}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
	}
	rwGeneric, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	fw := rwGeneric.(ir.FloatRepairWriter)

	rows := make(chan ir.Row, 2)
	rows <- ir.Row{"a": int64(1), "b": int64(2), "fl": float64(float32(8388608))}
	rows <- ir.Row{"a": int64(1), "b": int64(3), "fl": float64(float32(-123456.789))}
	close(rows)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"a", "b"}, rows); err != nil {
		t.Fatalf("UpdateFloatColumnsByPK: %v", err)
	}

	for _, tc := range []struct {
		a, b int
		want float64
	}{{1, 2, float64(float32(8388608))}, {1, 3, float64(float32(-123456.789))}} {
		var got float64
		if err := db.QueryRowContext(ctx, "SELECT (fl * 1E0) FROM frepair_c WHERE a = ? AND b = ?", tc.a, tc.b).Scan(&got); err != nil {
			t.Fatalf("read (%d,%d): %v", tc.a, tc.b, err)
		}
		if math.Float32bits(float32(got)) != math.Float32bits(float32(tc.want)) {
			t.Errorf("(%d,%d): composite-PK repair not exact: got %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
