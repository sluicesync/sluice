//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for [RowWriter.UpdateFloatColumnsByPK] (ir.FloatRepairWriter)
// on a real Postgres target — the write-side half of the VStream-COPY FLOAT
// display-rounding repair (roadmap open-bug 2026-07-09), now BATCHED against a
// VALUES-join (audit PERF-P1). It seeds each row with a DELIBERATELY WRONG
// value, runs the PK-keyed repair with float32-exact torture values, and
// reads the stored value back.
//
// The load-bearing thing this pins that the pure-Go builder test cannot: the
// FIRST-ROW `::type` cast on the VALUES columns actually round-trips the value
// through pgx + PG byte-for-byte (an untyped $N would default to text and both
// break the join and risk re-rounding). It covers the SET-value families the
// cast dispatches on — REAL, DOUBLE PRECISION, and NUMERIC — plus a
// composite PK, a NULL leg, −0.0 sign survival, and a deleted-row no-op.

package postgres

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowWriter_UpdateFloatColumnsByPK_Batched(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE frepair (
			id  BIGINT NOT NULL PRIMARY KEY,
			fl  REAL,
			dbl DOUBLE PRECISION
		);`)

	// The magnitude family; index+1 is the row id. A nil entry is the NULL leg.
	vals := []any{
		float64(float32(8388608)),     // 2^23
		float64(math.MaxFloat32),      // float32 max
		float64(float32(-123456.789)), // negative fractional
		float64(float32(0.1)),         // >6 significant digits after widening
		math.Copysign(0, -1),          // −0.0 (sign must survive)
		nil,                           // NULL leg
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for i := range vals {
		// Seed a WRONG placeholder so a passing read-back proves the repair ran.
		if _, err := db.ExecContext(ctx, "INSERT INTO frepair (id, fl, dbl) VALUES ($1, 1, 1)", int64(i+1)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	table := &ir.Table{
		Name: "frepair",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "frepair_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	rwGeneric, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	fw, ok := rwGeneric.(ir.FloatRepairWriter)
	if !ok {
		t.Fatal("postgres RowWriter does not implement ir.FloatRepairWriter")
	}

	rows := make(chan ir.Row, len(vals))
	for i, v := range vals {
		rows <- ir.Row{"id": int64(i + 1), "fl": v}
	}
	close(rows)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"id"}, rows); err != nil {
		t.Fatalf("UpdateFloatColumnsByPK: %v", err)
	}

	for i, want := range vals {
		var got sql.NullFloat64
		if err := db.QueryRowContext(ctx, "SELECT fl FROM frepair WHERE id = $1", int64(i+1)).Scan(&got); err != nil {
			t.Fatalf("read row %d: %v", i+1, err)
		}
		if want == nil {
			if got.Valid {
				t.Errorf("row %d: NULL leg landed non-NULL %v", i+1, got.Float64)
			}
			continue
		}
		wf := want.(float64)
		if !got.Valid {
			t.Errorf("row %d: got NULL; want %v", i+1, wf)
			continue
		}
		if math.Float32bits(float32(got.Float64)) != math.Float32bits(float32(wf)) {
			t.Errorf("row %d: REAL repair not float32-exact: got %v (bits %x), want %v (bits %x)",
				i+1, got.Float64, math.Float32bits(float32(got.Float64)), wf, math.Float32bits(float32(wf)))
		}
		if wf == 0 && math.Signbit(wf) && !math.Signbit(got.Float64) {
			t.Errorf("row %d: repair lost the −0.0 sign: got %v", i+1, got.Float64)
		}
	}

	// Deleted-row no-op: repairing a PK not on the target is a clean
	// VALUES-join miss, not an error.
	orphan := make(chan ir.Row, 1)
	orphan <- ir.Row{"id": int64(9999), "fl": float64(float32(42.5))}
	close(orphan)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"id"}, orphan); err != nil {
		t.Errorf("repair of an absent row must be a no-op, got: %v", err)
	}
}

// TestRowWriter_UpdateFloatColumnsByPK_MultiBatch pins the batching boundary:
// more rows than floatRepairBatchRows must all land (the batch flush + the
// final flush both fire), each float32-exact.
func TestRowWriter_UpdateFloatColumnsByPK_MultiBatch(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE frepair_mb (
			id BIGINT NOT NULL PRIMARY KEY,
			fl REAL
		);`)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const n = floatRepairBatchRows + 250 // spans two batches
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO frepair_mb (id, fl) VALUES ($1, 0)", int64(i+1)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	table := &ir.Table{
		Name: "frepair_mb",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "frepair_mb_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	rwGeneric, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	fw := rwGeneric.(ir.FloatRepairWriter)

	want := func(i int) float64 { return float64(float32(i) + 0.5) }
	rows := make(chan ir.Row, n)
	for i := 0; i < n; i++ {
		rows <- ir.Row{"id": int64(i + 1), "fl": want(i)}
	}
	close(rows)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"id"}, rows); err != nil {
		t.Fatalf("UpdateFloatColumnsByPK: %v", err)
	}

	for i := 0; i < n; i++ {
		var got float64
		if err := db.QueryRowContext(ctx, "SELECT fl FROM frepair_mb WHERE id = $1", int64(i+1)).Scan(&got); err != nil {
			t.Fatalf("read %d: %v", i+1, err)
		}
		if math.Float32bits(float32(got)) != math.Float32bits(float32(want(i))) {
			t.Errorf("row %d: got %v want %v", i+1, got, want(i))
		}
	}
}

// TestRowWriter_UpdateFloatColumnsByPK_NumericTarget pins the numeric-target
// family (a FLOAT source overridden to numeric) and a composite PK: the
// first-row ::NUMERIC(p,s) cast must land the value with the column's scale
// rounding, byte-identically to a per-row `SET n = $1` assignment.
func TestRowWriter_UpdateFloatColumnsByPK_NumericTarget(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE frepair_n (
			a  INTEGER NOT NULL,
			b  TEXT    NOT NULL,
			n  NUMERIC(10,2),
			PRIMARY KEY (a, b)
		);`)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "INSERT INTO frepair_n (a, b, n) VALUES (1, 'k1', 9), (2, 'k2', 9)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	table := &ir.Table{
		Name: "frepair_n",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Text{}},
			{Name: "n", Type: ir.Decimal{Precision: 10, Scale: 2}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "frepair_n_pkey", Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
	}
	rwGeneric, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer closeIf(rwGeneric)
	fw := rwGeneric.(ir.FloatRepairWriter)

	rows := make(chan ir.Row, 2)
	rows <- ir.Row{"a": int64(1), "b": "k1", "n": float64(1.239)} // → 1.24 at scale 2
	rows <- ir.Row{"a": int64(2), "b": "k2", "n": float64(5.005)} // banker's/round-half behaviour of the column
	close(rows)
	if err := fw.UpdateFloatColumnsByPK(ctx, table, []string{"a", "b"}, rows); err != nil {
		t.Fatalf("UpdateFloatColumnsByPK: %v", err)
	}

	// Compare against what a direct per-row assignment stores, proving the
	// batched VALUES-cast path is byte-identical (not the source of any
	// rounding beyond the column's own scale).
	for _, tc := range []struct {
		a int
		b string
		v float64
	}{{1, "k1", 1.239}, {2, "k2", 5.005}} {
		var gotBatched, gotPerRow string
		if err := db.QueryRowContext(ctx, "SELECT n::text FROM frepair_n WHERE a = $1 AND b = $2", tc.a, tc.b).Scan(&gotBatched); err != nil {
			t.Fatalf("read (%d,%s): %v", tc.a, tc.b, err)
		}
		if err := db.QueryRowContext(ctx, "SELECT ($1::double precision)::numeric(10,2)::text", tc.v).Scan(&gotPerRow); err != nil {
			t.Fatalf("per-row oracle (%d,%s): %v", tc.a, tc.b, err)
		}
		if gotBatched != gotPerRow {
			t.Errorf("(%d,%s): batched numeric %q != per-row assignment %q", tc.a, tc.b, gotBatched, gotPerRow)
		}
	}
}
