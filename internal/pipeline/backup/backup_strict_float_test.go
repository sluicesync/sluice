// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// lossyFloatRows is a stub snapshot [ir.RowReader] that declares itself a
// display-rounding VStream COPY reader ([ir.LossyFloatCopyReader]).
type lossyFloatRows struct{ rounds bool }

func (lossyFloatRows) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) { return nil, nil }
func (lossyFloatRows) Err() error                                                 { return nil }
func (r lossyFloatRows) CopyDisplayRoundsFloats() bool                            { return r.rounds }

func floatSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "measurements",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "reading", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			{Name: "precise", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
}

// TestCheckVStreamFloatLossy_StrictRepairableWraps pins that --strict-float
// on a REPAIRABLE table (PK + non-PK FLOAT) does NOT refuse upfront — it
// wraps for the exact re-read (exact-or-fail: exact here, since it can).
func TestCheckVStreamFloatLossy_StrictRepairableWraps(t *testing.T) {
	b := &Backup{StrictFloat: true}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}

	if err := b.applyVStreamFloatPolicy(context.Background(), snap, floatSchema()); err != nil {
		t.Fatalf("--strict-float on a repairable table must wrap (not refuse upfront); got: %v", err)
	}
	pr, ok := snap.Rows.(*floatExactPatchReader)
	if !ok {
		t.Fatalf("--strict-float must WRAP snap.Rows for the exact re-read; got %T", snap.Rows)
	}
	if !pr.strict {
		t.Error("the wrapped reader must carry strict=true so an over-cap table refuses")
	}
}

// TestCheckVStreamFloatLossy_StrictKeylessRefuses pins that --strict-float
// refuses UPFRONT with the coded error when a FLOAT column cannot be
// re-read exactly (keyless table) — "exact, or fail".
func TestCheckVStreamFloatLossy_StrictKeylessRefuses(t *testing.T) {
	b := &Backup{StrictFloat: true}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "keyless",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "reading", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
	}}}

	err := b.applyVStreamFloatPolicy(context.Background(), snap, schema)
	if err == nil {
		t.Fatal("--strict-float with an un-repairable (keyless) FLOAT table must refuse; got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("refusal is not a coded error: %v", err)
	}
	if ce.Code != sluicecode.CodeVStreamFloatLossy {
		t.Errorf("code = %s; want %s", ce.Code, sluicecode.CodeVStreamFloatLossy)
	}
	if info, _ := sluicecode.Describe(ce.Code); info.Class != sluicecode.ClassRefusal {
		t.Errorf("code class = %v; want refusal (exit 3)", info.Class)
	}
}

// TestCheckVStreamFloatLossy_DefaultWrapsForExactReread pins that the
// DEFAULT posture (no flags) wraps snap.Rows in the exact-re-read patch
// reader so archived FLOAT columns are exact.
func TestCheckVStreamFloatLossy_DefaultWrapsForExactReread(t *testing.T) {
	b := &Backup{}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}

	if err := b.applyVStreamFloatPolicy(context.Background(), snap, floatSchema()); err != nil {
		t.Fatalf("default must proceed (nil error); got: %v", err)
	}
	if _, ok := snap.Rows.(*floatExactPatchReader); !ok {
		t.Errorf("default posture must WRAP snap.Rows for the exact re-read; got %T", snap.Rows)
	}
}

// TestCheckVStreamFloatLossy_NoRereadKeepsRounded pins that
// --no-float-exact-reread proceeds WITHOUT wrapping (rounded, consistent).
func TestCheckVStreamFloatLossy_NoRereadKeepsRounded(t *testing.T) {
	b := &Backup{NoFloatExactReread: true}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}

	if err := b.applyVStreamFloatPolicy(context.Background(), snap, floatSchema()); err != nil {
		t.Fatalf("--no-float-exact-reread must proceed; got: %v", err)
	}
	if _, ok := snap.Rows.(*floatExactPatchReader); ok {
		t.Error("--no-float-exact-reread must NOT wrap snap.Rows (archive stays rounded-but-consistent)")
	}
}

// TestCheckVStreamFloatLossy_KeylessFallsBackToRounded pins that a keyless
// FLOAT table is not wrapped (can't be re-read) under the default posture.
func TestCheckVStreamFloatLossy_KeylessFallsBackToRounded(t *testing.T) {
	b := &Backup{}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "keyless",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "reading", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		// no PrimaryKey
	}}}
	if err := b.applyVStreamFloatPolicy(context.Background(), snap, schema); err != nil {
		t.Fatalf("keyless default must proceed with a WARN; got: %v", err)
	}
	if _, ok := snap.Rows.(*floatExactPatchReader); ok {
		t.Error("keyless FLOAT table has nothing repairable; snap.Rows must NOT be wrapped")
	}
}

// TestCheckVStreamFloatLossy_NonVStreamNoOp pins that a non-display-rounding
// reader (vanilla MySQL / PG snapshot) is a no-op even under --strict-float.
func TestCheckVStreamFloatLossy_NonVStreamNoOp(t *testing.T) {
	b := &Backup{StrictFloat: true}
	// rounds=false → CopyDisplayRoundsFloats() false → not lossy.
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: false}}
	if err := b.applyVStreamFloatPolicy(context.Background(), snap, floatSchema()); err != nil {
		t.Fatalf("non-display-rounding reader must be a no-op; got: %v", err)
	}

	// A reader that doesn't implement LossyFloatCopyReader at all.
	snap2 := &irbackup.Snapshot{Rows: plainRows{}}
	if err := b.applyVStreamFloatPolicy(context.Background(), snap2, floatSchema()); err != nil {
		t.Fatalf("reader without the surface must be a no-op; got: %v", err)
	}
}

// TestCheckVStreamFloatLossy_NoFloatColumnsNoOp pins that even a VStream
// reader is a no-op when the schema has no single-precision FLOAT column
// (only DOUBLE / non-float), under --strict-float.
func TestCheckVStreamFloatLossy_NoFloatColumnsNoOp(t *testing.T) {
	b := &Backup{StrictFloat: true}
	snap := &irbackup.Snapshot{Rows: lossyFloatRows{rounds: true}}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "d", Type: ir.Float{Precision: ir.FloatDouble}},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
	if err := b.applyVStreamFloatPolicy(context.Background(), snap, schema); err != nil {
		t.Fatalf("no single-precision FLOAT columns must be a no-op; got: %v", err)
	}
}

type plainRows struct{}

func (plainRows) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) { return nil, nil }
func (plainRows) Err() error                                                 { return nil }
