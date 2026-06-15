// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0091 F7b unit pins: StableID (PG attnum) drives the rename
// intercept's proven-vs-unproven decision, but is excluded from schema
// identity (signature + alter-detection) and from the same-type rename
// heuristic's equality lens.

// TestStableID_ExcludedFromAlterDetection pins that diffAlteredColumn
// (alter-type / alter-nullability) ignores StableID: a seed column
// (StableID=0) vs the first CDC snapshot (StableID=attnum) for an
// UNCHANGED column must NOT classify as altered.
func TestStableID_ExcludedFromAlterDetection(t *testing.T) {
	pre := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}, StableID: 0},
			{Name: "name", Type: ir.Text{Size: ir.TextLong}, StableID: 0},
		},
	}
	post := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}, StableID: 1},
			{Name: "name", Type: ir.Text{Size: ir.TextLong}, StableID: 2},
		},
	}
	_, _, _, hasAlter := diffAlteredColumn(pre, post)
	if hasAlter {
		t.Fatalf("StableID-only delta classified as an alter — must be ignored by alter-detection")
	}
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindNone {
		t.Fatalf("StableID-only delta classified as %s; want none", shape.Kind)
	}
}

// TestDiffRenameColumn_CarriesStableID pins that the same-type rename
// heuristic still FIRES across differing StableIDs (the equality lens
// zeroes them) AND that the returned before/after columns carry their
// REAL StableIDs (the intercept reads them to prove the rename).
func TestDiffRenameColumn_CarriesStableID(t *testing.T) {
	dropped := []*ir.Column{{Name: "old_email", Type: ir.Varchar{Length: 100}, StableID: 5}}
	// A genuine PG rename keeps the same attnum on the added column.
	added := []*ir.Column{{Name: "new_email", Type: ir.Varchar{Length: 100}, StableID: 5}}
	before, after, ok := diffRenameColumn(added, dropped)
	if !ok {
		t.Fatalf("rename heuristic did not fire across differing names with equal StableID")
	}
	if before.StableID != 5 || after.StableID != 5 {
		t.Fatalf("returned columns lost their StableIDs: before=%d after=%d (want 5/5)",
			before.StableID, after.StableID)
	}
	// A real drop+add: different attnums. The heuristic STILL fires
	// (same type, one drop, one add) — disambiguation is the intercept's
	// job via StableID, not the classifier's.
	added2 := []*ir.Column{{Name: "new_email", Type: ir.Varchar{Length: 100}, StableID: 9}}
	before2, after2, ok2 := diffRenameColumn(added2, dropped)
	if !ok2 {
		t.Fatalf("rename heuristic should fire regardless of StableID equality")
	}
	if before2.StableID != 5 || after2.StableID != 9 {
		t.Fatalf("returned columns lost their real StableIDs: before=%d after=%d (want 5/9)",
			before2.StableID, after2.StableID)
	}
}

// renameSnapWithIDs builds a SchemaSnapshot whose single non-PK column
// carries the given name + StableID, mirroring addColForwardTable.
func renameSnapWithIDs(t *testing.T, name, colName string, stableID int) ir.SchemaSnapshot {
	t.Helper()
	tbl := &ir.Table{
		Schema: "public",
		Name:   name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}, StableID: 1},
			{Name: colName, Type: ir.Varchar{Length: 100}, StableID: stableID},
		},
		PrimaryKey: &ir.Index{Name: "pk_" + name, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	return ir.SchemaSnapshot{
		Position: ir.Position{Engine: "postgres", Token: "lsn/1"},
		Schema:   tbl.Schema,
		Table:    tbl.Name,
		IR:       tbl,
	}
}

// runRenameIntercept feeds an anchor snapshot then a rename snapshot
// through the intercept against a fake applier and returns whether
// AlterRenameColumn fired plus any surfaced error.
func runRenameIntercept(t *testing.T, anchorID, renameID int) (renamed bool, errMsg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	// Anchor: column "old_email" with anchorID. Both snapshots arrive via
	// CDC (no seed), so the boundary is CDC→CDC and the seed-guard lifts.
	in <- renameSnapWithIDs(t, "users", "old_email", anchorID)
	in <- renameSnapWithIDs(t, "users", "new_email", renameID)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, 2*time.Second)
	for _, c := range applier.callNames() {
		if c == "AlterRenameColumn" {
			renamed = true
		}
	}
	if e := errStore.Load(); e != nil {
		errMsg = (*e).Error()
	}
	return renamed, errMsg
}

// TestIntercept_RenameProven_Forwards pins the F7b proven path: equal
// non-zero StableIDs (PG attnum survives the rename) → AlterRenameColumn
// fires, no refusal.
func TestIntercept_RenameProven_Forwards(t *testing.T) {
	renamed, errMsg := runRenameIntercept(t, 5, 5)
	if !renamed {
		t.Fatalf("proven rename (same attnum) did not forward AlterRenameColumn")
	}
	if errMsg != "" {
		t.Fatalf("proven rename surfaced an error: %s", errMsg)
	}
}

// TestIntercept_RenameUnproven_DifferentAttnum_Refuses pins that a real
// drop+add (different attnums) refuses loudly — attnum disambiguation.
func TestIntercept_RenameUnproven_DifferentAttnum_Refuses(t *testing.T) {
	renamed, errMsg := runRenameIntercept(t, 5, 9)
	if renamed {
		t.Fatalf("different-attnum drop+add mis-forwarded as a rename (silent-loss risk)")
	}
	if !strings.Contains(errMsg, "RENAME COLUMN") || !strings.Contains(errMsg, "cannot be auto-forwarded") {
		t.Fatalf("expected the ambiguous-rename refusal; got: %s", errMsg)
	}
}

// TestIntercept_RenameUnproven_ZeroAttnum_Refuses pins that a zero
// StableID (MySQL source, or unresolved PG lookup) is unprovable →
// refuse. This is the regression pin that F7b did NOT loosen the
// no-stable-id case (e.g. MySQL).
func TestIntercept_RenameUnproven_ZeroAttnum_Refuses(t *testing.T) {
	renamed, errMsg := runRenameIntercept(t, 0, 0)
	if renamed {
		t.Fatalf("zero-attnum (unprovable) rename mis-forwarded")
	}
	if !strings.Contains(errMsg, "RENAME COLUMN") {
		t.Fatalf("expected the ambiguous-rename refusal; got: %s", errMsg)
	}
}
