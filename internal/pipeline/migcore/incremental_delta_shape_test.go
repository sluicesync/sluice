// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// The alter-delta disposition gate. The rule these tests enforce is
// ONE sentence: every structural aspect the incremental schema diff can
// see is either APPLIED through the engine's delta surface, proven not
// to need DDL, or REFUSED loudly — no aspect reaches a silent skip.
//
// The aspect list is DERIVED from the diff's own comparator registries
// ([AllAlterAspects]), not hand-listed here, so a new comparator added
// to the diff fails TestAlterAspectsAllClassified (no disposition) and
// TestAlterAspectMatrixIsAppliedOrRefused (no fixture) until somebody
// classifies it. That is the whole point: the bug being fixed was a
// shape the diff emitted and the replay side never disposed of.

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ---- test doubles ----

// recordingSchemaWriter implements [ir.SchemaWriter] +
// [ir.ShapeDeltaApplier] and records every DDL call.
type recordingSchemaWriter struct {
	addedColumns  []*ir.Column
	droppedCols   []*ir.Column
	typed         []*ir.Column
	nullability   []*ir.Column
	createdIdx    []*ir.Index
	droppedIdx    []*ir.Index
	renamed       [][2]string
	addedChecks   []*ir.CheckConstraint
	droppedChecks []*ir.CheckConstraint
	modifiedCheck int
	err           error
}

func (w *recordingSchemaWriter) calls() int {
	return len(w.addedColumns) + len(w.droppedCols) + len(w.typed) + len(w.nullability) +
		len(w.createdIdx) + len(w.droppedIdx) + len(w.renamed) +
		len(w.addedChecks) + len(w.droppedChecks) + w.modifiedCheck
}

func (w *recordingSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (w *recordingSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error     { return nil }
func (w *recordingSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }
func (w *recordingSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (w *recordingSchemaWriter) CreateViews(context.Context, *ir.Schema) error { return nil }

func (w *recordingSchemaWriter) AlterAddColumn(_ context.Context, _ *ir.Table, cols []*ir.Column) error {
	w.addedColumns = append(w.addedColumns, cols...)
	return w.err
}

func (w *recordingSchemaWriter) AlterDropColumn(_ context.Context, _ *ir.Table, cols []*ir.Column) error {
	w.droppedCols = append(w.droppedCols, cols...)
	return w.err
}

func (w *recordingSchemaWriter) CreateShapeIndex(_ context.Context, _ *ir.Table, idx []*ir.Index) error {
	w.createdIdx = append(w.createdIdx, idx...)
	return w.err
}

func (w *recordingSchemaWriter) DropShapeIndex(_ context.Context, _ *ir.Table, idx []*ir.Index) error {
	w.droppedIdx = append(w.droppedIdx, idx...)
	return w.err
}

func (w *recordingSchemaWriter) AlterColumnType(_ context.Context, _ *ir.Table, want *ir.Column) error {
	w.typed = append(w.typed, want)
	return w.err
}

func (w *recordingSchemaWriter) AlterColumnNullability(_ context.Context, _ *ir.Table, want *ir.Column) error {
	w.nullability = append(w.nullability, want)
	return w.err
}

func (w *recordingSchemaWriter) AlterRenameColumn(_ context.Context, _ *ir.Table, oldName, newName string) error {
	w.renamed = append(w.renamed, [2]string{oldName, newName})
	return w.err
}

func (w *recordingSchemaWriter) AlterAddCheck(_ context.Context, _ *ir.Table, cs []*ir.CheckConstraint) error {
	w.addedChecks = append(w.addedChecks, cs...)
	return w.err
}

func (w *recordingSchemaWriter) AlterDropCheck(_ context.Context, _ *ir.Table, cs []*ir.CheckConstraint) error {
	w.droppedChecks = append(w.droppedChecks, cs...)
	return w.err
}

func (w *recordingSchemaWriter) AlterModifyCheck(_ context.Context, _ *ir.Table, _, _ *ir.CheckConstraint) error {
	w.modifiedCheck++
	return w.err
}

var (
	_ ir.SchemaWriter       = (*recordingSchemaWriter)(nil)
	_ ir.ShapeDeltaApplier  = (*recordingSchemaWriter)(nil)
	_ ir.SchemaDeltaApplier = (*recordingSchemaWriter)(nil)
)

// shapeSurfaceHidden implements [ir.SchemaDeltaApplier] but NOT
// [ir.ShapeDeltaApplier] — the "engine has no surface for this aspect"
// arm of the apply/refuse rule. It hides the shape-delta methods so a
// type assertion to [ir.ShapeDeltaApplier] fails while
// [ir.SchemaDeltaApplier] holds.
type shapeSurfaceHidden struct {
	ir.SchemaWriter
	inner *recordingSchemaWriter
}

func (w shapeSurfaceHidden) AlterAddColumn(ctx context.Context, t *ir.Table, cols []*ir.Column) error {
	return w.inner.AlterAddColumn(ctx, t, cols)
}

var _ ir.SchemaDeltaApplier = shapeSurfaceHidden{}

// ---- fixtures: one delta per aspect ----

func baseTable() *ir.Table {
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "amount", Type: ir.Decimal{Precision: 10, Scale: 2}},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "idx_amount", Columns: []ir.IndexColumn{{Column: "amount"}}},
		},
	}
}

// cloneTable deep-copies enough of the table for a fixture mutation.
func cloneTable(t *ir.Table) *ir.Table {
	out := *t
	out.Columns = make([]*ir.Column, len(t.Columns))
	for i, c := range t.Columns {
		cc := *c
		out.Columns[i] = &cc
	}
	out.Indexes = make([]*ir.Index, len(t.Indexes))
	for i, idx := range t.Indexes {
		ii := *idx
		out.Indexes[i] = &ii
	}
	return &out
}

// alterFixture is one aspect's minimal (before, after) pair plus the
// assertion for what the apply path must have done.
type alterFixture struct {
	// mutate turns a copy of baseTable into the after-shape.
	mutate func(after *ir.Table)
	// wantCall asserts the recorded DDL for an AlterApply aspect.
	wantCall func(t *testing.T, w *recordingSchemaWriter)
}

// alterFixtures must cover EVERY aspect in [AllAlterAspects]. A new
// aspect with no entry fails TestAlterAspectMatrixIsAppliedOrRefused —
// which is the mechanism that stops a new diff comparator from reaching
// a silent skip.
var alterFixtures = map[AlterAspect]alterFixture{
	AlterAspectTableIdentity: {
		mutate: func(a *ir.Table) { a.Name = "orders_v2" },
	},
	AlterAspectColumnAdded: {
		mutate: func(a *ir.Table) {
			a.Columns = append(a.Columns, &ir.Column{Name: "note", Type: ir.Varchar{Length: 32}, Nullable: true})
		},
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.addedColumns) != 1 || w.addedColumns[0].Name != "note" {
				t.Errorf("addedColumns = %+v; want [note]", w.addedColumns)
			}
		},
	},
	AlterAspectColumnDropped: {
		mutate: func(a *ir.Table) { a.Columns = a.Columns[:1] },
	},
	AlterAspectColumnOrder: {
		mutate: func(a *ir.Table) { a.Columns[0], a.Columns[1] = a.Columns[1], a.Columns[0] },
	},
	AlterAspectColumnType: {
		// THE regression: a pure retype, no added columns. Pre-fix both
		// replay sites hit `continue` here and the target kept
		// numeric(10,2) while the window replayed numeric(20,8) values.
		mutate: func(a *ir.Table) { a.Columns[1].Type = ir.Decimal{Precision: 20, Scale: 8} },
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.typed) != 1 || w.typed[0].Name != "amount" {
				t.Fatalf("AlterColumnType calls = %+v; want [amount]", w.typed)
			}
			dec, ok := w.typed[0].Type.(ir.Decimal)
			if !ok || dec.Precision != 20 || dec.Scale != 8 {
				t.Errorf("retyped column = %v; want decimal(20,8)", w.typed[0].Type)
			}
		},
	},
	AlterAspectColumnNullable: {
		mutate: func(a *ir.Table) { a.Columns[1].Nullable = true },
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.nullability) != 1 || !w.nullability[0].Nullable {
				t.Errorf("AlterColumnNullability calls = %+v; want [amount nullable]", w.nullability)
			}
		},
	},
	AlterAspectColumnGenerated: {
		mutate: func(a *ir.Table) { a.Columns[1].GeneratedExpr = "id * 2" },
	},
	AlterAspectColumnDefault: {
		mutate: func(a *ir.Table) { a.Columns[1].Default = ir.DefaultLiteral{Value: "0"} },
	},
	AlterAspectPrimaryKey: {
		mutate: func(a *ir.Table) {
			a.PrimaryKey = &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "amount"}}}
		},
	},
	AlterAspectIndexCreated: {
		mutate: func(a *ir.Table) {
			a.Indexes = append(a.Indexes, &ir.Index{Name: "idx_id", Columns: []ir.IndexColumn{{Column: "id"}}})
		},
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.createdIdx) != 1 || w.createdIdx[0].Name != "idx_id" {
				t.Errorf("CreateShapeIndex calls = %+v; want [idx_id]", w.createdIdx)
			}
		},
	},
	AlterAspectIndexDropped: {
		mutate: func(a *ir.Table) { a.Indexes = nil },
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.droppedIdx) != 1 || w.droppedIdx[0].Name != "idx_amount" {
				t.Errorf("DropShapeIndex calls = %+v; want [idx_amount]", w.droppedIdx)
			}
		},
	},
	AlterAspectIndexRedefined: {
		mutate: func(a *ir.Table) { a.Indexes[0].Unique = true },
		wantCall: func(t *testing.T, w *recordingSchemaWriter) {
			t.Helper()
			if len(w.droppedIdx) != 1 || len(w.createdIdx) != 1 {
				t.Fatalf("redefine emitted drop=%d create=%d; want 1 and 1", len(w.droppedIdx), len(w.createdIdx))
			}
			if !w.createdIdx[0].Unique {
				t.Errorf("recreated index = %+v; want the UNIQUE after-shape", w.createdIdx[0])
			}
		},
	},
}

// fixturePair renders one aspect's before/after tables.
func fixturePair(f alterFixture) (before, after *ir.Table) {
	before = baseTable()
	after = cloneTable(before)
	f.mutate(after)
	return before, after
}

// ---- the gate ----

// TestAlterAspectsAllClassified: every aspect the diff can report has a
// disposition, and every disposition carries the reason it puts in the
// log line or the refusal.
func TestAlterAspectsAllClassified(t *testing.T) {
	for _, aspect := range AllAlterAspects() {
		verdict, reason, ok := DispositionOf(aspect)
		if !ok {
			t.Errorf("aspect %q has no disposition — classify it in alterDispositions (apply / no-DDL-with-proof / refuse)", aspect)
			continue
		}
		if reason == "" {
			t.Errorf("aspect %q (verdict %v) has an empty reason", aspect, verdict)
		}
	}
	// The registry must not carry entries for aspects the diff cannot
	// report — a stale disposition is a claim nobody checks.
	known := map[AlterAspect]bool{}
	for _, a := range AllAlterAspects() {
		known[a] = true
	}
	for aspect := range alterDispositions {
		if !known[aspect] {
			t.Errorf("alterDispositions has an entry for %q, which no comparator can report", aspect)
		}
	}
}

// TestAlterAspectMatrixIsAppliedOrRefused is the pin the bug needed:
// for EVERY aspect, a delta carrying it either reaches the engine's
// delta surface or refuses with the coded error. Nothing returns nil
// having done nothing (except the proven no-DDL aspects).
func TestAlterAspectMatrixIsAppliedOrRefused(t *testing.T) {
	for _, aspect := range AllAlterAspects() {
		f, ok := alterFixtures[aspect]
		if !ok {
			t.Errorf("no fixture for aspect %q — add one so the matrix stays exhaustive", aspect)
			continue
		}
		t.Run(string(aspect), func(t *testing.T) {
			before, after := fixturePair(f)
			// 1. The diff really emits this shape as an alter_table
			// delta — otherwise the aspect is untestable at the replay
			// site and the fixture is lying.
			deltas := DiffSchemas(&ir.Schema{Tables: []*ir.Table{before}}, &ir.Schema{Tables: []*ir.Table{after}})
			if aspect != AlterAspectTableIdentity {
				if len(deltas) != 1 || deltas[0].Kind != irbackup.SchemaDeltaAlterTable {
					t.Fatalf("DiffSchemas emitted %d delta(s) %+v; want one alter_table", len(deltas), deltas)
				}
			}
			// 2. The classifier names exactly this aspect.
			res := ClassifyAlterDelta(before, after)
			if !res.Has(aspect) {
				t.Fatalf("ClassifyAlterDelta = %v; want it to report %q", res.Aspects, aspect)
			}
			if len(res.Aspects) != 1 {
				t.Fatalf("fixture for %q is not minimal: aspects = %v", aspect, res.Aspects)
			}

			w := &recordingSchemaWriter{}
			err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
				Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after,
			}, AlterDeltaContext{
				SourceEngine: "postgres", TargetEngine: "postgres",
				BackupID: "incr-0001", Origin: "chain restore",
			})

			verdict, _, _ := DispositionOf(aspect)
			switch verdict {
			case AlterRefuse:
				assertDeltaRefusal(t, err)
				if w.calls() != 0 {
					t.Errorf("a refused aspect emitted %d DDL call(s)", w.calls())
				}
			case AlterNoDDL:
				if err != nil {
					t.Fatalf("no-DDL aspect returned %v; want nil", err)
				}
				if w.calls() != 0 {
					t.Errorf("a no-DDL aspect emitted %d DDL call(s)", w.calls())
				}
			case AlterApply:
				if err != nil {
					t.Fatalf("apply aspect returned %v; want nil", err)
				}
				if w.calls() == 0 {
					t.Fatal("apply aspect emitted NO DDL — this is the silent skip the gate exists to catch")
				}
				if f.wantCall != nil {
					f.wantCall(t, w)
				}
			}
		})
	}
}

// TestTablesEqualIsDefinedByTheAspectList pins the equivalence that
// makes the gate meaningful: the diff's equality verdict and the
// classifier's aspect list are the same fact. Every fixture must make
// tablesEqual disagree, and an unmutated pair must not.
func TestTablesEqualIsDefinedByTheAspectList(t *testing.T) {
	if !tablesEqual(baseTable(), baseTable()) {
		t.Fatal("tablesEqual on two identical tables = false")
	}
	for aspect, f := range alterFixtures {
		before, after := fixturePair(f)
		if tablesEqual(before, after) {
			t.Errorf("tablesEqual said EQUAL for a pair differing in %q — the diff would emit no delta and the replay would never see it", aspect)
		}
	}
}

// TestApplyAlterDelta_RetypeRefusesWhenEngineHasNoShapeSurface: the
// "refusing is correct where the engine does not support it" arm. An
// engine exposing only AlterAddColumn cannot retype, and silently
// continuing is exactly the rounding bug.
func TestApplyAlterDelta_RetypeRefusesWhenEngineHasNoShapeSurface(t *testing.T) {
	before, after := fixturePair(alterFixtures[AlterAspectColumnType])
	inner := &recordingSchemaWriter{}
	sw := shapeSurfaceHidden{SchemaWriter: inner, inner: inner}
	if _, isShape := any(sw).(ir.ShapeDeltaApplier); isShape {
		t.Fatal("test double unexpectedly satisfies ir.ShapeDeltaApplier")
	}
	err := ApplyAlterDelta(context.Background(), sw, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after,
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "broker"})
	assertDeltaRefusal(t, err)
	if inner.calls() != 0 {
		t.Errorf("refused delta emitted %d DDL call(s)", inner.calls())
	}
}

// TestApplyAlterDelta_MultiAspectWindowAppliesEachAspect: an
// incremental's delta is a WHOLE-WINDOW aggregate, so it legitimately
// carries several DDLs at once (unlike the ADR-0054 boundary
// classifier, which sees exactly one). An ADD COLUMN in the same window
// as a retype must not mask the retype — that combination is how the
// old add-columns-only loop hid the rounding in a delta it DID act on.
func TestApplyAlterDelta_MultiAspectWindowAppliesEachAspect(t *testing.T) {
	before := baseTable()
	after := cloneTable(before)
	after.Columns[1].Type = ir.Decimal{Precision: 20, Scale: 8}
	after.Columns = append(after.Columns, &ir.Column{Name: "note", Type: ir.Varchar{Length: 32}, Nullable: true})

	w := &recordingSchemaWriter{}
	if err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after,
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "chain restore"}); err != nil {
		t.Fatalf("ApplyAlterDelta: %v", err)
	}
	if len(w.addedColumns) != 1 {
		t.Errorf("addedColumns = %+v; want [note]", w.addedColumns)
	}
	if len(w.typed) != 1 {
		t.Errorf("AlterColumnType calls = %+v; want [amount] — the retype must not be masked by the add", w.typed)
	}
}

// TestApplyAlterDelta_CrossEngineRetypeIsRetargeted: the retype rides
// the same [translate.RetargetForEngine] pass the ADD COLUMN path
// always did, so a PG-source UUID column retyped mid-window reaches a
// MySQL target as CHAR(36), not as an unemittable PG type.
func TestApplyAlterDelta_CrossEngineRetypeIsRetargeted(t *testing.T) {
	before := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "external_id", Type: ir.Varchar{Length: 36}},
	}}
	after := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "external_id", Type: ir.UUID{}},
	}}
	w := &recordingSchemaWriter{}
	if err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: "users", Before: before, After: after,
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "mysql", Origin: "chain restore"}); err != nil {
		t.Fatalf("ApplyAlterDelta: %v", err)
	}
	if len(w.typed) != 1 {
		t.Fatalf("AlterColumnType calls = %+v; want one", w.typed)
	}
	if c, ok := w.typed[0].Type.(ir.Char); !ok || c.Length != 36 {
		t.Errorf("retargeted type = %T %+v; want ir.Char{Length:36}", w.typed[0].Type, w.typed[0].Type)
	}
}

// TestApplyAlterDelta_MalformedEntryRefuses: [DiffSchemas] always fills
// both sides of an alter_table (the manifest format pins it), so a
// half-shaped entry is a malformed or forged manifest — refuse rather
// than guess.
func TestApplyAlterDelta_MalformedEntryRefuses(t *testing.T) {
	after := baseTable()
	w := &recordingSchemaWriter{}
	err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: "orders", After: after,
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "chain restore"})
	assertDeltaRefusal(t, err)
}

// TestApplyAlterDelta_EqualShapesAreANoOp: a delta whose before and
// after agree (a forged or phantom entry) emits nothing and refuses
// nothing.
func TestApplyAlterDelta_EqualShapesAreANoOp(t *testing.T) {
	tbl := baseTable()
	w := &recordingSchemaWriter{}
	if err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: tbl.Name, Before: tbl, After: cloneTable(tbl),
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "chain restore"}); err != nil {
		t.Fatalf("ApplyAlterDelta on identical shapes: %v", err)
	}
	if w.calls() != 0 {
		t.Errorf("identical shapes emitted %d DDL call(s)", w.calls())
	}
}

// TestApplyAlterDelta_EngineErrorPropagates: an ALTER that fails on the
// target surfaces, never swallowed.
func TestApplyAlterDelta_EngineErrorPropagates(t *testing.T) {
	before, after := fixturePair(alterFixtures[AlterAspectColumnType])
	want := errors.New("target connection refused")
	w := &recordingSchemaWriter{err: want}
	err := ApplyAlterDelta(context.Background(), w, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: before.Name, Before: before, After: after,
	}, AlterDeltaContext{SourceEngine: "postgres", TargetEngine: "postgres", Origin: "chain restore"})
	if !errors.Is(err, want) {
		t.Errorf("err = %v; want a wrap of %v", err, want)
	}
}

func assertDeltaRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil; want the coded schema-delta refusal")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupSchemaDeltaUnsupported {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeBackupSchemaDeltaUnsupported)
	}
}
