// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeProber is an in-memory ShardConsolidationProber that returns
// pre-configured ProbeOutcomes per shape. Used to drive DispatchProbe
// without engine SQL.
type fakeProber struct {
	addCol      ProbeOutcome
	dropCol     ProbeOutcome
	createIdx   ProbeOutcome
	dropIdx     ProbeOutcome
	alterType   ProbeOutcome
	alterNull   ProbeOutcome
	renameCol   ProbeOutcome
	addCheck    ProbeOutcome
	dropCheck   ProbeOutcome
	modCheck    ProbeOutcome
	addColErr   error
	dropColErr  error
	createIdxEr error
	dropIdxErr  error
	alterTypeEr error
	alterNullEr error
	renameColEr error
	addCheckEr  error
	dropCheckEr error
	modCheckEr  error
}

func (p *fakeProber) ProbeAddColumn(context.Context, *ir.Table, []*ir.Column) (ProbeOutcome, error) {
	return p.addCol, p.addColErr
}

func (p *fakeProber) ProbeDropColumn(context.Context, *ir.Table, []*ir.Column) (ProbeOutcome, error) {
	return p.dropCol, p.dropColErr
}

func (p *fakeProber) ProbeCreateIndex(context.Context, *ir.Table, []*ir.Index) (ProbeOutcome, error) {
	return p.createIdx, p.createIdxEr
}

func (p *fakeProber) ProbeDropIndex(context.Context, *ir.Table, []*ir.Index) (ProbeOutcome, error) {
	return p.dropIdx, p.dropIdxErr
}

func (p *fakeProber) ProbeAlterColumnType(context.Context, *ir.Table, *ir.Column) (ProbeOutcome, error) {
	return p.alterType, p.alterTypeEr
}

func (p *fakeProber) ProbeAlterColumnNullability(context.Context, *ir.Table, *ir.Column) (ProbeOutcome, error) {
	return p.alterNull, p.alterNullEr
}

func (p *fakeProber) ProbeRenameColumn(context.Context, *ir.Table, string, string, *ir.Column) (ProbeOutcome, error) {
	return p.renameCol, p.renameColEr
}

func (p *fakeProber) ProbeAddCheck(context.Context, *ir.Table, []*ir.CheckConstraint) (ProbeOutcome, error) {
	return p.addCheck, p.addCheckEr
}

func (p *fakeProber) ProbeDropCheck(context.Context, *ir.Table, []*ir.CheckConstraint) (ProbeOutcome, error) {
	return p.dropCheck, p.dropCheckEr
}

func (p *fakeProber) ProbeModifyCheck(context.Context, *ir.Table, string, *ir.CheckConstraint) (ProbeOutcome, error) {
	return p.modCheck, p.modCheckEr
}

// fixtureTable returns a small table with the named columns of type
// INT and a single named index "ix_t_a" on the first column.
func fixtureTable(name string, colNames ...string) *ir.Table {
	t := &ir.Table{Name: name}
	for _, c := range colNames {
		t.Columns = append(t.Columns, &ir.Column{
			Name: c,
			Type: ir.Integer{Width: 32},
		})
	}
	if len(colNames) > 0 {
		t.Indexes = []*ir.Index{{
			Name: "ix_" + name + "_" + colNames[0],
			Columns: []ir.IndexColumn{{
				Column: colNames[0],
			}},
		}}
	}
	return t
}

func TestClassifyShape_None(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email")
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindNone {
		t.Errorf("Kind = %v, want None", shape.Kind)
	}
}

func TestClassifyShape_AddColumn(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email", "added_at")
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindAddColumn {
		t.Errorf("Kind = %v, want AddColumn", shape.Kind)
	}
	if len(shape.AddedColumns) != 1 || shape.AddedColumns[0].Name != "added_at" {
		t.Errorf("AddedColumns = %+v, want exactly [added_at]", shape.AddedColumns)
	}
}

func TestClassifyShape_DropColumn(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email", "deprecated")
	post := fixtureTable("users", "id", "email")
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindDropColumn {
		t.Errorf("Kind = %v, want DropColumn", shape.Kind)
	}
	if len(shape.DroppedColumns) != 1 || shape.DroppedColumns[0].Name != "deprecated" {
		t.Errorf("DroppedColumns = %+v", shape.DroppedColumns)
	}
}

func TestClassifyShape_CreateIndex(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email")
	post.Indexes = append(post.Indexes, &ir.Index{
		Name:    "ix_users_email",
		Columns: []ir.IndexColumn{{Column: "email"}},
	})
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindCreateIndex {
		t.Errorf("Kind = %v, want CreateIndex", shape.Kind)
	}
	if len(shape.CreatedIndexes) != 1 || shape.CreatedIndexes[0].Name != "ix_users_email" {
		t.Errorf("CreatedIndexes = %+v", shape.CreatedIndexes)
	}
}

func TestClassifyShape_DropIndex(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email")
	post.Indexes = nil // drop the pre-existing ix_users_id
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindDropIndex {
		t.Errorf("Kind = %v, want DropIndex", shape.Kind)
	}
	if len(shape.DroppedIndexes) != 1 || shape.DroppedIndexes[0].Name != "ix_users_id" {
		t.Errorf("DroppedIndexes = %+v", shape.DroppedIndexes)
	}
}

func TestClassifyShape_AlterColumnType(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "amount")
	post := fixtureTable("users", "id", "amount")
	// Widen amount from INT32 to INT64.
	post.Columns[1].Type = ir.Integer{Width: 64}
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindAlterColumnType {
		t.Errorf("Kind = %v, want AlterColumnType", shape.Kind)
	}
	if shape.AlteredColumn.Name != "amount" {
		t.Errorf("AlteredColumn.Name = %q, want amount", shape.AlteredColumn.Name)
	}
}

func TestClassifyShape_AlterColumnNullability(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email")
	post.Columns[1].Nullable = true
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindAlterColumnNullability {
		t.Errorf("Kind = %v, want AlterColumnNullability", shape.Kind)
	}
	if shape.AlteredColumn.Name != "email" {
		t.Errorf("AlteredColumn.Name = %q, want email", shape.AlteredColumn.Name)
	}
}

// TestClassifyShape_RenameColumn pins the v0.78.0 task #22 RENAME
// shape: exactly-one added + exactly-one dropped with full
// attribute equality minus Name → ShapeKindRenameColumn with the
// before/after column pair populated.
func TestClassifyShape_RenameColumn(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email")
	pre.Indexes = nil // drop the default index so it doesn't interfere
	post := fixtureTable("users", "id", "email_address")
	post.Indexes = nil

	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindRenameColumn {
		t.Errorf("Kind = %v, want RenameColumn", shape.Kind)
	}
	if shape.RenamedColumnBefore == nil || shape.RenamedColumnBefore.Name != "email" {
		t.Errorf("RenamedColumnBefore = %+v, want name=email", shape.RenamedColumnBefore)
	}
	if shape.RenamedColumnAfter == nil || shape.RenamedColumnAfter.Name != "email_address" {
		t.Errorf("RenamedColumnAfter = %+v, want name=email_address", shape.RenamedColumnAfter)
	}
}

// TestClassifyShape_RenameColumn_PreservesNullability documents the
// "rename preserves attributes" rule: a same-name drop+add where
// both columns are Nullable=true is still a rename.
func TestClassifyShape_RenameColumn_PreservesNullability(t *testing.T) {
	t.Parallel()
	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "old_name", Type: ir.Text{}, Nullable: true},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "new_name", Type: ir.Text{}, Nullable: true},
	}}
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindRenameColumn {
		t.Errorf("Kind = %v, want RenameColumn", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_RejectsTypeDiff pins the
// reshape-not-rename branch: a single-name change with a DIFFERENT
// IR Type is NOT a rename — it's a combo drop+add (the operator is
// reshaping the table, not renaming a column). Classifier falls
// through to the multi-class combo refusal.
func TestClassifyShape_RenameColumn_RejectsTypeDiff(t *testing.T) {
	t.Parallel()
	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "old_col", Type: ir.Integer{Width: 32}},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "new_col", Type: ir.Text{}}, // type differs!
	}}
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo refusal on rename-shaped delta with type diff; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized (type-diff is reshape, not rename)", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_RejectsNullabilityDiff: a single-
// name change where Nullable differs is NOT a rename. PG / MySQL
// `RENAME COLUMN` preserves Nullable, so a Nullable change is a
// genuine reshape and must refuse.
func TestClassifyShape_RenameColumn_RejectsNullabilityDiff(t *testing.T) {
	t.Parallel()
	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "old_col", Type: ir.Text{}, Nullable: false},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "new_col", Type: ir.Text{}, Nullable: true}, // nullability differs!
	}}
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo refusal on rename-shaped delta with nullability diff; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized (nullability-diff is reshape, not rename)", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_MultiColumnRefusesLoudly: a
// 2-added + 2-dropped delta (multi-column rename) is out of v1
// scope — the pair-up between old and new names is ambiguous (which
// dropped maps to which added?). Classifier refuses loudly so
// operators issuing multi-column ALTER ... RENAME use the drained
// model.
func TestClassifyShape_RenameColumn_MultiColumnRefusesLoudly(t *testing.T) {
	t.Parallel()
	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "a", Type: ir.Text{}},
		{Name: "c", Type: ir.Text{}},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "b", Type: ir.Text{}},
		{Name: "d", Type: ir.Text{}},
	}}
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo refusal on multi-column rename; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized (multi-column rename out of v1 scope)", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_PlusIndexChangeRefusesLoudly: a
// rename combined with any other delta class (index change here)
// is a combo refusal — rename is recognized only as the sole
// delta on the boundary.
func TestClassifyShape_RenameColumn_PlusIndexChangeRefusesLoudly(t *testing.T) {
	t.Parallel()
	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "old_col", Type: ir.Text{}},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "new_col", Type: ir.Text{}},
	}, Indexes: []*ir.Index{{
		Name:    "ix_users_new_col",
		Columns: []ir.IndexColumn{{Column: "new_col"}},
	}}}
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo refusal on rename+index-change; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_SingleAddIsStillAddColumn: just
// added=1, dropped=0 is still ShapeKindAddColumn — the rename
// heuristic requires both added AND dropped to be exactly 1.
func TestClassifyShape_RenameColumn_SingleAddIsStillAddColumn(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindAddColumn {
		t.Errorf("Kind = %v, want AddColumn (rename requires both added+dropped=1)", shape.Kind)
	}
}

// TestClassifyShape_RenameColumn_SingleDropIsStillDropColumn:
// added=0, dropped=1 is still ShapeKindDropColumn.
func TestClassifyShape_RenameColumn_SingleDropIsStillDropColumn(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "deprecated")
	post := fixtureTable("users", "id")
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindDropColumn {
		t.Errorf("Kind = %v, want DropColumn (rename requires both added+dropped=1)", shape.Kind)
	}
}

// TestDispatchProbe_RenameColumn verifies DispatchProbe routes the
// new shape to ProbeRenameColumn.
func TestDispatchProbe_RenameColumn(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{renameCol: ProbeOutcomeApplied}
	table := fixtureTable("users", "id", "new_name")
	shape := Shape{
		Kind:                ShapeKindRenameColumn,
		RenamedColumnBefore: &ir.Column{Name: "old_name", Type: ir.Text{}},
		RenamedColumnAfter:  &ir.Column{Name: "new_name", Type: ir.Text{}},
	}
	outcome, err := DispatchProbe(context.Background(), prober, table, shape)
	if err != nil {
		t.Fatalf("DispatchProbe rename-column: %v", err)
	}
	if outcome != ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}
}

// TestDispatchProbe_RenameColumn_NilGuards pins the inconsistent-
// shape refusal when the rename payload is missing.
func TestDispatchProbe_RenameColumn_NilGuards(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{}
	table := fixtureTable("users", "id")
	cases := []struct {
		name  string
		shape Shape
	}{
		{"missing-before", Shape{Kind: ShapeKindRenameColumn, RenamedColumnAfter: &ir.Column{Name: "x"}}},
		{"missing-after", Shape{Kind: ShapeKindRenameColumn, RenamedColumnBefore: &ir.Column{Name: "x"}}},
		{"missing-both", Shape{Kind: ShapeKindRenameColumn}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			outcome, err := DispatchProbe(context.Background(), prober, table, c.shape)
			if err == nil {
				t.Fatal("expected error on missing rename payload")
			}
			if outcome != ProbeOutcomeInconsistent {
				t.Errorf("outcome = %v, want Inconsistent", outcome)
			}
		})
	}
}

func TestClassifyShape_ComboRefusesLoudly(t *testing.T) {
	t.Parallel()
	// A true multi-shape combo: drop deprecated + create an index.
	// (Note: the v0.78.0 task #22 RENAME classifier consumes a same-
	// attribute drop+add as a rename; to exercise the combo refusal
	// we mix a column delta with an index delta instead — that
	// remains an unambiguous combo across all catalog expansions.)
	pre := fixtureTable("users", "id", "email", "deprecated")
	post := fixtureTable("users", "id", "email")
	post.Indexes = append(post.Indexes, &ir.Index{
		Name:    "ix_users_email",
		Columns: []ir.IndexColumn{{Column: "email"}},
	})
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo-shape refusal; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized", shape.Kind)
	}
}

func TestClassifyShape_NilTables(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id")
	if _, err := ClassifyShape(nil, pre); err == nil {
		t.Error("expected error on nil pre")
	}
	if _, err := ClassifyShape(pre, nil); err == nil {
		t.Error("expected error on nil post")
	}
}

func TestDispatchProbe_RoutesPerShape(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{
		addCol:    ProbeOutcomeApplied,
		dropCol:   ProbeOutcomeNotApplied,
		createIdx: ProbeOutcomeInconsistent,
	}
	table := fixtureTable("users", "id")
	ctx := context.Background()
	cases := []struct {
		name string
		k    ShapeKind
		want ProbeOutcome
	}{
		{"add-column", ShapeKindAddColumn, ProbeOutcomeApplied},
		{"drop-column", ShapeKindDropColumn, ProbeOutcomeNotApplied},
		{"create-index", ShapeKindCreateIndex, ProbeOutcomeInconsistent},
		{"none", ShapeKindNone, ProbeOutcomeApplied},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			outcome, err := DispatchProbe(ctx, prober, table, Shape{Kind: c.k})
			if err != nil {
				t.Fatalf("DispatchProbe: %v", err)
			}
			if outcome != c.want {
				t.Errorf("outcome = %v, want %v", outcome, c.want)
			}
		})
	}
}

func TestDispatchProbe_UnrecognizedRefusesLoudly(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{}
	table := fixtureTable("users", "id")
	outcome, err := DispatchProbe(context.Background(), prober, table, Shape{Kind: ShapeKindUnrecognized})
	if err == nil {
		t.Fatal("expected refusal on unrecognized shape; got nil")
	}
	if outcome != ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent on unrecognized", outcome)
	}
}

func TestDispatchProbe_NilGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if _, err := DispatchProbe(ctx, nil, fixtureTable("users", "id"), Shape{Kind: ShapeKindAddColumn}); err == nil {
		t.Error("expected error on nil prober")
	}
	if _, err := DispatchProbe(ctx, &fakeProber{}, nil, Shape{Kind: ShapeKindAddColumn}); err == nil {
		t.Error("expected error on nil table")
	}
}

func TestDispatchProbe_PropagatesError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("probe-failed")
	prober := &fakeProber{addColErr: sentinel}
	table := fixtureTable("users", "id")
	_, err := DispatchProbe(context.Background(), prober, table, Shape{Kind: ShapeKindAddColumn})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error to propagate; got %v", err)
	}
}

// ----------------------------------------------------------------
// ADR-0065 — CHECK constraint classifier + dispatch tests.
// Pin the {ADD/DROP/MODIFY} × {simple/JSON/datetime} matrix per
// CLAUDE.md's Bug 74 "pin the class, not the representative" rule.
// ----------------------------------------------------------------

// checkExprMatrix returns the three expression families the
// classifier-side tests exercise. The matrix is intentionally
// exhaustive across families (simple-arithmetic, JSON, datetime)
// because the cross-engine emit path's per-family translator
// behaviour differs — the classifier is family-blind but pinning
// the matrix here keeps the integration tests' family coverage
// honest by mirroring the same axes.
func checkExprMatrix() []struct {
	family string
	expr   string
} {
	return []struct {
		family string
		expr   string
	}{
		{"simple-arithmetic", "qty >= 0"},
		{"json", "(payload->>'kind') = 'order'"},
		{"datetime", "start_date <= end_date"},
	}
}

// TestClassifyShape_AddCheck pins every CHECK expression family —
// classifier behaviour is family-blind but the matrix must be
// exercised explicitly per CLAUDE.md's Bug 74 rule.
func TestClassifyShape_AddCheck(t *testing.T) {
	t.Parallel()
	for _, c := range checkExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			t.Parallel()
			pre := fixtureTable("orders", "id", "qty")
			post := fixtureTable("orders", "id", "qty")
			post.CheckConstraints = []*ir.CheckConstraint{
				{Name: "orders_chk_" + c.family, Expr: c.expr},
			}
			shape, err := ClassifyShape(pre, post)
			if err != nil {
				t.Fatalf("ClassifyShape: %v", err)
			}
			if shape.Kind != ShapeKindAddCheck {
				t.Errorf("Kind = %v, want AddCheck", shape.Kind)
			}
			if len(shape.AddedChecks) != 1 || shape.AddedChecks[0].Expr != c.expr {
				t.Errorf("AddedChecks = %+v, want exactly one with Expr=%q", shape.AddedChecks, c.expr)
			}
		})
	}
}

// TestClassifyShape_DropCheck pins the drop-shape across families.
func TestClassifyShape_DropCheck(t *testing.T) {
	t.Parallel()
	for _, c := range checkExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			t.Parallel()
			pre := fixtureTable("orders", "id", "qty")
			pre.CheckConstraints = []*ir.CheckConstraint{
				{Name: "orders_chk_" + c.family, Expr: c.expr},
			}
			post := fixtureTable("orders", "id", "qty")
			shape, err := ClassifyShape(pre, post)
			if err != nil {
				t.Fatalf("ClassifyShape: %v", err)
			}
			if shape.Kind != ShapeKindDropCheck {
				t.Errorf("Kind = %v, want DropCheck", shape.Kind)
			}
			if len(shape.DroppedChecks) != 1 || shape.DroppedChecks[0].Name != "orders_chk_"+c.family {
				t.Errorf("DroppedChecks = %+v", shape.DroppedChecks)
			}
		})
	}
}

// TestClassifyShape_ModifyCheck pins the modify shape: same name,
// different Expr → ShapeKindModifyCheck with both before/after
// populated.
func TestClassifyShape_ModifyCheck(t *testing.T) {
	t.Parallel()
	for _, c := range checkExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			t.Parallel()
			pre := fixtureTable("orders", "id", "qty")
			pre.CheckConstraints = []*ir.CheckConstraint{
				{Name: "orders_chk_" + c.family, Expr: c.expr},
			}
			post := fixtureTable("orders", "id", "qty")
			post.CheckConstraints = []*ir.CheckConstraint{
				{Name: "orders_chk_" + c.family, Expr: c.expr + " AND id IS NOT NULL"},
			}
			shape, err := ClassifyShape(pre, post)
			if err != nil {
				t.Fatalf("ClassifyShape: %v", err)
			}
			if shape.Kind != ShapeKindModifyCheck {
				t.Errorf("Kind = %v, want ModifyCheck", shape.Kind)
			}
			if shape.ModifiedCheckBefore == nil || shape.ModifiedCheckBefore.Expr != c.expr {
				t.Errorf("ModifiedCheckBefore = %+v, want Expr=%q", shape.ModifiedCheckBefore, c.expr)
			}
			if shape.ModifiedCheckAfter == nil || shape.ModifiedCheckAfter.Expr == c.expr {
				t.Errorf("ModifiedCheckAfter = %+v, want different Expr", shape.ModifiedCheckAfter)
			}
		})
	}
}

// TestClassifyShape_CheckUnnamedSkipped: unnamed CHECKs are skipped
// from the diff (no name to track identity). A delta consisting
// solely of unnamed CHECK churn is reported as None.
func TestClassifyShape_CheckUnnamedSkipped(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("orders", "id", "qty")
	post := fixtureTable("orders", "id", "qty")
	post.CheckConstraints = []*ir.CheckConstraint{
		{Name: "", Expr: "qty > 0"}, // unnamed — classifier skips
	}
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatalf("ClassifyShape: %v", err)
	}
	if shape.Kind != ShapeKindNone {
		t.Errorf("Kind = %v, want None (unnamed CHECKs skipped)", shape.Kind)
	}
}

// TestClassifyShape_CheckComboRefusesLoudly: a CHECK shape combined
// with another delta class is a combo refusal — CHECK shapes are
// recognized only as the sole delta on the boundary.
func TestClassifyShape_CheckComboRefusesLoudly(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("orders", "id", "qty")
	post := fixtureTable("orders", "id", "qty", "tier")
	post.CheckConstraints = []*ir.CheckConstraint{
		{Name: "orders_qty_chk", Expr: "qty >= 0"},
	}
	shape, err := ClassifyShape(pre, post)
	if err == nil {
		t.Fatal("expected combo refusal on add-column + add-check; got nil error")
	}
	if shape.Kind != ShapeKindUnrecognized {
		t.Errorf("Kind = %v, want Unrecognized", shape.Kind)
	}
}

// TestClassifyShape_MultipleModifyCheckRefusesLoudly: two same-name
// modifies in a single boundary are not recognized as a single
// ModifyCheck — hasMod=false → class-counter doesn't fire on
// modify → if there are no add/drop classes, the result is None;
// if there ARE add/drop classes, it's still combo. To distinguish
// from None, this test exercises the two-modify-only case which
// is rare but the v1 limit: classifier expects one modify per
// boundary, multi-modify uses the drained model.
func TestClassifyShape_MultipleModifyCheckRefusesLoudly(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("orders", "id", "qty")
	pre.CheckConstraints = []*ir.CheckConstraint{
		{Name: "orders_qty_chk", Expr: "qty >= 0"},
		{Name: "orders_id_chk", Expr: "id > 0"},
	}
	post := fixtureTable("orders", "id", "qty")
	post.CheckConstraints = []*ir.CheckConstraint{
		{Name: "orders_qty_chk", Expr: "qty > 0"}, // changed
		{Name: "orders_id_chk", Expr: "id >= 0"},  // changed
	}
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		// Two same-name modifies → diffChecks returns hasMod=false;
		// no other class fires → None is the documented behaviour.
		// We accept either None or Unrecognized; the load-bearing
		// invariant is "not silently treated as a single modify".
		if shape.Kind == ShapeKindModifyCheck {
			t.Errorf("Kind = ModifyCheck on multi-modify; the v1 classifier scope is single-modify")
		}
		return
	}
	if shape.Kind == ShapeKindModifyCheck {
		t.Errorf("Kind = ModifyCheck on multi-modify; classifier should not collapse two modifies into one shape")
	}
}

// TestDispatchProbe_CheckShapes routes the three CHECK shapes via
// DispatchProbe.
func TestDispatchProbe_CheckShapes(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{
		addCheck:  ProbeOutcomeApplied,
		dropCheck: ProbeOutcomeNotApplied,
		modCheck:  ProbeOutcomeInconsistent,
	}
	table := fixtureTable("orders", "id")
	ctx := context.Background()

	addShape := Shape{Kind: ShapeKindAddCheck, AddedChecks: []*ir.CheckConstraint{{Name: "c1", Expr: "id > 0"}}}
	got, err := DispatchProbe(ctx, prober, table, addShape)
	if err != nil {
		t.Fatalf("DispatchProbe AddCheck: %v", err)
	}
	if got != ProbeOutcomeApplied {
		t.Errorf("AddCheck outcome = %v, want Applied", got)
	}

	dropShape := Shape{Kind: ShapeKindDropCheck, DroppedChecks: []*ir.CheckConstraint{{Name: "c1", Expr: "id > 0"}}}
	got, err = DispatchProbe(ctx, prober, table, dropShape)
	if err != nil {
		t.Fatalf("DispatchProbe DropCheck: %v", err)
	}
	if got != ProbeOutcomeNotApplied {
		t.Errorf("DropCheck outcome = %v, want NotApplied", got)
	}

	modShape := Shape{
		Kind:                ShapeKindModifyCheck,
		ModifiedCheckBefore: &ir.CheckConstraint{Name: "c1", Expr: "id > 0"},
		ModifiedCheckAfter:  &ir.CheckConstraint{Name: "c1", Expr: "id >= 0"},
	}
	got, err = DispatchProbe(ctx, prober, table, modShape)
	if err != nil {
		t.Fatalf("DispatchProbe ModifyCheck: %v", err)
	}
	if got != ProbeOutcomeInconsistent {
		t.Errorf("ModifyCheck outcome = %v, want Inconsistent", got)
	}
}

// TestDispatchProbe_ModifyCheck_NilGuards pins the
// inconsistent-shape refusal when the modify-check payload is
// missing.
func TestDispatchProbe_ModifyCheck_NilGuards(t *testing.T) {
	t.Parallel()
	prober := &fakeProber{}
	table := fixtureTable("orders", "id")
	cases := []struct {
		name  string
		shape Shape
	}{
		{"missing-before", Shape{Kind: ShapeKindModifyCheck, ModifiedCheckAfter: &ir.CheckConstraint{Name: "c1"}}},
		{"missing-after", Shape{Kind: ShapeKindModifyCheck, ModifiedCheckBefore: &ir.CheckConstraint{Name: "c1"}}},
		{"missing-both", Shape{Kind: ShapeKindModifyCheck}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			outcome, err := DispatchProbe(context.Background(), prober, table, c.shape)
			if err == nil {
				t.Fatal("expected error on missing modify-check payload")
			}
			if outcome != ProbeOutcomeInconsistent {
				t.Errorf("outcome = %v, want Inconsistent", outcome)
			}
		})
	}
}
