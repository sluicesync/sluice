// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/orware/sluice/internal/ir"
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
	addColErr   error
	dropColErr  error
	createIdxEr error
	dropIdxErr  error
	alterTypeEr error
	alterNullEr error
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

func TestClassifyShape_ComboRefusesLoudly(t *testing.T) {
	t.Parallel()
	pre := fixtureTable("users", "id", "email", "deprecated")
	post := fixtureTable("users", "id", "email", "added_at")
	// Both DropColumn (deprecated) AND AddColumn (added_at) in one
	// boundary — refuse loudly per ADR-0054 DP-E.
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
