// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCarrySourceDefaults pins the unit contract of the forwarded-ADD-COLUMN
// default carry (the pre-existing-row gate's defect 1): an added column whose
// change stream carried no DEFAULT gets the source's; a column that carried
// one keeps it; the shared snapshot is not mutated; a read failure refuses;
// and without a carrier the IR passes through unchanged.
func TestCarrySourceDefaults(t *testing.T) {
	base := func() ir.SchemaSnapshot {
		return ir.SchemaSnapshot{Schema: "public", Table: "w", IR: &ir.Table{Schema: "public", Name: "w", Columns: []*ir.Column{
			{Name: "id"},
			{Name: "a"},
			{Name: "b", Default: ir.DefaultLiteral{Value: "in-band"}},
		}}}
	}
	added := func(snap ir.SchemaSnapshot) []*ir.Column { return snap.IR.Columns[1:] }

	snap := base()
	var asked []string
	deps := schemaForwardDeps{defaultCarrier: func(_ context.Context, _, _, col string) (ir.DefaultValue, error) {
		asked = append(asked, col)
		return ir.DefaultLiteral{Value: "src-" + col}, nil
	}}
	post, err := carrySourceDefaults(context.Background(), deps, "public.w", snap, added(snap))
	if err != nil {
		t.Fatal(err)
	}
	if got := post.Columns[1].Default; got != (ir.DefaultLiteral{Value: "src-a"}) {
		t.Errorf("column a default = %#v; want the carried source default", got)
	}
	if got := post.Columns[2].Default; got != (ir.DefaultLiteral{Value: "in-band"}) {
		t.Errorf("column b default = %#v; an in-band default must be kept", got)
	}
	if len(asked) != 1 || asked[0] != "a" {
		t.Errorf("carrier asked for %v; want only the column the stream carried no default for", asked)
	}
	if snap.IR.Columns[1].Default != nil {
		t.Error("the shared snapshot IR was mutated; the carry must return a copy")
	}

	snap = base()
	deps.defaultCarrier = func(context.Context, string, string, string) (ir.DefaultValue, error) {
		return nil, errors.New("catalog read failed")
	}
	if _, err := carrySourceDefaults(context.Background(), deps, "public.w", snap, added(snap)); err == nil || !strings.Contains(err.Error(), "catalog read failed") {
		t.Errorf("a failed read = %v; want a refusal carrying the cause", err)
	}

	snap = base()
	post, err = carrySourceDefaults(context.Background(), schemaForwardDeps{}, "public.w", snap, added(snap))
	if err != nil || post != snap.IR {
		t.Errorf("no carrier: post=%p err=%v; want the snapshot IR unchanged", post, err)
	}
}
