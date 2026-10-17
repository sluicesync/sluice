// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyDoor_AcceptanceBoundToShape pins round-5 MEDIUM-1: an
// accepted key is honoured mid-stream only while it keeps the shape the
// open door recorded. A forwarded DROP COLUMN that narrows it under the same
// name refuses, naming the accepted shape, and so does an accepted key the
// open door never saw (its table joined while the stream ran).
func TestUniqueKeyDoor_AcceptanceBoundToShape(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	s, _, target := ukDoorStreamer(t, ir.CDCLogicalReplication,
		[]*ir.Table{ukDoorTable("kd", pk), ukDoorTable("kn", pk)},
		[]*ir.Table{ukDoorTable("kd", pk, ukDoorKey("uk", "k", "v"))})
	s.AcceptTargetOnlyUniqueKeys = []string{"kd.uk", "kn.uk"}
	ctx := context.Background()
	if err := s.phaseRefuseTargetOnlyUniqueKeys(ctx, nil, "s1", true); err != nil {
		t.Fatalf("open with the key accepted: %v", err)
	}
	boundary := s.uniqueKeyChecksFor().boundary

	// The key keeps its shape: still honoured.
	if refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kd", IR: ukDoorTable("kd", pk)}); err != nil || refusal != nil {
		t.Fatalf("unchanged accepted key: refusal %v, err %v", refusal, err)
	}
	// Narrowed under its name: refused, naming the accepted shape.
	target.schemas["tgt-dsn"].Tables[0] = ukDoorTable("kd", pk, ukDoorKey("uk", "k"))
	refusal, err := boundary(ctx, ir.SchemaSnapshot{Table: "kd", IR: ukDoorTable("kd", pk)})
	if err != nil || refusal == nil || refusal.Keys[0].AcceptedShape != "(k, v)" ||
		!strings.Contains(refusal.Error(), "accepted by name as (k, v)") {
		t.Fatalf("narrowed accepted key: refusal %v, err %v", refusal, err)
	}
	// An accepted key the open door never saw.
	target.schemas["tgt-dsn"].Tables = append(target.schemas["tgt-dsn"].Tables, ukDoorTable("kn", pk, ukDoorKey("uk", "k")))
	refusal, err = boundary(ctx, ir.SchemaSnapshot{Table: "kn", IR: ukDoorTable("kn", pk)})
	if err != nil || refusal == nil || !refusal.Keys[0].AcceptedUnrecorded {
		t.Fatalf("unrecorded accepted key: refusal %v, err %v", refusal, err)
	}
}

// TestUniqueKeyDoor_AmbiguousAcceptance pins round-5 LOW-3: on a
// multi-database stream a 2-part acceptance that accepts the key in two
// databases refuses at the open door, naming the 3-part form; the 3-part
// form passes.
func TestUniqueKeyDoor_AmbiguousAcceptance(t *testing.T) {
	pk := ukDoorKey("PRIMARY", "id")
	source := &ukDoorEngine{
		resumeDispatchEngine: &resumeDispatchEngine{name: "src", caps: ir.Capabilities{CDC: ir.CDCBinlog}},
		schemas: map[string]*ir.Schema{
			"src-a": {Tables: []*ir.Table{ukDoorTable("kd", pk)}}, "src-b": {Tables: []*ir.Table{ukDoorTable("kd", pk)}},
		},
	}
	tgt := func() *ukDoorEngine {
		return &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "tgt"},
			schemas: map[string]*ir.Schema{
				"tgt-a": {Tables: []*ir.Table{ukDoorTable("kd", pk, ukDoorKey("uk", "k"))}},
				"tgt-b": {Tables: []*ir.Table{ukDoorTable("kd", pk, ukDoorKey("uk", "k"))}},
			},
		}
	}
	scopes := []uniqueKeyDoorScope{
		{namespace: "a", targetNamespace: "a", sourceDSN: "src-a", targetDSN: "tgt-a"},
		{namespace: "b", targetNamespace: "b", sourceDSN: "src-b", targetDSN: "tgt-b"},
	}
	for _, c := range []struct {
		accept []string
		want   string // "" passes
	}{
		{[]string{"kd.uk"}, "<database>.kd.uk"},
		{[]string{"a.kd.uk", "b.kd.uk"}, ""},
	} {
		s := &Streamer{Source: source, Target: tgt(), AcceptTargetOnlyUniqueKeys: c.accept}
		_, _, err := s.compareUniqueKeys(context.Background(), scopes, uniqueKeyCompare{
			acceptedShapes: map[string]string{}, recordAccepted: true,
		})
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%v: %v", c.accept, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%v: err %v, want it to name %s", c.accept, err, c.want)
		}
	}
}

// TestAddTableUniqueKeyDoor_CollationAsRead pins round-5 LOW-6: add-table
// judges collation direction on the source table as READ, so an override
// toward a looser collation (`--type-override` to a `_ci` column onto a `_ci`
// target key) cannot hide that the source key is case-sensitive.
func TestAddTableUniqueKeyDoor_CollationAsRead(t *testing.T) {
	loose := map[string]ir.CollationLoosening{"bin": 0, "ci": ir.LooseCase | ir.LooseAccent}
	eng := ukLooseEngine{name: "mysql", loose: loose}
	table := func(coll string) *ir.Table {
		return &ir.Table{
			Name: "kd", Columns: []*ir.Column{{Name: "k", Type: ir.Varchar{Length: 9, Collation: coll}}},
			Indexes: []*ir.Index{ukDoorKey("uk", "k")},
		}
	}
	overridden, asRead, target := table("ci"), table("bin"), map[string]*ir.Table{"kd": table("ci")}
	if err := refuseAddTableTargetOnlyUniqueKeysAsRead(overridden, nil, target, eng, eng, nil); err != nil {
		t.Fatalf("the overridden table alone (the old behaviour) refused: %v", err)
	}
	err := refuseAddTableTargetOnlyUniqueKeysAsRead(overridden, asRead, target, eng, eng, nil)
	if err == nil || !strings.Contains(err.Error(), "the target's k compares case-") {
		t.Fatalf("the source as read is case-sensitive, the target key folds: err %v", err)
	}
}

// TestUniqueKeyDoorScopeFor pins round-5 LOW-5: a namespace no scope of a
// multi-database stream names is not scope 0 (whose filter would judge
// nothing for the boundary) but no scope, which the boundary check refuses.
func TestUniqueKeyDoorScopeFor(t *testing.T) {
	single := &uniqueKeyDoorState{scopes: []uniqueKeyDoorScope{{}}}
	if i, ok := single.scopeFor("anything"); i != 0 || !ok {
		t.Errorf("single-namespace: %d, %v", i, ok)
	}
	multi := &uniqueKeyDoorState{scopes: []uniqueKeyDoorScope{{namespace: "a"}, {namespace: "b"}}}
	if i, ok := multi.scopeFor("B"); i != 1 || !ok {
		t.Errorf("named namespace: %d, %v", i, ok)
	}
	if _, ok := multi.scopeFor("c"); ok {
		t.Error("an unnamed namespace found a scope")
	}
}

// r5Gate builds a boundary gate over table kd whose check records what it
// judged.
func r5Gate(seedCols map[string]string, seedTypes map[string]map[string]ir.Type) (gate *uniqueKeyBoundaryGate, judgedTables *[]string) {
	var judged []string
	g := newUniqueKeyBoundaryGate(uniqueKeyChecks{
		boundary: func(_ context.Context, s ir.SchemaSnapshot) (*ir.SourceUniqueKeyAbsentError, error) {
			judged = append(judged, s.Table)
			return nil, nil
		},
		colsKey:   func(_, table string) string { return table },
		seedCols:  seedCols,
		seedTypes: seedTypes,
	}, &atomic.Pointer[error]{})
	return g, &judged
}

func r5Snap(table string, cols map[string]ir.Type, order ...string) ir.SchemaSnapshot {
	t := &ir.Table{Name: table}
	for _, n := range order {
		t.Columns = append(t.Columns, &ir.Column{Name: n, Type: cols[n]})
	}
	return ir.SchemaSnapshot{Table: table, IR: t, Position: ir.Position{Token: "0/0"}}
}

// TestUniqueKeyBoundaryGate_EarlyNeedsUnchangedTypes pins round-5 LOW-1: a
// boundary that adds a column AND changes a kept column's collation is not
// judged before its DDL (it could change a target key), but at the late
// call; a purely additive one is judged early.
func TestUniqueKeyBoundaryGate_EarlyNeedsUnchangedTypes(t *testing.T) {
	ci := ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci"}
	bin := ir.Varchar{Length: 20, Collation: "utf8mb4_bin"}
	seed := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ci}, "id", "k")
	g, judged := r5Gate(map[string]string{"kd": columnList(seed.IR)}, map[string]map[string]ir.Type{"kd": columnTypes(seed.IR)})
	early, late := g.beforeDDLHook(), g.hook()
	ctx := context.Background()

	modified := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": bin, "n": ir.Integer{Width: 32}}, "id", "k", "n")
	if !early(ctx, modified) || len(*judged) != 0 {
		t.Fatalf("an ADD plus a kept column's collation change was judged before its DDL: %v", *judged)
	}
	if !late(ctx, modified) || len(*judged) != 1 {
		t.Fatalf("it was not judged after its DDL: %v", *judged)
	}
	added := r5Snap("kd", map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": bin, "n": ir.Integer{Width: 32}, "m": ir.Integer{Width: 32}}, "id", "k", "n", "m")
	if !early(ctx, added) || len(*judged) != 2 {
		t.Fatalf("a purely additive boundary was not judged early: %v", *judged)
	}
}

// TestUniqueKeyBoundaryGate_LateSkipOnlyNext pins round-5 LOW-2: the late
// call skips only the boundary the IMMEDIATELY preceding early call judged.
// A later snapshot with the same columns and the same (constant, pgoutput
// 0/0) position token, flagged UniqueKeysMoved, is judged.
func TestUniqueKeyBoundaryGate_LateSkipOnlyNext(t *testing.T) {
	types := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Text{}, "n": ir.Integer{Width: 32}}
	seed := r5Snap("kd", types, "id", "k")
	g, judged := r5Gate(map[string]string{"kd": columnList(seed.IR)}, nil)
	early, late := g.beforeDDLHook(), g.hook()
	ctx := context.Background()

	added := r5Snap("kd", types, "id", "k", "n")
	if !early(ctx, added) || !late(ctx, added) || len(*judged) != 1 {
		t.Fatalf("early then late: judged %v, want once", *judged)
	}
	again := r5Snap("kd", types, "id", "k", "n")
	again.UniqueKeysMoved = true
	if !late(ctx, again) || len(*judged) != 2 {
		t.Fatalf("a flagged snapshot with the same identity was skipped: judged %v", *judged)
	}
	// An early call whose late call never came (the intercept stopped) does
	// not let a later late call skip.
	g2, judged2 := r5Gate(map[string]string{"kd": columnList(seed.IR)}, nil)
	_ = g2.beforeDDLHook()(ctx, added)
	other := r5Snap("kd", types, "id", "k")
	_ = g2.hook()(ctx, other)
	moved := r5Snap("kd", types, "id", "k", "n")
	moved.UniqueKeysMoved = true
	if !g2.hook()(ctx, moved) || len(*judged2) != 3 {
		t.Fatalf("judged %v, want every call after the skipped one judged", *judged2)
	}
}

// TestUniqueKeyBoundaryGate_FoldedSeedIsReadOnly pins round-5 LOW-4: a
// table the change stream spells differently from the open read finds the
// open door's baseline, but a table that only folds onto a seeded one (one
// added mid-run) never overwrites that baseline; folded spellings that
// collide are no baseline at all.
func TestUniqueKeyBoundaryGate_FoldedSeedIsReadOnly(t *testing.T) {
	types := map[string]ir.Type{"id": ir.Integer{Width: 32}, "k": ir.Text{}, "n": ir.Integer{Width: 32}}
	base := columnList(r5Snap("t", types, "id", "k").IR)
	g, judged := r5Gate(map[string]string{"Orders": base, "Users": base, "users": base}, nil)
	ctx := context.Background()

	// "orders" folds onto the seeded "Orders": the moved list is judged.
	if !g.hook()(ctx, r5Snap("orders", types, "id", "k", "n")) || strings.Join(*judged, ",") != "orders" {
		t.Fatalf("a respelled seeded table was not judged against its seed: %v", *judged)
	}
	if g.prevCols["Orders"] != base {
		t.Fatalf("the seeded baseline was overwritten by another spelling: %q", g.prevCols["Orders"])
	}
	// "USERS" folds onto two seeded tables: no baseline, not judged.
	if !g.hook()(ctx, r5Snap("USERS", types, "id", "k", "n")) || len(*judged) != 1 {
		t.Fatalf("an ambiguous folded spelling found a baseline: %v", *judged)
	}
}
