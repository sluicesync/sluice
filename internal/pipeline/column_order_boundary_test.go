// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// kdTable builds the RENAME-SWAP-FORWARD fixture table kd(id, cols...).
func kdTable(cols ...*ir.Column) *ir.Table {
	all := append([]*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}, cols...)
	return &ir.Table{
		Schema: "public", Name: "kd", Columns: all,
		PrimaryKey: &ir.Index{Name: "pk_kd", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func kdInt(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 32}, Nullable: true}
}

func kdWith(c *ir.Column, mutate func(*ir.Column)) *ir.Column {
	out := *c
	mutate(&out)
	return &out
}

// kdRecord is a column-order record for kd: the source's and the target's
// orders, each beginning with id.
func kdRecord(source, target []string) ir.ColumnOrderRecord {
	return ir.ColumnOrderRecord{
		Schema: "public", Table: "kd",
		SourceColumns: append([]string{"id"}, source...),
		TargetColumns: append([]string{"id"}, target...),
	}
}

// kdWitness is a witness over a target holding target, with records (nil:
// the target keeps none).
func kdWitness(target *ir.Table, records []ir.ColumnOrderRecord) (*firstBoundaryWitness, *fakeCatalog) {
	cat := &fakeCatalog{tables: map[string]*ir.Table{"kd": target}}
	w := newFakeWitness(cat, "postgres", "postgres")
	w.orders = newColumnOrderRecords(records != nil, records)
	return w, cat
}

// TestClassifyShape_ColumnIdentityClass is the RENAME-SWAP-FORWARD
// classifier matrix: every shape a by-name comparison cannot see, and the
// sibling it read as "no change" (more than one altered column), each
// refused with its own error — and the shapes on either side of the class
// still classify as they did. The independent expected value is the table
// written here from the DDL each row stands for.
func TestClassifyShape_ColumnIdentityClass(t *testing.T) {
	t.Parallel()
	vc := func(name string) *ir.Column {
		return &ir.Column{Name: name, Type: ir.Varchar{Length: 20}, Nullable: true}
	}
	dec := func(name string, s int) *ir.Column {
		return &ir.Column{Name: name, Type: ir.Decimal{Precision: 12, Scale: s}, Nullable: true}
	}
	notNull := func(c *ir.Column) *ir.Column { return kdWith(c, func(c *ir.Column) { c.Nullable = false }) }
	withIDs := func(t *ir.Table, ids ...int) *ir.Table {
		for i, c := range t.Columns {
			c.StableID = ids[i]
		}
		return t
	}

	const (
		order   = "order"
		multi   = "multi"
		replace = "identity"
	)
	cases := []struct {
		name      string
		pre, post *ir.Table
		refuse    string    // the refusal expected; empty when it classifies
		want      ShapeKind // when it classifies
	}{
		// The class.
		{"same-type swap (RENAME b TO c, c TO b)", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("c"), kdInt("b")), order, 0},
		{"three-way rotation", kdTable(kdInt("b"), kdInt("c"), kdInt("d")), kdTable(kdInt("c"), kdInt("d"), kdInt("b")), order, 0},
		{"swap across types", kdTable(kdInt("b"), vc("c")), kdTable(kdInt("c"), vc("b")), order, 0},
		{"swap with a retype", kdTable(kdInt("b"), kdInt("c")), kdTable(dec("c", 0), kdInt("b")), order, 0},
		{"swap beside an added column", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("c"), kdInt("b"), kdInt("x")), order, 0},
		{"reorder (MODIFY c AFTER id)", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("c"), kdInt("b")), order, 0},
		{"stable-id permutation", withIDs(kdTable(kdInt("b"), kdInt("c")), 1, 2, 3), withIDs(kdTable(kdInt("c"), kdInt("b")), 1, 2, 3), order, 0},
		{"dropped and added back under the same name", withIDs(kdTable(kdInt("b"), kdInt("c")), 1, 2, 3), withIDs(kdTable(kdInt("b"), kdInt("c")), 1, 2, 4), replace, 0},
		{"replaced beside an added column", withIDs(kdTable(kdInt("b")), 1, 2), withIDs(kdTable(kdInt("b"), kdInt("x")), 1, 5, 6), replace, 0},
		// Review F2: `DROP COLUMN b; RENAME COLUMN c TO b` was a DROP c to
		// the classifier — and the first cut exempted the id that moved.
		{"PG DROP b then RENAME c TO b", withIDs(kdTable(kdInt("b"), kdInt("c")), 1, 2, 3), withIDs(kdTable(kdInt("b")), 1, 3), replace, 0},
		// Review F3's shape: on PG the moved id refuses it before a rename
		// is ever classified.
		{"PG rename chain b→x, c→b", withIDs(kdTable(kdInt("b"), kdInt("c")), 1, 2, 3), withIDs(kdTable(kdInt("x"), kdInt("b")), 1, 2, 3), replace, 0},
		// The sibling.
		{"two columns widened", kdTable(dec("b", 2), dec("c", 2)), kdTable(dec("b", 4), dec("c", 4)), multi, 0},
		{"two columns' nullability", kdTable(kdInt("b"), kdInt("c")), kdTable(notNull(kdInt("b")), notNull(kdInt("c"))), multi, 0},
		{"one type and one nullability", kdTable(dec("b", 2), kdInt("c")), kdTable(dec("b", 4), notNull(kdInt("c"))), multi, 0},
		// Either side of the class, unchanged.
		{"no change", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("b"), kdInt("c")), "", ShapeKindNone},
		{"no change, stable ids on one side only", kdTable(kdInt("b")), withIDs(kdTable(kdInt("b")), 1, 2), "", ShapeKindNone},
		// Stable ids on both sides that did not move refuse nothing.
		{"add column, stable ids on both sides", withIDs(kdTable(kdInt("b")), 1, 2), withIDs(kdTable(kdInt("b"), kdInt("x")), 1, 2, 3), "", ShapeKindAddColumn},
		{"proven single rename, stable ids on both sides", withIDs(kdTable(kdInt("b")), 1, 2), withIDs(kdTable(kdInt("z")), 1, 2), "", ShapeKindRenameColumn},
		{"add column", kdTable(kdInt("b")), kdTable(kdInt("b"), kdInt("x")), "", ShapeKindAddColumn},
		{"add column placed first", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("x"), kdInt("b"), kdInt("c")), "", ShapeKindAddColumn},
		{"single rename", kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("b"), kdInt("z")), "", ShapeKindRenameColumn},
		{"single widen", kdTable(dec("b", 2), kdInt("c")), kdTable(dec("b", 4), kdInt("c")), "", ShapeKindAlterColumnType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shape, err := ClassifyShape(tc.pre, tc.post)
			if tc.refuse == "" {
				if err != nil || shape.Kind != tc.want {
					t.Fatalf("ClassifyShape = %v, %v; want %v", shape.Kind, err, tc.want)
				}
				return
			}
			if err == nil || shape.Kind != ShapeKindUnrecognized {
				t.Fatalf("ClassifyShape = %v, %v; want a refusal (%s)", shape.Kind, err, tc.refuse)
			}
			var mca *multiColumnAlterError
			switch tc.refuse {
			case order:
				if !errors.Is(err, ir.ErrColumnOrderChanged) {
					t.Errorf("refusal %v does not carry COLUMN-ORDER-CHANGED", err)
				}
			case multi:
				if !errors.As(err, &mca) {
					t.Errorf("refusal %v is not the multi-column alter refusal", err)
				}
			case replace:
				if errors.Is(err, ir.ErrColumnOrderChanged) || !strings.Contains(err.Error(), "now name a different column") {
					t.Errorf("refusal %v is not the column-identity refusal", err)
				}
			}
		})
	}
}

// TestColumnOrderFlips pins the pair arithmetic the check rests on: only
// columns both sides list are compared, in the NEW shape's order.
func TestColumnOrderFlips(t *testing.T) {
	t.Parallel()
	got := columnOrderFlips(kdTable(kdInt("b"), kdInt("c"), kdInt("d")), kdTable(kdInt("x"), kdInt("d"), kdInt("b")))
	if len(got) != 1 || got[0] != (columnOrderFlip{first: "d", second: "b"}) {
		t.Errorf("flips = %v; want exactly d before b", got)
	}
	if got := columnOrderFlips(nil, kdTable(kdInt("b"))); got != nil {
		t.Errorf("flips against a nil prior = %v; want none", got)
	}
	if !declaresBefore([]string{"id", "C", "b"}, "c", "b") {
		t.Error("a case-folded target column did not resolve")
	}
	if declaresBefore([]string{"id", "b"}, "c", "b") {
		t.Error("a pair naming a column the list lacks was held")
	}
}

// TestJudgeColumnOrder_MovementIsTheProof is the truth table of the rule
// the review's F1 forced: a flipped pair is accepted only when the record
// holds it in the OLD order and the target now holds it in the NEW order.
// A target that already held the new order — the forwarded `ADD COLUMN x
// AFTER id` landing at the END of the target table — proves nothing.
func TestJudgeColumnOrder_MovementIsTheProof(t *testing.T) {
	t.Parallel()
	ids := func(names ...string) []string { return append([]string{"id"}, names...) }
	cases := []struct {
		name                      string
		record                    *ir.ColumnOrderRecord
		prior, target, post       []string
		unapplied, unprovable, ok int
	}{
		{"swap, target moved", &ir.ColumnOrderRecord{SourceColumns: ids("b", "c"), TargetColumns: ids("b", "c")}, ids("b", "c"), ids("c", "b"), ids("c", "b"), 0, 0, 1},
		{"swap, target not moved", &ir.ColumnOrderRecord{SourceColumns: ids("b", "c"), TargetColumns: ids("b", "c")}, ids("b", "c"), ids("b", "c"), ids("c", "b"), 1, 0, 0},
		{"F1: target already in the new order", &ir.ColumnOrderRecord{SourceColumns: ids("x", "b"), TargetColumns: ids("b", "x")}, ids("x", "b"), ids("b", "x"), ids("b", "x"), 0, 1, 0},
		{"F1: the operator's correct renames on that target", &ir.ColumnOrderRecord{SourceColumns: ids("x", "b"), TargetColumns: ids("b", "x")}, ids("x", "b"), ids("x", "b"), ids("b", "x"), 0, 1, 0},
		{"no record", nil, ids("b", "c"), ids("c", "b"), ids("c", "b"), 0, 1, 0},
		{"record lacks a flipped column", &ir.ColumnOrderRecord{SourceColumns: ids("b"), TargetColumns: ids("b")}, ids("x", "b"), ids("b", "x"), ids("b", "x"), 0, 1, 0},
		{"change made while stopped, seen through the record only", &ir.ColumnOrderRecord{SourceColumns: ids("b", "c"), TargetColumns: ids("b", "c")}, nil, ids("b", "c"), ids("c", "b"), 1, 0, 0},
		{"no change", &ir.ColumnOrderRecord{SourceColumns: ids("b", "c"), TargetColumns: ids("c", "b")}, ids("b", "c"), ids("c", "b"), ids("b", "c"), 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := judgeColumnOrder(tc.record, tc.prior, tc.target, tc.post)
			if len(v.unapplied) != tc.unapplied || len(v.unprovable) != tc.unprovable || len(v.flips)-len(v.unapplied)-len(v.unprovable) != tc.ok {
				t.Fatalf("verdict %+v; want unapplied=%d unprovable=%d accepted=%d", v, tc.unapplied, tc.unprovable, tc.ok)
			}
		})
	}
}

// kdIntercept runs the forward intercept over one seeded table and one
// boundary, returning what reached the applier, the DDL, and the refusal.
func kdIntercept(t *testing.T, seed, post *ir.Table, witness *firstBoundaryWitness) ([]ir.Change, []string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	in <- addColForwardSnap(post)
	close(in)
	applier := &fakeShapeApplier{}
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, []ir.SchemaSnapshot{addColForwardSnap(seed)}, schemaForwardDeps{
		applier: applier, sourceEngineName: "postgres", targetEngineName: "postgres", witness: witness,
	}, errStore)
	got := drainChannel(t, out, 2*time.Second)
	var err error
	if e := errStore.Load(); e != nil {
		err = *e
	}
	return got, applier.callNames(), err
}

// TestForwardIntercept_ColumnOrderBoundary pins the live forward path,
// both directions: every unproven swap refuses before the boundary goes
// downstream (no witness, no record, a target not moved, a target that
// already held the new order), and a proven one passes carrying the
// table's new record for the applier — with an ADD beside it still
// forwarded.
func TestForwardIntercept_ColumnOrderBoundary(t *testing.T) {
	t.Parallel()
	pre := kdTable(kdInt("b"), kdInt("c"))
	swapped := kdTable(kdInt("c"), kdInt("b"))
	oldRecord := []ir.ColumnOrderRecord{kdRecord([]string{"b", "c"}, []string{"b", "c"})}

	refuses := func(t *testing.T, got []ir.Change, calls []string, err error, want string) {
		t.Helper()
		if !errors.Is(err, ir.ErrColumnOrderChanged) || len(got) != 0 || len(calls) != 0 {
			t.Fatalf("err %v, downstream %d, DDL %v; want the COLUMN-ORDER-CHANGED refusal and nothing passed", err, len(got), calls)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not carry %q: %v", want, err)
		}
	}
	t.Run("no target witness refuses", func(t *testing.T) {
		got, calls, err := kdIntercept(t, pre, swapped, nil)
		refuses(t, got, calls, err, "--reset-target-data")
	})
	t.Run("no record refuses even a reconciled target", func(t *testing.T) {
		w, _ := kdWitness(swapped, nil)
		got, calls, err := kdIntercept(t, pre, swapped, w)
		refuses(t, got, calls, err, "--reset-target-data")
	})
	t.Run("a target not moved refuses, after a fresh read", func(t *testing.T) {
		w, cat := kdWitness(pre, oldRecord)
		got, calls, err := kdIntercept(t, pre, swapped, w)
		refuses(t, got, calls, err, "RENAME COLUMN statements")
		if cat.reads < 2 {
			t.Errorf("catalog read %d time(s); a refusal must be decided on a fresh read", cat.reads)
		}
	})
	t.Run("F1: a target already in the new order refuses", func(t *testing.T) {
		// `ADD COLUMN x INT AFTER id` was forwarded to the END of the
		// target: source (id, x, b), target (id, b, x). The source then
		// swaps x and b by RENAME and declares (id, b, x).
		srcPre := kdTable(kdInt("x"), kdInt("b"))
		srcPost := kdTable(kdInt("b"), kdInt("x"))
		w, _ := kdWitness(kdTable(kdInt("b"), kdInt("x")), []ir.ColumnOrderRecord{kdRecord([]string{"x", "b"}, []string{"b", "x"})})
		got, calls, err := kdIntercept(t, srcPre, srcPost, w)
		refuses(t, got, calls, err, "--reset-target-data")
	})
	t.Run("a moved target passes with its new record", func(t *testing.T) {
		w, _ := kdWitness(swapped, oldRecord)
		got, calls, err := kdIntercept(t, pre, swapped, w)
		if err != nil || len(got) != 1 || len(calls) != 0 {
			t.Fatalf("err %v, downstream %d, DDL %v; want the boundary passed with no DDL", err, len(got), calls)
		}
		snap, ok := got[0].(ir.SchemaSnapshot)
		if !ok || strings.Join(snap.TargetColumns, ",") != "id,c,b" {
			t.Fatalf("passed %#v; want the snapshot carrying the target's new order id,c,b", got[0])
		}
		if rec, _ := w.orders.lookup("public", "kd"); strings.Join(rec.SourceColumns, ",") != "id,c,b" {
			t.Errorf("in-memory record %+v; want it moved to the accepted shape", rec)
		}
	})
	t.Run("a moved target beside an ADD COLUMN still forwards the ADD", func(t *testing.T) {
		w, _ := kdWitness(swapped, oldRecord)
		post := kdTable(kdInt("c"), kdInt("b"), kdInt("x"))
		got, calls, err := kdIntercept(t, pre, post, w)
		if err != nil || len(got) < 1 || len(calls) != 1 || calls[0] != "AlterAddColumn" {
			t.Fatalf("err %v, downstream %d, DDL %v; want one AlterAddColumn", err, len(got), calls)
		}
	})
}

// TestFirstBoundaryWitness_ColumnOrder pins the restart half: a first
// boundary is judged against the record and the retained history (or,
// with neither, the target itself), and refuses unless the target's order
// moved.
func TestFirstBoundaryWitness_ColumnOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	old, swapped := kdTable(kdInt("b"), kdInt("c")), kdTable(kdInt("c"), kdInt("b"))
	oldRecord := []ir.ColumnOrderRecord{kdRecord([]string{"b", "c"}, []string{"b", "c"})}
	cases := []struct {
		name            string
		history, target *ir.Table
		records         []ir.ColumnOrderRecord
		post            *ir.Table
		refuse          bool
	}{
		{"record, target not moved", nil, old, oldRecord, swapped, true},
		{"record, target moved", nil, swapped, oldRecord, swapped, false},
		{"history only (no record), target moved", old, swapped, nil, swapped, true},
		{"neither, target in the old order", nil, old, nil, swapped, true},
		{
			"F1 on restart: target already in the new order", kdTable(kdInt("x"), kdInt("b")), kdTable(kdInt("b"), kdInt("x")),
			[]ir.ColumnOrderRecord{kdRecord([]string{"x", "b"}, []string{"b", "x"})},
			kdTable(kdInt("b"), kdInt("x")), true,
		},
		{
			"no change since the record, target diverged through a forwarded ADD", nil, kdTable(kdInt("c"), kdInt("b"), kdInt("x")),
			[]ir.ColumnOrderRecord{kdRecord([]string{"c", "x", "b"}, []string{"c", "b", "x"})},
			kdTable(kdInt("c"), kdInt("x"), kdInt("b")), false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := kdWitness(tc.target, tc.records)
			if tc.history != nil {
				w.history = []*ir.Table{tc.history}
			}
			v, err := w.verdict(ctx, tc.post, ir.Position{})
			if err != nil {
				t.Fatal(err)
			}
			if refused := v.kind == witnessRefuse; refused != tc.refuse {
				t.Fatalf("verdict %d (%s); want refuse=%t", v.kind, v.render(), tc.refuse)
			}
			if tc.refuse && !strings.Contains(v.render(), ir.ErrColumnOrderChanged.Error()) {
				t.Errorf("the refusal does not name %s: %s", ir.ErrColumnOrderChanged, v.render())
			}
		})
	}
}

// TestColumnOrderRecords_LookupNeverCrossesNamespaces pins review M1: a
// multi-namespace stream keeps one record set, and a same-named table in
// another namespace must never answer for this one — it would stand in for
// the target-as-prior fallback and accept an unapplied swap (the probe's
// db2.kd judged by db1.kd's post-swap record). Only a record with no
// schema (a single-stream MySQL cold start's) answers by name.
func TestColumnOrderRecords_LookupNeverCrossesNamespaces(t *testing.T) {
	t.Parallel()
	db1 := ir.ColumnOrderRecord{Schema: "db1", Table: "kd", SourceColumns: []string{"id", "c", "b"}, TargetColumns: []string{"id", "c", "b"}}
	bare := ir.ColumnOrderRecord{Table: "kd", SourceColumns: []string{"id", "b", "c"}, TargetColumns: []string{"id", "b", "c"}}

	r := newColumnOrderRecords(true, []ir.ColumnOrderRecord{db1})
	if rec, ok := r.lookup("db2", "kd"); ok {
		t.Fatalf("db2.kd resolved to %+v; a record of another namespace answered", rec)
	}
	// The judgement that the wrong record silenced: with no record, the
	// target is its own prior and the unapplied swap refuses.
	rec, _ := r.lookup("db2", "kd")
	if v := judgeColumnOrder(rec, []string{"id", "b", "c"}, []string{"id", "b", "c"}, []string{"id", "c", "b"}); !v.refuses() {
		t.Fatalf("db2.kd's unapplied swap is accepted: %+v", v)
	}
	if got, ok := r.lookup("db1", "kd"); !ok || got.Schema != "db1" {
		t.Fatalf("db1.kd's own record not found: %+v %t", got, ok)
	}

	r = newColumnOrderRecords(true, []ir.ColumnOrderRecord{bare, db1})
	if got, ok := r.lookup("db1", "kd"); !ok || got.Schema != "db1" {
		t.Fatalf("the exact record must win over the bare one: %+v %t", got, ok)
	}
	if got, ok := r.lookup("app", "kd"); !ok || got.Schema != "" {
		t.Fatalf("a schema-less record must answer by name: %+v %t", got, ok)
	}
}

// TestUnforwardedCheck_ColumnOrder pins the --schema-changes=refuse face:
// the swap refuses as SCHEMA-CHANGE-REFUSED carrying COLUMN-ORDER-CHANGED,
// and passes once the target's order has moved from the record's.
func TestUnforwardedCheck_ColumnOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	coldStart := kdTable(kdInt("b"), kdInt("c"))
	swapped := kdTable(kdInt("c"), kdInt("b"))
	for _, tc := range []struct {
		name    string
		target  *ir.Table
		records []ir.ColumnOrderRecord
		refuse  bool
	}{
		{"target not moved", kdTable(kdInt("b"), kdInt("c")), []ir.ColumnOrderRecord{kdRecord([]string{"b", "c"}, []string{"b", "c"})}, true},
		{"target moved", kdTable(kdInt("c"), kdInt("b")), []ir.ColumnOrderRecord{kdRecord([]string{"b", "c"}, []string{"b", "c"})}, false},
		{"no record", kdTable(kdInt("c"), kdInt("b")), nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := kdWitness(tc.target, tc.records)
			deps := unforwardedBoundaryDeps{
				witnessFor: func(string) *firstBoundaryWitness { return w },
				why:        "--schema-changes=refuse",
			}
			last := acceptedBoundary{compared: coldStart, raw: coldStart, origin: priorColdStart}
			_, err := judgeUnforwardedBoundary(ctx, deps, "public.kd", last, swapped, addColForwardSnap(swapped))
			if tc.refuse {
				if !errors.Is(err, ir.ErrSchemaChangeRefused) || !errors.Is(err, ir.ErrColumnOrderChanged) {
					t.Fatalf("err = %v; want SCHEMA-CHANGE-REFUSED carrying COLUMN-ORDER-CHANGED", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v; want the moved target accepted", err)
			}
		})
	}
}

// TestColumnOrderRemedy_IsRunnableOnTheTarget pins review F4: the remedy
// names what the target can do. A target that cannot reorder in place
// (Postgres) is never told to `MODIFY … AFTER`; an unprovable change is
// always sent to the re-copy, with the command spelled out.
func TestColumnOrderRemedy_IsRunnableOnTheTarget(t *testing.T) {
	t.Parallel()
	f := []columnOrderFlip{{first: "c", second: "b"}}
	unapplied := columnOrderVerdict{flips: f, unapplied: f}
	unprovable := columnOrderVerdict{flips: f, unprovable: f}
	if r := columnOrderRemedy(unapplied, true); !strings.Contains(r, "MODIFY … AFTER/FIRST") {
		t.Errorf("a reordering target's remedy does not name the reorder: %s", r)
	}
	if r := columnOrderRemedy(unapplied, false); strings.Contains(r, "the same MODIFY") || !strings.Contains(r, "--reset-target-data") {
		t.Errorf("a target that cannot reorder was told to reorder, or not sent to the re-copy: %s", r)
	}
	for _, reorders := range []bool{true, false} {
		if r := columnOrderRemedy(unprovable, reorders); !strings.Contains(r, "sluice sync start --reset-target-data --yes") ||
			strings.Contains(r, "schema add-table") && !strings.Contains(r, "is NOT offered") {
			t.Errorf("an unprovable change's remedy is not the re-copy command: %s", r)
		}
	}
}

// TestRefuseRenameAmbiguous_ARenameChainIsNotOfferedAsOnePair pins review
// F3: `RENAME b TO x, RENAME c TO b` on a source with no stable column ids
// classifies as the one rename c→x, and the refusal used to send the
// operator to rename exactly that on the target — leaving b's values under
// x and c's under b. It now names the chain as ambiguous and the re-copy;
// a genuine single rename keeps its message.
func TestRefuseRenameAmbiguous_ARenameChainIsNotOfferedAsOnePair(t *testing.T) {
	t.Parallel()
	pre := kdTable(kdInt("b"), kdInt("c"))
	chain := kdTable(kdInt("x"), kdInt("b"))
	shape, err := ClassifyShape(pre, chain)
	if err != nil || shape.Kind != ShapeKindRenameColumn {
		t.Fatalf("ClassifyShape = %v, %v; want the single-rename shape the chain looks like", shape.Kind, err)
	}
	msg := refuseRenameAmbiguous("public.kd", shape, pre, chain).Error()
	if !strings.Contains(msg, "renamed more than one column") || !strings.Contains(msg, "--reset-target-data") ||
		strings.Contains(msg, "Rename on the target manually") {
		t.Errorf("the chain's refusal offers the inferred pair or no re-copy: %s", msg)
	}
	single := kdTable(kdInt("b"), kdInt("z"))
	shape, _ = ClassifyShape(pre, single)
	if msg := refuseRenameAmbiguous("public.kd", shape, pre, single).Error(); !strings.Contains(msg, "Rename on the target manually") {
		t.Errorf("a genuine single rename lost its remedy: %s", msg)
	}
}
