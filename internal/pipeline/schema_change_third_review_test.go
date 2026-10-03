// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// GC-44 F5 third review: the unit pins.
//
//   - HIGH-1: a source NARROWING read against a target that kept the wider
//     type was a WARN on every stream that forwards no DDL, and a WARN-keep
//     at the forward path's first boundary — the source's own ALTER had
//     rounded its rows and the target's copies kept the old values. With a
//     prior that proves the narrowing (this intercept's own last boundary,
//     or the stream's retained history, never a replay) the refuse check
//     refuses and the forward first boundary forwards it. The same prior
//     refuses the source's DROP COLUMN while the target still holds the
//     column (a RENAME COLUMN is reported as such), and a column the source
//     replaced under the same name.
//   - HIGH-2: a negative-scale numeric held no integer, yet was judged to.
//   - MEDIUM-1: an empty write-ahead record that nothing ever clears.
//   - MEDIUM-2: a narrowing --type-override refused every cold-started
//     refuse-mode stream (the check had no prior on a cold start).
//
// The independent expected value of every table here is the table itself,
// written from what the source's ALTER does to the values it stores.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// narrowingFamilies is every family [witnessWidthOrder] orders, each as a
// source narrowing wide → narrow: the Bug 74 rule — one representative per
// family, not one for all.
func narrowingFamilies() []struct {
	name         string
	wide, narrow ir.Type
} {
	return []struct {
		name         string
		wide, narrow ir.Type
	}{
		{"numeric on both axes", ir.Decimal{Precision: 14, Scale: 4}, ir.Decimal{Precision: 12, Scale: 2}},
		{"numeric integer digits", ir.Decimal{Precision: 12, Scale: 2}, ir.Decimal{Precision: 10, Scale: 2}},
		{"numeric to integer", ir.Decimal{Precision: 12, Scale: 2}, ir.Integer{Width: 32}},
		{"timestamp precision", ir.Timestamp{Precision: 6}, ir.Timestamp{Precision: 0}},
		{"timestamptz precision", ir.Timestamp{Precision: 6, WithTimeZone: true}, ir.Timestamp{Precision: 3, WithTimeZone: true}},
		{"time precision", ir.Time{Precision: 6}, ir.Time{Precision: 0}},
		{"datetime fsp", ir.DateTime{Precision: 6}, ir.DateTime{Precision: 0}},
		{"double to real", ir.Float{Precision: ir.FloatDouble}, ir.Float{Precision: ir.FloatSingle}},
		{"bigint to integer", ir.Integer{Width: 64}, ir.Integer{Width: 32}},
		{"varchar length", ir.Varchar{Length: 64}, ir.Varchar{Length: 16}},
		{"char length", ir.Char{Length: 20}, ir.Char{Length: 10}},
		{"enum label removed", ir.Enum{Values: []string{"a", "b", "c"}}, ir.Enum{Values: []string{"a", "b"}}},
		{"set label removed", ir.Set{Values: []string{"a", "b", "c"}}, ir.Set{Values: []string{"a", "b"}}},
		{"array element numeric", ir.Array{Element: ir.Decimal{Precision: 12, Scale: 4}}, ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}}},
		{"array element timestamp", ir.Array{Element: ir.Timestamp{Precision: 6}}, ir.Array{Element: ir.Timestamp{Precision: 0}}},
	}
}

// TestJudgeUnforwardedColumns_SourceNarrowedEveryFamily is HIGH-1 at the
// refuse check's judgement, family by family: the target holds the source's
// old (wider) type and the source now sends the narrower one.
func TestJudgeUnforwardedColumns_SourceNarrowedEveryFamily(t *testing.T) {
	t.Parallel()
	families := narrowingFamilies()
	if len(families) < 15 {
		t.Fatalf("matrix holds %d families; floor 15 — the universe went vacuous", len(families))
	}
	for _, f := range families {
		post, target := witnessTable(wcol("v", f.narrow)), witnessTable(wcol("v", f.wide))
		for _, tc := range []struct {
			name    string
			prior   judgedPrior
			refused bool
		}{
			{"the prior proves the narrowing", judgedPrior{expected: witnessTable(wcol("v", f.wide)), judges: true}, true},
			{"no prior: kept with a WARN", judgedPrior{}, false},
			{"a replay (the prior cannot judge it): kept", judgedPrior{expected: witnessTable(wcol("v", f.wide))}, false},
			{"the source unchanged, the target widened ahead: kept", judgedPrior{expected: witnessTable(wcol("v", f.narrow)), judges: true}, false},
		} {
			j := judgeUnforwardedColumns(post, target, witnessOptions{}, tc.prior)
			if got := len(j.refused) > 0; got != tc.refused {
				t.Errorf("%s / %s: refused = %v (%v, ahead %v), want %v", f.name, tc.name, got, j.refused, j.ahead, tc.refused)
				continue
			}
			if tc.refused && (!j.narrowed || !strings.Contains(renderWitnessDiffs(j.refused), "narrowed from")) {
				t.Errorf("%s / %s: the refusal does not name the narrowing: %+v", f.name, tc.name, j)
			}
			if tc.refused {
				err := j.settle(context.Background(), "src.w", unforwardedBoundaryDeps{why: "--schema-changes=refuse"})
				if !errors.Is(err, ir.ErrSchemaChangeRefused) || !strings.Contains(err.Error(), "NARROWED") {
					t.Errorf("%s: settle = %v; want %s with the narrowing remedy", f.name, err, schemaChangeRefusedMarker)
				}
			}
		}
	}
	t.Run("an overridden column narrowed by the source refuses too", func(t *testing.T) {
		post := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
		target := witnessTable(wcol("v", ir.Decimal{Precision: 20, Scale: 8}))
		opts := witnessOptions{pinned: map[string]bool{"v": true}}
		prior := judgedPrior{expected: witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4})), judges: true}
		if j := judgeUnforwardedColumns(post, target, opts, prior); !j.narrowed {
			t.Errorf("judgement = %+v; want the narrowing refused under the override", j)
		}
		prior.judges = false
		if j := judgeUnforwardedColumns(post, target, opts, prior); len(j.refused) != 0 {
			t.Errorf("replayed: %+v; want accepted (the override is wider)", j)
		}
	})
	t.Run("a column the source replaced under the same name refuses", func(t *testing.T) {
		tbl := witnessTable(wcol("a", ir.Integer{Width: 32}))
		j := judgeUnforwardedColumns(tbl, tbl, witnessOptions{}, judgedPrior{judges: true, returned: map[string]bool{"a": true}})
		if !j.returned || len(j.refused) != 1 {
			t.Fatalf("judgement = %+v; want the returned column refused", j)
		}
		if err := j.settle(context.Background(), "src.w", unforwardedBoundaryDeps{}); !strings.Contains(err.Error(), "OLD column") {
			t.Errorf("settle = %v; want the returned-column remedy", err)
		}
	})
}

// TestClassifyWitness_HistoryProvesSourceNarrowed is HIGH-1's sibling at
// the forward path's first boundary: with the retained history proving the
// source held the target's type and narrowed it, the narrowing is forwarded
// (it converges, as a live forward does); a target holding anything else
// refuses; with no history the WARN-keep stands.
func TestClassifyWitness_HistoryProvesSourceNarrowed(t *testing.T) {
	t.Parallel()
	for _, f := range narrowingFamilies() {
		_, isArray := f.wide.(ir.Array)
		post, target := witnessTable(wcol("v", f.narrow)), witnessTable(wcol("v", f.wide))
		forward := witnessForwardAlter
		if isArray {
			forward = witnessRefuse // no first boundary ALTERs an array element
		}
		if v := classifyWitness(post, target, witnessOptions{priorExpected: witnessTable(wcol("v", f.wide))}); v.kind != forward {
			t.Errorf("%s: history == target: verdict %d, want %d (%s)", f.name, v.kind, forward, v.render())
		} else if v.kind == witnessForwardAlter && (v.narrowedFrom == nil || !strings.Contains(v.render(), "narrowed from")) {
			t.Errorf("%s: the forwarded narrowing does not say so: %s", f.name, v.render())
		}
		if v := classifyWitness(post, target, witnessOptions{}); v.kind != witnessTargetWider {
			t.Errorf("%s: no history: verdict %d, want the WARN-keep", f.name, v.kind)
		}
	}
	t.Run("the target widened past the source's old type refuses", func(t *testing.T) {
		post := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
		target := witnessTable(wcol("v", ir.Decimal{Precision: 20, Scale: 8}))
		prior := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
		if v := classifyWitness(post, target, witnessOptions{priorExpected: prior}); v.kind != witnessRefuse {
			t.Errorf("verdict %d, want refuse (%s)", v.kind, v.render())
		}
	})
	t.Run("history equal to the snapshot: the target was widened ahead, kept", func(t *testing.T) {
		post := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
		target := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
		if v := classifyWitness(post, target, witnessOptions{priorExpected: post}); v.kind != witnessTargetWider {
			t.Errorf("verdict %d, want the WARN-keep", v.kind)
		}
	})
	t.Run("an overridden column narrowed by the source refuses", func(t *testing.T) {
		post := witnessTable(wcol("v", ir.Integer{Width: 32}))
		target := witnessTable(wcol("v", ir.Decimal{Precision: 20}))
		opts := witnessOptions{pinned: map[string]bool{"v": true}, priorExpected: witnessTable(wcol("v", ir.Integer{Width: 64}))}
		if v := classifyWitness(post, target, opts); v.kind != witnessRefuse {
			t.Errorf("verdict %d, want refuse (%s)", v.kind, v.render())
		}
	})
}

// TestHistoryPriorAt_AReplayIsNotEvidence pins the replay rule both ways:
// a boundary PROVEN before the retained version's anchor is a replay and
// gets no prior; one at or after it, or one whose order is unknown, does.
func TestHistoryPriorAt_AReplayIsNotEvidence(t *testing.T) {
	t.Parallel()
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
	older := witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2}))
	w := newFakeWitness(&fakeCatalog{}, "postgres", "postgres")
	w.history = []*ir.Table{wide}
	w.retained = retainedHistory{wide: {anchor: testPos(10)}}
	w.orderer = numericOrderer{}
	for _, tc := range []struct {
		name  string
		pos   ir.Position
		prior bool
	}{
		{"a replay before the anchor", testPos(5), false},
		{"at the anchor (a Postgres 0/0 anchor is always this)", testPos(10), false},
		{"strictly after the anchor", testPos(20), true},
		{"no position (unknown order)", ir.Position{}, false},
	} {
		if got := w.historyPriorAt("w", tc.pos, narrow) != nil; got != tc.prior {
			t.Errorf("%s: prior = %v, want %v", tc.name, got, tc.prior)
		}
	}
	w.orderer = nil
	if w.historyPriorAt("w", testPos(20), narrow) != nil {
		t.Error("no orderer: nothing proves the boundary is not a replay, so no prior may be used to forward")
	}
	// Strictly after the anchor but showing a shape the stream recorded: a
	// replay can re-deliver only those, so it is still not proof.
	w.orderer = numericOrderer{}
	w.retained = retainedHistory{wide: {anchor: testPos(10), recorded: []*ir.Table{older, wide}}}
	if w.historyPriorAt("w", testPos(20), narrow) != nil {
		t.Error("a boundary showing a recorded shape was judged against the history")
	}
	if w.historyPriorAt("w", testPos(20), witnessTable(wcol("v", ir.Decimal{Precision: 9, Scale: 1}))) == nil {
		t.Error("a never-recorded narrower shape past the anchor got no prior")
	}
}

// refuseAt is a refuse-intercept snapshot of tbl at test position n.
func refuseAt(tbl *ir.Table, n int) ir.SchemaSnapshot {
	s := refuseSnap(tbl)
	s.Position = testPos(n)
	return s
}

// TestInterceptSchemaChangeRefuse_SourceNarrowing drives HIGH-1 through the
// intercept: the reviewer's live narrowing, its replay counterpart, the
// history prior across a restart, and the returning-column shapes.
func TestInterceptSchemaChangeRefuse_SourceNarrowing(t *testing.T) {
	t.Parallel()
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
	deps := func(target *ir.Table, history ...*ir.Table) unforwardedBoundaryDeps {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": target}}, "postgres", "postgres")
		w.orderer = numericOrderer{}
		if len(history) > 0 {
			w.history = history
			w.retained = retainedHistory{history[0]: {anchor: testPos(10)}}
		}
		return unforwardedBoundaryDeps{
			witnessFor: func(string) *firstBoundaryWitness { return w },
			orderer:    numericOrderer{}, why: "--schema-changes=refuse",
		}
	}
	refused := func(t *testing.T, err error, what string) {
		t.Helper()
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || !strings.Contains(err.Error(), what) {
			t.Fatalf("err = %v; want %s naming %q", err, schemaChangeRefusedMarker, what)
		}
	}

	t.Run("a live narrowing refuses (numeric(14,4) -> numeric(12,2))", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(wide), refuseAt(wide, 1), refuseAt(narrow, 2))
		refused(t, err, "narrowed from")
		if len(out) != 1 {
			t.Errorf("%d boundaries went downstream; want only the first", len(out))
		}
	})
	t.Run("a replay goes narrow then wide, and passes", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(wide), refuseAt(narrow, 2), refuseAt(wide, 3))
		if err != nil || len(out) != 2 {
			t.Fatalf("out %d, err %v; want both passed", len(out), err)
		}
	})
	t.Run("an in-process replay of an earlier relation passes", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(wide), refuseAt(wide, 5), refuseAt(narrow, 3), refuseAt(wide, 6))
		if err != nil || len(out) != 3 {
			t.Fatalf("out %d, err %v; want all passed", len(out), err)
		}
	})
	t.Run("after a restart the history proves it", func(t *testing.T) {
		_, err := runRefuseIntercept(t, deps(wide, wide), refuseAt(narrow, 20))
		refused(t, err, "narrowed from")
	})
	t.Run("after a restart, a replay before the history's anchor passes", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(wide, wide), refuseAt(narrow, 5), refuseAt(wide, 6))
		if err != nil || len(out) != 2 {
			t.Fatalf("out %d, err %v; want the replay passed", len(out), err)
		}
	})
	t.Run("Postgres anchors (all 0/0): a replay of recorded shapes passes, a new narrowing refuses", func(t *testing.T) {
		d := deps(wide, wide)
		w := d.witnessFor("")
		w.retained = retainedHistory{wide: {anchor: testPos(0), recorded: []*ir.Table{narrow, wide}}}
		out, err := runRefuseIntercept(t, d, refuseAt(narrow, 0), refuseAt(wide, 0))
		if err != nil || len(out) != 2 {
			t.Fatalf("replay: out %d, err %v; want both passed", len(out), err)
		}
		narrower := witnessTable(wcol("v", ir.Decimal{Precision: 9, Scale: 1}))
		_, err = runRefuseIntercept(t, d, refuseAt(narrower, 0))
		refused(t, err, "narrowed from")
	})
	t.Run("the drained-model remedy: narrow the target too, then restart", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(narrow, wide), refuseAt(narrow, 20))
		if err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want accepted", len(out), err)
		}
	})

	a := wcol("a", ir.Integer{Width: 32})
	withA, withoutA := witnessTable(a), witnessTable()
	t.Run("a live DROP COLUMN the target still holds refuses", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(withA), refuseAt(withA, 1), refuseAt(withoutA, 2), refuseAt(withA, 3))
		refused(t, err, "DROP COLUMN a")
		if !strings.Contains(err.Error(), "Drained-model recovery") || len(out) != 1 {
			t.Errorf("out %d, err %v; want the first passed and the drained-model hint", len(out), err)
		}
	})
	t.Run("after a restart the history proves the DROP", func(t *testing.T) {
		_, err := runRefuseIntercept(t, deps(withA, withA), refuseAt(withoutA, 20))
		refused(t, err, "DROP COLUMN a")
	})
	t.Run("the DROP remedy passes: the target dropped it too", func(t *testing.T) {
		if out, err := runRefuseIntercept(t, deps(withoutA, withA), refuseAt(withoutA, 20)); err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want accepted", len(out), err)
		}
	})
	t.Run("a column only the target has, with no prior, stays the WARN", func(t *testing.T) {
		if out, err := runRefuseIntercept(t, deps(withA), refuseAt(withoutA, 1)); err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want accepted with the WARN", len(out), err)
		}
	})
	t.Run("a RENAME COLUMN refuses naming the likely rename", func(t *testing.T) {
		v := witnessTable(wcol("v", ir.Text{Size: ir.TextLong}))
		w := witnessTable(wcol("w", ir.Text{Size: ir.TextLong}))
		_, err := runRefuseIntercept(t, deps(v), refuseAt(v, 1), refuseAt(w, 2))
		refused(t, err, "likely RENAME COLUMN v → w")
		if !strings.Contains(err.Error(), "Drained-model recovery") {
			t.Errorf("the rename refusal lacks the drained-model hint: %v", err)
		}
	})
	t.Run("a same-name column with a new stable id refuses", func(t *testing.T) {
		before := witnessTable(&ir.Column{Name: "a", Type: ir.Integer{Width: 32}, StableID: 2})
		after := witnessTable(&ir.Column{Name: "a", Type: ir.Integer{Width: 32}, StableID: 3})
		_, err := runRefuseIntercept(t, deps(withA), refuseAt(before, 1), refuseAt(after, 2))
		refused(t, err, "replaced on the source")
	})
	t.Run("the ADD COLUMN remedy passes: history lacks it, the target holds it", func(t *testing.T) {
		// A column absent from the prior and present on the target is NOT
		// "returning": it is exactly what the hint asks the operator to do
		// (refused ADD -> ALTER the target -> restart). Refusing it would
		// wedge every refuse-mode ADD COLUMN forever.
		out, err := runRefuseIntercept(t, deps(withA, withoutA), refuseAt(withA, 20))
		if err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want accepted", len(out), err)
		}
	})
	t.Run("a replayed ADD COLUMN transaction does not read as a drop", func(t *testing.T) {
		out, err := runRefuseIntercept(t, deps(withA, withA), refuseAt(withoutA, 5), refuseAt(withA, 6))
		if err != nil || len(out) != 2 {
			t.Fatalf("out %d, err %v; want both passed", len(out), err)
		}
	})
}

// TestInterceptSchemaChangeRefuse_NarrowingOverrideColdStart is MEDIUM-2: a
// deliberate narrowing --type-override on a cold-started stream that
// forwards no DDL. Its first boundary has no intercept prior and no history;
// the cold start's raw source read is the prior that recognises the
// narrowing as the override the copy applied. A later genuine change still
// refuses, and a stream with neither prior keeps the documented residual
// (GC-44 F12: the warm-resume half).
func TestInterceptSchemaChangeRefuse_NarrowingOverrideColdStart(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		source, override, gen ir.Type
	}{
		{"unconstrained numeric under numeric(18,4)", ir.Decimal{Unconstrained: true}, ir.Decimal{Precision: 18, Scale: 4}, ir.Decimal{Precision: 10, Scale: 2}},
		{"int under smallint", ir.Integer{Width: 32}, ir.Integer{Width: 16}, ir.Integer{Width: 64}},
		{"varchar(255) under varchar(64)", ir.Varchar{Length: 255}, ir.Varchar{Length: 64}, ir.Varchar{Length: 512}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := witnessTable(wcol("c", tc.source))
			later := witnessTable(wcol("c", tc.gen))
			target := witnessTable(wcol("c", tc.override))
			deps := func(coldStart bool) unforwardedBoundaryDeps {
				w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": target}}, "postgres", "postgres")
				w.mappings = []config.Mapping{{Table: "w", Column: "c", TargetType: "x"}}
				d := unforwardedBoundaryDeps{witnessFor: func(string) *firstBoundaryWitness { return w }}
				if coldStart {
					d.coldStart = []ir.SchemaSnapshot{refuseSnap(src)}
				}
				return d
			}
			if out, err := runRefuseIntercept(t, deps(true), refuseSnap(src)); err != nil || len(out) != 1 {
				t.Fatalf("cold start: out %d, err %v; want the override's own narrowing accepted", len(out), err)
			}
			if _, err := runRefuseIntercept(t, deps(true), refuseSnap(src), refuseSnap(later)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
				t.Errorf("a later change past the override: err %v; want %s", err, schemaChangeRefusedMarker)
			}
			if _, err := runRefuseIntercept(t, deps(false), refuseSnap(src)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
				t.Errorf("no prior at all (GC-44 F12's residual): err %v; want the refusal kept", err)
			}
		})
	}
	// The cold-start read never judges a narrowing: its fidelity differs
	// from the change stream's (ADR-0091 §3).
	t.Run("a cold-start prior proves no narrowing", func(t *testing.T) {
		wide := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
		narrow := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": wide}}, "postgres", "postgres")
		d := unforwardedBoundaryDeps{witnessFor: func(string) *firstBoundaryWitness { return w }, coldStart: []ir.SchemaSnapshot{refuseSnap(wide)}}
		if out, err := runRefuseIntercept(t, d, refuseSnap(narrow)); err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want the WARN-keep", len(out), err)
		}
	})
}

// TestCheckShapeAFirstBoundary_HistoryProvesSourceNarrowed is the Shape A
// route of the narrowing: routed through the lease against the target's type.
func TestCheckShapeAFirstBoundary_HistoryProvesSourceNarrowed(t *testing.T) {
	t.Parallel()
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": wide}}, "postgres", "postgres")
	w.history = []*ir.Table{wide}
	w.retained = retainedHistory{wide: {anchor: testPos(10)}}
	w.orderer = numericOrderer{}
	b, err := checkShapeAFirstBoundary(context.Background(), w, "w", narrow, narrow, testPos(20))
	if err != nil || !b.routed() {
		t.Fatalf("decision = %+v, %v; want the narrowing routed", b, err)
	}
	if shape, err := ClassifyShape(b.pre, narrow); err != nil || shape.Kind != ShapeKindAlterColumnType {
		t.Errorf("routed shape = %v, %v; want an ALTER COLUMN TYPE", shape.Kind, err)
	}
	b, err = checkShapeAFirstBoundary(context.Background(), w, "w", narrow, narrow, testPos(5))
	if err != nil || b.routed() {
		t.Errorf("a replay before the anchor: %+v, %v; want the WARN-keep baseline", b, err)
	}
}

// TestCheckShapeAFirstBoundary_UnwitnessedStillFindsPeerAdded: the
// unwitnessable arm reaches the peer-added check (the CHANGELOG's "whatever
// else that shard's first boundary finds").
func TestCheckShapeAFirstBoundary_UnwitnessedStillFindsPeerAdded(t *testing.T) {
	t.Parallel()
	post := witnessTable(wcol("tier", ir.Integer{Width: 32}))
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": post}}, "postgres", "sqlite")
	w.history = []*ir.Table{witnessTable()}
	b, err := checkShapeAFirstBoundary(context.Background(), w, "w", post, post, ir.Position{})
	if err != nil || b.routed() || len(b.peerAdded) != 1 {
		t.Fatalf("decision = %+v, %v; want tier owed, nothing routed", b, err)
	}
}

// TestShapeAFirstBoundary_ForwardAlterLeavesNoWriteAhead is MEDIUM-1, the
// reviewer's reproduction: a first boundary that routes only an ALTER owes
// no backfill, so no write-ahead record may be left behind — an empty one
// was never cleared and made the next start refuse.
func TestShapeAFirstBoundary_ForwardAlterLeavesNoWriteAhead(t *testing.T) {
	clock := newMockClock(testClockNow())
	mgr := newTestLeaseManager(t, newFakeLeaseStore(clock.Now), "stream-b",
		LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
	store := &fakeRefusalStore{}
	s, _ := writeAheadStreamer(store)
	applier := &alterObservingApplier{fakeShapeApplier: &fakeShapeApplier{}, store: store}
	router, err := NewBoundaryRouter(mgr, applier, &fakeProber{}, "postgres", "postgres", sourceDefaultReaders{})
	if err != nil {
		t.Fatal(err)
	}
	bf := &schemaForwardBackfill{reader: staticBackfillReader(&pagedBackfillReader{}), batchSize: 10, ledger: &s.addedColumnBackfills, streamID: "s"}
	router.beforeAddColumn = bf.writeAhead
	narrow := &ir.Column{Name: "v", Type: ir.Decimal{Precision: 10, Scale: 2}, Nullable: true}
	wide := &ir.Column{Name: "v", Type: ir.Decimal{Precision: 12, Scale: 4}, Nullable: true}
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"dj": addColForwardTable("dj", narrow)}}, "postgres", "postgres")
	w.history = []*ir.Table{addColForwardTable("dj", narrow)}
	router.firstBoundary = w
	in := make(chan ir.Change, 1)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "dj", Position: ir.Position{Token: "p1"}, IR: addColForwardTable("dj", wide)}
	close(in)
	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainChanges(t, interceptSchemaSnapshotsForCoordination(ctx, in, nil, router, nil, bf, &errStore), 2*time.Second)
	if e := errStore.Load(); e != nil {
		t.Fatalf("refused: %v", *e)
	}
	if calls := applier.callNames(); len(calls) != 1 || calls[0] != "AlterColumnType" {
		t.Fatalf("applier calls = %v; want the forwarded ALTER alone", calls)
	}
	s.addedColumnBackfills.clearWriteAheadIfSettled(ctx)
	if store.has || len(store.recorded) != 0 {
		t.Errorf("a write-ahead record was left on the target after a forward-ALTER first boundary: %q", store.recorded)
	}
}

// TestInterceptAddColumnForward_HistoryProvesSourceNarrowed drives the
// forward-mode sibling through the intercept: a narrowing made while the
// stream was stopped is forwarded as an ALTER COLUMN TYPE when the retained
// history proves the source held the target's type; without history, or
// for a replay before the history's anchor, the target is kept.
func TestInterceptAddColumnForward_HistoryProvesSourceNarrowed(t *testing.T) {
	wide := witnessTable(wcol("v", ir.Decimal{Precision: 14, Scale: 4}))
	narrow := witnessTable(wcol("v", ir.Decimal{Precision: 12, Scale: 2}))
	for _, tc := range []struct {
		name    string
		history bool
		pos     int
		calls   int
	}{
		{"history proves it: forwarded", true, 20, 1},
		{"a replay before the anchor: kept", true, 5, 0},
		{"no history: kept", false, 20, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": wide}}, "postgres", "postgres")
			w.orderer = numericOrderer{}
			if tc.history {
				w.history = []*ir.Table{wide}
				w.retained = retainedHistory{wide: {anchor: testPos(10)}}
			}
			applier := &fakeShapeApplier{}
			deps := schemaForwardDeps{applier: applier, sourceEngineName: "postgres", targetEngineName: "postgres", witness: w}
			in := make(chan ir.Change, 1)
			in <- refuseAt(narrow, tc.pos)
			close(in)
			var errStore atomic.Pointer[error]
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			drainChannel(t, interceptAddColumnForward(ctx, in, nil, deps, &errStore), 2*time.Second)
			if p := errStore.Load(); p != nil {
				t.Fatalf("refused: %v", *p)
			}
			if got := applier.callNames(); len(got) != tc.calls || (tc.calls == 1 && got[0] != "AlterColumnType") {
				t.Errorf("applier calls = %v; want %d AlterColumnType", got, tc.calls)
			}
		})
	}
}

// TestAddedColumnBackfillLedger_EmptyWriteAheadIsANoOp pins the ledger's own
// guard, which covers every caller whatever it passes.
func TestAddedColumnBackfillLedger_EmptyWriteAheadIsANoOp(t *testing.T) {
	store := &fakeRefusalStore{}
	s, _ := writeAheadStreamer(store)
	if err := s.addedColumnBackfills.writeAhead(context.Background(), "t", nil); err != nil {
		t.Fatal(err)
	}
	if store.has || s.addedColumnBackfills.outstanding() {
		t.Errorf("an empty write-ahead recorded %q / left the ledger outstanding", store.recorded)
	}
}
