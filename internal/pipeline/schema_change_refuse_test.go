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

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// TestJudgeUnforwardedColumns_EveryFamilyBothDirections is the truth table
// behind the unforwarded-stream check's one type exemption (GC-44 F5): a
// target column at least as wide as the source's within one family is the
// drained model run ahead of the source (or a Postgres replay's pre-ALTER
// relation) and passes; anything else refuses. Every family the forward
// path's direction rule ([witnessWidthOrder]) admits is pinned in both
// directions at THIS judgement — the target wider (passes) and the source
// wider (refuses) — and the excluded shapes are pinned as refusing, so a
// change to the shared rule that loosens refuse mode fails here, not only in
// the forward truth table. The independent expected value is the table,
// written from what each target type stores.
func TestJudgeUnforwardedColumns_EveryFamilyBothDirections(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		target, source ir.Type
		passes         bool
	}{
		{"datetime fsp: target wider", ir.DateTime{Precision: 6}, ir.DateTime{Precision: 0}, true},
		{"datetime fsp: source wider", ir.DateTime{Precision: 0}, ir.DateTime{Precision: 6}, false},
		{"datetime: an unspecified target is (6)", ir.DateTime{PrecisionUnspecified: true}, ir.DateTime{Precision: 3}, true},
		{"datetime: an unspecified source needs (6)", ir.DateTime{Precision: 3}, ir.DateTime{PrecisionUnspecified: true}, false},
		{"time fsp: target wider", ir.Time{Precision: 6}, ir.Time{Precision: 0}, true},
		{"time fsp: source wider", ir.Time{Precision: 0}, ir.Time{Precision: 6}, false},
		{"time: the zone kind differs", ir.Time{Precision: 6, WithTimeZone: true}, ir.Time{Precision: 0}, false},
		{"timestamp fsp: target wider", ir.Timestamp{Precision: 6}, ir.Timestamp{Precision: 0}, true},
		{"timestamp fsp: source wider", ir.Timestamp{Precision: 0}, ir.Timestamp{Precision: 6}, false},
		{"timestamptz fsp: target wider", ir.Timestamp{Precision: 6, WithTimeZone: true}, ir.Timestamp{Precision: 3, WithTimeZone: true}, true},
		{"timestamptz fsp: source wider", ir.Timestamp{Precision: 3, WithTimeZone: true}, ir.Timestamp{Precision: 6, WithTimeZone: true}, false},
		{"the session-zone sibling swap", ir.Timestamp{Precision: 6, WithTimeZone: true}, ir.DateTime{Precision: 0}, false},
		{"decimal: target wider on both axes", ir.Decimal{Precision: 12, Scale: 4}, ir.Decimal{Precision: 10, Scale: 2}, true},
		{"decimal scale: source wider", ir.Decimal{Precision: 10, Scale: 2}, ir.Decimal{Precision: 10, Scale: 4}, false},
		{"decimal integer digits: source wider", ir.Decimal{Precision: 10, Scale: 2}, ir.Decimal{Precision: 12, Scale: 2}, false},
		{"decimal: wider on one axis, narrower on the other", ir.Decimal{Precision: 10, Scale: 4}, ir.Decimal{Precision: 10, Scale: 2}, false},
		{"decimal: an unconstrained target", ir.Decimal{Unconstrained: true}, ir.Decimal{Precision: 65, Scale: 30}, true},
		{"decimal: an unconstrained source", ir.Decimal{Precision: 65, Scale: 30}, ir.Decimal{Unconstrained: true}, false},
		{"varchar: target wider", ir.Varchar{Length: 64}, ir.Varchar{Length: 16}, true},
		{"varchar: source wider", ir.Varchar{Length: 16}, ir.Varchar{Length: 64}, false},
		{"char: target wider", ir.Char{Length: 20}, ir.Char{Length: 10}, true},
		{"char: source wider", ir.Char{Length: 10}, ir.Char{Length: 20}, false},
		{"float: double holds single", ir.Float{Precision: ir.FloatDouble}, ir.Float{Precision: ir.FloatSingle}, true},
		{"float: single does not hold double", ir.Float{Precision: ir.FloatSingle}, ir.Float{Precision: ir.FloatDouble}, false},
		{"integer: target wider", ir.Integer{Width: 64}, ir.Integer{Width: 32}, true},
		{"integer: source wider", ir.Integer{Width: 32}, ir.Integer{Width: 64}, false},
		// Across families: witnessWidthOrder's one-sided "target holds every
		// value" relations (GC-44 second review) pass; the rest refuse.
		{"integer: unsigned under a wider signed target", ir.Integer{Width: 64}, ir.Integer{Width: 32, Unsigned: true}, true},
		{"integer: a sign change at the same width", ir.Integer{Width: 32}, ir.Integer{Width: 32, Unsigned: true}, false},
		{"text into varchar: across families", ir.Varchar{Length: 65535}, ir.Text{Size: ir.TextLong}, false},
		{"varchar into text", ir.Text{Size: ir.TextLong}, ir.Varchar{Length: 64}, true},
		// GC-44 F5 third review: CHAR is not held across families — a
		// Postgres source sends bpchar PADDED while the target's rows hold
		// the unpadded value, so a key-scoped UPDATE/DELETE matches nothing.
		{"char into varchar(m ≥ n) refuses (bpchar arrives padded)", ir.Varchar{Length: 20}, ir.Char{Length: 10}, false},
		{"char into text refuses (bpchar arrives padded)", ir.Text{Size: ir.TextLong}, ir.Char{Length: 10}, false},
		// A negative scale rounds to a power of ten, so it holds no integer
		// type whatever precision − scale counts (Postgres 15+).
		{"smallint into numeric(5,-2) refuses", ir.Decimal{Precision: 5, Scale: -2}, ir.Integer{Width: 16}, false},
		{"smallint into numeric(5,0)", ir.Decimal{Precision: 5}, ir.Integer{Width: 16}, true},
		{"numeric(5,-2) into numeric(7,0): integer digits held, scale wider", ir.Decimal{Precision: 7}, ir.Decimal{Precision: 5, Scale: -2}, true},
		{"numeric(7,0) into numeric(5,-2) refuses", ir.Decimal{Precision: 5, Scale: -2}, ir.Decimal{Precision: 7}, false},
		{"numeric(3,0) into numeric(5,-2) refuses (mixed axes)", ir.Decimal{Precision: 5, Scale: -2}, ir.Decimal{Precision: 3}, false},
		{"numeric(2,5) into numeric(3,6)", ir.Decimal{Precision: 3, Scale: 6}, ir.Decimal{Precision: 2, Scale: 5}, true},
		{"numeric(3,6) into numeric(2,5) refuses", ir.Decimal{Precision: 2, Scale: 5}, ir.Decimal{Precision: 3, Scale: 6}, false},
		// SET renders in declaration order: a reordered superset changes the
		// string a value reads back as.
		{"set labels under an in-order superset", ir.Set{Values: []string{"a", "b", "c"}}, ir.Set{Values: []string{"a", "b"}}, true},
		{"set labels under a reordered superset refuse", ir.Set{Values: []string{"b", "a", "c"}}, ir.Set{Values: []string{"a", "b"}}, false},
		{"enum labels under a reordered superset", ir.Enum{Values: []string{"b", "a", "c"}}, ir.Enum{Values: []string{"a", "b"}}, true},
		// Arrays, compared through their element modifier.
		{"array element: target wider", ir.Array{Element: ir.Decimal{Precision: 12, Scale: 4}}, ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}}, true},
		{"array element: source wider", ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}}, ir.Array{Element: ir.Decimal{Precision: 10, Scale: 4}}, false},
		{"array element: timestamp(0)[] under (6)[]", ir.Array{Element: ir.Timestamp{Precision: 6}}, ir.Array{Element: ir.Timestamp{Precision: 0}}, true},
		{"array element: timestamp(6)[] over (0)[]", ir.Array{Element: ir.Timestamp{Precision: 0}}, ir.Array{Element: ir.Timestamp{Precision: 6}}, false},
		{"char into varchar(m < n)", ir.Varchar{Length: 5}, ir.Char{Length: 10}, false},
		{"integer into a decimal that holds it", ir.Decimal{Precision: 30, Scale: 0}, ir.Integer{Width: 32}, true},
		{"integer into a decimal too narrow for it", ir.Decimal{Precision: 9, Scale: 0}, ir.Integer{Width: 32}, false},
		{"json into jsonb (jsonb normalizes)", ir.JSON{Binary: true}, ir.JSON{}, false},
	} {
		j := judgeUnforwardedColumns(witnessTable(wcol("c", tc.source)), witnessTable(wcol("c", tc.target)), witnessOptions{}, judgedPrior{})
		if passes := len(j.refused) == 0; passes != tc.passes {
			t.Errorf("%s: target %s, source %s: passes = %v (refused %v), want %v",
				tc.name, tc.target, tc.source, passes, j.refused, tc.passes)
		}
	}
}

// TestJudgeUnforwardedColumns is the column-by-column judgement: what
// refuses, what passes, and that a single refusing column refuses the
// boundary whatever else it carries.
func TestJudgeUnforwardedColumns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		snap, target   *ir.Table
		refused, ahead int
		targetOnly     int
	}{
		{"a match", witnessTable(wcol("ts", ir.DateTime{})), witnessTable(wcol("ts", ir.DateTime{})), 0, 0, 0},
		{"fsp widened on the source", witnessTable(wcol("ts", ir.DateTime{Precision: 6})), witnessTable(wcol("ts", ir.DateTime{})), 1, 0, 0},
		{"fsp widened on the target first", witnessTable(wcol("ts", ir.DateTime{})), witnessTable(wcol("ts", ir.DateTime{Precision: 6})), 0, 1, 0},
		{"decimal scale widened on the source", witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 4})), witnessTable(wcol("v", ir.Decimal{Precision: 10, Scale: 2})), 1, 0, 0},
		{"varchar widened on the source", witnessTable(wcol("v", ir.Varchar{Length: 64})), witnessTable(wcol("v", ir.Varchar{Length: 16})), 1, 0, 0},
		{"a column added on the source", witnessTable(wcol("x", ir.Integer{Width: 32})), witnessTable(), 1, 0, 0},
		{"a column the target alone has", witnessTable(), witnessTable(wcol("x", ir.Integer{Width: 32})), 0, 0, 1},
		{"a rename reads as add + target-only, and refuses", witnessTable(wcol("b", ir.Integer{Width: 32})), witnessTable(wcol("a", ir.Integer{Width: 32})), 1, 0, 1},
		{"one column ahead, one behind", witnessTable(wcol("a", ir.DateTime{}), wcol("b", ir.DateTime{Precision: 6})), witnessTable(wcol("a", ir.DateTime{Precision: 6}), wcol("b", ir.DateTime{})), 1, 1, 0},
		{"across families", witnessTable(wcol("v", ir.Text{Size: ir.TextLong})), witnessTable(wcol("v", ir.Integer{Width: 32})), 1, 0, 0},
	} {
		j := judgeUnforwardedColumns(tc.snap, tc.target, witnessOptions{}, judgedPrior{})
		if len(j.refused) != tc.refused || len(j.ahead) != tc.ahead || len(j.targetOnly) != tc.targetOnly {
			t.Errorf("%s: refused %d, ahead %d, target-only %d; want %d, %d, %d",
				tc.name, len(j.refused), len(j.ahead), len(j.targetOnly), tc.refused, tc.ahead, tc.targetOnly)
		}
		err := j.settle(context.Background(), "src.w", unforwardedBoundaryDeps{why: "--schema-changes=refuse"})
		if refused := err != nil; refused != (tc.refused > 0) {
			t.Errorf("%s: settle refused = %v (%v), want %v", tc.name, refused, err, tc.refused > 0)
		}
		if err != nil && !errors.Is(err, ir.ErrSchemaChangeRefused) {
			t.Errorf("%s: the refusal does not carry %s: %v", tc.name, schemaChangeRefusedMarker, err)
		}
	}
}

// runRefuseIntercept feeds snaps through the intercept and returns what came
// out the other side and the stored error.
func runRefuseIntercept(t *testing.T, deps unforwardedBoundaryDeps, snaps ...ir.SchemaSnapshot) ([]ir.Change, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := make(chan ir.Change, len(snaps))
	for _, s := range snaps {
		in <- s
	}
	close(in)
	var errStore atomic.Pointer[error]
	var out []ir.Change
	for c := range interceptSchemaChangeRefuse(ctx, in, deps, &errStore) {
		out = append(out, c)
	}
	if p := errStore.Load(); p != nil {
		return out, *p
	}
	return out, nil
}

func refuseSnap(tbl *ir.Table) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{Schema: tbl.Schema, Table: tbl.Name, IR: tbl}
}

// TestInterceptSchemaChangeRefuse pins the intercept itself: a refused
// boundary never goes downstream, a stale memo is re-read before refusing,
// the unwitnessed fallback compares CDC with CDC, and a catalog failure is
// not the marker.
func TestInterceptSchemaChangeRefuse(t *testing.T) {
	t.Parallel()
	narrow := witnessTable(wcol("ts", ir.DateTime{}))
	wide := witnessTable(wcol("ts", ir.DateTime{Precision: 6}))
	single := func(w *firstBoundaryWitness) func(string) *firstBoundaryWitness {
		return func(string) *firstBoundaryWitness { return w }
	}

	t.Run("a match passes the boundary on", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": narrow}}
		out, err := runRefuseIntercept(t, unforwardedBoundaryDeps{witnessFor: single(newFakeWitness(cat, "mysql", "mysql"))}, refuseSnap(narrow))
		if err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want the snapshot passed on", len(out), err)
		}
	})
	t.Run("a widen the target cannot hold refuses before the boundary goes downstream", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": narrow}}
		out, err := runRefuseIntercept(t, unforwardedBoundaryDeps{witnessFor: single(newFakeWitness(cat, "mysql", "mysql")), why: "--schema-changes=refuse"}, refuseSnap(wide))
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || !strings.Contains(err.Error(), schemaChangeRefusedMarker) {
			t.Fatalf("err = %v; want the %s refusal", err, schemaChangeRefusedMarker)
		}
		if !strings.Contains(err.Error(), `"ts"`) || !strings.Contains(err.Error(), "--schema-changes=refuse") {
			t.Errorf("the refusal names neither the column nor the mode: %v", err)
		}
		if len(out) != 0 {
			t.Errorf("%d changes went downstream past the refusal", len(out))
		}
		if cat.reads != 3 {
			t.Errorf("catalog read %d times; want 3 (the first read, the verdict's re-read, and this check's own before refusing)", cat.reads)
		}
	})
	t.Run("the target altered after the verdict's one re-read passes on this check's own", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": narrow}}
		w := newFakeWitness(cat, "mysql", "mysql")
		// An earlier disagreement on this table spent the verdict's once-per-
		// table re-read (it reads the catalog, still narrow).
		if j, err := w.judgeUnforwarded(context.Background(), wide, unforwardedPrior{}); err != nil || len(j.refused) == 0 {
			t.Fatalf("the first judgement = %+v, %v; want a refusal", j, err)
		}
		cat.tables["w"] = wide // the operator's drained-model ALTER on the target
		out, err := runRefuseIntercept(t, unforwardedBoundaryDeps{witnessFor: single(w)}, refuseSnap(wide))
		if err != nil || len(out) != 1 {
			t.Fatalf("out %d, err %v; want the boundary accepted after the re-read", len(out), err)
		}
	})
	t.Run("unwitnessed: the first boundary passes, a later change refuses", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": narrow}}
		deps := unforwardedBoundaryDeps{witnessFor: single(newFakeWitness(cat, "postgres", "sqlite"))}
		out, err := runRefuseIntercept(t, deps, refuseSnap(narrow), refuseSnap(narrow))
		if err != nil || len(out) != 2 {
			t.Fatalf("unchanged: out %d, err %v; want both passed", len(out), err)
		}
		out, err = runRefuseIntercept(t, deps, refuseSnap(narrow), refuseSnap(wide))
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || len(out) != 1 {
			t.Fatalf("changed: out %d, err %v; want the first passed and the second refused", len(out), err)
		}
	})
	t.Run("no witness at all behaves as unwitnessed", func(t *testing.T) {
		out, err := runRefuseIntercept(t, unforwardedBoundaryDeps{}, refuseSnap(narrow), refuseSnap(wide))
		if !errors.Is(err, ir.ErrSchemaChangeRefused) || len(out) != 1 {
			t.Fatalf("out %d, err %v; want the first passed and the second refused", len(out), err)
		}
	})
	t.Run("a catalog read failure refuses without the marker", func(t *testing.T) {
		cat := &fakeCatalog{err: errors.New("connection refused")}
		_, err := runRefuseIntercept(t, unforwardedBoundaryDeps{witnessFor: single(newFakeWitness(cat, "mysql", "mysql"))}, refuseSnap(narrow))
		if err == nil || errors.Is(err, ir.ErrSchemaChangeRefused) {
			t.Fatalf("err = %v; want a catalog error that is not %s", err, schemaChangeRefusedMarker)
		}
	})
}

// refuseWiringEngine is the source and target engine of the wiring roster:
// a name, and a target catalog read through OpenSchemaReader (and, for the
// multi-database fan-out, through a per-namespace DSN). Any other call
// panics through the nil embedded interface.
type refuseWiringEngine struct {
	ir.Engine
	name    string
	catalog *ir.Table
	// readErr, when set, fails every catalog read.
	readErr error
}

func (e refuseWiringEngine) Name() string { return e.name }

func (e refuseWiringEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	if e.readErr != nil {
		return nil, e.readErr
	}
	return refuseWiringReader{e.catalog}, nil
}

func (refuseWiringEngine) WithDatabase(dsn, database string) (string, error) {
	return dsn + "/" + database, nil
}

func (refuseWiringEngine) EnsureDatabase(context.Context, string, string) error { return nil }

type refuseWiringReader struct{ t *ir.Table }

func (r refuseWiringReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return &ir.Schema{Tables: []*ir.Table{r.t}}, nil
}

// TestPhaseWireInterceptChain_EveryUnforwardedStreamShapeIsChecked is the
// wiring roster: every Streamer shape that wires neither forwarding
// intercept — refuse mode, a multi-database stream (forward or refuse),
// Shape A under --no-coordinate-live-ddl — refuses a boundary the target
// cannot hold and passes one it matches. Before GC-44 F5 each of them passed
// both, unchecked.
func TestPhaseWireInterceptChain_EveryUnforwardedStreamShapeIsChecked(t *testing.T) {
	t.Parallel()
	shard := ShardColumnSpec{Name: "shard", Value: "a"}
	narrow := witnessTable(wcol("ts", ir.DateTime{}))
	wide := witnessTable(wcol("ts", ir.DateTime{Precision: 6}))
	for _, tc := range []struct {
		name string
		s    func() *Streamer
	}{
		{"--schema-changes=refuse", func() *Streamer { return &Streamer{SchemaChanges: "refuse"} }},
		{"multi-database (forward)", func() *Streamer { return &Streamer{AllDatabases: true} }},
		{"multi-database under refuse", func() *Streamer { return &Streamer{AllDatabases: true, SchemaChanges: "refuse"} }},
		{"Shape A with --no-coordinate-live-ddl", func() *Streamer {
			return &Streamer{InjectShardColumn: shard, NoCoordinateLiveDDL: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, c := range []struct {
				snap       *ir.Table
				wantRefuse bool
			}{{narrow, false}, {wide, true}} {
				target := narrow
				if tc.name == "Shape A with --no-coordinate-live-ddl" {
					// The target carries the discriminator the cold start
					// injected.
					cp := *narrow
					cp.Columns = append(append([]*ir.Column{}, narrow.Columns...), &ir.Column{Name: "shard", Type: ir.Varchar{Length: 64}})
					target = &cp
				}
				s := tc.s()
				s.Source = refuseWiringEngine{name: "mysql"}
				tgt := refuseWiringEngine{name: "mysql", catalog: target}
				s.Target = tgt
				if s.multiDatabaseMode() {
					// What the multi-database open hands over after its
					// flat-target and fold preflights.
					s.namespaceTargetDeriver = tgt
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				in := make(chan ir.Change, 1)
				in <- refuseSnap(c.snap)
				close(in)
				var got []ir.Change
				for ch := range s.phaseWireInterceptChain(ctx, in, nil, "roster") {
					got = append(got, ch)
				}
				cancel()
				var err error
				if p := s.schemaSnapshotErr.Load(); p != nil {
					err = *p
				}
				if c.wantRefuse {
					if !errors.Is(err, ir.ErrSchemaChangeRefused) || len(got) != 0 {
						t.Errorf("a widen the target cannot hold: passed %d, err %v; want refused with %s", len(got), err, schemaChangeRefusedMarker)
					}
					continue
				}
				if err != nil || len(got) != 1 {
					t.Errorf("a boundary matching the target: passed %d, err %v; want passed", len(got), err)
				}
			}
		})
	}
}

// TestOverriddenColumnRefused_TruthTable pins the GC-44 F5 review's override
// class at the judgement: an overridden column, which the witness lens does
// not compare by type, is still judged on width. The independent expected
// value is the table, written from what each target type stores.
func TestOverriddenColumnRefused_TruthTable(t *testing.T) {
	t.Parallel()
	dec := func(p, s int) ir.Type { return ir.Decimal{Precision: p, Scale: s} }
	for _, tc := range []struct {
		name                  string
		target, source, prior ir.Type
		refused               bool
	}{
		{"the review's shape: a live widen past the override", dec(12, 2), dec(14, 4), dec(10, 2), true},
		{"a widen past the override with no prior", dec(12, 2), dec(14, 4), nil, true},
		{"the override is wider than the source", dec(12, 2), dec(10, 2), dec(10, 2), false},
		{"the override is wider than the widened source", dec(16, 6), dec(14, 4), dec(10, 2), false},
		{"a deliberate narrowing override, unchanged since", dec(10, 2), dec(14, 4), dec(14, 4), false},
		{"a deliberate narrowing override, with no prior", dec(10, 2), dec(14, 4), nil, true},
		{"a varchar override narrower than a widened source", ir.Varchar{Length: 32}, ir.Varchar{Length: 64}, ir.Varchar{Length: 16}, true},
		{"a temporal override narrower than a widened source", ir.DateTime{Precision: 0}, ir.DateTime{Precision: 6}, ir.DateTime{Precision: 0}, true},
		{"an integer override narrower than a widened source", ir.Integer{Width: 32}, ir.Integer{Width: 64}, ir.Integer{Width: 32}, true},
		{"an override across families is the operator's decision", ir.JSON{}, ir.Text{Size: ir.TextLong}, nil, false},
		{"a smallint override on a boolean", ir.Integer{Width: 16}, ir.Boolean{}, nil, false},
	} {
		if got := overriddenColumnRefused(tc.target, tc.source, tc.prior); got != tc.refused {
			t.Errorf("%s: overriddenColumnRefused(%v, %v, %v) = %v, want %v", tc.name, tc.target, tc.source, tc.prior, got, tc.refused)
		}
	}
}

// TestJudgeUnforwarded_OverriddenColumnThroughTheWitness drives the override
// class through the witness the intercept uses: an override on the column
// (so the lens equates its types), a live source widen past it — refused,
// naming the override — and the same override with an unchanged source,
// which passes.
func TestJudgeUnforwarded_OverriddenColumnThroughTheWitness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	before := witnessTable(wcol("amount", ir.Decimal{Precision: 10, Scale: 2}))
	after := witnessTable(wcol("amount", ir.Decimal{Precision: 14, Scale: 4}))
	target := witnessTable(wcol("amount", ir.Decimal{Precision: 12, Scale: 2}))
	newW := func() *firstBoundaryWitness {
		w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": target}}, "mysql", "mysql")
		w.mappings = []config.Mapping{{Table: "w", Column: "amount", TargetType: "numeric", TargetTypeOptions: map[string]any{"precision": 12, "scale": 2}}}
		return w
	}
	j, err := newW().judgeUnforwarded(ctx, after, unforwardedPrior{raw: before})
	if err != nil || len(j.refused) != 1 || !strings.Contains(j.refused[0].target, "--type-override") {
		t.Fatalf("live widen past the override: %+v, %v; want one refusal naming the override", j, err)
	}
	if err := j.settle(ctx, "src.w", unforwardedBoundaryDeps{why: "--schema-changes=refuse"}); !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Errorf("settle = %v; want %s", err, schemaChangeRefusedMarker)
	}
	if j, err := newW().judgeUnforwarded(ctx, before, unforwardedPrior{raw: before}); err != nil || len(j.refused) != 0 {
		t.Errorf("unchanged source under a wider override: %+v, %v; want accepted", j, err)
	}
}

// TestUnforwardedRecoveryHint pins the hint's two qualifications: the
// forward remedy is offered only where that flag forwards, and an ADD COLUMN
// remedy says the rows the target holds are not backfilled.
func TestUnforwardedRecoveryHint(t *testing.T) {
	t.Parallel()
	if h := unforwardedRecoveryHint("t", false, false); strings.Contains(h, "--schema-changes=forward") || strings.Contains(h, "backfill") {
		t.Errorf("multi-database / Shape A hint offers forward or a backfill note: %s", h)
	}
	if h := unforwardedRecoveryHint("t", true, false); !strings.Contains(h, "--schema-changes=forward") {
		t.Errorf("refuse-mode hint lacks the forward remedy: %s", h)
	}
	if h := unforwardedRecoveryHint("t", false, true); !strings.Contains(h, "not backfilled") {
		t.Errorf("ADD COLUMN hint lacks the no-backfill note: %s", h)
	}
	for _, tc := range []struct {
		name string
		s    *Streamer
		want bool
	}{
		{"refuse mode", &Streamer{SchemaChanges: "refuse"}, true},
		{"multi-database", &Streamer{AllDatabases: true, SchemaChanges: "refuse"}, false},
		{"Shape A drained", &Streamer{SchemaChanges: "refuse", InjectShardColumn: ShardColumnSpec{Name: "s", Value: "a"}, NoCoordinateLiveDDL: true}, false},
	} {
		if got := tc.s.unforwardedForwardRemedy(); got != tc.want {
			t.Errorf("%s: forward remedy offered = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestInterceptSchemaChangeRefuse_UnwitnessedRepeatsFromHistory pins that a
// refusal on a table the target cannot witness repeats on the next start:
// the boundary it refused was never recorded, so the stream's retained
// history still holds the shape before it, and the fresh intercept compares
// against that rather than accepting its first boundary.
func TestInterceptSchemaChangeRefuse_UnwitnessedRepeatsFromHistory(t *testing.T) {
	t.Parallel()
	narrow := witnessTable(wcol("ts", ir.DateTime{}))
	wide := witnessTable(wcol("ts", ir.DateTime{Precision: 6}))
	w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{}}, "postgres", "sqlite")
	w.history = []*ir.Table{narrow}
	deps := unforwardedBoundaryDeps{witnessFor: func(string) *firstBoundaryWitness { return w }}
	if _, err := runRefuseIntercept(t, deps, refuseSnap(wide)); !errors.Is(err, ir.ErrSchemaChangeRefused) {
		t.Errorf("first boundary after a restart with history: err %v; want %s", err, schemaChangeRefusedMarker)
	}
	if out, err := runRefuseIntercept(t, deps, refuseSnap(narrow)); err != nil || len(out) != 1 {
		t.Errorf("unchanged against history: out %d, err %v; want accepted", len(out), err)
	}
}

// schemaGateStub records the reader-gate mode it was given.
type schemaGateStub struct {
	ir.CDCReader
	relaxed, set bool
}

func (s *schemaGateStub) SetSchemaForward(enabled bool) { s.relaxed, s.set = enabled, true }

// TestWireSchemaDeltaArming_ReaderGateRelaxedOnlyUnderTheForwardIntercept
// pins the reader's own mid-stream schema gate at the one helper every
// reader-open site reaches: relaxed ONLY where the single-stream forward
// intercept runs (forward mode, a single database, no
// --inject-shard-column) and kept everywhere else — refuse mode,
// multi-database / multi-schema in either mode, Shape A drained and
// coordinated. That is v0.156.9's behaviour, restored by operator decision
// (2026-10-03) after the GC-44 F5 review had relaxed it on every stream an
// intercept judges; the wedge the relaxation removed is GC-44 F24.
func TestWireSchemaDeltaArming_ReaderGateRelaxedOnlyUnderTheForwardIntercept(t *testing.T) {
	t.Parallel()
	shard := ShardColumnSpec{Name: "shard", Value: "a"}
	for _, tc := range []struct {
		name string
		s    *Streamer
		want bool
	}{
		{"forward (the zero value)", &Streamer{}, true},
		{"explicit forward", &Streamer{SchemaChanges: "forward"}, true},
		{"--schema-changes=refuse", &Streamer{SchemaChanges: "refuse"}, false},
		{"multi-database", &Streamer{AllDatabases: true}, false},
		{"multi-database refuse", &Streamer{AllDatabases: true, SchemaChanges: "refuse"}, false},
		{"Shape A drained", &Streamer{InjectShardColumn: shard, NoCoordinateLiveDDL: true}, false},
		{"Shape A coordinated", &Streamer{InjectShardColumn: shard}, false},
	} {
		r := &schemaGateStub{}
		tc.s.wireSchemaDeltaArming(r)
		if !r.set || r.relaxed != tc.want {
			t.Errorf("%s: gate set %v relaxed %v; want relaxed %v", tc.name, r.set, r.relaxed, tc.want)
		}
	}
}

// TestPhaseWireInterceptChain_MultiDatabaseCatalogReadErrorIsLoud pins that a
// multi-database namespace whose target catalog cannot be read stops the
// stream (not the marker — restartable) instead of degrading to "not held"
// and accepting the boundary with a WARN.
func TestPhaseWireInterceptChain_MultiDatabaseCatalogReadErrorIsLoud(t *testing.T) {
	t.Parallel()
	tgt := refuseWiringEngine{name: "mysql", readErr: errors.New("access denied")}
	s := &Streamer{AllDatabases: true, Source: refuseWiringEngine{name: "mysql"}, Target: tgt, namespaceTargetDeriver: tgt}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	in <- refuseSnap(witnessTable(wcol("ts", ir.DateTime{Precision: 6})))
	close(in)
	var got []ir.Change
	for ch := range s.phaseWireInterceptChain(ctx, in, nil, "roster") {
		got = append(got, ch)
	}
	p := s.schemaSnapshotErr.Load()
	if p == nil || len(got) != 0 || errors.Is(*p, ir.ErrSchemaChangeRefused) || !strings.Contains((*p).Error(), "access denied") {
		t.Fatalf("passed %d, err %v; want a loud, restartable catalog-read error", len(got), p)
	}
}
