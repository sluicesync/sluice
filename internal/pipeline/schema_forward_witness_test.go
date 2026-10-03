// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// witnessTable builds a one-PK table named w with the given extra columns.
func witnessTable(cols ...*ir.Column) *ir.Table {
	all := append([]*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, cols...)
	return &ir.Table{
		Schema: "src", Name: "w", Columns: all,
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func wcol(name string, t ir.Type) *ir.Column { return &ir.Column{Name: name, Type: t, Nullable: true} }

// TestClassifyWitness_VerdictTruthTable is the GC-44 verdict truth table:
// every verdict kind, the allowlist family by family (forward vs refuse at
// each family's edge), and every lens arm in both its "equal" direction and
// — where the arm keeps a signal — its "still differs" direction. The
// independent expected value is the table here, written from the design,
// not derived from the classifier.
func TestClassifyWitness_VerdictTruthTable(t *testing.T) {
	t.Parallel()
	dt := func(p int) ir.Type { return ir.DateTime{Precision: p} }
	cases := []struct {
		name      string
		post, tgt *ir.Table
		want      witnessVerdictKind
		added     []string
		altered   string
	}{
		{"identical", witnessTable(wcol("ts", dt(6))), witnessTable(wcol("ts", dt(6))), witnessMatch, nil, ""},
		{
			"post-only columns forward ADD in source order",
			witnessTable(wcol("b", ir.Text{Size: ir.TextLong}), wcol("a", dt(0)), wcol("c", ir.Boolean{})),
			witnessTable(wcol("a", dt(0))), witnessForwardAdd,
			[]string{"b", "c"},
			"",
		},
		{"target-only column WARNs", witnessTable(), witnessTable(wcol("gone", dt(0))), witnessTargetOnly, nil, ""},
		{
			"target-only GENERATED column is not a divergence", witnessTable(),
			witnessTable(&ir.Column{Name: "g", Type: ir.Integer{Width: 32}, GeneratedExpr: "id + 1"}), witnessMatch, nil, "",
		},

		// The allowlist, one family at a time.
		{"DATETIME fsp widen", witnessTable(wcol("ts", dt(6))), witnessTable(wcol("ts", dt(0))), witnessForwardAlter, nil, "ts"},
		{"TIME precision", witnessTable(wcol("t", ir.Time{Precision: 6})), witnessTable(wcol("t", ir.Time{Precision: 0})), witnessForwardAlter, nil, "t"},
		{
			"TIMETZ vs TIME refuses (zone kind changed, not the zone-sibling pair)",
			witnessTable(wcol("t", ir.Time{Precision: 6})), witnessTable(wcol("t", ir.Time{Precision: 6, WithTimeZone: true})), witnessRefuse, nil, "",
		},
		{
			"TIMESTAMPTZ precision", witnessTable(wcol("ts", ir.Timestamp{Precision: 6, WithTimeZone: true})),
			witnessTable(wcol("ts", ir.Timestamp{Precision: 3, WithTimeZone: true})), witnessForwardAlter, nil, "ts",
		},
		{
			"zone-sibling swap routes to the forward path's own zone door",
			witnessTable(wcol("ts", ir.Timestamp{WithTimeZone: true})), witnessTable(wcol("ts", dt(0))), witnessForwardAlter, nil, "ts",
		},
		{
			"DECIMAL scale", witnessTable(wcol("d", ir.Decimal{Precision: 10, Scale: 4})),
			witnessTable(wcol("d", ir.Decimal{Precision: 10, Scale: 2})), witnessForwardAlter, nil, "d",
		},
		{
			"FLOAT to DOUBLE", witnessTable(wcol("f", ir.Float{Precision: ir.FloatDouble})),
			witnessTable(wcol("f", ir.Float{Precision: ir.FloatSingle})), witnessForwardAlter, nil, "f",
		},
		{"VARCHAR widen", witnessTable(wcol("v", ir.Varchar{Length: 64})), witnessTable(wcol("v", ir.Varchar{Length: 16})), witnessForwardAlter, nil, "v"},
		{"CHAR length", witnessTable(wcol("c", ir.Char{Length: 8})), witnessTable(wcol("c", ir.Char{Length: 4})), witnessForwardAlter, nil, "c"},
		{"INT width", witnessTable(wcol("i", ir.Integer{Width: 64})), witnessTable(wcol("i", ir.Integer{Width: 32})), witnessForwardAlter, nil, "i"},
		{
			"INT sign change refuses", witnessTable(wcol("i", ir.Integer{Width: 32, Unsigned: true})),
			witnessTable(wcol("i", ir.Integer{Width: 32})), witnessRefuse, nil, "",
		},
		{
			"VARCHAR to TEXT refuses (across families)", witnessTable(wcol("v", ir.Text{Size: ir.TextLong})),
			witnessTable(wcol("v", ir.Varchar{Length: 16})), witnessRefuse, nil, "",
		},
		{
			"CHAR to VARCHAR refuses (across families)", witnessTable(wcol("v", ir.Varchar{Length: 8})),
			witnessTable(wcol("v", ir.Char{Length: 8})), witnessRefuse, nil, "",
		},
		{
			"DECIMAL to INT refuses", witnessTable(wcol("d", ir.Integer{Width: 64})),
			witnessTable(wcol("d", ir.Decimal{Precision: 10, Scale: 0})), witnessRefuse, nil, "",
		},
		{
			"two type changes refuse", witnessTable(wcol("a", dt(6)), wcol("b", ir.Varchar{Length: 64})),
			witnessTable(wcol("a", dt(0)), wcol("b", ir.Varchar{Length: 16})), witnessRefuse, nil, "",
		},
		{
			"added plus target-only (a possible rename) refuses", witnessTable(wcol("new_name", dt(0))),
			witnessTable(wcol("old_name", dt(0))), witnessRefuse, nil, "",
		},
		{
			"added plus a type change refuses", witnessTable(wcol("a", dt(6)), wcol("b", dt(0))),
			witnessTable(wcol("a", dt(0))), witnessRefuse, nil, "",
		},
		{
			"target-only plus a type change refuses", witnessTable(wcol("a", dt(6))),
			witnessTable(wcol("a", dt(0)), wcol("z", dt(0))), witnessRefuse, nil, "",
		},

		// The lens: every arm's "equal" direction.
		{
			"lens: AutoIncrement", witnessTable(wcol("n", ir.Integer{Width: 32})),
			witnessTable(wcol("n", ir.Integer{Width: 32, AutoIncrement: true})), witnessMatch, nil, "",
		},
		{
			"lens: charset/collation", witnessTable(wcol("v", ir.Varchar{Length: 9, Charset: "utf8mb4", Collation: "utf8mb4_bin"})),
			witnessTable(wcol("v", ir.Varchar{Length: 9, Collation: "C"})), witnessMatch, nil, "",
		},
		{
			"lens: Decimal{0,0} is unconstrained", witnessTable(wcol("d", ir.Decimal{})),
			witnessTable(wcol("d", ir.Decimal{Unconstrained: true})), witnessMatch, nil, "",
		},
		{
			"lens: geometry subtype/SRID", witnessTable(wcol("g", ir.Geometry{})),
			witnessTable(wcol("g", ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326})), witnessMatch, nil, "",
		},
		{
			"lens: geometry vs geography still differs", witnessTable(wcol("g", ir.Geometry{})),
			witnessTable(wcol("g", ir.Geometry{IsGeography: true})), witnessRefuse, nil, "",
		},
		{
			"lens: enum labels unknown on one side", witnessTable(wcol("e", ir.Enum{})),
			witnessTable(wcol("e", ir.Enum{TypeName: "mood", Values: []string{"a", "b"}})), witnessMatch, nil, "",
		},
		{
			"lens: enum type name", witnessTable(wcol("e", ir.Enum{Values: []string{"a"}})),
			witnessTable(wcol("e", ir.Enum{TypeName: "t_e_enum", Values: []string{"a"}})), witnessMatch, nil, "",
		},
		{
			"lens: enum labels both known and different still differ", witnessTable(wcol("e", ir.Enum{Values: []string{"a", "b", "c"}})),
			witnessTable(wcol("e", ir.Enum{Values: []string{"a", "b"}})), witnessRefuse, nil, "",
		},
		{
			"lens: MariaDB JSON (a LONGTEXT on the binlog) vs a JSON target", witnessTable(wcol("j", ir.Text{Size: ir.TextLong})),
			witnessTable(wcol("j", ir.JSON{})), witnessMatch, nil, "",
		},
		{
			"lens: JSON vs long TEXT, the other way round", witnessTable(wcol("j", ir.JSON{Binary: true})),
			witnessTable(wcol("j", ir.Text{Size: ir.TextLong})), witnessMatch, nil, "",
		},
		{
			"lens: JSON vs a SHORTER text still differs", witnessTable(wcol("j", ir.Text{Size: ir.TextMedium})),
			witnessTable(wcol("j", ir.JSON{})), witnessRefuse, nil, "",
		},
		{
			"lens: domain read through its storage", witnessTable(wcol("m", ir.Text{Size: ir.TextLong})),
			witnessTable(wcol("m", ir.Domain{Name: "email", BaseType: ir.Text{Size: ir.TextLong}})), witnessMatch, nil, "",
		},
		{
			"lens: array element modifier", witnessTable(wcol("a", ir.Array{Element: ir.Decimal{Unconstrained: true}})),
			witnessTable(wcol("a", ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}})), witnessMatch, nil, "",
		},
		{
			"lens: array element family still differs", witnessTable(wcol("a", ir.Array{Element: ir.Text{Size: ir.TextLong}})),
			witnessTable(wcol("a", ir.Array{Element: ir.Integer{Width: 32}})), witnessRefuse, nil, "",
		},
		{
			"nullability and PK presence are not compared",
			&ir.Table{Name: "w", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}},
			&ir.Table{Name: "w", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}, Nullable: true}}}, witnessMatch, nil, "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := classifyWitness(tc.post, tc.tgt)
			if v.kind != tc.want {
				t.Fatalf("verdict = %d, want %d (%s)", v.kind, tc.want, v.render())
			}
			if tc.added != nil && !reflect.DeepEqual(v.added, tc.added) {
				t.Errorf("added = %v, want %v", v.added, tc.added)
			}
			if tc.altered != "" {
				if v.altered != tc.altered {
					t.Errorf("altered = %q, want %q", v.altered, tc.altered)
				}
				want := columnsByNameIR(tc.tgt)[tc.altered].Type
				if !reflect.DeepEqual(v.targetType, want) {
					t.Errorf("targetType = %v, want the target's raw read-back %v", v.targetType, want)
				}
			}
		})
	}
}

// fakeCatalog is a target catalog the witness reads, counting reads.
type fakeCatalog struct {
	tables map[string]*ir.Table
	reads  int
	err    error
}

func (f *fakeCatalog) load(context.Context) (map[string]*ir.Table, error) {
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]*ir.Table, len(f.tables))
	for k, v := range f.tables {
		out[k] = v
	}
	return out, nil
}

func newFakeWitness(cat *fakeCatalog, src, tgt string) *firstBoundaryWitness {
	return &firstBoundaryWitness{catalog: newTargetCatalogWitness(cat.load, nil), sourceEngine: src, targetEngine: tgt}
}

func TestFirstBoundaryWitness_Verdict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("the target does not hold the table", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{}}
		v, err := newFakeWitness(cat, "postgres", "postgres").verdict(ctx, witnessTable())
		if err != nil || v.kind != witnessUnwitnessed {
			t.Fatalf("verdict = %d, %v; want unwitnessed", v.kind, err)
		}
	})
	t.Run("no storage-shape rendering for the pair", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable()}}
		v, err := newFakeWitness(cat, "postgres", "sqlite").verdict(ctx, witnessTable())
		if err != nil || v.kind != witnessUnwitnessed || cat.reads != 0 {
			t.Fatalf("verdict = %d, %v, reads %d; want unwitnessed with no catalog read", v.kind, err, cat.reads)
		}
	})
	t.Run("memoized; a miss re-reads once per table", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable()}}
		w := newFakeWitness(cat, "mysql", "mysql")
		for range 3 {
			if v, err := w.verdict(ctx, witnessTable()); err != nil || v.kind != witnessMatch {
				t.Fatalf("verdict = %d, %v; want match", v.kind, err)
			}
		}
		if cat.reads != 1 {
			t.Fatalf("catalog read %d times for three lookups of one held table; want 1", cat.reads)
		}
		other := witnessTable()
		other.Name = "late"
		for range 2 {
			if v, _ := w.verdict(ctx, other); v.kind != witnessUnwitnessed {
				t.Fatalf("verdict for an absent table = %d; want unwitnessed", v.kind)
			}
		}
		if cat.reads != 2 {
			t.Fatalf("catalog read %d times; want 2 (one refresh for the miss, none after)", cat.reads)
		}
	})
	t.Run("a table created after the first read is found by the refresh", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable()}}
		w := newFakeWitness(cat, "mysql", "mysql")
		_, _ = w.verdict(ctx, witnessTable())
		late := witnessTable()
		late.Name = "late"
		cat.tables["late"] = late
		if v, _ := w.verdict(ctx, late); v.kind != witnessMatch {
			t.Fatalf("verdict = %d; want match after the refresh", v.kind)
		}
	})
	t.Run("a case-folded target name resolves", func(t *testing.T) {
		tgt := witnessTable()
		tgt.Name = "orders"
		cat := &fakeCatalog{tables: map[string]*ir.Table{"orders": tgt}}
		post := witnessTable()
		post.Name = "Orders"
		if v, _ := newFakeWitness(cat, "mysql", "mysql").verdict(ctx, post); v.kind != witnessMatch {
			t.Fatalf("verdict = %d; want match", v.kind)
		}
	})
	t.Run("a catalog read error is returned, not degraded", func(t *testing.T) {
		cat := &fakeCatalog{err: errors.New("target down")}
		if _, err := newFakeWitness(cat, "mysql", "mysql").verdict(ctx, witnessTable()); err == nil {
			t.Fatal("verdict swallowed a catalog read error")
		}
	})
	t.Run("a --type-override holds the column to its override", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable(wcol("ts", ir.Text{Size: ir.TextLong}))}}
		w := newFakeWitness(cat, "mysql", "mysql")
		w.mappings = []config.Mapping{
			{Table: "w", Column: "ts", TargetType: "text"},
			{Table: "w", Column: "dropped_since", TargetType: "text"},
			{Table: "other", Column: "x", TargetType: "text"},
		}
		v, err := w.verdict(ctx, witnessTable(wcol("ts", ir.DateTime{Precision: 6})))
		if err != nil || v.kind != witnessMatch {
			t.Fatalf("verdict = %d, %v (%s); want match", v.kind, err, v.render())
		}
	})
	t.Run("Shape A: the discriminator the target carries is expected", func(t *testing.T) {
		tgt := witnessTable(wcol("_sluice_shard", ir.Varchar{Length: 64}))
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": tgt}}
		w := newFakeWitness(cat, "mysql", "mysql")
		w.shardColumn = "_sluice_shard"
		if v, err := w.verdict(ctx, witnessTable()); err != nil || v.kind != witnessMatch {
			t.Fatalf("verdict = %d, %v (%s); want match", v.kind, err, v.render())
		}
	})
}

// runWitnessIntercept drives interceptAddColumnForward over snaps with the
// given seed and catalog, returning what it forwarded and stored.
func runWitnessIntercept(t *testing.T, seed []ir.SchemaSnapshot, cat *fakeCatalog, snaps ...ir.SchemaSnapshot) ([]ir.Change, *fakeShapeApplier, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in := make(chan ir.Change, len(snaps))
	for _, s := range snaps {
		in <- s
	}
	close(in)
	applier := &fakeShapeApplier{}
	deps := schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		witness:          newFakeWitness(cat, "postgres", "postgres"),
	}
	var errStore atomic.Pointer[error]
	got := drainChannel(t, interceptAddColumnForward(ctx, in, seed, deps, &errStore), 2*time.Second)
	var err error
	if p := errStore.Load(); p != nil {
		err = *p
	}
	return got, applier, err
}

// TestInterceptAddColumnForward_FirstBoundaryIsWitnessed pins D1 on the
// single-stream intercept: a table's first snapshot is forwarded against
// the target, not cached blind (mutation M1 — restore the blind baseline —
// fails the alter and add cases).
func TestInterceptAddColumnForward_FirstBoundaryIsWitnessed(t *testing.T) {
	cases := []struct {
		name      string
		post, tgt *ir.Table
		calls     []string
		refuse    bool
		forwarded int
	}{
		{
			"match accepts the baseline", witnessTable(wcol("ts", ir.DateTime{Precision: 6})),
			witnessTable(wcol("ts", ir.DateTime{Precision: 6})), nil, false, 1,
		},
		{
			"fsp widen forwards ALTER COLUMN TYPE", witnessTable(wcol("ts", ir.DateTime{Precision: 6})),
			witnessTable(wcol("ts", ir.DateTime{Precision: 0})),
			[]string{"AlterColumnType"},
			false, 1,
		},
		{
			"a new column forwards ADD COLUMN", witnessTable(wcol("ts", ir.DateTime{}), wcol("extra", ir.Text{Size: ir.TextLong})),
			witnessTable(wcol("ts", ir.DateTime{})),
			[]string{"AlterAddColumn"},
			false, 1,
		},
		{
			"a dropped column WARNs and accepts", witnessTable(),
			witnessTable(wcol("gone", ir.Text{Size: ir.TextLong})), nil, false, 1,
		},
		{
			"a rename refuses before the snapshot goes downstream", witnessTable(wcol("renamed", ir.Text{Size: ir.TextLong})),
			witnessTable(wcol("original", ir.Text{Size: ir.TextLong})), nil, true, 0,
		},
		{
			"a zone swap refuses through the zone door", witnessTable(wcol("ts", ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true})),
			witnessTable(wcol("ts", ir.DateTime{PrecisionUnspecified: true})), nil, true, 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat := &fakeCatalog{tables: map[string]*ir.Table{"w": tc.tgt}}
			got, applier, err := runWitnessIntercept(t, nil, cat, addColForwardSnap(tc.post))
			if calls := applier.callNames(); !reflect.DeepEqual(calls, tc.calls) && (len(calls) != 0 || len(tc.calls) != 0) {
				t.Errorf("applier calls = %v, want %v", calls, tc.calls)
			}
			if tc.refuse {
				if err == nil {
					t.Fatal("no refusal stored")
				}
				if tc.name != "a zone swap refuses through the zone door" && !strings.Contains(err.Error(), resumeDivergenceMarker) {
					t.Errorf("refusal lacks the %s marker: %v", resumeDivergenceMarker, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if len(got) != tc.forwarded {
				t.Errorf("forwarded %d changes downstream, want %d (history is written only after a match or an applied forward)", len(got), tc.forwarded)
			}
		})
	}
}

// TestInterceptAddColumnForward_SeedGuardConsultsTheWitness pins D2: an
// ALTER COLUMN TYPE classified against the cold-start seed is a phantom
// when the target already agrees with the snapshot, and genuine when it
// does not (mutation M5 — restore the blind seed-guard skip — fails the
// genuine case).
func TestInterceptAddColumnForward_SeedGuardConsultsTheWitness(t *testing.T) {
	seedTbl := witnessTable(wcol("ts", ir.DateTime{Precision: 0}))
	seed := []ir.SchemaSnapshot{{Schema: seedTbl.Schema, Table: seedTbl.Name, IR: seedTbl}}
	post := witnessTable(wcol("ts", ir.DateTime{Precision: 6}))
	t.Run("target agrees with the snapshot: phantom, skipped", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable(wcol("ts", ir.DateTime{Precision: 6}))}}
		_, applier, err := runWitnessIntercept(t, seed, cat, addColForwardSnap(post))
		if err != nil || len(applier.callNames()) != 0 {
			t.Fatalf("calls %v, err %v; want no forward", applier.callNames(), err)
		}
	})
	t.Run("target still holds the seed's type: genuine, forwarded", func(t *testing.T) {
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": witnessTable(wcol("ts", ir.DateTime{Precision: 0}))}}
		_, applier, err := runWitnessIntercept(t, seed, cat, addColForwardSnap(post))
		if err != nil || !reflect.DeepEqual(applier.callNames(), []string{"AlterColumnType"}) {
			t.Fatalf("calls %v, err %v; want one AlterColumnType", applier.callNames(), err)
		}
	})
	t.Run("other mutating shapes still skip, at WARN", func(t *testing.T) {
		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		seedWithCol := witnessTable(wcol("ts", ir.DateTime{}), wcol("gone", ir.Text{Size: ir.TextLong}))
		cat := &fakeCatalog{tables: map[string]*ir.Table{"w": seedWithCol}}
		_, applier, err := runWitnessIntercept(t,
			[]ir.SchemaSnapshot{{Schema: "src", Table: "w", IR: seedWithCol}}, cat,
			addColForwardSnap(witnessTable(wcol("ts", ir.DateTime{}))))
		if err != nil || len(applier.callNames()) != 0 {
			t.Fatalf("calls %v, err %v; want the drop skipped", applier.callNames(), err)
		}
		if !strings.Contains(buf.String(), "skipping a destructive/mutating shape") {
			t.Errorf("the seed-guard skip was not logged at WARN:\n%s", buf.String())
		}
	})
}

// TestCheckShapeAFirstBoundary_RefusesAMismatch pins F4: the Shape A
// intercept's first boundary refuses any difference it cannot accept.
func TestCheckShapeAFirstBoundary_RefusesAMismatch(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		tgt    *ir.Table
		refuse bool
	}{
		{"match", witnessTable(wcol("ts", ir.DateTime{Precision: 6})), false},
		{"target-only column", witnessTable(wcol("ts", ir.DateTime{Precision: 6}), wcol("z", ir.Boolean{})), false},
		{"fsp differs", witnessTable(wcol("ts", ir.DateTime{Precision: 0})), true},
		{"column missing on target", witnessTable(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newFakeWitness(&fakeCatalog{tables: map[string]*ir.Table{"w": tc.tgt}}, "postgres", "postgres")
			err := checkShapeAFirstBoundary(ctx, w, "w", witnessTable(wcol("ts", ir.DateTime{Precision: 6})))
			if (err != nil) != tc.refuse {
				t.Fatalf("err = %v, want refuse=%v", err, tc.refuse)
			}
			if err != nil && !strings.Contains(err.Error(), resumeDivergenceMarker) {
				t.Errorf("refusal lacks the marker: %v", err)
			}
		})
	}
	if err := checkShapeAFirstBoundary(ctx, nil, "w", witnessTable()); err != nil {
		t.Fatalf("a nil witness must accept: %v", err)
	}
}

// firstTouchStub records whether it was armed for first-touch boundaries.
type firstTouchStub struct {
	ir.CDCReader
	armed bool
}

func (f *firstTouchStub) ArmFirstTouchSchemaBoundaries() { f.armed = true }

// TestWireSchemaDeltaArming_ArmsFirstTouchWhereAnInterceptConsumesIt pins
// the D3 arming condition at the one helper every reader-open site reaches
// (TestSchemaDeltaArming_ReachesEveryReaderOpenSite holds the sites):
// armed under the single-stream intercept and Shape A's, not in
// multi-database mode, not under --schema-changes=refuse.
func TestWireSchemaDeltaArming_ArmsFirstTouchWhereAnInterceptConsumesIt(t *testing.T) {
	shard := ShardColumnSpec{Name: "shard", Value: "a"}
	for _, tc := range []struct {
		name string
		s    *Streamer
		want bool
	}{
		{"single-stream forward (the default)", &Streamer{}, true},
		{"--schema-changes=refuse", &Streamer{SchemaChanges: "refuse"}, false},
		{"multi-database", &Streamer{AllDatabases: true}, false},
		{"Shape A coordination", &Streamer{InjectShardColumn: shard}, true},
		{"Shape A with --no-coordinate-live-ddl", &Streamer{InjectShardColumn: shard, NoCoordinateLiveDDL: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &firstTouchStub{}
			tc.s.wireSchemaDeltaArming(r)
			if r.armed != tc.want {
				t.Errorf("first-touch armed = %v, want %v", r.armed, tc.want)
			}
		})
	}
}
