// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The pipeline half of SLM-1 (audit 2026-09-01): the session-zone cast
// door on every path that turns a classified ALTER COLUMN TYPE into target
// DDL, the warm-resume prior-shape seed, and the roster that keeps the
// door on every applyShapeDelta caller.

// TestSessionZoneSiblingSwap_EveryFamilyAndShape derives the pair
// universe from the IR temporal types themselves — every (family, zoned)
// member × scalar / array — and asserts the predicate matches exactly the
// same-family, different-zone pairs at the same dimension. Non-temporal
// and no-sibling temporal types (Date, Integer) are in the universe so
// the predicate's silence on them is asserted, not assumed.
func TestSessionZoneSiblingSwap_EveryFamilyAndShape(t *testing.T) {
	type member struct {
		name   string
		typ    ir.Type
		family string
		zoned  bool
		ok     bool
	}
	scalars := []member{
		{"timestamptz", ir.Timestamp{WithTimeZone: true}, "timestamp", true, true},
		{"timestamp", ir.Timestamp{}, "timestamp", false, true},
		{"datetime", ir.DateTime{Precision: 3}, "timestamp", false, true},
		{"timetz", ir.Time{WithTimeZone: true}, "time", true, true},
		{"time", ir.Time{Precision: 6}, "time", false, true},
		{"date", ir.Date{}, "", false, false},
		{"integer", ir.Integer{Width: 64}, "", false, false},
	}
	universe := make([]member, 0, 2*len(scalars))
	for _, m := range scalars {
		universe = append(universe, m, member{"array:" + m.name, ir.Array{Element: m.typ}, "array:" + m.family, m.zoned, m.ok})
	}
	if len(universe) < 14 {
		t.Fatalf("universe holds %d members; floor 14 — the derivation went vacuous", len(universe))
	}
	wantSwaps := 0
	for _, a := range universe {
		for _, b := range universe {
			if a.name == b.name {
				continue
			}
			// Same array depth is a precondition for the whole class: a
			// scalar ⇄ array change needs an explicit USING on Postgres, so a
			// forwarded bare ALTER fails loudly rather than diverging.
			sameDepth := strings.HasPrefix(a.name, "array:") == strings.HasPrefix(b.name, "array:")
			// The sibling half (SL-2 / SLM-1): same family, zone differs.
			sibling := a.ok && b.ok && a.family == b.family && a.zoned != b.zoned
			// The SLM-5 half, measured 2026-09-03 on mysql:8.0.46 and
			// postgres:16: a cast is session-dependent when exactly one side
			// is SESSION-NORMALISED (stored UTC, rendered through the session
			// zone — timestamptz / MySQL TIMESTAMP), or when the target
			// carries a zone the source did not (an offset is invented, e.g.
			// time → timetz). timetz is NOT session-normalised: it stores its
			// offset per value, so timetz → text/time measured byte-identical
			// under Asia/Tokyo and UTC.
			normalised := func(m member) bool { return m.ok && m.zoned && strings.HasSuffix(m.family, "timestamp") }
			zoneInvented := b.ok && b.zoned && (!a.ok || !a.zoned)
			want := sibling || (sameDepth && (normalised(a) != normalised(b) || zoneInvented))
			if sibling {
				wantSwaps++
			}
			if got := sessionZoneSiblingSwap(a.typ, b.typ); got != want {
				t.Errorf("sessionZoneSiblingSwap(%s → %s) = %v; want %v", a.name, b.name, got, want)
			}
		}
	}
	// Anti-vacuity floor: timestamptz⇄{timestamp,datetime} and timetz⇄time,
	// each direction, scalar and array = (2+2+1+1)×2 = 12 ordered pairs.
	if wantSwaps != 12 {
		t.Fatalf("derived %d ordered zone-sibling pairs; want 12", wantSwaps)
	}
}

// zoneSwapPre / zoneSwapPost are the two sides of the MySQL swap as the
// seed (SchemaReader IR) and the first CDC snapshot present them.
func zoneSwapPre() *ir.Table {
	return &ir.Table{Name: "events", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "c", Type: ir.Timestamp{WithTimeZone: true}},
	}}
}

func zoneSwapPost() *ir.Table {
	return &ir.Table{Name: "events", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "c", Type: ir.DateTime{}},
	}}
}

// requireZoneSwapRefusal asserts the loud shape: column, mechanism,
// drained-model remedy.
func requireZoneSwapRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("zone-sibling ALTER COLUMN TYPE was forwarded; want the session-zone cast refusal")
	}
	for _, want := range []string{"cannot be forwarded", `column "c"`, "zone setting", "drained model"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q; got: %v", want, err)
		}
	}
}

// TestIntercept_SessionZoneSwapAtFirstBoundary_NeverReachesAlterColumnType
// is the Shape A gate the audit asked for: a TIMESTAMP seed and a DATETIME
// first snapshot fed into the coordination intercept must not reach the
// applier's AlterColumnType. This is the exact boundary SLM-1 observed
// forwarding (the reader had no prior; the router classified against the
// seed and applied). Mutation: delete the door in RouteBoundary and the
// applier records "AlterColumnType".
func TestIntercept_SessionZoneSwapAtFirstBoundary_NeverReachesAlterColumnType(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, seed, first *ir.Table) (*fakeShapeApplier, error) {
		t.Helper()
		clock := newMockClock(testClockNow())
		store := newFakeLeaseStore(clock.Now)
		applier := &fakeShapeApplier{}
		mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
		router, err := NewBoundaryRouter(mgr, applier, &fakeProber{}, "postgres", "postgres")
		if err != nil {
			t.Fatalf("NewBoundaryRouter: %v", err)
		}
		in := make(chan ir.Change, 2)
		in <- ir.SchemaSnapshot{Schema: "src", Table: "events", Position: ir.Position{Token: "p1"}, IR: first}
		close(in)
		var errStore atomic.Pointer[error]
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := interceptSchemaSnapshotsForCoordination(ctx, in, []ir.SchemaSnapshot{{Table: seed.Name, IR: seed}}, router, nil, &errStore)
		_ = drainChanges(t, out, 2*time.Second)
		if e := errStore.Load(); e != nil {
			return applier, *e
		}
		return applier, nil
	}

	for _, tc := range []struct {
		name        string
		seed, first *ir.Table
	}{
		{"TIMESTAMP→DATETIME", zoneSwapPre(), zoneSwapPost()},
		{"DATETIME→TIMESTAMP", zoneSwapPost(), zoneSwapPre()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applier, err := run(t, tc.seed, tc.first)
			requireZoneSwapRefusal(t, err)
			for _, call := range applier.callNames() {
				if call == "AlterColumnType" {
					t.Fatalf("AlterColumnType reached the applier on a zone-sibling swap at the first boundary (calls %v)", applier.callNames())
				}
			}
		})
	}

	t.Run("precision-only MODIFY still routes (the door is narrow)", func(t *testing.T) {
		seed := zoneSwapPre()
		first := zoneSwapPre()
		first.Columns[1].Type = ir.Timestamp{WithTimeZone: true, Precision: 6}
		applier, err := run(t, seed, first)
		if err != nil {
			t.Fatalf("TIMESTAMP(0)→TIMESTAMP(6) refused: %v", err)
		}
		calls := applier.callNames()
		if len(calls) != 1 || calls[0] != "AlterColumnType" {
			t.Errorf("applier calls = %v; want exactly [AlterColumnType] — the positive control that the door only closes on the swap", calls)
		}
	})
}

// TestForwardSchema_SessionZoneSwap_RefusesOnCDCBoundary is the sibling
// door on the ADR-0091 single-stream path: a CDC→CDC zone-sibling swap
// refuses before applyShapeForward, while the same swap classified
// against a cold-start SEED stays the §3 skip it always was — the door is
// deliberately behind the seed guard so an operator's --type-override
// onto the zone sibling is not reported as a swap they never made.
func TestForwardSchema_SessionZoneSwap_RefusesOnCDCBoundary(t *testing.T) {
	run := func(t *testing.T, seed []ir.SchemaSnapshot, snaps ...*ir.Table) (*fakeShapeApplier, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		in := make(chan ir.Change, len(snaps))
		for _, s := range snaps {
			in <- addColForwardSnap(s)
		}
		close(in)
		applier := &fakeShapeApplier{}
		errStore := &atomic.Pointer[error]{}
		out := interceptAddColumnForward(ctx, in, seed, schemaForwardDeps{
			applier:          applier,
			sourceEngineName: "mysql",
			targetEngineName: "mysql",
		}, errStore)
		_ = drainChannel(t, out, time.Second)
		if e := errStore.Load(); e != nil {
			return applier, *e
		}
		return applier, nil
	}

	t.Run("CDC→CDC swap refuses", func(t *testing.T) {
		applier, err := run(t, nil, zoneSwapPre(), zoneSwapPost())
		requireZoneSwapRefusal(t, err)
		if calls := applier.callNames(); len(calls) != 0 {
			t.Errorf("applier reached on a refused swap: %v", calls)
		}
	})
	t.Run("seed-classified swap stays the ADR-0091 §3 skip", func(t *testing.T) {
		applier, err := run(t, []ir.SchemaSnapshot{addColForwardSnap(zoneSwapPre())}, zoneSwapPost())
		if err != nil {
			t.Fatalf("seed-classified swap refused; the door is meant to sit behind the seed guard: %v", err)
		}
		if calls := applier.callNames(); len(calls) != 0 {
			t.Errorf("applier reached on a seed-guarded shape: %v", calls)
		}
	})
	t.Run("CDC→CDC precision-only still forwards", func(t *testing.T) {
		pre, post := zoneSwapPre(), zoneSwapPre()
		post.Columns[1].Type = ir.Timestamp{WithTimeZone: true, Precision: 6}
		applier, err := run(t, nil, pre, post)
		if err != nil {
			t.Fatalf("precision-only MODIFY refused: %v", err)
		}
		if calls := applier.callNames(); len(calls) != 1 || calls[0] != "AlterColumnType" {
			t.Errorf("applier calls = %v; want [AlterColumnType]", calls)
		}
	})
}

// ---- the warm-resume prior-shape seed ----

// seedHistoryApplier is a ChangeApplier whose only real surface is the
// schema-history listing loadRetainedSchemaSeed reads. The embedded nil
// interface makes every other method a panic — a call to one is a test
// failure, not a silent no-op.
type seedHistoryApplier struct {
	ir.ChangeApplier
	rows []ir.RetainedSchemaVersionRow
	err  error
}

func (a *seedHistoryApplier) ListSchemaHistory(context.Context, string, int) ([]ir.RetainedSchemaVersionRow, error) {
	return a.rows, a.err
}

// tokenOrderer orders positions by their integer token — a total order
// standing in for the engine's causal predicate.
type tokenOrderer struct{}

func (tokenOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pv, err := strconv.Atoi(p.Token)
	if err != nil {
		return false, err
	}
	av, err := strconv.Atoi(anchor.Token)
	if err != nil {
		return false, err
	}
	return pv >= av, nil
}

// orderedStubEngine is a stubEngine that also orders positions.
type orderedStubEngine struct {
	stubEngine
	tokenOrderer
}

func historyRow(schema, table, anchor string, tbl *ir.Table) ir.RetainedSchemaVersionRow {
	payload, err := ir.MarshalTable(tbl)
	if err != nil {
		panic(err)
	}
	return ir.RetainedSchemaVersionRow{SchemaName: schema, TableName: table, AnchorPosition: anchor, TableJSON: payload}
}

func TestLoadRetainedSchemaSeed(t *testing.T) {
	ctx := context.Background()
	persisted := ir.Position{Engine: "mysql", Token: "50"}
	tsTable := zoneSwapPre()
	dtTable := zoneSwapPost()
	otherTable := &ir.Table{Name: "other", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}}
	source := orderedStubEngine{}

	columnType := func(tables []*ir.Table, name, col string) ir.Type {
		for _, tbl := range tables {
			if tbl.Name != name {
				continue
			}
			for _, c := range tbl.Columns {
				if c.Name == col {
					return c.Type
				}
			}
		}
		return nil
	}

	t.Run("applier without schema history yields no seed", func(t *testing.T) {
		got, err := loadRetainedSchemaSeed(ctx, &stubApplier{}, source, "s", persisted)
		if err != nil || got != nil {
			t.Fatalf("got (%v, %v); want (nil, nil)", got, err)
		}
	})

	t.Run("one version per table seeds directly, most recent listing first", func(t *testing.T) {
		app := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{
			historyRow("src", "events", "10", tsTable),
			historyRow("src", "other", "5", otherTable),
		}}
		got, err := loadRetainedSchemaSeed(ctx, app, source, "s", persisted)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("seeded %d tables; want 2", len(got))
		}
		if got[0].Schema != "src" || got[0].Name != "events" {
			t.Errorf("seed[0] = %s.%s; want src.events (schema/name restored from the row)", got[0].Schema, got[0].Name)
		}
		if _, ok := columnType(got, "events", "c").(ir.Timestamp); !ok {
			t.Errorf("events.c seeded as %T; want the retained ir.Timestamp", columnType(got, "events", "c"))
		}
	})

	t.Run("several versions resolve at the persisted position under the orderer, whatever the listing order", func(t *testing.T) {
		// Listed OLDEST first — the created_at tie shape — and with a
		// version anchored AFTER persisted that must not win either.
		app := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{
			historyRow("src", "events", "10", tsTable),
			historyRow("src", "events", "40", dtTable),
			historyRow("src", "events", "60", otherTable),
		}}
		got, err := loadRetainedSchemaSeed(ctx, app, source, "s", persisted)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("seeded %d tables; want 1", len(got))
		}
		if _, ok := columnType(got, "events", "c").(ir.DateTime); !ok {
			t.Errorf("events.c seeded as %T; want the ir.DateTime version anchored at 40 (the greatest anchor at or before 50)", columnType(got, "events", "c"))
		}
	})

	t.Run("several versions and no orderer: that table resumes without a prior, the rest still seed", func(t *testing.T) {
		app := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{
			historyRow("src", "events", "10", tsTable),
			historyRow("src", "events", "40", dtTable),
			historyRow("src", "other", "5", otherTable),
		}}
		got, err := loadRetainedSchemaSeed(ctx, app, stubEngine{}, "s", persisted)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Name != "other" {
			t.Fatalf("seeded %v; want exactly [other] (events is unresolvable without an orderer and must be WARN-skipped, never guessed)", got)
		}
	})

	t.Run("a history read error is loud", func(t *testing.T) {
		app := &seedHistoryApplier{err: errors.New("control table unreachable")}
		if _, err := loadRetainedSchemaSeed(ctx, app, source, "s", persisted); err == nil {
			t.Fatal("read error degraded to an empty seed; the armed refusal would silently lose its prior")
		}
	})

	t.Run("a corrupt retained row is loud", func(t *testing.T) {
		app := &seedHistoryApplier{rows: []ir.RetainedSchemaVersionRow{
			{SchemaName: "src", TableName: "events", AnchorPosition: "10", TableJSON: []byte("{not json")},
		}}
		if _, err := loadRetainedSchemaSeed(ctx, app, source, "s", persisted); err == nil {
			t.Fatal("corrupt row degraded to an empty seed")
		}
	})
}

// TestWireReaderSchemaSeed_HandsOffOnceAndClears pins the lifecycle: the
// seed reaches a reader that accepts it, and the loader is cleared so
// the next attempt cannot inherit it; a reader without the surface never
// runs the loader (SLM-1b: the warm-resume loader reads the TARGET's
// catalog, and a reader without the seed surface — the trigger-CDC lanes
// — must not pay for, or fail on, a read it consumes nothing from); a
// loader error is the caller's to surface, never swallowed.
func TestWireReaderSchemaSeed_HandsOffOnceAndClears(t *testing.T) {
	ctx := context.Background()
	rec := &seedRecordingReader{}
	s := &Streamer{readerSchemaSeed: staticSchemaSeed([]*ir.Table{zoneSwapPre()})}
	if err := s.wireReaderSchemaSeed(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.seed) != 1 || rec.seed[0].Name != "events" {
		t.Fatalf("reader received %v; want the pending seed", rec.seed)
	}
	if s.readerSchemaSeed != nil {
		t.Fatal("seed not cleared after hand-off; a warm resume after a cold start in the same process would inherit the cold-start prior")
	}

	// A reader without the surface never runs the loader, and still clears.
	loads := 0
	s.readerSchemaSeed = func(context.Context) ([]*ir.Table, error) {
		loads++
		return []*ir.Table{zoneSwapPre()}, nil
	}
	if err := s.wireReaderSchemaSeed(ctx, &stubCDCReader{}); err != nil {
		t.Fatal(err)
	}
	if loads != 0 {
		t.Fatalf("loader ran %d time(s) for a reader without the seed surface; want 0 — the target-catalog read must be lazy", loads)
	}
	if s.readerSchemaSeed != nil {
		t.Fatal("seed not cleared for a reader without the surface")
	}

	// A loader error reaches the caller.
	s.readerSchemaSeed = func(context.Context) ([]*ir.Table, error) {
		return nil, errors.New("target catalog unreachable")
	}
	if err := s.wireReaderSchemaSeed(ctx, &seedRecordingReader{}); err == nil {
		t.Fatal("loader error swallowed; the armed refusal would silently lose its prior")
	}
}

type seedRecordingReader struct {
	ir.CDCReader
	seed []*ir.Table
}

func (r *seedRecordingReader) SetSchemaSeed(tables []*ir.Table) { r.seed = tables }

type stubCDCReader struct{ ir.CDCReader }

// ---- the roster ----

// zoneDoorSite classifies one applyShapeDelta / applyShape call site: the
// function holding the session-zone door it sits behind, and every direct
// caller of the site's enclosing function with the reason that caller is
// fine. Callers are DERIVED from the AST; a new or renamed caller fails
// the gate until classified here — the mechanical form of "which call
// PATHS reach this door" (CLAUDE.md, the moved-refusal shape).
type zoneDoorSite struct {
	doorFile string
	doorFunc string
	callers  map[string]string
}

// sessionZoneSwapDoorRoster is keyed "<file>::<enclosingFunc>::<symbol>".
var sessionZoneSwapDoorRoster = map[string]zoneDoorSite{
	"shard_consolidation_router.go::(*BoundaryRouter).applyShape::applyShapeDelta": {
		doorFile: "shard_consolidation_router.go", doorFunc: "(*BoundaryRouter).RouteBoundary",
		callers: map[string]string{
			"(*BoundaryRouter).handleHeldLease": "reached only from RouteBoundary, after the door (both its applyShape calls: lease-holder apply and takeover re-apply)",
		},
	},
	"shard_consolidation_router.go::(*BoundaryRouter).handleHeldLease::applyShape": {
		doorFile: "shard_consolidation_router.go", doorFunc: "(*BoundaryRouter).RouteBoundary",
		callers: map[string]string{
			"(*BoundaryRouter).RouteBoundary": "the door itself; refuseSessionZoneSwap runs before the lease is acquired",
		},
	},
	"schema_forward_intercept.go::applyShapeForward::applyShapeDelta": {
		doorFile: "schema_forward_intercept.go", doorFunc: "routeForwardBoundary",
		callers: map[string]string{
			"routeForwardBoundary": "the door itself; refuseSessionZoneSwap runs after the §3 seed guard and before the per-shape switch",
			"routeRenameColumn":    "dispatches ShapeKindRenameColumn only — ShapeKindAlterColumnType cannot reach applyShapeDelta through it",
		},
	},
}

// TestSessionZoneSwapDoorRoster_EveryShapeApplyCallSite derives every
// applyShapeDelta / applyShape call site in the pipeline package from the
// AST and every direct caller of each site's enclosing function, and holds
// both to the roster; then confirms each named door function actually
// calls refuseSessionZoneSwap. Every claim the roster makes about reach
// is checked against the tree; only the REASONS are human.
func TestSessionZoneSwapDoorRoster_EveryShapeApplyCallSite(t *testing.T) {
	sites, calls := discoverZoneDoorSites(t)

	if len(sites) < 3 {
		t.Fatalf("anti-vacuity: found %d applyShapeDelta/applyShape call sites; floor 3 — the matcher is broken", len(sites))
	}

	var unclassified []string
	for key := range sites {
		if _, ok := sessionZoneSwapDoorRoster[key]; !ok {
			unclassified = append(unclassified, key)
		}
	}
	sort.Strings(unclassified)
	if len(unclassified) > 0 {
		t.Fatalf("new/renamed shape-apply call site(s) not classified in sessionZoneSwapDoorRoster:\n  %s\n\n"+
			"Each must sit behind a refuseSessionZoneSwap door (name the door function) or be exempt with a reason.",
			strings.Join(unclassified, "\n  "))
	}

	doorsChecked := 0
	for key, site := range sessionZoneSwapDoorRoster {
		enclosing, ok := sites[key]
		if !ok {
			t.Errorf("stale roster entry %q: no such call site in the pipeline AST", key)
			continue
		}
		// (a) the door exists where the roster says it does.
		doorCalls, ok := calls[site.doorFile+"::"+site.doorFunc]
		if !ok {
			t.Errorf("%s: door function %s::%s does not exist", key, site.doorFile, site.doorFunc)
			continue
		}
		if !doorCalls["refuseSessionZoneSwap"] {
			t.Errorf("%s: door function %s::%s does not call refuseSessionZoneSwap — the roster names a door that is not there", key, site.doorFile, site.doorFunc)
		}
		doorsChecked++
		// (b) the enclosing function's direct callers are exactly the
		// classified ones.
		got := map[string]bool{}
		for fn, called := range calls {
			if called[shortFuncName(enclosing)] {
				got[strings.SplitN(fn, "::", 2)[1]] = true
			}
		}
		for caller := range got {
			if _, ok := site.callers[caller]; !ok {
				t.Errorf("%s: %s is called from %s, which the roster does not classify — state whether that path passes the door", key, enclosing, caller)
			}
		}
		for caller := range site.callers {
			if !got[caller] {
				t.Errorf("%s: roster lists caller %s, but nothing by that name calls %s any more", key, caller, enclosing)
			}
		}
	}
	if doorsChecked < 3 {
		t.Fatalf("anti-vacuity: checked %d doors; floor 3", doorsChecked)
	}
}

// TestSchemaSeed_WiredWhereverTheRefusalIsArmed is the wiring-parity gate:
// every function in the package that arms the refusal (type-asserts
// schemaDeltaTargetApplySetter) must also hand the reader its seed
// (wireReaderSchemaSeed) — arming without seeding is the SLM-1 state, a
// refusal that is armed and inert at every first boundary — and the
// warm-resume dispatcher must load the seed it hands over
// (loadRetainedSchemaSeed). Derived from the AST so a new open path that
// copies the arming line without the seeding line fails here.
func TestSchemaSeed_WiredWhereverTheRefusalIsArmed(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	arming := 0
	loaders := map[string]map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			arms, seeds := false, false
			called := map[string]bool{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.TypeAssertExpr:
					if id, ok := v.Type.(*ast.Ident); ok && id.Name == "schemaDeltaTargetApplySetter" {
						arms = true
					}
				case *ast.CallExpr:
					switch fun := v.Fun.(type) {
					case *ast.SelectorExpr:
						called[fun.Sel.Name] = true
						if fun.Sel.Name == "wireReaderSchemaSeed" {
							seeds = true
						}
					case *ast.Ident:
						called[fun.Name] = true
					}
				}
				return true
			})
			fn := name + "::" + funcDeclName(fd)
			if arms {
				arming++
				if !seeds {
					t.Errorf("%s arms the session-zone refusal (schemaDeltaTargetApplySetter) but never calls wireReaderSchemaSeed — armed and inert at every first boundary (SLM-1)", fn)
				}
			}
			loaders[fn] = called
		}
	}
	// Anti-vacuity: the cold-start and warm-resume open paths both arm.
	if arming < 2 {
		t.Fatalf("found %d arming sites; floor 2 (coldStartBeginCDC, warmResume) — the walk is vacuous", arming)
	}
	// The warm-resume dispatcher installs the loader, and the loader reads
	// BOTH priors — the target witness and the history fallback (SLM-1b):
	// a loader that dropped either would resume on a narrower prior with
	// nothing failing.
	if !loaders["streamer_run_phases.go::(*Streamer).phaseOpenChangeStream"]["warmResumeSchemaSeedLoader"] {
		t.Errorf("phaseOpenChangeStream no longer installs warmResumeSchemaSeedLoader before dispatching to warmResume")
	}
	loader := loaders["schema_seed.go::(*Streamer).loadWarmResumeSchemaSeed"]
	for _, want := range []string{"loadRetainedSchemaSeed", "loadTargetZoneWitness", "mergeWarmResumeSeed"} {
		if !loader[want] {
			t.Errorf("loadWarmResumeSchemaSeed no longer calls %s; the warm-resume prior lost one of its two witnesses", want)
		}
	}
}

// shortFuncName strips a "(*Recv)." prefix so a method's call sites (which
// appear as selector calls on a receiver) match by method name.
func shortFuncName(name string) string {
	if i := strings.LastIndex(name, ")."); i >= 0 {
		return name[i+2:]
	}
	return name
}

// discoverZoneDoorSites walks the package's non-test files and returns
// (1) every applyShapeDelta / applyShape call site keyed
// "<file>::<enclosingFunc>::<symbol>" → enclosingFunc, and (2) for every
// function "<file>::<func>", the set of function/method names it calls.
func discoverZoneDoorSites(t *testing.T) (sites map[string]string, calls map[string]map[string]bool) {
	t.Helper()
	wanted := map[string]bool{"applyShapeDelta": true, "applyShape": true}
	sites, calls = map[string]string{}, map[string]map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := funcDeclName(fd)
			called := map[string]bool{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var sym string
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					sym = fun.Name
				case *ast.SelectorExpr:
					sym = fun.Sel.Name
				default:
					return true
				}
				called[sym] = true
				if wanted[sym] {
					sites[name+"::"+fn+"::"+sym] = fn
				}
				return true
			})
			calls[name+"::"+fn] = called
		}
	}
	return sites, calls
}
