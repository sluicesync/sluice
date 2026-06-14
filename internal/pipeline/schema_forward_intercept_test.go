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

// addColForwardTable builds a single-table test fixture with cols
// columns plus an INT PK. Adapter for the test's expected shape.
func addColForwardTable(name string, cols ...*ir.Column) *ir.Table {
	pk := &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}
	all := append([]*ir.Column{pk}, cols...)
	return &ir.Table{
		Schema:  "public",
		Name:    name,
		Columns: all,
		PrimaryKey: &ir.Index{
			Name:    "pk_" + name,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

func addColForwardSnap(table *ir.Table) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{
		Position: ir.Position{Engine: "postgres", Token: "lsn/1"},
		Schema:   table.Schema,
		Table:    table.Name,
		IR:       table,
	}
}

// drainChannel collects the changes pushed onto out until it closes
// or the deadline elapses.
func drainChannel(t *testing.T, out <-chan ir.Change, deadline time.Duration) []ir.Change {
	t.Helper()
	var got []ir.Change
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		select {
		case c, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, c)
		case <-timer.C:
			return got
		}
	}
}

// TestForwardAddColumn_FlagOff_NoIntercept verifies that the
// intercept isn't engaged when ForwardSchemaAddColumn is false — i.e.
// the streamer's wiring path skips the call. This test exercises the
// shape directly (`schemaForwardDeps{}` with a nil applier returns the
// input channel verbatim).
func TestForwardAddColumn_NilApplier_PassThrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	tbl := addColForwardTable("users")
	snap := addColForwardSnap(tbl)
	in <- snap
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1 pass-through", len(got))
	}
	if errStore.Load() != nil {
		t.Errorf("errStore set on pass-through path: %v", *errStore.Load())
	}
}

// TestForwardAddColumn_FirstSnapshotIsAnchor verifies that the first
// SchemaSnapshot per table seeds the cache without calling the
// applier (it's the post-cold-start anchor).
func TestForwardAddColumn_FirstSnapshotIsAnchor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users")
	in <- addColForwardSnap(tbl)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1 (anchor pass-through)", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn called %d times on anchor; want 0", applier.addColCalls)
	}
}

// TestForwardAddColumn_AddColumnShape_CallsApplier verifies the
// load-bearing branch: a (pre → post) delta of one added column fires
// exactly one AlterAddColumn call and forwards the snapshot
// downstream so ADR-0049 schema-history still records.
func TestForwardAddColumn_AddColumnShape_CallsApplier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2 (both snapshots forwarded)", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want exactly 1", applier.addColCalls)
	}
	if e := errStore.Load(); e != nil {
		t.Errorf("errStore set on happy path: %v", *e)
	}
}

// TestForwardAddColumn_SeededPre_ClassifiesFirstCDCSnapshot pins the
// Bug 89 fix: a cold-start seed pre-populates the per-table cache so
// the FIRST CDC SchemaSnapshot is classified as a real boundary (not
// treated as the anchor). Without this, MySQL sources silently passed
// the ALTER through because pgoutput's first-touch Relation has no
// MySQL binlog equivalent — the first SchemaSnapshot emitted on the
// MySQL CDC reader is already POST-DDL.
func TestForwardAddColumn_SeededPre_ClassifiesFirstCDCSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	// Only ONE CDC SchemaSnapshot — the post-DDL one. The seed
	// supplies the pre.
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	seed := []ir.SchemaSnapshot{addColForwardSnap(pre)}
	out := interceptAddColumnForward(ctx, in, seed, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1 (single CDC snapshot forwarded after ALTER applied)", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want exactly 1 (Bug 89: seed classifies first CDC snap as boundary)", applier.addColCalls)
	}
	if e := errStore.Load(); e != nil {
		t.Errorf("errStore set on happy path: %v", *e)
	}
}

// TestForwardSchema_SeedGuard_SkipsDestructiveAgainstSeed pins the
// ADR-0091 §3 seed-guard: a destructive/mutating shape classified
// against a cold-start SEED pre-state is NOT forwarded (it may be a
// phantom from seed-vs-CDC fidelity asymmetry — e.g. a PG generated
// column or secondary index pgoutput omits), it is skipped as a no-op.
// The same shape on a CDC→CDC boundary forwards (covered by
// TestForwardSchema_ForwardsUnambiguousShapes).
func TestForwardSchema_SeedGuard_SkipsDestructiveAgainstSeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	applier := &fakeShapeApplier{}
	// Seed has the column; the first (and only) CDC snapshot lacks it —
	// looks like a DROP against the seed. Under the guard this is skipped.
	seedTable := addColForwardTable("users", &ir.Column{Name: "phantom", Type: ir.Varchar{Length: 100}, Nullable: true})
	cdcTable := addColForwardTable("users")
	in <- addColForwardSnap(cdcTable)
	close(in)
	errStore := &atomic.Pointer[error]{}
	seed := []ir.SchemaSnapshot{addColForwardSnap(seedTable)}
	out := interceptAddColumnForward(ctx, in, seed, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if e := errStore.Load(); e != nil {
		t.Fatalf("seed-guard must skip silently, not error; got %v", *e)
	}
	if calls := applier.callNames(); len(calls) != 0 {
		t.Errorf("applier called %v on a destructive shape against the seed; want none (seed-guard skip)", calls)
	}
	// The snapshot is still forwarded downstream (schema-history records).
	if len(got) != 1 {
		t.Errorf("got %d changes downstream; want 1 (snapshot still forwarded after skip)", len(got))
	}
}

// TestForwardAddColumn_SeededPre_BareName_FallbackResolves pins the
// MySQL Bug-83-equivalent for the ADR-0058 intercept: the cold-start
// seed's QualifiedName() is the bare table name (MySQL SchemaReader
// leaves Schema empty), but the CDC-emitted SchemaSnapshot
// QualifiedName() is "<db>.<table>". The lookupSeedCache fallback must
// promote the bare-name seed entry to the qualified key so subsequent
// snapshots resolve directly. Without this, the first CDC snapshot
// would miss the seed and be treated as the anchor — Bug 89 reopens.
func TestForwardAddColumn_SeededPre_BareName_FallbackResolves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 1)
	applier := &fakeShapeApplier{}
	// Pre IR has Schema="" (mirrors MySQL SchemaReader output).
	pre := addColForwardTable("users")
	pre.Schema = ""
	// Post IR has Schema="source_db" (mirrors MySQL CDC reader output).
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	post.Schema = "source_db"
	in <- ir.SchemaSnapshot{
		Position: ir.Position{Engine: "mysql", Token: "binlog/1"},
		Schema:   post.Schema,
		Table:    post.Name,
		IR:       post,
	}
	close(in)
	errStore := &atomic.Pointer[error]{}
	seed := []ir.SchemaSnapshot{{
		Schema: pre.Schema, // ""
		Table:  pre.Name,
		IR:     pre,
	}}
	out := interceptAddColumnForward(ctx, in, seed, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "mysql",
		targetEngineName: "mysql",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d changes, want 1", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want exactly 1 (bare-name fallback must resolve to seed)", applier.addColCalls)
	}
}

// TestForwardSchema_ForwardsUnambiguousShapes verifies ADR-0091: every
// unambiguous source schema change forwards to the target via the
// matching [ir.ShapeDeltaApplier] method (NOT refuse-loudly as in the
// ADR-0058 ADD-only era). Drop / alter-type / alter-nullability are the
// column-shape representatives; the index + CHECK shapes share the same
// applyShapeDelta dispatch (pinned by the router tests) and the
// cross-engine integration matrix.
func TestForwardSchema_ForwardsUnambiguousShapes(t *testing.T) {
	cases := []struct {
		name     string
		pre      *ir.Table
		post     *ir.Table
		wantCall string
	}{
		{
			name:     "drop-column",
			pre:      addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true}),
			post:     addColForwardTable("users"),
			wantCall: "AlterDropColumn",
		},
		{
			name:     "alter-column-type",
			pre:      addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true}),
			post:     addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 64}, Nullable: true}),
			wantCall: "AlterColumnType",
		},
		{
			name:     "alter-column-nullability",
			pre:      addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true}),
			post:     addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: false}),
			wantCall: "AlterColumnNullability",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			in <- addColForwardSnap(tc.pre)
			in <- addColForwardSnap(tc.post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			if ePtr := errStore.Load(); ePtr != nil {
				t.Fatalf("expected forward (no error); got %v", *ePtr)
			}
			calls := applier.callNames()
			if len(calls) != 1 || calls[0] != tc.wantCall {
				t.Errorf("applier calls = %v; want exactly [%s]", calls, tc.wantCall)
			}
		})
	}
}

// TestForwardSchema_RefusesAmbiguousShapes pins the ADR-0091 §2/§3
// refuse-loudly catalog under the default forward mode: RENAME COLUMN
// (indistinguishable from drop+add of a same-type column — forwarding
// the wrong guess risks silent data loss) and a multi-shape combo (a
// drop + an add of a DIFFERENT type) both refuse and emit NO DDL.
func TestForwardSchema_RefusesAmbiguousShapes(t *testing.T) {
	cases := []struct {
		name   string
		pre    *ir.Table
		post   *ir.Table
		wantIn string
	}{
		{
			name:   "rename-column",
			pre:    addColForwardTable("users", &ir.Column{Name: "old", Type: ir.Varchar{Length: 100}, Nullable: true}),
			post:   addColForwardTable("users", &ir.Column{Name: "new", Type: ir.Varchar{Length: 100}, Nullable: true}),
			wantIn: "RENAME COLUMN",
		},
		{
			// drop "a" + add "b" of a DIFFERENT type → two delta classes
			// → ShapeKindUnrecognized → classify-error refusal.
			name:   "multi-shape-combo",
			pre:    addColForwardTable("users", &ir.Column{Name: "a", Type: ir.Integer{Width: 32}, Nullable: true}),
			post:   addColForwardTable("users", &ir.Column{Name: "b", Type: ir.Varchar{Length: 50}, Nullable: true}),
			wantIn: "multi-shape combo",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			in <- addColForwardSnap(tc.pre)
			in <- addColForwardSnap(tc.post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			ePtr := errStore.Load()
			if ePtr == nil {
				t.Fatalf("expected refuse-loudly error; got nil")
			}
			if !strings.Contains((*ePtr).Error(), tc.wantIn) {
				t.Errorf("error %q does not contain %q", (*ePtr).Error(), tc.wantIn)
			}
			if calls := applier.callNames(); len(calls) != 0 {
				t.Errorf("applier made %v calls on refuse-loudly path; want none", calls)
			}
		})
	}
}

// TestForwardAddColumn_ComputedDefault_Refuse pins the ADR-0058 §2a
// refuse-loudly for ir.DefaultExpression. The intercept must reject
// before issuing the ALTER, regardless of the rest of the shape.
func TestForwardAddColumn_ComputedDefault_Refuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{
		Name:     "created_at",
		Type:     ir.Timestamp{},
		Nullable: true,
		Default:  ir.DefaultExpression{Expr: "NOW()"},
	})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refuse-loudly on DefaultExpression; got nil")
	}
	if !strings.Contains((*ePtr).Error(), "DEFAULT expression") {
		t.Errorf("error %q does not mention DEFAULT expression", (*ePtr).Error())
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn called %d times on DefaultExpression refusal; want 0", applier.addColCalls)
	}
}

// TestForwardAddColumn_ProbedVolatileDefault_Refuse pins the Bug 90
// production path (v0.79.1): the CDC reader's RelationMessage /
// TableMapEvent projection drops the column's DEFAULT clause, so the
// post-DDL SchemaSnapshot's IR arrives with Default == nil. The
// intercept must call the source-side default prober to surface the
// canonical text — if the prober returns a volatile/stateful
// DEFAULT, the intercept must refuse loudly.
//
// Class-pin (Bug 74): cover representative volatility classes
// (time-volatile, sequence-stateful, random, session-state) plus
// refuse-on-uncertainty (unknown function name).
func TestForwardAddColumn_ProbedVolatileDefault_Refuse(t *testing.T) {
	cases := []struct {
		name       string
		probedExpr string
		wantIn     string // substring expected in the refusal (lower-cased)
	}{
		{"now-pg", "now()", "now"},
		{"current-timestamp-mysql", "CURRENT_TIMESTAMP", "current_timestamp"},
		{"nextval-pg", "nextval('my_seq'::regclass)", "nextval"},
		{"random-pg", "random()", "random"},
		{"uuid-mysql", "UUID()", "uuid"},
		{"unknown-fn-uncertainty", "my_custom_default()", "unknown function"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			pre := addColForwardTable("users")
			// Post IR carries the new column with Default=nil — the
			// production CDC-projection case. The prober is the
			// source of truth.
			post := addColForwardTable("users", &ir.Column{
				Name: "v_col", Type: ir.Timestamp{}, Nullable: true,
			})
			in <- addColForwardSnap(pre)
			in <- addColForwardSnap(post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
				defaultProber: func(_ context.Context, _, _, col string) (ir.DefaultValue, error) {
					if col == "v_col" {
						return ir.DefaultExpression{Expr: tc.probedExpr}, nil
					}
					return ir.DefaultNone{}, nil
				},
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			ePtr := errStore.Load()
			if ePtr == nil {
				t.Fatalf("expected refuse-loudly on probed volatile DEFAULT %q; got nil", tc.probedExpr)
			}
			errStr := strings.ToLower((*ePtr).Error())
			if !strings.Contains(errStr, tc.wantIn) {
				t.Errorf("error %q does not mention %q", errStr, tc.wantIn)
			}
			if !strings.Contains((*ePtr).Error(), "ADR-0058 §2a") {
				t.Errorf("error %q does not cite ADR-0058 §2a", (*ePtr).Error())
			}
			if applier.addColCalls != 0 {
				t.Errorf("AlterAddColumn called %d times on probed-volatile path; want 0", applier.addColCalls)
			}
		})
	}
}

// TestForwardAddColumn_ProbedLiteralDefault_Forwards verifies the
// happy-path on the production CDC case: the IR carries Default=nil,
// the prober returns a DefaultLiteral / DefaultNone, the ALTER
// forwards cleanly.
func TestForwardAddColumn_ProbedLiteralDefault_Forwards(t *testing.T) {
	cases := []struct {
		name       string
		probed     ir.DefaultValue
		probeCalls int
	}{
		{"no-default", ir.DefaultNone{}, 1},
		{"literal-string", ir.DefaultLiteral{Value: "pending"}, 1},
		{"literal-numeric", ir.DefaultLiteral{Value: "0"}, 1},
		{"expr-literal-quoted", ir.DefaultExpression{Expr: "'active'"}, 1},
		{"expr-allowlist-coalesce", ir.DefaultExpression{Expr: "COALESCE(NULL, 0)"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			pre := addColForwardTable("users")
			post := addColForwardTable("users", &ir.Column{
				Name: "status", Type: ir.Varchar{Length: 20}, Nullable: true,
			})
			in <- addColForwardSnap(pre)
			in <- addColForwardSnap(post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			calls := 0
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
				defaultProber: func(_ context.Context, _, _, _ string) (ir.DefaultValue, error) {
					calls++
					return tc.probed, nil
				},
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			if e := errStore.Load(); e != nil {
				t.Errorf("safe probed default rejected: %v", *e)
			}
			if applier.addColCalls != 1 {
				t.Errorf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
			}
			if calls != tc.probeCalls {
				t.Errorf("prober calls = %d; want %d", calls, tc.probeCalls)
			}
		})
	}
}

// TestForwardAddColumn_ProberError_Refuse pins refuse-on-uncertainty
// when the source-side probe itself errors (connection drop, catalog
// permission, column-missing). The intercept must refuse rather than
// silently forwarding without knowing the DEFAULT shape.
func TestForwardAddColumn_ProberError_Refuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{
		Name: "newcol", Type: ir.Integer{Width: 32}, Nullable: true,
	})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	probeErr := errors.New("source catalog read failed: connection reset")
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		defaultProber: func(_ context.Context, _, _, _ string) (ir.DefaultValue, error) {
			return nil, probeErr
		},
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refuse-on-uncertainty on probe error; got nil")
	}
	if !errors.Is(*ePtr, probeErr) {
		t.Errorf("error chain does not include probe error: %v", *ePtr)
	}
	if !strings.Contains((*ePtr).Error(), "refusing on uncertainty") {
		t.Errorf("error %q does not mention refuse-on-uncertainty", (*ePtr).Error())
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn called %d times on probe-error path; want 0", applier.addColCalls)
	}
}

// TestForwardAddColumn_LiteralDefault_Forwards verifies that
// ir.DefaultLiteral (a static constant) does NOT trip the
// computed-default refusal — operators using `DEFAULT 0` or
// `DEFAULT 'pending'` get the standard forwarding path.
func TestForwardAddColumn_LiteralDefault_Forwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{
		Name:     "status",
		Type:     ir.Varchar{Length: 20},
		Nullable: false,
		Default:  ir.DefaultLiteral{Value: "'pending'"},
	})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	if e := errStore.Load(); e != nil {
		t.Errorf("DefaultLiteral was rejected: %v", *e)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
	}
}

// TestForwardAddColumn_ApplierError_Rewinds verifies that an
// applier-side error propagates through errStore AND that the cache
// is rewound to the pre-state (so a retry's next snapshot routes
// against the same pre).
func TestForwardAddColumn_ApplierError_Rewinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	injected := errors.New("alter failed: lock timeout")
	applier := &fakeShapeApplier{injectErr: injected}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected applier error propagation; got nil")
	}
	if !errors.Is(*ePtr, injected) {
		t.Errorf("error chain does not include injected error: got %v, want %v", *ePtr, injected)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1 (one attempt before error)", applier.addColCalls)
	}
}

// TestForwardAddColumn_NoneShape_Passthrough verifies a redundant
// SchemaSnapshot (same IR as the cache) forwards verbatim with no
// applier call. Bug-shape: a CDC reader emitting a snapshot on every
// transaction boundary even when nothing changed.
func TestForwardAddColumn_NoneShape_Passthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true})
	in <- addColForwardSnap(tbl)
	in <- addColForwardSnap(tbl)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2 (both forwarded)", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn calls = %d on NoneShape; want 0", applier.addColCalls)
	}
	if e := errStore.Load(); e != nil {
		t.Errorf("errStore set on NoneShape: %v", *e)
	}
}

// TestForwardAddColumn_NonSnapshotChange_Forwards verifies that
// non-SchemaSnapshot events (Insert, Update, TxBegin/Commit) flow
// through unchanged.
func TestForwardAddColumn_NonSnapshotChange_Forwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 3)
	applier := &fakeShapeApplier{}
	tbl := addColForwardTable("users")
	in <- addColForwardSnap(tbl)
	in <- ir.Insert{Position: ir.Position{Engine: "postgres", Token: "lsn/2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1)}}
	in <- ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: "lsn/3"}}
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	got := drainChannel(t, out, time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d changes, want 3", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("AlterAddColumn calls = %d on data events; want 0", applier.addColCalls)
	}
}

// TestSynthesizeBackfillUpdate verifies the synthetic UPDATE event
// shape: Before carries PK columns, After carries the added column,
// Position matches the SchemaSnapshot.
func TestSynthesizeBackfillUpdate(t *testing.T) {
	tbl := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	snap := addColForwardSnap(tbl)
	row := ir.Row{"id": int64(42), "nickname": "alpha"}
	upd := synthesizeBackfillUpdate(snap, row, []string{"id"}, map[string]struct{}{"nickname": {}})
	if upd.Schema != "public" {
		t.Errorf("Schema = %q; want public", upd.Schema)
	}
	if upd.Table != "users" {
		t.Errorf("Table = %q; want users", upd.Table)
	}
	if upd.Position.Token != "lsn/1" {
		t.Errorf("Position.Token = %q; want lsn/1", upd.Position.Token)
	}
	if got, ok := upd.Before["id"]; !ok || got != int64(42) {
		t.Errorf("Before[id] = %v, ok=%t; want 42, true", got, ok)
	}
	if _, hasNickname := upd.Before["nickname"]; hasNickname {
		t.Errorf("Before should not contain non-PK column nickname")
	}
	if got, ok := upd.After["nickname"]; !ok || got != "alpha" {
		t.Errorf("After[nickname] = %v, ok=%t; want alpha, true", got, ok)
	}
	if _, hasID := upd.After["id"]; hasID {
		t.Errorf("After should not contain PK column id (would be a redundant SET)")
	}
}

// fakeBatchedRowReader is a minimal in-memory BatchedRowReader for
// the backfill loop. Returns one batch then EOF.
type fakeBatchedRowReader struct {
	rows []ir.Row
	// callCount tracks how many ReadRowsBatch calls happened. The
	// backfill loop calls until a batch returns 0 rows.
	callCount int
}

func (f *fakeBatchedRowReader) ReadRows(_ context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	close(out)
	return out, nil
}

func (f *fakeBatchedRowReader) ReadRowsBatch(_ context.Context, _ *ir.Table, _ []any, _ int) (<-chan ir.Row, error) {
	out := make(chan ir.Row, len(f.rows))
	if f.callCount == 0 {
		for _, r := range f.rows {
			out <- r
		}
	}
	// Second + subsequent calls return EOF (empty channel).
	f.callCount++
	close(out)
	return out, nil
}

func (f *fakeBatchedRowReader) Err() error { return nil }

// TestForwardAddColumn_Backfill_EmitsUpdates verifies the backfill
// loop emits one synthetic UPDATE per source row after the ALTER
// lands. The applier sees exactly: SchemaSnapshot(pre),
// SchemaSnapshot(post), UPDATE(row1), UPDATE(row2), UPDATE(row3) (in
// some order — backfill batches are PK-ordered, so the order is
// deterministic per the fake reader's emission order).
func TestForwardAddColumn_Backfill_EmitsUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	reader := &fakeBatchedRowReader{
		rows: []ir.Row{
			{"id": int64(1), "nickname": "alpha"},
			{"id": int64(2), "nickname": "beta"},
			{"id": int64(3), "nickname": "gamma"},
		},
	}
	pre := addColForwardTable("users")
	post := addColForwardTable("users", &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- addColForwardSnap(pre)
	in <- addColForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{
			reader:    reader,
			streamID:  "test-stream",
			batchSize: 100,
		},
	}, errStore)
	got := drainChannel(t, out, 2*time.Second)
	// Expect: anchor snapshot + post snapshot + 3 backfill UPDATEs.
	if len(got) != 5 {
		t.Fatalf("got %d changes, want 5 (2 snapshots + 3 backfill updates); got = %#v", len(got), got)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
	}
	updates := 0
	for _, c := range got {
		if u, ok := c.(ir.Update); ok {
			updates++
			if u.Table != "users" {
				t.Errorf("backfill Update.Table = %q; want users", u.Table)
			}
			if _, hasNickname := u.After["nickname"]; !hasNickname {
				t.Errorf("backfill Update.After missing nickname: %v", u.After)
			}
		}
	}
	if updates != 3 {
		t.Errorf("backfill Update count = %d; want 3", updates)
	}
}

// TestForwardAddColumn_Backfill_NoPK_Refuses verifies a table
// without a primary key fails the backfill cursor refusal. Tables
// without PKs are already excluded from bulk-copy resume (ADR-0018);
// backfill applies the same constraint.
func TestForwardAddColumn_Backfill_NoPK_Refuses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	reader := &fakeBatchedRowReader{}
	pre := &ir.Table{Schema: "public", Name: "nopk", Columns: []*ir.Column{{Name: "x", Type: ir.Integer{Width: 32}}}}
	post := &ir.Table{Schema: "public", Name: "nopk", Columns: []*ir.Column{
		{Name: "x", Type: ir.Integer{Width: 32}},
		{Name: "y", Type: ir.Integer{Width: 32}, Nullable: true},
	}}
	in <- ir.SchemaSnapshot{Position: ir.Position{Engine: "postgres", Token: "lsn/1"}, Schema: "public", Table: "nopk", IR: pre}
	in <- ir.SchemaSnapshot{Position: ir.Position{Engine: "postgres", Token: "lsn/2"}, Schema: "public", Table: "nopk", IR: post}
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{
			reader:    reader,
			streamID:  "test-stream",
			batchSize: 100,
		},
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refusal on no-PK table; got nil")
	}
	if !strings.Contains((*ePtr).Error(), "primary key") {
		t.Errorf("error %q does not mention primary key", (*ePtr).Error())
	}
}
