// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 262 — the Shape A boundary router handed the target SchemaWriter
// and prober the SOURCE's CDC projection: Table.Schema still the source
// namespace (a MySQL database name), column types still the source
// dialect. PG's qualifyTable / probeSchemaFor honour Table.Schema over
// the writer's bound schema, so every forwarded DDL addressed a schema
// named after the source database and died with `schema "source_db"
// does not exist`. The single-stream forwarder and chain restore had
// always run [retargetShapeForTarget] first; the router had not.
//
// These pins grade the router's OWN resolve — every recognised shape ×
// every arm that reaches the applier or the prober (held-lease apply,
// takeover probe, takeover re-apply) — with an anti-vacuity floor over
// the ShapeKind enumeration, so a new shape kind cannot land without a
// row here. The real-engine pin is
// TestStreamer_Bug262_ShapeA_MySQLToPG_ForwardsDDLIntoTheBoundSchema.

// capturedTable is one applier / prober call the router made: which
// method, the table it was handed, and the type-bearing payload column
// (nil for the name-only shapes).
type capturedTable struct {
	method string
	table  *ir.Table
	col    *ir.Column
}

// capturingShapeApplier records every ShapeDeltaApplier call's table and
// payload. Unlike fakeShapeApplier it keeps the arguments — the point of
// Bug 262 is WHAT the applier was handed, not whether it was called.
type capturingShapeApplier struct {
	mu   sync.Mutex
	seen []capturedTable
}

func (c *capturingShapeApplier) record(method string, t *ir.Table, col *ir.Column) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, capturedTable{method: method, table: t, col: col})
	return nil
}

func firstOrNil(cols []*ir.Column) *ir.Column {
	if len(cols) == 0 {
		return nil
	}
	return cols[0]
}

func (c *capturingShapeApplier) AlterAddColumn(_ context.Context, t *ir.Table, cols []*ir.Column) error {
	return c.record("AlterAddColumn", t, firstOrNil(cols))
}

func (c *capturingShapeApplier) AlterDropColumn(_ context.Context, t *ir.Table, _ []*ir.Column) error {
	return c.record("AlterDropColumn", t, nil)
}

func (c *capturingShapeApplier) CreateShapeIndex(_ context.Context, t *ir.Table, _ []*ir.Index) error {
	return c.record("CreateShapeIndex", t, nil)
}

func (c *capturingShapeApplier) DropShapeIndex(_ context.Context, t *ir.Table, _ []*ir.Index) error {
	return c.record("DropShapeIndex", t, nil)
}

func (c *capturingShapeApplier) AlterColumnType(_ context.Context, t *ir.Table, col *ir.Column) error {
	return c.record("AlterColumnType", t, col)
}

func (c *capturingShapeApplier) AlterColumnNullability(_ context.Context, t *ir.Table, col *ir.Column) error {
	return c.record("AlterColumnNullability", t, col)
}

func (c *capturingShapeApplier) AlterRenameColumn(_ context.Context, t *ir.Table, _, _ string) error {
	return c.record("AlterRenameColumn", t, nil)
}

func (c *capturingShapeApplier) AlterAddCheck(_ context.Context, t *ir.Table, _ []*ir.CheckConstraint) error {
	return c.record("AlterAddCheck", t, nil)
}

func (c *capturingShapeApplier) AlterDropCheck(_ context.Context, t *ir.Table, _ []*ir.CheckConstraint) error {
	return c.record("AlterDropCheck", t, nil)
}

func (c *capturingShapeApplier) AlterModifyCheck(_ context.Context, t *ir.Table, _, _ *ir.CheckConstraint) error {
	return c.record("AlterModifyCheck", t, nil)
}

// capturingProber records every probe's table and payload and answers
// NotApplied, so the takeover arm goes on to re-apply through the
// applier as well — both takeover consumers are captured in one route.
type capturingProber struct {
	mu   sync.Mutex
	seen []capturedTable
}

func (p *capturingProber) record(method string, t *ir.Table, col *ir.Column) (ProbeOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, capturedTable{method: method, table: t, col: col})
	return ProbeOutcomeNotApplied, nil
}

func (p *capturingProber) ProbeAddColumn(_ context.Context, t *ir.Table, cols []*ir.Column) (ProbeOutcome, error) {
	return p.record("ProbeAddColumn", t, firstOrNil(cols))
}

func (p *capturingProber) ProbeDropColumn(_ context.Context, t *ir.Table, _ []*ir.Column) (ProbeOutcome, error) {
	return p.record("ProbeDropColumn", t, nil)
}

func (p *capturingProber) ProbeCreateIndex(_ context.Context, t *ir.Table, _ []*ir.Index) (ProbeOutcome, error) {
	return p.record("ProbeCreateIndex", t, nil)
}

func (p *capturingProber) ProbeDropIndex(_ context.Context, t *ir.Table, _ []*ir.Index) (ProbeOutcome, error) {
	return p.record("ProbeDropIndex", t, nil)
}

func (p *capturingProber) ProbeAlterColumnType(_ context.Context, t *ir.Table, col *ir.Column) (ProbeOutcome, error) {
	return p.record("ProbeAlterColumnType", t, col)
}

func (p *capturingProber) ProbeAlterColumnNullability(_ context.Context, t *ir.Table, col *ir.Column) (ProbeOutcome, error) {
	return p.record("ProbeAlterColumnNullability", t, col)
}

func (p *capturingProber) ProbeRenameColumn(_ context.Context, t *ir.Table, _, _ string, _ *ir.Column) (ProbeOutcome, error) {
	return p.record("ProbeRenameColumn", t, nil)
}

func (p *capturingProber) ProbeAddCheck(_ context.Context, t *ir.Table, _ []*ir.CheckConstraint) (ProbeOutcome, error) {
	return p.record("ProbeAddCheck", t, nil)
}

func (p *capturingProber) ProbeDropCheck(_ context.Context, t *ir.Table, _ []*ir.CheckConstraint) (ProbeOutcome, error) {
	return p.record("ProbeDropCheck", t, nil)
}

func (p *capturingProber) ProbeModifyCheck(_ context.Context, t *ir.Table, _ string, _ *ir.CheckConstraint) (ProbeOutcome, error) {
	return p.record("ProbeModifyCheck", t, nil)
}

// bug262ShapeFixture is one recognised shape as a (pre, post) pair, both
// stamped with the SOURCE namespace exactly as a CDC snapshot would be.
type bug262ShapeFixture struct {
	kind      ShapeKind
	pre, post *ir.Table
}

// bug262ShapeMatrix builds one fixture per recognised, applier-reaching
// ShapeKind. Every table carries Schema = sourceNS: the CDC reader
// stamps the source database / schema on every snapshot.
func bug262ShapeMatrix(sourceNS string) []bug262ShapeFixture {
	stamp := func(pre, post *ir.Table) (*ir.Table, *ir.Table) {
		pre.Schema, post.Schema = sourceNS, sourceNS
		return pre, post
	}
	var out []bug262ShapeFixture
	add := func(kind ShapeKind, pre, post *ir.Table) {
		pre, post = stamp(pre, post)
		out = append(out, bug262ShapeFixture{kind: kind, pre: pre, post: post})
	}

	add(ShapeKindAddColumn, fixtureTable("users", "id"), fixtureTable("users", "id", "added_at"))
	add(ShapeKindDropColumn, fixtureTable("users", "id", "deprecated"), fixtureTable("users", "id"))

	post := fixtureTable("users", "id", "email")
	post.Indexes = append(post.Indexes, &ir.Index{Name: "ix_users_email", Columns: []ir.IndexColumn{{Column: "email"}}})
	add(ShapeKindCreateIndex, fixtureTable("users", "id", "email"), post)

	post = fixtureTable("users", "id", "email")
	post.Indexes = nil
	add(ShapeKindDropIndex, fixtureTable("users", "id", "email"), post)

	post = fixtureTable("users", "id", "amount")
	post.Columns[1].Type = ir.Integer{Width: 64}
	add(ShapeKindAlterColumnType, fixtureTable("users", "id", "amount"), post)

	post = fixtureTable("users", "id", "email")
	post.Columns[1].Nullable = true
	add(ShapeKindAlterColumnNullability, fixtureTable("users", "id", "email"), post)

	pre := fixtureTable("users", "id", "email")
	pre.Indexes = nil
	post = fixtureTable("users", "id", "email_address")
	post.Indexes = nil
	add(ShapeKindRenameColumn, pre, post)

	post = fixtureTable("users", "id", "qty")
	post.CheckConstraints = []*ir.CheckConstraint{{Name: "users_chk_qty", Expr: "qty > 0"}}
	add(ShapeKindAddCheck, fixtureTable("users", "id", "qty"), post)

	pre = fixtureTable("users", "id", "qty")
	pre.CheckConstraints = []*ir.CheckConstraint{{Name: "users_chk_qty", Expr: "qty > 0"}}
	add(ShapeKindDropCheck, pre, fixtureTable("users", "id", "qty"))

	pre = fixtureTable("users", "id", "qty")
	pre.CheckConstraints = []*ir.CheckConstraint{{Name: "users_chk_qty", Expr: "qty > 0"}}
	post = fixtureTable("users", "id", "qty")
	post.CheckConstraints = []*ir.CheckConstraint{{Name: "users_chk_qty", Expr: "qty > 0 AND id IS NOT NULL"}}
	add(ShapeKindModifyCheck, pre, post)

	return out
}

// TestRouteBoundary_Bug262_EveryShapeAndArmResolvesToTheTarget: for
// every recognised shape, on both the held-lease arm and the takeover
// arm (probe + re-apply), the table the applier and the prober receive
// has its Schema scrubbed — so the target writer's bound namespace wins,
// exactly as the row applier's appliershared.Schema does — and the
// caller's post IR is left untouched (it is the intercept's cache and
// the forwarded schema-history snapshot). The lease is still keyed on
// the source-qualified name: the coordination identity did not move.
func TestRouteBoundary_Bug262_EveryShapeAndArmResolvesToTheTarget(t *testing.T) {
	t.Parallel()
	const sourceNS = "source_db"
	matrix := bug262ShapeMatrix(sourceNS)

	// Anti-vacuity floor: every ShapeKind between None and Unrecognized
	// is a recognised, applier-reaching shape and owes a row here.
	covered := map[ShapeKind]bool{}
	for _, f := range matrix {
		covered[f.kind] = true
	}
	for k := ShapeKindNone + 1; k < ShapeKindUnrecognized; k++ {
		if !covered[k] {
			t.Fatalf("ShapeKind %s has no Bug 262 fixture — every applier-reaching shape must be graded", k)
		}
	}

	arms := []struct {
		name     string
		takeover bool
	}{
		{name: "held-lease-apply", takeover: false},
		{name: "takeover-probe-and-reapply", takeover: true},
	}

	for _, f := range matrix {
		f := f
		for _, arm := range arms {
			arm := arm
			t.Run(f.kind.String()+"/"+arm.name, func(t *testing.T) {
				t.Parallel()
				if shape, err := ClassifyShape(f.pre, f.post); err != nil || shape.Kind != f.kind {
					t.Fatalf("fixture classifies as %v (err %v); want %v", shape.Kind, err, f.kind)
				}
				clock := newMockClock(time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC))
				store := newFakeLeaseStore(clock.Now)
				applier := &capturingShapeApplier{}
				prober := &capturingProber{}
				const key = sourceNS + ".users"
				const ddl = "ir-schema:users:bug262"

				if arm.takeover {
					// A prior holder acquired, crashed and expired.
					mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
					leaseA, err := mgrA.Acquire(context.Background(), key, ddl)
					if err != nil {
						t.Fatalf("prior holder Acquire: %v", err)
					}
					mgrA.Release(context.Background(), leaseA)
					clock.Advance(31 * time.Second)
				}

				router := newTestRouter(t, store, "stream-b", prober, applier, clock)
				router.sourceEngine, router.targetEngine = "mysql", "postgres"
				if err := router.RouteBoundary(context.Background(), key, f.pre, f.post, ddl, 1, ir.Position{}); err != nil {
					t.Fatalf("RouteBoundary: %v", err)
				}

				wantProbes := 0
				if arm.takeover {
					wantProbes = 1
				}
				if len(prober.seen) != wantProbes {
					t.Fatalf("prober calls = %d, want %d", len(prober.seen), wantProbes)
				}
				if len(applier.seen) != 1 {
					t.Fatalf("applier calls = %d, want 1 (%+v)", len(applier.seen), applier.seen)
				}
				for _, got := range slices.Concat(prober.seen, applier.seen) {
					if got.table == nil {
						t.Fatalf("%s received a nil table", got.method)
					}
					if got.table.Schema != "" {
						t.Errorf("%s received Table.Schema = %q; want scrubbed so the target's bound namespace resolves it (Bug 262)", got.method, got.table.Schema)
					}
					if got.table.Name != "users" {
						t.Errorf("%s received table %q; want users", got.method, got.table.Name)
					}
					if got.table == f.post {
						t.Errorf("%s received the caller's own post table; the resolve must operate on a copy", got.method)
					}
				}
				if f.post.Schema != sourceNS {
					t.Errorf("post.Schema = %q after the route; want %q untouched (it is the cached pre-state and the forwarded snapshot)", f.post.Schema, sourceNS)
				}
				if _, ok := store.snapshot(key); !ok {
					t.Errorf("lease row missing under the source-qualified key %q; the coordination identity must not move", key)
				}
			})
		}
	}
}

// TestRouteBoundary_Bug262_CrossEngineTypesReachTheTargetDialect grades
// the type half of the resolve: on a PG→MySQL pair (the one pair
// [translate.RetargetForEngine] carries an emit-lane rule for), the
// type-bearing payloads the applier AND the takeover prober receive are
// in the target's dialect — a PG uuid arrives as MySQL CHAR(36) — for
// both shapes that carry a typed column.
func TestRouteBoundary_Bug262_CrossEngineTypesReachTheTargetDialect(t *testing.T) {
	t.Parallel()
	shapes := []struct {
		kind ShapeKind
		pre  func() *ir.Table
		post func() *ir.Table
	}{
		{
			kind: ShapeKindAddColumn,
			pre:  func() *ir.Table { return fixtureTable("users", "id") },
			post: func() *ir.Table {
				p := fixtureTable("users", "id", "ext_id")
				p.Columns[1].Type = ir.UUID{}
				return p
			},
		},
		{
			kind: ShapeKindAlterColumnType,
			pre:  func() *ir.Table { return fixtureTable("users", "id", "ext_id") },
			post: func() *ir.Table {
				p := fixtureTable("users", "id", "ext_id")
				p.Columns[1].Type = ir.UUID{}
				return p
			},
		},
	}
	for _, s := range shapes {
		s := s
		for _, takeover := range []bool{false, true} {
			takeover := takeover
			name := s.kind.String() + "/held"
			if takeover {
				name = s.kind.String() + "/takeover"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				pre, post := s.pre(), s.post()
				pre.Schema, post.Schema = "public", "public"
				clock := newMockClock(time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC))
				store := newFakeLeaseStore(clock.Now)
				applier := &capturingShapeApplier{}
				prober := &capturingProber{}
				const key = "public.users"
				if takeover {
					mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
					leaseA, err := mgrA.Acquire(context.Background(), key, "ddl")
					if err != nil {
						t.Fatalf("prior holder Acquire: %v", err)
					}
					mgrA.Release(context.Background(), leaseA)
					clock.Advance(31 * time.Second)
				}
				router := newTestRouter(t, store, "stream-b", prober, applier, clock)
				router.sourceEngine, router.targetEngine = "postgres", "mysql"
				if err := router.RouteBoundary(context.Background(), key, pre, post, "ddl", 1, ir.Position{}); err != nil {
					t.Fatalf("RouteBoundary: %v", err)
				}
				seen := slices.Concat(prober.seen, applier.seen)
				if len(seen) == 0 {
					t.Fatal("no applier/prober call captured")
				}
				for _, got := range seen {
					if got.col == nil {
						t.Fatalf("%s received no payload column", got.method)
					}
					if _, isChar := got.col.Type.(ir.Char); !isChar {
						t.Errorf("%s received column %q of type %T; want the MySQL-dialect ir.Char (PG uuid → CHAR(36))", got.method, got.col.Name, got.col.Type)
					}
				}
				// The source-side IR is untouched by the resolve.
				if _, isUUID := post.Columns[1].Type.(ir.UUID); !isUUID {
					t.Errorf("post column type mutated to %T; the resolve must not rewrite the caller's IR", post.Columns[1].Type)
				}
			})
		}
	}
}
