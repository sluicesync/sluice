// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"sluicesync.dev/sluice/internal/ir"
)

// THE SCOPE GATE'S TWO ARMS, EXERCISED, ON ALL THREE LANES.
//
// # Why the structural gates were not enough
//
// The scope gate landed with two pins and both are structural:
// TestSessionTZRefusalSitesAreScopeGated walks the AST and requires each
// refusal site's function to MENTION a predicate, and
// TestEveryMySQLCDCLaneTakesTheScopePredicate requires each lane to
// implement the setter. Neither can see which condition the answer is
// wired into, and every pre-existing refusal test runs with a NIL
// predicate — so the in-scope and out-of-scope arms were both unexercised
// on all three lanes.
//
// The pre-tag value-fidelity review named the mutation that exposes it:
// swap the two arguments at the VStream call, so the KEYSPACE is tested
// against the table filter instead of the table. `Allows("ks")` is false
// for essentially every filter, the refusal is then permanently and
// silently disabled on both VStream lanes, and not one test in the tree
// fails. A gate that proves a function mentions a predicate has not
// proved the predicate decides anything.
//
// So this grades behaviour: with a predicate that ADMITS the fixture's
// table the refusal must still fire, and with one that EXCLUDES it the
// refusal must not — while the boundary is still emitted and the lane's
// own signature memo still advances, which is what pins that the gate
// narrowed the refusal and nothing else.
func TestSessionTZRefusal_InScopeArmStillRefuses(t *testing.T) {
	t.Run("binlog", func(t *testing.T) {
		err := runBinlogTZSwap(t, func(_, table string) bool { return table == "events" })
		requireSessionTZRefusal(t, err, "created_at")
	})
	t.Run("vstream", func(t *testing.T) {
		err := runVStreamTZSwap(t, func(_, table string) bool { return table == "users" })
		requireSessionTZRefusal(t, err, "created_at")
	})
	t.Run("vstream cold-start snapshot", func(t *testing.T) {
		err := runVStreamSnapshotTZSwap(t, func(_, table string) bool { return table == "users" })
		requireSessionTZRefusal(t, err, "created_at")
	})
}

// TestSessionTZRefusal_OutOfScopeArmDoesNotRefuse is the half the fix was
// FOR: a table the sync excludes must not kill the stream over a schema
// change sluice will never forward for it.
func TestSessionTZRefusal_OutOfScopeArmDoesNotRefuse(t *testing.T) {
	excludeEverything := func(_, _ string) bool { return false }

	t.Run("binlog", func(t *testing.T) {
		if err := runBinlogTZSwap(t, excludeEverything); err != nil {
			t.Errorf("an EXCLUDED table's TIMESTAMP/DATETIME swap killed the stream: %v\n\n"+
				"The sync emits nothing to the target for this table, so the re-zoning cannot diverge "+
				"anything (audit 2026-09-09 A0909-AQ-M-2)", err)
		}
	})
	t.Run("vstream", func(t *testing.T) {
		if err := runVStreamTZSwap(t, excludeEverything); err != nil {
			t.Errorf("an EXCLUDED table's swap killed the VStream stream: %v", err)
		}
	})
	t.Run("vstream cold-start snapshot", func(t *testing.T) {
		if err := runVStreamSnapshotTZSwap(t, excludeEverything); err != nil {
			t.Errorf("an EXCLUDED table's swap killed the cold-start snapshot stream: %v", err)
		}
	})
}

// TestSessionTZRefusal_OutOfScopeStillAdvancesTheMemo pins that the gate
// narrowed the REFUSAL and nothing else. The signature memo advancing is
// what makes the next boundary compare against the post-ALTER shape
// rather than a stale one, so an excluded table that is later brought
// into scope by `schema add-table` is graded from where the source
// actually is.
func TestSessionTZRefusal_OutOfScopeStillAdvancesTheMemo(t *testing.T) {
	r := &CDCReader{
		schema:                     "app",
		snapshotSig:                map[string]ir.SchemaSignature{},
		pendingDDLActive:           true,
		pendingDDLAnchor:           ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor"},
		schemaDeltaAppliesToTarget: true,
		scopeAllowed:               func(_, _ string) bool { return false },
	}
	out := make(chan ir.Change, 8)
	v1 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{tsCol("created_at", 0)}}
	v2 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 0)}}
	ctx := context.Background()
	if err := r.maybeSnapshotSchemaB1(ctx, "app.events", v1, out); err != nil {
		t.Fatalf("prime boundary: %v", err)
	}
	if err := r.maybeSnapshotSchemaB1(ctx, "app.events", v2, out); err != nil {
		t.Fatalf("out-of-scope swap must not refuse: %v", err)
	}
	got, ok := r.priorSig["app.events"]
	if !ok {
		t.Fatal("priorSig has no entry for the excluded table; the scope gate swallowed the memo write " +
			"as well as the refusal, so the NEXT boundary would compare against nothing")
	}
	if want := ir.SchemaSignatureOf(projectTableIR(v2)); !got.Equal(want) {
		t.Errorf("priorSig holds the PRE-swap shape for an excluded table; it must track the source's "+
			"current shape so a later add-table grades from where the source actually is\n  got:  %v\n  want: %v",
			got, want)
	}
}

// runBinlogTZSwap primes the binlog reader with a TIMESTAMP column and
// then hands it the DATETIME version, under the supplied scope
// predicate. Mirrors TestB1_MaybeSnapshot_SessionTZCastRefuses' fixture.
func runBinlogTZSwap(t *testing.T, scope func(schema, table string) bool) error {
	t.Helper()
	r := &CDCReader{
		schema:                     "app",
		snapshotSig:                map[string]ir.SchemaSignature{},
		pendingDDLActive:           true,
		pendingDDLAnchor:           ir.Position{Engine: engineNameMySQL, Token: "ddl-anchor"},
		schemaDeltaAppliesToTarget: true,
		scopeAllowed:               scope,
	}
	out := make(chan ir.Change, 8)
	v1 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{tsCol("created_at", 0)}}
	v2 := &tableSchema{Schema: "app", Name: "events", Columns: []*ir.Column{dtCol("created_at", 0)}}
	ctx := context.Background()
	if err := r.maybeSnapshotSchemaB1(ctx, "app.events", v1, out); err != nil {
		t.Fatalf("prime boundary: %v", err)
	}
	return r.maybeSnapshotSchemaB1(ctx, "app.events", v2, out)
}

// runVStreamTZSwap is the standalone VStream lane's equivalent. The
// fixture's table is `users` in keyspace `ks` (see fieldEvent), which is
// what makes the argument-order mutation visible: a predicate keyed on
// "users" admits, one accidentally handed "ks" does not.
func runVStreamTZSwap(t *testing.T, scope func(schema, table string) bool) error {
	t.Helper()
	r := newVStreamTestReader()
	r.schemaDeltaAppliesToTarget = true
	r.scopeAllowed = scope
	out := make(chan ir.Change, 16)
	ctx := context.Background()
	for _, ev := range []*binlogdata.VEvent{vgtidEvent("gtid-1"), tzFieldEvent("timestamp"), vgtidEvent("gtid-2")} {
		if err := r.dispatch(ctx, ev, out); err != nil {
			t.Fatalf("prime: %v", err)
		}
	}
	return r.dispatch(ctx, tzFieldEvent("datetime"), out)
}

// runVStreamSnapshotTZSwap is the cold-start snapshot lane's equivalent.
func runVStreamSnapshotTZSwap(t *testing.T, scope func(schema, table string) bool) error {
	t.Helper()
	s := newVStreamSnapshotTestStream()
	s.schemaDeltaAppliesToTarget = true
	s.scopeAllowed = scope
	out := make(chan ir.Change, 16)
	ctx := context.Background()
	for _, ev := range []*binlogdata.VEvent{vgtidEvent("gtid-pre"), tzFieldEvent("timestamp"), vgtidEvent("gtid-post")} {
		if err := s.dispatchCDCEvent(ctx, ev, out); err != nil {
			t.Fatalf("prime: %v", err)
		}
	}
	return s.dispatchCDCEvent(ctx, tzFieldEvent("datetime"), out)
}
