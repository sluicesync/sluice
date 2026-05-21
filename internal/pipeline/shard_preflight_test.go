// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// stubShardProber implements both [ir.TableEmptyChecker] and
// [shardPreflightProber] for the populated-target Shape-A
// preflight tests. Canned per-table answers via maps keyed on
// table.Name; an empty map for a check returns the zero answer.
type stubShardProber struct {
	empty           map[string]bool
	hasNull         map[string]bool
	valuesPresent   map[string]map[any]bool
	pkLeads         map[string]bool
	emptyErr        error
	hasNullErr      error
	valuePresentErr error
	pkLeadsErr      error
	calls           map[string]int
}

func newStubShardProber() *stubShardProber {
	return &stubShardProber{
		empty:         map[string]bool{},
		hasNull:       map[string]bool{},
		valuesPresent: map[string]map[any]bool{},
		pkLeads:       map[string]bool{},
		calls:         map[string]int{},
	}
}

func (s *stubShardProber) record(name string) { s.calls[name]++ }
func (s *stubShardProber) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error {
	return errors.New("stubShardProber.WriteRows should not be called by pre-flight")
}

func (s *stubShardProber) IsTableEmpty(_ context.Context, table *ir.Table) (bool, error) {
	s.record("IsTableEmpty:" + table.Name)
	if s.emptyErr != nil {
		return false, s.emptyErr
	}
	v, ok := s.empty[table.Name]
	if !ok {
		// Default: non-empty so the three-point check actually runs
		// in the typical test setup.
		return false, nil
	}
	return v, nil
}

func (s *stubShardProber) HasNullShardColumn(_ context.Context, table *ir.Table, _ string) (bool, error) {
	s.record("HasNullShardColumn:" + table.Name)
	if s.hasNullErr != nil {
		return false, s.hasNullErr
	}
	return s.hasNull[table.Name], nil
}

func (s *stubShardProber) ShardValuePresent(_ context.Context, table *ir.Table, _ string, value any) (bool, error) {
	s.record("ShardValuePresent:" + table.Name)
	if s.valuePresentErr != nil {
		return false, s.valuePresentErr
	}
	if m, ok := s.valuesPresent[table.Name]; ok {
		return m[value], nil
	}
	return false, nil
}

func (s *stubShardProber) CompositePKLeadsWith(_ context.Context, table *ir.Table, _ string) (bool, error) {
	s.record("CompositePKLeadsWith:" + table.Name)
	if s.pkLeadsErr != nil {
		return false, s.pkLeadsErr
	}
	// Default: TRUE (good case) — tests opt INTO the failing case
	// per-table.
	v, ok := s.pkLeads[table.Name]
	if !ok {
		return true, nil
	}
	return v, nil
}

func shardOneTableSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "customer",
		Columns: []*ir.Column{
			{Name: "customer_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "customer_id"}}},
	}}}
}

// TestPreflightShardConsolidation_NoFlag short-circuits when the
// operator hasn't set --inject-shard-column. The opt-in shape is
// load-bearing: every existing single-source flow must pay zero
// cost when Shape A isn't engaged.
func TestPreflightShardConsolidation_NoFlag(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	if err := preflightShardConsolidation(context.Background(), schema, rw, "", nil); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
	if len(rw.calls) != 0 {
		t.Errorf("expected no probes when flag unset; got %v", rw.calls)
	}
}

// TestPreflightShardConsolidation_EmptyTablePasses is shard 1's
// legitimate cold-start path: target table is empty, the flag is
// set, no three-point check needed (nothing to collide with).
func TestPreflightShardConsolidation_EmptyTablePasses(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	rw.empty["customer"] = true
	if err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1"); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
	if rw.calls["HasNullShardColumn:customer"] != 0 {
		t.Errorf("expected no NULL probe when table empty; got %d", rw.calls["HasNullShardColumn:customer"])
	}
}

// TestPreflightShardConsolidation_HappyPopulatedPath: target
// non-empty, every existing row has a non-NULL discriminator
// distinct from the incoming shard's value, composite PK leads
// with the discriminator — all three pass.
func TestPreflightShardConsolidation_HappyPopulatedPath(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	// empty=false (default), hasNull=false (default),
	// valuesPresent={} (no row has the incoming value),
	// pkLeads=true (default).
	if err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-2"); err != nil {
		t.Errorf("expected nil on happy populated path; got %v", err)
	}
	if rw.calls["HasNullShardColumn:customer"] != 1 ||
		rw.calls["ShardValuePresent:customer"] != 1 ||
		rw.calls["CompositePKLeadsWith:customer"] != 1 {
		t.Errorf("expected all three probes to run once each; got %v", rw.calls)
	}
}

// TestPreflightShardConsolidation_RefusesOnNullRows: existing
// row has a NULL discriminator ⇒ refuse loudly with table name +
// operator-actionable recovery (backfill / reset-target-data).
func TestPreflightShardConsolidation_RefusesOnNullRows(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	rw.hasNull["customer"] = true
	err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1")
	if err == nil {
		t.Fatal("expected refusal on NULL discriminator; got nil")
	}
	if !errors.Is(err, errShardConsolidationRefused) {
		t.Errorf("expected errors.Is(errShardConsolidationRefused); got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `"customer"`) {
		t.Errorf("expected table name in error; got %q", msg)
	}
	if !strings.Contains(msg, "NULL") {
		t.Errorf("expected error to name the NULL discriminator; got %q", msg)
	}
	if !strings.Contains(msg, "--reset-target-data") {
		t.Errorf("expected operator-actionable recovery hint; got %q", msg)
	}
}

// TestPreflightShardConsolidation_RefusesOnValuePresent: the
// incoming shard's VALUE is already present on the target ⇒ refuse
// (double-load / cross-shard collision).
func TestPreflightShardConsolidation_RefusesOnValuePresent(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	rw.valuesPresent["customer"] = map[any]bool{"us-east-1": true}
	err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1")
	if err == nil {
		t.Fatal("expected refusal on present shard value; got nil")
	}
	if !errors.Is(err, errShardConsolidationRefused) {
		t.Errorf("expected errors.Is sentinel; got %v", err)
	}
	if !strings.Contains(err.Error(), "us-east-1") {
		t.Errorf("expected error to name the colliding shard value; got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "--resume") {
		t.Errorf("expected error to suggest --resume as a recovery path; got %q", err.Error())
	}
}

// TestPreflightShardConsolidation_RefusesOnNonLeadingPK: composite
// PK exists but doesn't lead with the discriminator ⇒ refuse
// (disjointness invariant void).
func TestPreflightShardConsolidation_RefusesOnNonLeadingPK(t *testing.T) {
	schema := shardOneTableSchema()
	rw := newStubShardProber()
	rw.pkLeads["customer"] = false
	err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1")
	if err == nil {
		t.Fatal("expected refusal on non-leading PK; got nil")
	}
	if !errors.Is(err, errShardConsolidationRefused) {
		t.Errorf("expected errors.Is sentinel; got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "composite PRIMARY KEY") {
		t.Errorf("expected error to name the composite-PK refusal; got %q", msg)
	}
	if !strings.Contains(msg, "--exclude-table") {
		t.Errorf("expected operator-actionable recovery (--exclude-table); got %q", msg)
	}
}

// TestPreflightShardConsolidation_NoProberSurfaceSkips: engines
// that don't implement shardPreflightProber are opportunistically
// skipped — the orchestrator falls back on the existing
// preflightColdStart's loud refusal in that case.
func TestPreflightShardConsolidation_NoProberSurfaceSkips(t *testing.T) {
	schema := shardOneTableSchema()
	if err := preflightShardConsolidation(context.Background(), schema, stubWriterNoChecker{}, "source_shard_id", "us-east-1"); err != nil {
		t.Errorf("expected nil when prober surface absent; got %v", err)
	}
}

// TestPreflightShardConsolidation_ProbeErrorsPropagate: each
// probe's error surfaces verbatim (wrapped) — operator must see
// connection/permission failures, not a silent "all green."
func TestPreflightShardConsolidation_ProbeErrorsPropagate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*stubShardProber)
		wantSub string
	}{
		{
			name:    "empty probe error",
			mutate:  func(s *stubShardProber) { s.emptyErr = errors.New("empty probe failure") },
			wantSub: "empty probe failure",
		},
		{
			name:    "null probe error",
			mutate:  func(s *stubShardProber) { s.hasNullErr = errors.New("null probe failure") },
			wantSub: "null probe failure",
		},
		{
			name:    "value-present probe error",
			mutate:  func(s *stubShardProber) { s.valuePresentErr = errors.New("value probe failure") },
			wantSub: "value probe failure",
		},
		{
			name:    "pk-leads probe error",
			mutate:  func(s *stubShardProber) { s.pkLeadsErr = errors.New("pk probe failure") },
			wantSub: "pk probe failure",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := shardOneTableSchema()
			rw := newStubShardProber()
			tc.mutate(rw)
			err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1")
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q missing %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// TestPreflightShardConsolidation_StopsAtFirstOffender: when two
// tables would refuse, the first-by-iteration is the one named.
// This matches preflightColdStart's deterministic-first-failure
// shape — operators get the same error every run, not a list that
// changes order between calls.
func TestPreflightShardConsolidation_StopsAtFirstOffender(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "alpha", PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}},
		{Name: "beta", PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}},
	}}
	rw := newStubShardProber()
	rw.hasNull["alpha"] = true
	rw.hasNull["beta"] = true
	err := preflightShardConsolidation(context.Background(), schema, rw, "source_shard_id", "us-east-1")
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	if !strings.Contains(err.Error(), `"alpha"`) || strings.Contains(err.Error(), `"beta"`) {
		t.Errorf("expected alpha-only first-failure; got %q", err.Error())
	}
}
