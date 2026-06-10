// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// stubPartitionProber implements [partitionPreflightProber] for the
// unit-test surface — the orchestrator-side preflight logic doesn't
// need a live PG to exercise.
type stubPartitionProber struct {
	tables []string
	err    error
}

func (s stubPartitionProber) PartitionedTables(_ context.Context) ([]string, error) {
	return s.tables, s.err
}

// TestPreflightPartitionedTables_NonPGSourceSkips pins the
// PostgresBackend capability gate: a MySQL source short-circuits
// even if its handle would satisfy the prober interface (none does today; the pin guards a
// future engine that might).
func TestPreflightPartitionedTables_NonPGSourceSkips(t *testing.T) {
	p := stubPartitionProber{tables: []string{"events"}} // would refuse on PG
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "events"}}}
	if err := preflightPartitionedTables(context.Background(), p, capsMySQL, schema); err != nil {
		t.Errorf("got %v; want nil (non-PG source short-circuits)", err)
	}
}

// TestPreflightPartitionedTables_HandleWithoutProberSkips pins the
// opportunistic-skip posture: a PG handle that doesn't implement the
// prober interface skips silently (matches preflightRLS).
func TestPreflightPartitionedTables_HandleWithoutProberSkips(t *testing.T) {
	type bareSchemaReader struct{}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "events"}}}
	if err := preflightPartitionedTables(context.Background(), bareSchemaReader{}, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (handle without prober skips silently)", err)
	}
}

// TestPreflightPartitionedTables_NoPartitionedTables pins the
// happy-path no-op: a PG source whose namespace has no partitioned
// parents passes through cleanly.
func TestPreflightPartitionedTables_NoPartitionedTables(t *testing.T) {
	p := stubPartitionProber{tables: nil}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "events"}}}
	if err := preflightPartitionedTables(context.Background(), p, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (no partitioned tables)", err)
	}
}

// TestPreflightPartitionedTables_OneInScopeRefuses is the canonical
// Bug 100 (v0.92.0) repro: one partitioned-parent table is in
// migration scope → loud refusal with the table named, the sentinel
// wrapped, and the three operator-actionable recovery paths surfaced.
func TestPreflightPartitionedTables_OneInScopeRefuses(t *testing.T) {
	p := stubPartitionProber{tables: []string{"events"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "events"}, {Name: "users"}}}
	err := preflightPartitionedTables(context.Background(), p, capsSlotPG, schema)
	if err == nil {
		t.Fatal("got nil; want loud refusal — partitioned-parent silent-flatten would drop key + children + PK")
	}
	if !errors.Is(err, errPartitionedTableRefused) {
		t.Errorf("want errPartitionedTableRefused sentinel; got: %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"events", "--exclude-table", "PARTITION BY", "Recovery"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal should mention %q; got: %v", want, err)
		}
	}
}

// TestPreflightPartitionedTables_ExcludedFromScopePasses pins the
// `--exclude-table=<parent>` recovery path: an operator who already
// excluded the partitioned-parent table from the migration scope
// must not get refused. The preflight cross-references the prober's
// raw list against the post-filter schema set.
func TestPreflightPartitionedTables_ExcludedFromScopePasses(t *testing.T) {
	p := stubPartitionProber{tables: []string{"events"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	if err := preflightPartitionedTables(context.Background(), p, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (`events` already excluded — operator took recovery path (a))", err)
	}
}

// TestPreflightPartitionedTables_MultiplePartitionedRefuses pins the
// multi-table refusal shape: every partitioned-parent in-scope is
// named in the error so the operator sees the full set in one run
// instead of fix-rerun-fix-rerun cycles.
func TestPreflightPartitionedTables_MultiplePartitionedRefuses(t *testing.T) {
	p := stubPartitionProber{tables: []string{"events", "metrics", "audit"}}
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "events"}, {Name: "metrics"}, {Name: "audit"}, {Name: "users"},
		},
	}
	err := preflightPartitionedTables(context.Background(), p, capsSlotPG, schema)
	if err == nil {
		t.Fatal("got nil; want loud refusal")
	}
	msg := err.Error()
	for _, want := range []string{"events", "metrics", "audit"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal should mention %q; got: %v", want, err)
		}
	}
}

// TestPreflightPartitionedTables_ProberErrorPropagates pins the
// fail-loudly posture on a prober error — the partition probe is a
// connect-phase query against pg_partitioned_table; a probe failure
// is the operator's signal that something is wrong with the source
// connection, NOT a reason to silently skip partition detection.
func TestPreflightPartitionedTables_ProberErrorPropagates(t *testing.T) {
	p := stubPartitionProber{err: errors.New("source connection refused")}
	err := preflightPartitionedTables(context.Background(), p, capsSlotPG, nil)
	if err == nil {
		t.Fatal("got nil; want prober error propagated")
	}
	if !strings.Contains(err.Error(), "source connection refused") {
		t.Errorf("err should preserve the prober failure; got: %v", err)
	}
}

// TestPreflightPartitionedTables_PostgresTriggerAlsoGated pins the
// capability gate's PG-server set: a `postgres-trigger` source (which
// also declares PostgresBackend) exercises partition detection too
// (its schema reader delegates to the postgres engine's, so the
// prober is present).
func TestPreflightPartitionedTables_PostgresTriggerAlsoGated(t *testing.T) {
	p := stubPartitionProber{tables: []string{"events"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "events"}}}
	err := preflightPartitionedTables(context.Background(), p, capsTriggerPG, schema)
	if err == nil {
		t.Fatal("got nil; want refusal — postgres-trigger also declares ir.Capabilities.PostgresBackend")
	}
	if !errors.Is(err, errPartitionedTableRefused) {
		t.Errorf("want errPartitionedTableRefused sentinel; got: %v", err)
	}
}
