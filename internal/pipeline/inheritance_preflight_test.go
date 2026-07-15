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

// stubInheritanceProber implements [inheritancePreflightProber] for
// the unit-test surface — mirrors stubPartitionProber.
type stubInheritanceProber struct {
	parents []string
	err     error
}

func (s stubInheritanceProber) InheritanceParents(_ context.Context) ([]string, error) {
	return s.parents, s.err
}

// TestPreflightInheritanceTables_NonPGSourceSkips pins the
// PostgresBackend capability gate.
func TestPreflightInheritanceTables_NonPGSourceSkips(t *testing.T) {
	p := stubInheritanceProber{parents: []string{"measurements"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "measurements"}}}
	if err := preflightInheritanceTables(context.Background(), p, capsMySQL, schema); err != nil {
		t.Errorf("got %v; want nil (non-PG source short-circuits)", err)
	}
}

// TestPreflightInheritanceTables_HandleWithoutProberSkips pins the
// opportunistic-skip posture (matches the partition preflight).
func TestPreflightInheritanceTables_HandleWithoutProberSkips(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "measurements"}}}
	if err := preflightInheritanceTables(context.Background(), struct{}{}, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (handle without prober skips silently)", err)
	}
}

// TestPreflightInheritanceTables_NoParents pins the happy-path no-op.
func TestPreflightInheritanceTables_NoParents(t *testing.T) {
	p := stubInheritanceProber{}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	if err := preflightInheritanceTables(context.Background(), p, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (no inheritance parents)", err)
	}
}

// TestPreflightInheritanceTables_InScopeRefuses is the canonical
// item-68b repro: an old-style inheritance parent in migration scope
// refuses loudly, wrapping the sentinel, naming the parent, the
// duplication mechanism (parent SELECT returns child rows while
// children copy independently), and the recovery paths.
func TestPreflightInheritanceTables_InScopeRefuses(t *testing.T) {
	p := stubInheritanceProber{parents: []string{"measurements"}}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "measurements"}, {Name: "measurements_2025"}, {Name: "users"},
	}}
	err := preflightInheritanceTables(context.Background(), p, capsSlotPG, schema)
	if err == nil {
		t.Fatal("got nil; want loud refusal — old-style inheritance would silently duplicate the child rows")
	}
	if !errors.Is(err, errInheritanceTableRefused) {
		t.Errorf("want errInheritanceTableRefused sentinel; got: %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"measurements", "INHERITS", "twice", "--exclude-table", "ONLY", "Recovery",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal should mention %q; got: %v", want, err)
		}
	}
}

// TestPreflightInheritanceTables_ExcludedFromScopePasses pins recovery
// path (a): an operator who already excluded the parent is not
// refused. The preflight cross-references the prober's raw list
// against the post-filter schema set.
func TestPreflightInheritanceTables_ExcludedFromScopePasses(t *testing.T) {
	p := stubInheritanceProber{parents: []string{"measurements"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "measurements_2025"}, {Name: "users"}}}
	if err := preflightInheritanceTables(context.Background(), p, capsSlotPG, schema); err != nil {
		t.Errorf("got %v; want nil (`measurements` already excluded — operator took recovery path (a))", err)
	}
}

// TestPreflightInheritanceTables_MultipleParentsRefuses pins the
// multi-table refusal shape: every in-scope parent is named in one
// run, no fix-rerun-fix-rerun cycles.
func TestPreflightInheritanceTables_MultipleParentsRefuses(t *testing.T) {
	p := stubInheritanceProber{parents: []string{"cities", "logs", "measurements"}}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "cities"}, {Name: "logs"}, {Name: "measurements"}, {Name: "users"},
	}}
	err := preflightInheritanceTables(context.Background(), p, capsSlotPG, schema)
	if err == nil {
		t.Fatal("got nil; want loud refusal")
	}
	msg := err.Error()
	for _, want := range []string{"cities", "logs", "measurements"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal should mention %q; got: %v", want, err)
		}
	}
}

// TestPreflightInheritanceTables_ProberErrorPropagates pins the
// fail-loudly posture on a probe failure.
func TestPreflightInheritanceTables_ProberErrorPropagates(t *testing.T) {
	p := stubInheritanceProber{err: errors.New("source connection refused")}
	err := preflightInheritanceTables(context.Background(), p, capsSlotPG, nil)
	if err == nil {
		t.Fatal("got nil; want prober error propagated")
	}
	if !strings.Contains(err.Error(), "source connection refused") {
		t.Errorf("err should preserve the prober failure; got: %v", err)
	}
}

// TestPreflightInheritanceTables_PostgresTriggerAlsoGated pins the
// capability gate's PG-server set: postgres-trigger (which delegates
// the same SchemaReader) exercises inheritance detection too.
func TestPreflightInheritanceTables_PostgresTriggerAlsoGated(t *testing.T) {
	p := stubInheritanceProber{parents: []string{"measurements"}}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "measurements"}}}
	err := preflightInheritanceTables(context.Background(), p, capsTriggerPG, schema)
	if err == nil {
		t.Fatal("got nil; want refusal — postgres-trigger also declares ir.Capabilities.PostgresBackend")
	}
	if !errors.Is(err, errInheritanceTableRefused) {
		t.Errorf("want errInheritanceTableRefused sentinel; got: %v", err)
	}
}
