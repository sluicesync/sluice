// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// staleReaperEngine is a fake ir.Engine implementing
// [ir.TargetStaleBackendReaper] with a canned report. It embeds
// stubEngine so any Open* the path-under-test reaches panics (the
// preflight only detects). It records the args it saw so the test can
// assert the schema list and reap flag were threaded.
type staleReaperEngine struct {
	stubEngine
	report  ir.StaleBackendReport
	openErr error

	gotSchemas []string
	gotReap    bool
	calls      int
}

func (e *staleReaperEngine) DetectStaleBackends(_ context.Context, _ string, schemas []string, reap bool) (ir.StaleBackendReport, error) {
	e.gotSchemas = schemas
	e.gotReap = reap
	e.calls++
	if e.openErr != nil {
		return ir.StaleBackendReport{}, e.openErr
	}
	return e.report, nil
}

// noReaperEngine has no reaper — models a MySQL target where the step
// must be a clean no-op.
type noReaperEngine struct{ stubEngine }

func TestPreflightStaleBackends_NoReaperIsNoOp(t *testing.T) {
	if err := preflightStaleBackends(context.Background(), noReaperEngine{}, "dsn", []string{"public"}, true); err != nil {
		t.Fatalf("no-reaper engine should be a clean no-op; got %v", err)
	}
}

func TestPreflightStaleBackends_ReportOnlyProceeds(t *testing.T) {
	eng := &staleReaperEngine{report: ir.StaleBackendReport{
		Backends: []ir.StaleBackend{{PID: 123, ApplicationName: "sluice/snapshot/x", State: "idle in transaction"}},
	}}
	// reap=false: detection found an orphan but the step must NOT block.
	if err := preflightStaleBackends(context.Background(), eng, "dsn", []string{"public"}, false); err != nil {
		t.Fatalf("report-only path must proceed, never block; got %v", err)
	}
	if eng.gotReap {
		t.Error("reap flag should have been false")
	}
	if eng.calls != 1 {
		t.Errorf("expected exactly one detect call, got %d", eng.calls)
	}
}

func TestPreflightStaleBackends_ReapThreadsFlag(t *testing.T) {
	eng := &staleReaperEngine{report: ir.StaleBackendReport{
		Backends: []ir.StaleBackend{{PID: 123}, {PID: 456}},
		Reaped:   []int{123, 456},
	}}
	if err := preflightStaleBackends(context.Background(), eng, "dsn", []string{"analytics"}, true); err != nil {
		t.Fatalf("reap path must proceed after terminating; got %v", err)
	}
	if !eng.gotReap {
		t.Error("reap flag should have been threaded as true")
	}
	if len(eng.gotSchemas) != 1 || eng.gotSchemas[0] != "analytics" {
		t.Errorf("schema list not threaded; got %v", eng.gotSchemas)
	}
}

func TestPreflightStaleBackends_ProbeFailedDegrades(t *testing.T) {
	eng := &staleReaperEngine{report: ir.StaleBackendReport{
		ProbeFailed: true,
		Warning:     "catalog quirk",
	}}
	if err := preflightStaleBackends(context.Background(), eng, "dsn", []string{"public"}, false); err != nil {
		t.Fatalf("probe-failed must degrade (not error); got %v", err)
	}
}

func TestPreflightStaleBackends_OpenErrorSurfaces(t *testing.T) {
	eng := &staleReaperEngine{openErr: errors.New("bad dsn")}
	if err := preflightStaleBackends(context.Background(), eng, "dsn", []string{"public"}, false); err == nil {
		t.Fatal("a connection-open error should surface, not be swallowed")
	}
}

func TestPreflightStaleBackends_NoOrphansProceeds(t *testing.T) {
	eng := &staleReaperEngine{report: ir.StaleBackendReport{}}
	if err := preflightStaleBackends(context.Background(), eng, "dsn", []string{"public"}, true); err != nil {
		t.Fatalf("no orphans should proceed cleanly; got %v", err)
	}
}

func TestTargetWriteSchemas(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{Schema: "public", Name: "a"},
		{Schema: "analytics", Name: "b"},
		{Schema: "public", Name: "c"}, // dup schema
		{Schema: "", Name: "flat"},    // MySQL flat scope — dropped
	}}

	// --target-schema override leads, then distinct table schemas.
	got := targetWriteSchemas(schema, "warehouse")
	want := []string{"warehouse", "public", "analytics"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// No override, nil schema → empty (the engine folds in control).
	if s := targetWriteSchemas(nil, ""); len(s) != 0 {
		t.Errorf("nil schema + no override should be empty; got %v", s)
	}
}

func TestContainsInt(t *testing.T) {
	if !containsInt([]int{1, 2, 3}, 2) {
		t.Error("expected 2 to be found")
	}
	if containsInt([]int{1, 2, 3}, 9) {
		t.Error("did not expect 9 to be found")
	}
	if containsInt(nil, 1) {
		t.Error("nil slice contains nothing")
	}
}
