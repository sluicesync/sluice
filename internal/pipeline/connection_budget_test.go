// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// budgetProberEngine is a fake ir.Engine that implements
// [ir.TargetConnectionBudgetProber] by returning a canned report. It
// embeds stubEngine so the Open* methods panic if the orchestrator path
// under test ever reaches them (it shouldn't — the budget step only
// probes).
type budgetProberEngine struct {
	stubEngine
	report    ir.ConnectionBudget
	openErr   error
	gotReq    int
	gotCeil   int
	probeCall int
}

func (b *budgetProberEngine) ProbeTargetConnectionBudget(_ context.Context, _ string, requested, ceiling int) (ir.ConnectionBudget, error) {
	b.gotReq = requested
	b.gotCeil = ceiling
	b.probeCall++
	if b.openErr != nil {
		return ir.ConnectionBudget{}, b.openErr
	}
	return b.report, nil
}

// noProberEngine is a plain engine WITHOUT the prober — models a MySQL
// target where the budget step must be a clean no-op.
type noProberEngine struct{ stubEngine }

func TestResolveTargetCopyParallelism_NoProberIsNoOp(t *testing.T) {
	got, _, err := resolveTargetCopyParallelism(context.Background(), noProberEngine{}, "dsn", 8, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 8 {
		t.Errorf("no-prober engine should pass requested through unchanged; got %d, want 8", got)
	}
}

func TestResolveTargetCopyParallelism_CapsDown(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		EffectiveParallelism: 3,
		Capped:               true,
		CopyBudget:           3,
	}}
	got, _, err := resolveTargetCopyParallelism(context.Background(), eng, "dsn", 8, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 3 {
		t.Errorf("effective parallelism = %d, want 3 (capped)", got)
	}
	if eng.gotReq != 8 {
		t.Errorf("prober saw requested = %d, want 8", eng.gotReq)
	}
}

func TestResolveTargetCopyParallelism_PassesCeiling(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{EffectiveParallelism: 5}}
	if _, _, err := resolveTargetCopyParallelism(context.Background(), eng, "dsn", 8, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eng.gotCeil != 5 {
		t.Errorf("prober saw ceiling = %d, want 5", eng.gotCeil)
	}
}

func TestResolveTargetCopyParallelism_RefuseSurfacesError(t *testing.T) {
	sentinel := errors.New("budget exhausted")
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		Refuse:       true,
		RefusalError: sentinel,
	}}
	_, _, err := resolveTargetCopyParallelism(context.Background(), eng, "dsn", 8, 0)
	if err == nil {
		t.Fatal("expected a refusal error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("refusal error should wrap the engine's RefusalError; got %v", err)
	}
}

func TestResolveTargetCopyParallelism_ProbeFailedDegrades(t *testing.T) {
	eng := &budgetProberEngine{report: ir.ConnectionBudget{
		ProbeFailed: true,
		Warning:     "catalog quirk",
	}}
	got, _, err := resolveTargetCopyParallelism(context.Background(), eng, "dsn", 8, 0)
	if err != nil {
		t.Fatalf("probe-failed must NOT error (degrade to blind behaviour); got %v", err)
	}
	if got != 8 {
		t.Errorf("probe-failed should return the requested value unchanged; got %d, want 8", got)
	}
}

func TestResolveTargetCopyParallelism_OpenErrorSurfaces(t *testing.T) {
	eng := &budgetProberEngine{openErr: errors.New("bad dsn")}
	_, _, err := resolveTargetCopyParallelism(context.Background(), eng, "dsn", 8, 0)
	if err == nil {
		t.Fatal("a connection-open error should surface, not be swallowed")
	}
}
