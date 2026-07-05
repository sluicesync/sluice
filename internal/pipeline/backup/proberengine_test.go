// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// budgetProberEngine is a fake ir.Engine that implements
// [ir.TargetConnectionBudgetProber] by returning a canned report. It
// embeds stubEngine so the Open* methods panic if the orchestrator path
// under test ever reaches them (it shouldn't — the budget step only
// probes). Mirror of the pipeline-root test copy (a test-only helper,
// duplicated across the two package test trees so neither imports the
// other's).
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

// noProberEngine is a plain engine WITHOUT the prober — models a MySQL target
// where the connection-budget step must be a clean no-op. Mirror of the
// pipeline-root test copy.
type noProberEngine struct{ stubEngine }
