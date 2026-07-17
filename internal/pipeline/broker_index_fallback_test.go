// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the audit-MED-A1 gap-#12 broker threading of the ADR-0148
// deploy-request index-build fallback: a --reset-target-data cold start
// runs a [backup.ChainRestore] whose segment-0 full builds the deferred
// indexes, so the broker carries the fallback field onto that restore. An
// unarmed broker (every non-CLI caller's zero value) leaves the field nil —
// byte-identical to before the fallback reached this mode. The
// engine-half classification is pinned against the real MySQL writer in
// internal/engines/mysql/schema_writer_index_fallback_test.go; this covers
// the broker→ChainRestore seam that was missing.

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// brokerFakeIndexFallback is an inert ir.IndexBuildFallback with an identity
// for the threading assertion.
type brokerFakeIndexFallback struct{ id string }

func (brokerFakeIndexFallback) BuildIndexDDL(context.Context, string, []string, error) error {
	return nil
}

// TestSyncFromBackup_ColdStartChainRestoreCarriesIndexBuildFallback pins the
// armed path: the ChainRestore the broker builds for --reset-target-data
// carries the exact fallback value.
func TestSyncFromBackup_ColdStartChainRestoreCarriesIndexBuildFallback(t *testing.T) {
	fb := brokerFakeIndexFallback{id: "armed"}
	cr := (&SyncFromBackup{IndexBuildFallback: fb}).newColdStartChainRestore()
	if cr.IndexBuildFallback != ir.IndexBuildFallback(fb) {
		t.Errorf("cold-start ChainRestore fallback = %#v; want the broker's armed value", cr.IndexBuildFallback)
	}
}

// TestSyncFromBackup_ColdStartChainRestoreUnarmed pins the zero-value
// default: an unarmed broker's cold-start ChainRestore leaves the fallback
// nil, so the reset-leg's index phase is byte-identical to before ADR-0148
// reached this mode.
func TestSyncFromBackup_ColdStartChainRestoreUnarmed(t *testing.T) {
	cr := (&SyncFromBackup{}).newColdStartChainRestore()
	if cr.IndexBuildFallback != nil {
		t.Errorf("unarmed cold-start ChainRestore fallback = %#v; want nil", cr.IndexBuildFallback)
	}
}
