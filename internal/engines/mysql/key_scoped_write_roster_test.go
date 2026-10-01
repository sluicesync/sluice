// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// TestKeyScopedWriteRoster is GC-42's gate on the MySQL applier: every call
// site of buildUpdateSQL / buildDeleteSQL must sit in a case (or function)
// that also runs checkKeyScopedResult. Derived from the AST, so a new arm or
// dispatcher cannot skip the check by not being listed.
//
// Reach, stated: see [applyorder.GuardedCallSites]. Today's sites: dispatch
// (update, delete). The coalesced forms are different builders and exempt by
// construction — buildMultiRowDeleteSQL is keyed by the target's own PRIMARY
// KEY, and the ADR-0140 update-as-upsert has no WHERE.
func TestKeyScopedWriteRoster(t *testing.T) {
	sites, problems, err := applyorder.GuardedCallSites(".",
		[]string{"buildUpdateSQL", "buildDeleteSQL"},
		[]string{"checkKeyScopedResult"})
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	for _, p := range problems {
		t.Error(p)
	}
	if sites < 2 {
		t.Fatalf("found %d builder call sites; want at least 2 — the walk is not finding the write paths", sites)
	}
}
