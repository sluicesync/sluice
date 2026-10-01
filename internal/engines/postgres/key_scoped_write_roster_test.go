// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/applyorder"
)

// TestKeyScopedWriteRoster is GC-42's gate on the Postgres applier: every
// call site of buildUpdateSQL / buildDeleteSQL — the only builders of a
// before-image-scoped UPDATE/DELETE — must sit in a case (or function) that
// also runs the multi-row check: checkKeyScopedResult on a serial exec,
// guardKeyScopedWrite on a queued statement. Derived from the AST, so a new
// dispatcher or arm cannot skip the check by not being listed.
//
// Reach, stated: see [applyorder.GuardedCallSites] — presence of the guard in
// the scope, not proof it reads that statement's count. Today's sites:
// dispatch (update, delete) and dispatchPipelined (update, delete).
func TestKeyScopedWriteRoster(t *testing.T) {
	sites, problems, err := applyorder.GuardedCallSites(".",
		[]string{"buildUpdateSQL", "buildDeleteSQL"},
		[]string{"checkKeyScopedResult", "guardKeyScopedWrite"})
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	for _, p := range problems {
		t.Error(p)
	}
	if sites < 4 {
		t.Fatalf("found %d builder call sites; want at least 4 — the walk is not finding the write paths", sites)
	}
}
