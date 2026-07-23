// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDropStreamPublication_GuardNeverTouchesSharedOrEmpty pins the
// dropOwnPublicationIfPerStream guard semantics on the decommission
// surface: an empty name and the shared default (`sluice_pub`) return
// SkippedShared WITHOUT any database round-trip — the manager below
// has a nil *sql.DB, so any query attempt would panic, making the
// no-touch claim structural rather than asserted.
func TestDropStreamPublication_GuardNeverTouchesSharedOrEmpty(t *testing.T) {
	m := &SlotManager{db: nil}
	for _, name := range []string{"", defaultPublication} {
		for _, dryRun := range []bool{false, true} {
			out, err := m.DropStreamPublication(context.Background(), name, dryRun)
			if err != nil {
				t.Errorf("name=%q dryRun=%v: err = %v; the guard must not touch the DB", name, dryRun, err)
			}
			if out != ir.PublicationDropSkippedShared {
				t.Errorf("name=%q dryRun=%v: outcome = %v; want PublicationDropSkippedShared", name, dryRun, out)
			}
		}
	}
}
