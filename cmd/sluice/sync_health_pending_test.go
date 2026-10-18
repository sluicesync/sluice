// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApplyPendingAddsHealth pins the fresh/stale classifier `sync health`
// runs on the add-table pending record (Bug 321 F1): a fresh entry is an
// add-table still copying — reported, not tripped — and only a stale one
// trips exit 1, with a remedy that names --abandon.
func TestApplyPendingAddsHealth(t *testing.T) {
	now := time.Now()
	fresh := ir.PendingAddedTable{Table: "live", StartedAt: now.Add(-time.Hour), HeartbeatAt: now.Add(-ir.PendingAddStaleAfter + time.Second)}
	stale := ir.PendingAddedTable{Table: "dead", StartedAt: now.Add(-time.Hour), HeartbeatAt: now.Add(-ir.PendingAddStaleAfter - time.Second)}

	r := HealthResult{Found: true}
	applyPendingAddsHealth(&r, []ir.PendingAddedTable{fresh}, now)
	if r.PendingAddedTablesTripped || len(r.PendingAddedTables) != 1 || !r.PendingAddedTables[0].InProgress {
		t.Fatalf("a fresh entry: %+v; want reported in progress, not tripped", r)
	}

	r = HealthResult{Found: true}
	applyPendingAddsHealth(&r, []ir.PendingAddedTable{fresh, stale}, now)
	if !r.PendingAddedTablesTripped {
		t.Fatalf("a stale entry did not trip: %+v", r)
	}
	var out bytes.Buffer
	if err := renderHealthText(&out, r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ADD-TABLE INCOMPLETE", "pending_added_table: live in progress", "pending_added_table: dead stopped without completing", "--abandon"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("health text lacks %q:\n%s", want, out.String())
		}
	}
	if err := (pendingAddedTablesError{streamID: "st", result: r}); !strings.Contains(err.Error(), "dead") || strings.Contains(err.Error(), "live (") {
		t.Fatalf("the error names %q", err.Error())
	}
}
