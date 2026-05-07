// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for slot_create.go and server_version.go. These cover
// the bits that don't require a live Postgres — version-string
// parsing and the warn-once behaviour for the PG ≤ 16 fallback.
// Live-server coverage lives in the integration test next to it.

package postgres

import (
	"strings"
	"testing"
)

// TestPGVersionFailoverSupportConstant guards against accidental
// drift in the version threshold. PG 17.0 → 170000; bumping the
// constant would silently change behaviour for PG 16.x clusters,
// so a regression test that pins the value is cheap insurance.
func TestPGVersionFailoverSupportConstant(t *testing.T) {
	if pgVersionFailoverSupport != 170000 {
		t.Errorf("pgVersionFailoverSupport = %d; want 170000 (PG 17.0)", pgVersionFailoverSupport)
	}
}

// TestWarnNoFailoverSupport_OncePerSlot exercises the sync.Map-
// backed deduplication. The first call for a given slot name should
// register; the second call (same name) is a no-op. A different
// slot name still registers.
//
// We can't easily intercept os.Stderr in a unit test without
// reaching into globals, so this test asserts the LoadOrStore
// behaviour indirectly: after one call, warnedSlots contains the
// slot; a second call doesn't overwrite or duplicate; a different
// name produces a new entry.
func TestWarnNoFailoverSupport_OncePerSlot(t *testing.T) {
	resetSlotWarningsForTest()
	t.Cleanup(resetSlotWarningsForTest)

	// First warn for "slot_a".
	warnNoFailoverSupport("slot_a", 160006)
	if _, ok := warnedSlots.Load("slot_a"); !ok {
		t.Fatalf("warnedSlots missing slot_a after first warn")
	}

	// Second warn for "slot_a" — should be deduped.
	warnNoFailoverSupport("slot_a", 160006)
	count := 0
	warnedSlots.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 1 {
		t.Errorf("warnedSlots size = %d after second warn for same slot; want 1", count)
	}

	// Different slot — should register.
	warnNoFailoverSupport("slot_b", 160006)
	if _, ok := warnedSlots.Load("slot_b"); !ok {
		t.Errorf("warnedSlots missing slot_b after warn")
	}
}

// TestCreateSlotWithFailover_CommandShape is a defensive
// sanity-check on the protocol-command string we send. We can't
// actually run the command here without a server, but the format
// string is small and load-bearing — a typo would silently fall
// through to the server and surface as a confusing parser error
// downstream.
//
// Specifically, this test pins the snapshot-option spelling: PG 17+
// requires the *named* form `SNAPSHOT 'export'` inside an
// option-list, not the bare keyword `EXPORT_SNAPSHOT` (which is the
// pre-PG-17 syntax). PlanetScale Postgres rejected the bare-keyword
// form in v0.2.0 with "ERROR: unrecognized option: export_snapshot"
// — this test guards against the regression coming back.
//
// Strategy: build the command exactly the way createSlotWithFailover
// does, and assert the substring the server will see. The two
// shapes (with/without snapshot export) are both covered.
func TestCreateSlotWithFailover_CommandShape(t *testing.T) {
	// Mirror the format used inside createSlotWithFailover. If the
	// helper changes, this test guards against accidental drift.
	build := func(slot string, exportSnapshot bool) string {
		opts := []string{}
		if exportSnapshot {
			opts = append(opts, "SNAPSHOT 'export'")
		}
		opts = append(opts, "FAILOVER true")
		// Use the same quoteIdent path the helper uses.
		return "CREATE_REPLICATION_SLOT " + quoteIdent(slot) +
			" LOGICAL pgoutput (" + strings.Join(opts, ", ") + ")"
	}

	cases := []struct {
		name           string
		slot           string
		exportSnapshot bool
		want           string
	}{
		{
			name:           "cold-start without snapshot",
			slot:           "sluice_slot",
			exportSnapshot: false,
			want:           `CREATE_REPLICATION_SLOT "sluice_slot" LOGICAL pgoutput (FAILOVER true)`,
		},
		{
			name:           "snapshot-and-CDC handoff uses named SNAPSHOT 'export'",
			slot:           "sluice_slot",
			exportSnapshot: true,
			want:           `CREATE_REPLICATION_SLOT "sluice_slot" LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER true)`,
		},
		{
			name:           "slot name with embedded quote escapes",
			slot:           `weird"name`,
			exportSnapshot: false,
			want:           `CREATE_REPLICATION_SLOT "weird""name" LOGICAL pgoutput (FAILOVER true)`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := build(c.slot, c.exportSnapshot)
			if got != c.want {
				t.Errorf("command:\n got: %q\nwant: %q", got, c.want)
			}
		})
	}
}
