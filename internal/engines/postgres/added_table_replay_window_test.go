// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

func pgTestPos(t *testing.T, slot, lsn, systemID string, timeline int32) ir.Position {
	t.Helper()
	p, err := encodePGPos(pgPos{Slot: slot, LSN: lsn, SystemID: systemID, Timeline: timeline})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestAddedTableCopyPosition_BoundsTheSnapshot pins the Postgres window bound
// (Bug 336): one past the add snapshot's consistent point, in the STREAM's
// slot. Row positions are a transaction's commit LSN, so a transaction the
// snapshot holds (commit LSN below the consistent point, or at it, whichever
// end of the commit record FinalLSN names) is in the window, and the first
// one committed past it is out. A bound left in the temporary slot's name
// would never be passed: the orderer does not compare positions of two slots.
func TestAddedTableCopyPosition_BoundsTheSnapshot(t *testing.T) {
	e := Engine{}
	const sys = "7300000000000000001"
	snapshot := pgTestPos(t, "sluice_addtable_newt", "0/16B3748", sys, 1)
	resume := pgTestPos(t, "sluice_slot", "0/1000000", sys, 1)
	bound, err := e.AddedTableCopyPosition(snapshot, resume)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := decodePGPos(bound)
	if err != nil {
		t.Fatal(err)
	}
	if want := (pgPos{Slot: "sluice_slot", LSN: "0/16B3749", SystemID: sys, Timeline: 1}); got != want {
		t.Fatalf("bound = %+v, want %+v", got, want)
	}
	for _, c := range []struct {
		lsn      string
		inWindow bool
	}{
		{"0/1500000", true},  // committed well before the snapshot
		{"0/16B3747", true},  // just before the consistent point
		{"0/16B3748", true},  // at it: ambiguous, so idempotent
		{"0/16B3749", false}, // the first commit past it
		{"1/0", false},
	} {
		past, err := e.PositionAtOrAfter(pgTestPos(t, "sluice_slot", c.lsn, sys, 1), bound)
		if err != nil {
			t.Fatal(err)
		}
		if !past != c.inWindow {
			t.Errorf("row at %s: in window = %v, want %v", c.lsn, !past, c.inWindow)
		}
	}
	// The trap the slot rewrite avoids: against the temporary slot's own
	// position, no row of the stream's slot is ever at or after it.
	if past, _ := e.PositionAtOrAfter(pgTestPos(t, "sluice_slot", "9/0", sys, 1), snapshot); past {
		t.Fatal("PREMISE: the orderer now compares positions across slots; the bound's slot rewrite is no longer what closes the window")
	}
	for name, args := range map[string][2]ir.Position{
		"another system":          {pgTestPos(t, "tmp", "0/10", "7300000000000000002", 1), resume},
		"a snapshot not decoding": {{Engine: "postgres", Token: "{"}, resume},
		"no stream position":      {snapshot, {}},
	} {
		if _, err := e.AddedTableCopyPosition(args[0], args[1]); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	// The bound round-trips through the stored record byte for byte.
	w := ir.AddedTableReplayWindow{Table: "newt", CopyPosition: bound, Key: []string{"id"}, Columns: []string{"id", "big"}}
	edit, err := appliershared.ReplayWindowsReset([]ir.AddedTableReplayWindow{w})
	if err != nil {
		t.Fatal(err)
	}
	next, _ := edit.Apply(nil, false, time.Time{})
	entries := make([]string, 0, len(next))
	for e := range next {
		entries = append(entries, e)
	}
	back, err := appliershared.DecodeReplayWindows(appliershared.EncodeTableSet(entries))
	if err != nil || len(back) != 1 || !reflect.DeepEqual(back[0], w) {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
}
