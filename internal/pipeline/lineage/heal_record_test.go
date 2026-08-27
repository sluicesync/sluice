// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the A3 heal-log LOW tails (audit backlog 2026-08-27): the
// no-trailing-newline append belt and the ReadHealRecords line-cap
// posture. The heal FLOW pins (evidence preservation, no-op boundaries,
// verify surfacing) live in internal/pipeline/backup's
// chain_maintenance_heal_test.go; these two are properties of the
// record codec itself, so they pin here beside it.

package lineage

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func healRec(op string) HealRecord {
	return HealRecord{
		HealedAt:      time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Operation:     op,
		KeyID:         "key-1",
		VerifyFailure: "stale signature",
		PreservedSig:  PreHealLineageSigPrefix + "1",
	}
}

// TestAppendHealRecord_NonNewlineTerminatedPriorBody pins the
// A3-APPEND-NEWLINE-GUARD belt: a prior log body whose trailing newline
// is missing (unreachable under AppendHealRecord's own writes; a store
// anomaly or hand edit) must not have the next record glued onto its
// last line — the guard inserts the separator and both records read
// back intact.
func TestAppendHealRecord_NonNewlineTerminatedPriorBody(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	// First record, then strip its trailing newline in place.
	if err := AppendHealRecord(ctx, store, healRec("backup compact")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	body := store.data[MaintenanceHealLogFileName]
	if len(body) == 0 || body[len(body)-1] != '\n' {
		t.Fatalf("fixture assumption broken: append did not end in a newline")
	}
	store.data[MaintenanceHealLogFileName] = bytes.TrimRight(body, "\n")

	if err := AppendHealRecord(ctx, store, healRec("prune")); err != nil {
		t.Fatalf("second append onto newline-less body: %v", err)
	}
	recs, err := ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("read after guarded append: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d record(s); want 2 — a missing separator glues the records into one unreadable line", len(recs))
	}
	if recs[0].Operation != "backup compact" || recs[1].Operation != "prune" {
		t.Errorf("records corrupted by the append: %+v", recs)
	}
}

// TestReadHealRecords_LongLines pins the A3-SCANNER-CAP posture, both
// directions: a record whose VerifyFailure overflows bufio.Scanner's
// 64 KiB DEFAULT line cap still reads (pre-fix the log was unreadable
// forever), and a line past the raised 1 MiB ceiling refuses LOUDLY
// rather than being skipped — the log is evidence.
func TestReadHealRecords_LongLines(t *testing.T) {
	ctx := context.Background()

	t.Run("a 100 KiB VerifyFailure reads back", func(t *testing.T) {
		store := newMemStore()
		rec := healRec("backup compact")
		rec.VerifyFailure = strings.Repeat("x", 100*1024)
		if err := AppendHealRecord(ctx, store, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
		recs, err := ReadHealRecords(ctx, store)
		if err != nil {
			t.Fatalf("a >64KiB record must still read (the pre-fix unreadable-forever shape): %v", err)
		}
		if len(recs) != 1 || len(recs[0].VerifyFailure) != 100*1024 {
			t.Fatalf("long record did not round-trip: got %d record(s)", len(recs))
		}
	})

	t.Run("a line past 1 MiB refuses loudly", func(t *testing.T) {
		store := newMemStore()
		rec := healRec("prune")
		rec.VerifyFailure = strings.Repeat("y", healLogMaxLineBytes+1024)
		if err := AppendHealRecord(ctx, store, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := ReadHealRecords(ctx, store); err == nil {
			t.Fatal("a line past the 1 MiB ceiling must refuse loudly, never be silently skipped")
		}
	})
}
