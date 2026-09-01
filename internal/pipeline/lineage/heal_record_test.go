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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// countingCASStore is a memStore with the conditional-create capability
// and counters for both write verbs — the observable that distinguishes
// "the store arbitrated the claim" from "a probe guessed the path was
// free". (casMemStore next door carries fault-injection knobs instead;
// this one needs counts, and keeping them separate keeps each helper
// honest about what it is for.)
type countingCASStore struct {
	*memStore

	puts        int
	putIfAbsent int
}

func newConditionalMemStore() *countingCASStore {
	return &countingCASStore{memStore: newMemStore()}
}

func (s *countingCASStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.puts++
	return s.memStore.Put(ctx, path, r)
}

func (s *countingCASStore) PutIfAbsent(ctx context.Context, path string, r io.Reader) error {
	s.putIfAbsent++
	if _, taken := s.data[path]; taken {
		return fmt.Errorf("countingcas: %q: %w", path, irbackup.ErrPathExists)
	}
	// Deliberately NOT through this type's own Put — that would inflate
	// the plain-Put counter the assertions read.
	return s.memStore.Put(ctx, path, r)
}

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

// TestPreserveLineageSigForHeal_NeverOverwritesPriorEvidence is the audit
// 2026-08-31 C-3 pin: the "an overwrite must be impossible" promise is
// enforced by the STORE's create-only claim, not by a probe.
//
// The probe-then-Put it replaces was a TOCTOU across processes, and the
// window is far wider than "nanosecond timestamps collide rarely"
// suggests — the wall clock advances in ~505 µs ticks on Windows, so two
// heals starting within half a millisecond used to pick the same path,
// both see it free, and the second silently overwrote the first's
// evidence. The cell that matters is the SAME timestamp, which is why
// both calls here are handed one.
func TestPreserveLineageSigForHeal_NeverOverwritesPriorEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)

	t.Run("a conditional-put store arbitrates the collision", func(t *testing.T) {
		store := newConditionalMemStore()
		store.data[LineageSigFileName] = []byte("first signature")
		first, err := PreserveLineageSigForHeal(ctx, store, now)
		if err != nil {
			t.Fatalf("first preserve: %v", err)
		}
		// A second heal at the SAME wall-clock instant, over a different
		// signature: it must land somewhere else, and the first copy must
		// still hold the first bytes.
		store.data[LineageSigFileName] = []byte("second signature")
		second, err := PreserveLineageSigForHeal(ctx, store, now)
		if err != nil {
			t.Fatalf("second preserve at the same timestamp: %v", err)
		}
		if first == second {
			t.Fatalf("both heals preserved to %q — the second overwrote the first's evidence", first)
		}
		if got := string(store.data[first]); got != "first signature" {
			t.Errorf("preserved evidence at %q = %q; want %q — prior evidence must be immutable", first, got, "first signature")
		}
		if got := string(store.data[second]); got != "second signature" {
			t.Errorf("preserved evidence at %q = %q; want %q", second, got, "second signature")
		}
		// The claim must have gone through PutIfAbsent, not Put: that is
		// the whole point (a probe cannot arbitrate across processes).
		if store.puts != 0 {
			t.Errorf("PreserveLineageSigForHeal used %d plain Put(s); want 0 — the create-only claim IS the arbitration", store.puts)
		}
		if store.putIfAbsent < 3 {
			t.Errorf("PutIfAbsent calls = %d; want >=3 (two successes plus at least one refused collision)", store.putIfAbsent)
		}
	})

	t.Run("a store without the capability still degrades to probe-then-put", func(t *testing.T) {
		// memStore implements irbackup.Store only. Both stores sluice
		// ships have the capability, so this is the third-party path — it
		// keeps working, with the older (weaker) semantics.
		store := newMemStore()
		store.data[LineageSigFileName] = []byte("first signature")
		first, err := PreserveLineageSigForHeal(ctx, store, now)
		if err != nil {
			t.Fatalf("first preserve: %v", err)
		}
		store.data[LineageSigFileName] = []byte("second signature")
		second, err := PreserveLineageSigForHeal(ctx, store, now)
		if err != nil {
			t.Fatalf("second preserve: %v", err)
		}
		if first == second {
			t.Fatalf("the degradation path overwrote prior evidence at %q", first)
		}
	})
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
