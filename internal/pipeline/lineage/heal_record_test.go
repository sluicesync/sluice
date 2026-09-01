// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the heal artifacts' own codec: the no-trailing-newline append
// belt and the size posture (audit backlog 2026-08-27), the pre-heal
// evidence copy's create-only claim (2026-08-31 C-3), and the per-line
// read posture that stops one junk byte hiding every record (SEC-7). The
// heal FLOW pins (evidence preservation end to end, no-op boundaries,
// verify surfacing) live in internal/pipeline/backup's
// chain_maintenance_heal_test.go; these are properties of the record
// codec itself, so they pin here beside it.

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
	recs, defects, err := ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("read after guarded append: %v", err)
	}
	if len(defects) != 0 {
		t.Errorf("defects = %v; want none — the separator guard must keep both lines decodable", defects)
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

// TestReadHealRecords_LongLines pins the A3-SCANNER-CAP successor. The
// old 1 MiB per-LINE ceiling was an artifact of bufio.Scanner; the whole-
// body read has no per-line limit, so an arbitrarily long VerifyFailure
// now reads back instead of stopping the scan. The loud boundary moved to
// the whole-file cap, and reaching it is a DEFECT (the tail was not read),
// never a silent truncation.
func TestReadHealRecords_LongLines(t *testing.T) {
	ctx := context.Background()

	t.Run("a 100 KiB VerifyFailure reads back", func(t *testing.T) {
		store := newMemStore()
		rec := healRec("backup compact")
		rec.VerifyFailure = strings.Repeat("x", 100*1024)
		if err := AppendHealRecord(ctx, store, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
		recs, defects, err := ReadHealRecords(ctx, store)
		if err != nil {
			t.Fatalf("a >64KiB record must still read (the pre-fix unreadable-forever shape): %v", err)
		}
		if len(recs) != 1 || len(recs[0].VerifyFailure) != 100*1024 {
			t.Fatalf("long record did not round-trip: got %d record(s)", len(recs))
		}
		if len(defects) != 0 {
			t.Errorf("defects = %v; want none", defects)
		}
	})

	t.Run("a 2 MiB line reads back too — the per-line ceiling is gone", func(t *testing.T) {
		store := newMemStore()
		rec := healRec("prune")
		rec.VerifyFailure = strings.Repeat("y", 2<<20)
		if err := AppendHealRecord(ctx, store, rec); err != nil {
			t.Fatalf("append: %v", err)
		}
		recs, defects, err := ReadHealRecords(ctx, store)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(recs) != 1 || len(recs[0].VerifyFailure) != 2<<20 {
			t.Fatalf("a line past the old 1 MiB ceiling did not round-trip: got %d record(s)", len(recs))
		}
		if len(defects) != 0 {
			t.Errorf("defects = %v; want none", defects)
		}
	})

	t.Run("a body past the whole-file cap reports a defect, never a silent truncation", func(t *testing.T) {
		store := newMemStore()
		if err := AppendHealRecord(ctx, store, healRec("backup compact")); err != nil {
			t.Fatalf("append: %v", err)
		}
		// Pad past the cap. The first record still parses; the read must
		// SAY that everything past the cap went unread.
		store.data[MaintenanceHealLogFileName] = append(
			store.data[MaintenanceHealLogFileName],
			bytes.Repeat([]byte("z"), healLogMaxBytes)...,
		)
		recs, defects, err := ReadHealRecords(ctx, store)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(recs) != 1 {
			t.Errorf("records = %d; want the 1 that parsed before the cap", len(recs))
		}
		if len(defects) == 0 {
			t.Fatal("a body past the read cap must be reported — a silent truncation is exactly the evidence loss this file exists to prevent")
		}
		if !strings.Contains(defects[0].Reason, "read cap") {
			t.Errorf("first defect = %+v; want it to name the read cap", defects[0])
		}
	})
}

// TestReadHealRecords_OneJunkByteDoesNotHideTheOtherRecords is the audit
// 2026-08-31 SEC-7 pin. Under the old refuse-the-whole-file posture an
// adversary with store WRITE access but no signing key — strictly weaker
// than one who can trigger a heal — appended a single non-JSON byte and
// every prior heal record became invisible, behind a WARN, at exit 0.
// Records now parse independently and the unreadable line is counted and
// named.
func TestReadHealRecords_OneJunkByteDoesNotHideTheOtherRecords(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	for _, op := range []string{"backup compact", "prune"} {
		if err := AppendHealRecord(ctx, store, healRec(op)); err != nil {
			t.Fatalf("append %s: %v", op, err)
		}
	}
	// The one-byte lever: a junk line appended to a well-formed log.
	store.data[MaintenanceHealLogFileName] = append(
		store.data[MaintenanceHealLogFileName], []byte("\xff\n")...,
	)

	recs, defects, err := ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("malformed content must never be an error return (that is reserved for store failures): %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("records = %d; want 2 — one junk byte must not hide the heal records that DID parse", len(recs))
	}
	if recs[0].Operation != "backup compact" || recs[1].Operation != "prune" {
		t.Errorf("records out of order or corrupted: %+v", recs)
	}
	// Loud, counted, and located — not a silent skip.
	if len(defects) != 1 {
		t.Fatalf("defects = %v; want exactly 1 (the junk line), reported not swallowed", defects)
	}
	if defects[0].Line != 3 {
		t.Errorf("defect line = %d; want 3 — the defect must locate the bad line for an operator", defects[0].Line)
	}
	if defects[0].Reason == "" {
		t.Error("defect carries no reason")
	}

	// A torn TAIL (no trailing newline) is the same class and must behave
	// the same way: the completed records survive, the partial one is
	// named.
	store.data[MaintenanceHealLogFileName] = append(
		bytes.TrimSuffix(store.data[MaintenanceHealLogFileName], []byte("\xff\n")),
		[]byte(`{"healed_at":"2026-08-31T0`)...,
	)
	recs, defects, err = ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("torn tail: %v", err)
	}
	if len(recs) != 2 || len(defects) != 1 {
		t.Fatalf("torn tail: records=%d defects=%v; want 2 records and 1 defect", len(recs), defects)
	}
}
