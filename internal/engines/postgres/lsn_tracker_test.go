// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"sync"
	"testing"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLSNTracker_MonotonicAdvance verifies that ReportApplied only
// advances the stored LSN when the supplied value exceeds the
// current floor. Out-of-order reports (which shouldn't happen with
// the single-goroutine applier today, but are an invariant the CAS
// loop preserves under future concurrency) are no-ops.
func TestLSNTracker_MonotonicAdvance(t *testing.T) {
	tr := newLSNTracker()
	if got := tr.LoadApplied(); got != 0 {
		t.Errorf("initial applied = %v; want 0", got)
	}

	tr.ReportApplied(pglogrepl.LSN(100))
	if got := tr.LoadApplied(); got != pglogrepl.LSN(100) {
		t.Errorf("after first report, applied = %v; want 100", got)
	}

	tr.ReportApplied(pglogrepl.LSN(200))
	if got := tr.LoadApplied(); got != pglogrepl.LSN(200) {
		t.Errorf("after monotonic advance, applied = %v; want 200", got)
	}

	// Out-of-order report — must not regress.
	tr.ReportApplied(pglogrepl.LSN(150))
	if got := tr.LoadApplied(); got != pglogrepl.LSN(200) {
		t.Errorf("after stale report, applied = %v; want 200", got)
	}

	// Equal-value report — also a no-op.
	tr.ReportApplied(pglogrepl.LSN(200))
	if got := tr.LoadApplied(); got != pglogrepl.LSN(200) {
		t.Errorf("after equal report, applied = %v; want 200", got)
	}

	// Zero is a no-op (defensive: an empty position token shouldn't
	// reset the floor).
	tr.ReportApplied(0)
	if got := tr.LoadApplied(); got != pglogrepl.LSN(200) {
		t.Errorf("after zero report, applied = %v; want 200", got)
	}
}

// TestLSNTracker_ConcurrentSingleProducer verifies the CAS loop is
// safe under concurrent ReportApplied calls — single-producer is
// the realistic shape, but the tracker's correctness shouldn't
// depend on that. A stress loop with multiple writers and one
// reader pins the invariant.
func TestLSNTracker_ConcurrentSingleProducer(t *testing.T) {
	tr := newLSNTracker()
	const n = 1000

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 1; i <= n; i++ {
			tr.ReportApplied(pglogrepl.LSN(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 1; i <= n; i++ {
			tr.ReportApplied(pglogrepl.LSN(n - i + 1))
		}
	}()
	wg.Wait()

	if got := tr.LoadApplied(); got != pglogrepl.LSN(n) {
		t.Errorf("after concurrent reports, applied = %v; want %v", got, n)
	}
}

// TestAckLSN_AnchorsAtStartLSNUntilFirstApply is the regression
// guard for Bug 15 (CLI path). The pre-fix branch on `applied == 0`
// returned streamedLSN — which advances as the pump parses
// CommitMessages off the WAL stream, well before the applier has
// committed. On warm-resume, the tracker starts at 0 and the
// keepalive would ack confirmed_flush_lsn past the persisted
// position. A subsequent stop or crash mid-batch then permanently
// lost events between persisted_position and confirmed_flush_lsn.
//
// The fix anchors the ack at startLSN until the applier reports
// its first commit. Once applied > 0, the tracker takes over.
func TestAckLSN_AnchorsAtStartLSNUntilFirstApply(t *testing.T) {
	tr := newLSNTracker()
	r := &CDCReader{appliedLSN: tr}

	startLSN := pglogrepl.LSN(0x100)
	// Pump has parsed several commits past startLSN but the applier
	// hasn't committed any of them yet — applied is still 0.
	streamed := pglogrepl.LSN(0x500)

	got := r.ackLSN(streamed, startLSN)
	if got != startLSN {
		t.Errorf("with fresh tracker, ackLSN = %v; want startLSN=%v (got streamedLSN — Bug 15 regression)",
			got, startLSN)
	}

	// First applied report — tracker advances past startLSN but
	// stays behind streamed.
	applied1 := pglogrepl.LSN(0x300)
	tr.ReportApplied(applied1)
	got = r.ackLSN(streamed, startLSN)
	if got != applied1 {
		t.Errorf("after first applied=%v, ackLSN = %v; want applied", applied1, got)
	}

	// Subsequent applied report advances the tracker further.
	applied2 := pglogrepl.LSN(0x450)
	tr.ReportApplied(applied2)
	got = r.ackLSN(streamed, startLSN)
	if got != applied2 {
		t.Errorf("after applied=%v, ackLSN = %v; want applied", applied2, got)
	}
}

// TestAckLSN_NoTrackerReturnsStreamedLSN preserves the legacy
// behaviour for non-streamer callers (no tracker wired) — useful for
// snapshot-stream test paths and pre-v0.5.0 compatibility shims.
func TestAckLSN_NoTrackerReturnsStreamedLSN(t *testing.T) {
	r := &CDCReader{appliedLSN: nil}

	startLSN := pglogrepl.LSN(0x100)
	streamed := pglogrepl.LSN(0x500)

	got := r.ackLSN(streamed, startLSN)
	if got != streamed {
		t.Errorf("with nil tracker, ackLSN = %v; want streamedLSN=%v", got, streamed)
	}
}

// TestLSNFromPositionToken_RoundTrip verifies the helper extracts
// the LSN from a canonical pgPos token, returns 0 on the empty-
// token case, and propagates parse errors on malformed tokens.
func TestLSNFromPositionToken_RoundTrip(t *testing.T) {
	pos, err := encodePGPos(pgPos{Slot: "sluice_slot", LSN: "0/16B7350"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	lsn, err := lsnFromPositionToken(pos.Token)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	wantLSN, err := pglogrepl.ParseLSN("0/16B7350")
	if err != nil {
		t.Fatalf("expected lsn parse: %v", err)
	}
	if lsn != wantLSN {
		t.Errorf("lsn = %v; want %v", lsn, wantLSN)
	}
}

func TestLSNFromPositionToken_EmptyTokenIsZero(t *testing.T) {
	lsn, err := lsnFromPositionToken("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lsn != 0 {
		t.Errorf("lsn = %v; want 0", lsn)
	}
}

func TestLSNFromPositionToken_MalformedReturnsError(t *testing.T) {
	if _, err := lsnFromPositionToken("not json"); err == nil {
		t.Error("expected error for malformed token")
	}
}

// TestLSNFromPositionToken_NonPGArrayTokenIsSilentZero pins soak finding
// F2: a MySQL / VStream source writes its CDC position as a JSON array of
// per-shard GTIDs. Fed to the Postgres applier's applied-LSN tracker (a
// Postgres-source-only mechanism), such a token carries no LSN and must
// no-op silently — not surface a parse error the commit path then logs on
// every single apply. Leading whitespace before the '[' is tolerated.
func TestLSNFromPositionToken_NonPGArrayTokenIsSilentZero(t *testing.T) {
	for _, tok := range []string{
		`[{"keyspace":"commerce","shard":"-","gtid":"MySQL56/a1b2:1-100"}]`,
		`[]`,
		"  \n\t[{\"shard\":\"-80\"}]",
	} {
		lsn, err := lsnFromPositionToken(tok)
		if err != nil {
			t.Errorf("array token %q: unexpected error: %v", tok, err)
		}
		if lsn != 0 {
			t.Errorf("array token %q: lsn = %v; want 0", tok, lsn)
		}
	}
	// A malformed pgPos *object* token (not an array) still errors —
	// the guard is array-shape-specific, preserving genuine-corruption
	// diagnostics for a real Postgres source.
	if _, err := lsnFromPositionToken(`{"slot":"s","lsn":"nonsense"}`); err == nil {
		t.Error("expected error for malformed object token")
	}
}

// TestLSNFromPositionToken_PositionShape pins the helper to the
// canonical position shape so a future change to pgPos's wire
// format is caught here.
func TestLSNFromPositionToken_PositionShape(t *testing.T) {
	// Construct a position the same way encodePGPos does, then
	// pull the token out and feed it back in.
	encoded, err := encodePGPos(pgPos{Slot: "x", LSN: "1/2"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded.Engine != engineNamePostgres {
		t.Errorf("engine tag drifted: got %q; want %q", encoded.Engine, engineNamePostgres)
	}
	// And confirm an ir.Position with the right engine tag round-
	// trips through the helper.
	pos := ir.Position{Engine: engineNamePostgres, Token: encoded.Token}
	if _, err := lsnFromPositionToken(pos.Token); err != nil {
		t.Errorf("round-trip parse: %v", err)
	}
}
