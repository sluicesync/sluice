// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Perf gate for the smart compactor's per-key removal (audit P-2).
//
// [smartCompactor.pkChangeBarrier] removes TWO keys from the insertion-order
// structure per PK-changing UPDATE. When that structure was a []string, each
// removal was an O(n) front-to-back scan + slice splice, so a segment of ~n
// such UPDATEs against ~n live accumulators (near the 32 MiB retention cap)
// paid O(n) per event → O(n²) total, and `backup compact` on a rekey-dense
// chain went from seconds to tens of minutes of CPU. Correctness was never
// affected; only the wall clock. The fix caches each accumulator's list node so
// removal is O(1).
//
// # Why this is a REFERENCE-ratio gate, not a raw N-vs-2N wall-time ratio
//
// The obvious "time N, time 2N, assert <3×" gate was tried and REJECTED: the
// workload's per-event allocation drags GC into the measurement, so the raw
// wall time is too noisy for a clean 2×-vs-4× read (a 2N run was observed
// finishing FASTER than a lucky 4× and slower than an unlucky 2×). Measured old
// vs new absolute times were unambiguous (160k churn: 37.5s old, 0.49s new),
// but the *ratio of two noisy absolute times* was not a reliable gate.
//
// So the gate instead compares the rekey-dense workload against a REFERENCE
// workload with the SAME event count and allocation profile whose UPDATEs are
// in-place (Before PK == After PK), which routes normally and never calls
// flushKeyEmitting — the linear baseline. Both are timed on the same machine in
// the same process, so machine speed and allocation cost cancel and what is
// left is the removal's super-linear overhead. Observed:
//
//	           rekey/ref ratio
//	           n=20000   n=40000
//	  O(1) fix   ~0.8      ~0.8      (rekey is even faster — it frees chains,
//	  O(n) old   ~9.2     ~24.7       the reference retains them; and the ratio
//	                                  GROWS with n, the O(n) removal signature)
//
// The gate fails at ratio > 4: the fix clears it with ~5× margin, and the old
// []string scan trips it with ~6× margin at n=40000 (and worse as n grows).
//
// # The independent expected value (2026-08-01 rule)
//
// The baseline this compares against is the reference workload's OWN wall time —
// an independent measurement of the same event stream minus the removal scan —
// never a number derived from the code under test. Mutation check: revert
// [smartCompactor.flushKeyEmitting]/[smartCompactor.evict] to the linear
// []string scan and this test fails with a ratio near 25.

package backup

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// discardSink drops every event. Used by the perf workload so the sink's own
// retention cannot skew the timing (the default sliceSink would grow O(events)
// and drag GC into the measurement).
type discardSink struct{ n int64 }

func (d *discardSink) emit(ir.Change) error {
	d.n++
	return nil
}

// runChurnWorkload streams `filler` untouched INSERTs to build a long insertion
// order, then `churn` INSERT+UPDATE pairs, and returns the wall time of that
// second phase. When rekey is true the UPDATE changes the PK — the P-2 path,
// which removes the row's accumulator from the order via
// [smartCompactor.pkChangeBarrier] → [smartCompactor.flushKeyEmitting]. When
// false it is an in-place UPDATE with the SAME event count and allocation
// profile that never removes anything: the linear reference.
//
// The retention cap is disabled so the order grows to `filler` and stays there,
// isolating the per-key removal cost from the eviction machinery (which has its
// own already-bounded sort and is not what P-2 is about).
func runChurnWorkload(tb testing.TB, filler, churn int, rekey bool) time.Duration {
	tb.Helper()
	c := newSmartCompactor(PKStrategyPK, pkChangeSchema())
	c.maxRetainedBytes = 0 // no eviction; keep every filler chain live
	c.sink = &discardSink{}

	for i := 0; i < filler; i++ {
		if err := c.process(ir.Insert{Position: pkPos("f"), Table: "t", Row: ir.Row{"id": int64(i), "body": "x"}}); err != nil {
			tb.Fatalf("filler insert %d: %v", i, err)
		}
	}

	start := time.Now()
	for i := 0; i < churn; i++ {
		oldID := int64(filler + i)
		if err := c.process(ir.Insert{Position: pkPos("c"), Table: "t", Row: ir.Row{"id": oldID, "body": "y"}}); err != nil {
			tb.Fatalf("churn insert %d: %v", i, err)
		}
		newID := oldID
		if rekey {
			// A disjoint id range so the UPDATE is a genuine PK change (the
			// accumulator under oldID is at the tail of the order and gets
			// removed) and the new key never resurrects a filler.
			newID = int64(filler + churn + i)
		}
		if err := c.process(ir.Update{
			Position: pkPos("c"), Table: "t",
			Before: ir.Row{"id": oldID},
			After:  ir.Row{"id": newID, "body": "z"},
		}); err != nil {
			tb.Fatalf("churn update %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	if _, err := c.finalize(); err != nil {
		tb.Fatalf("finalize: %v", err)
	}
	return elapsed
}

// bestOfChurn runs the workload `rounds` times and returns the minimum wall
// time, which filters transient upward spikes (GC, scheduler) better than a
// mean.
func bestOfChurn(tb testing.TB, rounds, filler, churn int, rekey bool) time.Duration {
	tb.Helper()
	best := time.Duration(1<<62 - 1)
	for r := 0; r < rounds; r++ {
		if d := runChurnWorkload(tb, filler, churn, rekey); d < best {
			best = d
		}
	}
	return best
}

// TestSmartCompact_PKChurnRemovalIsNotSuperLinear is the audit-P-2 gate: the
// per-key removal must not add super-linear overhead. It times a rekey-dense
// workload (which removes an accumulator per UPDATE) against an equal-event
// in-place-UPDATE reference (which removes nothing), and fails if the removal
// path makes the run more than 4× the reference. See the file header for why a
// reference ratio is used rather than a raw N-vs-2N wall-time ratio, and for the
// observed O(1)-vs-O(n) numbers.
func TestSmartCompact_PKChurnRemovalIsNotSuperLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive; skipped under -short")
	}
	const (
		n      = 40000
		rounds = 3
	)
	rekey := bestOfChurn(t, rounds, n, n, true)
	ref := bestOfChurn(t, rounds, n, n, false)

	ratio := float64(rekey) / float64(ref)
	t.Logf("PK-churn removal: rekey(n=%d)=%v vs in-place reference=%v, ratio=%.2f (O(1)≈0.8, O(n) scan≈25)",
		n, rekey, ref, ratio)

	// A near-zero reference would make the ratio meaningless; require a floor of
	// real work so the comparison is against a measurable baseline.
	if ref < 1*time.Millisecond {
		t.Fatalf("reference workload ran in %v — too fast to measure a ratio reliably; raise n", ref)
	}
	if ratio > 4.0 {
		t.Fatalf("rekey-dense churn took %.2f× the equal-event in-place reference (want <=4×).\n\n"+
			"The only difference between the two is that every rekey UPDATE removes an accumulator "+
			"from the insertion order. A ratio this high is the O(n) linear-scan+splice removal "+
			"(audit P-2) reasserting itself; removal must be O(1) via the accumulator's cached list "+
			"element.", ratio)
	}
}

// BenchmarkSmartCompact_PKChurn drives the rekey-dense workload for manual
// before/after numbers. Run e.g.:
//
//	go test ./internal/pipeline/backup/ -run=^$ -bench=PKChurn -benchtime=1x
//
// at a couple of sizes (adjust churnSize) to see the linear-vs-quadratic shape;
// the committed fix is linear (measured ~26ms at 10k rising to ~485ms at 160k,
// vs the old []string scan's ~100ms rising to ~37.5s).
func BenchmarkSmartCompact_PKChurn(b *testing.B) {
	const churnSize = 40000
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runChurnWorkload(b, churnSize, churnSize, true)
	}
}
