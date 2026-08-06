// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Bug 228 pins that copy_parallelism_gate_deadlock_test.go could not be.
//
// That file's three property tests were written first, were GREEN against the
// broken binary, and were right about everything they checked. They modelled a
// pool in which every token belongs to a chunk worker that will hand it back.
// The shipped topology does not: under the ADR-0123 run-wide gate a table-pool
// worker holds ONE token for the whole table copy while blocked in
// errgroup.Wait on that table's chunks, and that is the token the pre-fix
// "at least one always survives" argument was leaving alive.
//
// Ground truth from the Phase-A forensic harness, on the shipped 16-token
// shape (one base token + fifteen chunk tokens, 63 chunk workers):
//
//	pre-fix : live_tokens_in_pool=0 effective=1 retire=0 — 15 of 63 chunks
//	          done, then ZERO further progress over the following second
//	post-fix: all 63 chunks done, effective back at the 16-token ceiling
//
// So these pins name the token CLASSES they reach, and the base class has its
// own — CLAUDE.md's "a gate whose coverage is narrower than its name is worse
// than no gate", in its sharpest form.

package migcore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stormPolicy is proportional to the shipped DefaultCopyBackoffPolicy — the
// transient clears well inside a chunk's own retry budget, as the reproduction's
// 6s injection does inside the shipped 250ms→4s / 30s envelope — but scaled for
// a unit test. A policy whose storm outlives the budget measures the give-up
// path instead of the stall, which is a different and separately-pinned
// property (TestCopyParallelismGate_OneChunkStillGivesUp).
func stormPolicy() CopyBackoffPolicy {
	return CopyBackoffPolicy{
		MaxRetries:   6,
		BaseDelay:    20 * time.Millisecond,
		MaxDelay:     50 * time.Millisecond,
		MaxTotalWait: 30 * time.Second,
	}
}

// TestCopyParallelismGate_BaseHolderNeverOwnsTheLastSlot is THE Bug 228 pin.
//
// Shipped shape: --bulk-parallelism=16 --table-parallelism=1 ⇒ a 16-token
// budget shared by one long-lived base token and fifteen chunk tokens. A
// transient 53300 walks the ladder 16→8→4→2→1. The pre-fix gate retired
// fifteen tokens — every chunk token — and the sole survivor was the base
// token, held by a goroutine parked in errgroup.Wait on the very chunks that
// could no longer start. The Acquire below blocked forever: no error, no
// give-up (the give-up bound only runs on a slot-exhaustion a blocked worker
// can never reach), no exit code.
func TestCopyParallelismGate_BaseHolderNeverOwnsTheLastSlot(t *testing.T) {
	const budget = 16
	ctx := context.Background()
	g := NewCopyParallelismGate(budget, gateDeadlockPolicy())

	// The table-pool worker takes its base token and holds it until every
	// chunk below has finished (migrate_table_pool.go).
	if err := g.AcquireBase(ctx); err != nil {
		t.Fatalf("base acquire on a fresh gate: %v", err)
	}
	// The chunk workers that fit alongside it.
	for i := range budget - 1 {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("chunk worker %d could not acquire from a fresh gate: %v", i, err)
		}
	}

	// Every chunk worker meets the shortage and shrinks, keeping its token
	// across the backoff (the documented contract).
	for i := range budget - 1 {
		if _, err := g.ShrinkAndBackoff(ctx, gateTestTable, i); err != nil {
			t.Fatalf("chunk %d gave up on attempt 1, which the per-chunk budget must not do: %v", i, err)
		}
	}

	// The shortage CLEARS and every chunk worker finishes and releases. The
	// base holder does not — it is waiting for the chunks that come next.
	for range budget - 1 {
		g.Release()
	}

	acqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := g.Acquire(acqCtx); err != nil {
		t.Fatalf("BUG 228: with a base token held, a transient shortage that CLEARED left the gate "+
			"unable to admit any chunk — the copy blocks in Acquire forever, with no error and no exit "+
			"code.\n\neffective=%d. The surviving token belongs to the table-pool worker, which cannot "+
			"make progress until these chunks do. Acquire returned: %v", g.Effective(), err)
	}
	g.Release()
}

// TestCopyParallelismGate_RecoversAfterTransientClears is the OTHER half, and
// the half a "did it abort?" test cannot see: a shortage that has gone away
// must give the parallelism back.
//
// Without it Bug 228's hang becomes a many-hour crawl instead — every remaining
// chunk of a 6M-row table copied one at a time, because nothing in the gate
// could ever raise the cap again (the pre-fix `effective` field had exactly one
// writer, the decrease). "It completed eventually" is not the contract; "a
// transient slot shortage degrades to slower-but-correct" is, and
// slower-FOREVER is not that.
func TestCopyParallelismGate_RecoversAfterTransientClears(t *testing.T) {
	const budget = 16
	ctx := context.Background()
	g := NewCopyParallelismGate(budget, gateDeadlockPolicy())

	// A storm collapses the cap to the floor.
	for i := range 8 {
		if _, err := g.ShrinkAndBackoff(ctx, gateTestTable, i); err != nil {
			t.Fatalf("shrink %d: %v", i, err)
		}
	}
	if got := g.Effective(); got != 1 {
		t.Fatalf("effective after a full storm = %d, want the floor of 1", got)
	}

	// The transient clears: chunk after chunk now opens successfully. That —
	// and only that — is the evidence the additive increase runs on.
	for range budget {
		g.NoteOpenSucceeded()
	}
	if got := g.Effective(); got != budget {
		t.Fatalf("BUG 228 (recovery half): after the shortage cleared and %d chunks opened cleanly, "+
			"effective parallelism is %d, want the full %d.\n\nA copy that never recovers finishes a "+
			"large table one chunk at a time. The AIMD is named for an additive increase; this asserts "+
			"there is one.", budget, got, budget)
	}

	// …and never past the post-preflight budget, however long it runs clean.
	for range 100 {
		g.NoteOpenSucceeded()
	}
	if got := g.Effective(); got != budget {
		t.Errorf("effective grew to %d, past the %d-connection budget the preflight measured; the "+
			"increase must be capped at the ceiling", got, budget)
	}

	// The recovered cap is real, not just a number: it admits exactly that many.
	for i := range budget {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d under the recovered cap: %v", i, err)
		}
	}
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		blocked <- g.Acquire(c)
	}()
	if err := <-blocked; err == nil {
		t.Error("the recovered gate admitted more than the budget")
	}
}

// TestCopyParallelismGate_TransientStormUnderRealTopologyCompletes is the
// class-level gate: the whole ADR-0123 topology, run concurrently, against the
// injection from the Bug 228 reproduction (sluice-testing
// workspace/v112/f3b_hang.sh) — a shortage that arrives mid-copy and then GOES
// AWAY.
//
// It asserts both halves at once, which is the point: every chunk finishes (no
// deadlock) AND the parallelism comes back (no permanent crawl). Run under
// -race in CI; this is a concurrency chunk.
func TestCopyParallelismGate_TransientStormUnderRealTopologyCompletes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		budget       int
		baseTokens   int
		chunkWorkers int
	}{
		// The shipped default: --bulk-parallelism=16 --table-parallelism=1,
		// 64 chunks (chunk 0 rides the primaries and takes no token).
		{"shipped/tableP=1/budget=16", 16, 1, 63},
		// Cross-table: one table's shrink must not freeze its peers.
		{"cross-table/tableP=4/budget=32", 32, 4, 60},
		// The pre-ADR-0123 per-table gate — no long-lived holder at all.
		{"per-table-gate/no-base-token", 64, 0, 63},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			g := NewCopyParallelismGate(tc.budget, stormPolicy())

			var shortage atomic.Bool
			shortage.Store(true)

			for i := range tc.baseTokens {
				if err := g.AcquireBase(ctx); err != nil {
					t.Fatalf("base token %d: %v", i, err)
				}
			}

			var (
				completed atomic.Int64
				shrinks   atomic.Int64
				wg        sync.WaitGroup
			)
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()

			for k := 1; k <= tc.chunkWorkers; k++ {
				wg.Add(1)
				go func(chunk int) {
					defer wg.Done()
					if err := g.Acquire(runCtx); err != nil {
						return
					}
					defer g.Release()
					for {
						if !shortage.Load() {
							g.NoteOpenSucceeded() // the open succeeded
							completed.Add(1)
							return
						}
						delay, err := g.ShrinkAndBackoff(runCtx, gateTestTable, chunk)
						if err != nil {
							t.Errorf("chunk %d hit the give-up bound during a TRANSIENT the gate "+
								"exists to ride out: %v", chunk, err)
							return
						}
						shrinks.Add(1)
						select {
						case <-time.After(delay):
						case <-runCtx.Done():
							return
						}
					}
				}(k)
			}

			// Hold the shortage until the ladder has bottomed out — the real
			// run's last log line was "copy parallelism already at floor" —
			// then take it away, as the repro's `ALTER ROLE … CONNECTION
			// LIMIT -1` does.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				g.mu.Lock()
				bottomed := g.aimdTarget == 1
				g.mu.Unlock()
				if bottomed && shrinks.Load() > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if shrinks.Load() == 0 {
				t.Fatal("the storm never fired a single shrink — this test would be vacuous")
			}
			shortage.Store(false)

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				g.mu.Lock()
				eff, held, base := g.effective, g.held, g.baseHeld
				g.mu.Unlock()
				cancelRun()
				wg.Wait()
				t.Fatalf("BUG 228: the copy STALLED after a transient shortage cleared — %d of %d "+
					"chunks done, and no error, no give-up and no exit code would ever be produced.\n\n"+
					"gate: effective=%d held=%d baseHeld=%d (admissible slots=%d)",
					completed.Load(), tc.chunkWorkers, eff, held, base, eff-held)
			}

			if got := completed.Load(); got != int64(tc.chunkWorkers) {
				t.Errorf("completed %d of %d chunks", got, tc.chunkWorkers)
			}
			if got := g.Effective(); got != tc.budget {
				t.Errorf("after the shortage cleared and %d chunks opened cleanly, effective "+
					"parallelism is %d, want the full budget of %d.\n\nA transient that has CLEARED "+
					"must return the copy to full throughput; a test asserting only 'it did not abort' "+
					"would pass on a copy crawling at parallelism 1 for the rest of the run.",
					tc.chunkWorkers, got, tc.budget)
			}
		})
	}
}

// TestCopyParallelismGate_BaseHoldersCannotConsumeTheWholeBudget covers the
// degenerate shape found while ground-truthing the fix's own doc comment: a
// budget EQUAL to the number of long-lived base holders.
//
// It is reachable without any slot shortage. budget = tableParallelism ×
// withinParallelism, and a fresh run cannot reach the chunked path at
// withinParallelism <= 1 — but a --resume whose RECORDED chunk plan re-engages
// chunking skips that check (migrate_parallel.go's hasRecordedChunks branch).
// Such a run gets budget == tableParallelism, the table-pool workers take every
// token as a base token, and their chunk workers then wait on a pool that
// nothing will refill until those same tables finish. Pre-fix that is a
// deadlock with no 53300 anywhere in it; the base-aware floor is what makes it
// merely one connection over budget.
func TestCopyParallelismGate_BaseHoldersCannotConsumeTheWholeBudget(t *testing.T) {
	for _, budget := range []int{1, 2, 4} {
		ctx := context.Background()
		g := NewCopyParallelismGate(budget, gateDeadlockPolicy())

		// Every token goes to a long-lived base holder.
		for i := range budget {
			if err := g.AcquireBase(ctx); err != nil {
				t.Fatalf("budget=%d: base acquire %d: %v", budget, i, err)
			}
		}

		// A chunk worker must STILL be admissible: the base holders cannot
		// finish until it does.
		acqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := g.Acquire(acqCtx)
		cancel()
		if err != nil {
			t.Errorf("budget=%d: every token is held by a table-pool worker waiting on its chunks, "+
				"and no chunk can start — a deadlock reachable on --resume with a recorded chunk plan, "+
				"with no slot shortage involved. Acquire: %v", budget, err)
			continue
		}
		g.Release()
	}
}
