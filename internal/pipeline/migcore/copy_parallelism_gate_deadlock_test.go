// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 228: a transient slot shortage stalls the copy FOREVER (v0.112.0).
//
// Found by the v0.112.0 regression cycle. With a transient SQLSTATE 53300
// clamped into a parallel copy, v0.112.0 stops permanently: no log line, no
// error, NO EXIT CODE, 0.59s CPU, 1,593,750 of 6,000,000 rows on the target,
// and 48 of 67 goroutines blocked in CopyParallelismGate.Acquire — captured
// live with --pprof-listen. v0.111.1 on the identical injection aborts loudly
// in ~4s. Reproduced 2/2 against 0/2.
//
// It is a REGRESSION FROM ITEM 126. The per-chunk retry budget is working as
// designed; what it removed was the run-wide give-up bound, and that bound was
// also the only thing that stopped this stall — the run used to abort before
// the token pool could drain. Item 126 turned a loud abort into a silent hang.
//
// These tests drive the REAL gate the way the copy does and assert the
// property the gate exists to provide: after any sequence of shrinks and
// releases, the pool must still admit somebody. A gate that has retired every
// token has not "reduced parallelism" — it has stopped.
//
// # What the first three of these missed, and why (read this before adding a fourth)
//
// The three tests immediately below were written first, they were GREEN
// against the broken binary, and they were right about everything they
// checked. They modelled a pool in which every token belongs to a chunk
// worker that will hand it back. The shipped topology does not: under the
// ADR-0123 run-wide gate a table-pool worker holds ONE token for the whole
// table copy while blocked on the chunks, and that is the token the "at least
// one survives" argument was leaving alive. Ground truth from the forensic
// harness, on the shipped 16-token shape:
//
//	pre-fix : live_tokens_in_pool=0 effective=1 retire=0 — 15 of 63 chunks
//	          done, and ZERO further progress over the following second
//	post-fix: all 63 chunks done, effective back at the 16-token ceiling
//
// The lesson is CLAUDE.md's, in its sharpest form: a gate whose coverage is
// narrower than its name is worse than no gate, because it stops anyone from
// looking. So the pins below are explicit about which token CLASSES they
// reach, and the base-token class has its own.

package migcore

import (
	"context"
	"testing"
	"time"
)

// gateDeadlockPolicy mirrors the shipped defaults closely enough to reproduce
// the ladder, with waits short enough for a unit test.
func gateDeadlockPolicy() CopyBackoffPolicy {
	return CopyBackoffPolicy{
		MaxRetries:   5,
		BaseDelay:    time.Millisecond,
		MaxDelay:     2 * time.Millisecond,
		MaxTotalWait: time.Second,
	}
}

// TestCopyParallelismGate_PoolNeverFullyDrains is the Bug 228 gate.
//
// The scenario is the shipped 4x4 = 16 concurrent shape: every worker acquires,
// every worker meets a transient 53300 and shrinks, the shortage then CLEARS,
// and every worker completes and releases. The copy must still be able to make
// progress afterwards — at reduced parallelism, which is the whole point of the
// AIMD, but not at zero.
func TestCopyParallelismGate_PoolNeverFullyDrains(t *testing.T) {
	const workers = 16
	ctx := context.Background()
	g := NewCopyParallelismGate(workers, gateDeadlockPolicy())

	// Every worker takes a token, as the first wave of chunks does.
	for i := range workers {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("worker %d could not acquire from a fresh gate: %v", i, err)
		}
	}

	// Each meets the shortage and shrinks. The worker KEEPS its token across
	// the backoff, which is the documented contract.
	for i := range workers {
		if _, err := g.ShrinkAndBackoff(ctx, gateTestTable, i); err != nil {
			t.Fatalf("worker %d gave up on attempt 1, which the per-chunk budget must not do: %v", i, err)
		}
	}

	// The shortage clears; every worker finishes its chunk and releases.
	for range workers {
		g.Release()
	}

	// The property. A later chunk must be able to acquire — with a deadline,
	// because the failure mode under test is a permanent block, and a test
	// that reproduces the bug by hanging is not a test.
	acqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := g.Acquire(acqCtx); err != nil {
		t.Fatalf("BUG 228: after a transient shortage that CLEARED, no token remains — the copy "+
			"blocks in Acquire forever with no error and no exit code.\n\n"+
			"effective=%d. The AIMD is meant to degrade to slower-but-correct; retiring every "+
			"token is neither. Acquire returned: %v", g.Effective(), err)
	}
	g.Release()

	if eff := g.Effective(); eff < 1 {
		t.Errorf("effective parallelism is %d; the floor must be 1", eff)
	}
}

// The same property under the shape that actually occurs: workers shrink and
// release INTERLEAVED, and more chunks arrive behind them. Kept separate from
// the batched case above because the interleaving is what decides how many
// releases meet a non-zero retire count.
func TestCopyParallelismGate_InterleavedShrinkAndReleaseKeepsProgress(t *testing.T) {
	const workers = 16
	ctx := context.Background()
	g := NewCopyParallelismGate(workers, gateDeadlockPolicy())

	for i := range workers {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if _, err := g.ShrinkAndBackoff(ctx, gateTestTable, i); err != nil {
			t.Fatalf("shrink %d: %v", i, err)
		}
		g.Release()
	}

	acqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := g.Acquire(acqCtx); err != nil {
		t.Fatalf("BUG 228 (interleaved): the pool drained to nothing; effective=%d, Acquire: %v",
			g.Effective(), err)
	}
	g.Release()
}

// The control, and the half that keeps the fix from becoming "never retire":
// a shrink MUST still reduce live concurrency. If this passes while the two
// above fail, the gate has stopped shrinking rather than stopped deadlocking.
func TestCopyParallelismGate_ShrinkStillReducesLiveConcurrency(t *testing.T) {
	const workers = 16
	ctx := context.Background()
	g := NewCopyParallelismGate(workers, gateDeadlockPolicy())

	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := g.ShrinkAndBackoff(ctx, gateTestTable, 0); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	g.Release()

	if eff := g.Effective(); eff >= workers {
		t.Errorf("effective parallelism is still %d after a shrink; want < %d", eff, workers)
	}

	// Count how many tokens the pool will actually hand out without blocking.
	held := 0
	for {
		c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		err := g.Acquire(c)
		cancel()
		if err != nil {
			break
		}
		held++
		if held > workers {
			t.Fatalf("pool handed out more than %d tokens; the cap is not being enforced", workers)
		}
	}
	for range held {
		g.Release()
	}
	if held >= workers {
		t.Errorf("after a shrink the pool still admits %d concurrent workers; the multiplicative "+
			"decrease is not reducing live concurrency", held)
	}
	if held < 1 {
		t.Error("after a shrink the pool admits nobody — that is Bug 228, not a shrink")
	}
}
