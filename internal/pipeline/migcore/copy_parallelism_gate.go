// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared effective-parallelism gate for the connection-resilience
// Phase 2b adaptive backoff. The parallel bulk-copy pool spawns one
// goroutine per chunk; each goroutine acquires a token from this gate
// before opening its target writer / source reader connection. The gate
// caps how many chunk connections are concurrently *open*; a
// slot-exhaustion event (SQLSTATE 53300) multiplicatively shrinks that
// cap and a proven-good open additively grows it back.
//
// # Two token CLASSES, and why the distinction is load-bearing (Bug 228)
//
// Under ADR-0123 ONE gate is shared by the whole run, and two very
// different kinds of worker draw from it:
//
//   - CHUNK tokens (Acquire/Release) are short-lived. A chunk worker takes
//     one, opens its connections, copies its PK range, and gives the token
//     back. It can always finish on its own.
//   - BASE tokens (AcquireBase/ReleaseBase) are long-lived. A table-pool
//     worker takes one for the table's PRIMARY connection and holds it for
//     the WHOLE table copy — which means it is holding the token while
//     blocked in errgroup.Wait on that table's chunk workers. It can NEVER
//     finish until chunk tokens are available.
//
// Bug 228 is what happens when the shrink forgets that distinction. The
// pre-fix gate modelled the shrink by "retiring" tokens — decrement the cap,
// remember how many future Releases must swallow their token instead of
// returning it — and argued it was safe because the retirement telescopes to
// at most initial-1, "so at least one token always survives to make forward
// progress". The arithmetic was right and the conclusion was wrong: the
// surviving token was the BASE token, held by the goroutine waiting on the
// chunks that could no longer start. Measured on the shipped
// --bulk-parallelism=16 --table-parallelism=1 shape (budget 16 = 1 base +
// 15 chunk tokens), a transient 53300 walked the ladder 16->8->4->2->1,
// retired all 15 chunk tokens, and left the pool with ZERO live tokens and
// one base token pinned in Wait. A 6M-row migrate stopped dead with no log
// line, no error and no exit code. See TestCopyParallelismGate_* for the
// executable form.
//
// So the floor is not "1 token". It is "one token that can actually make
// progress":
//
//	effective == max(aimdTarget, baseHeld+1)
//
// Flooring at baseHeld+1 is not a fudge — it is the honest number. A base
// connection is ALREADY OPEN and a shrink cannot close it, so the target is
// carrying those connections whatever the gate says; the only thing the
// shrink can actually govern is how many MORE connections get opened, and
// that is what the +1 preserves. With the floor in place the copy degrades
// to one chunk at a time, which is what "a transient slot shortage degrades
// to slower-but-correct" always claimed.
//
// # Concurrency model
//
// The gate is a RESIZABLE counting semaphore, not a fixed-capacity channel.
// State (held, baseHeld, aimdTarget, effective) lives under mu; a waiter
// parks on a broadcast channel that Release/AcquireBase/NoteOpenSucceeded
// close-and-replace whenever a slot may have opened. Admission is exactly
// `held < effective`, evaluated under mu, so:
//
//   - a shrink takes effect IMMEDIATELY for new acquires rather than lazily
//     as workers happen to finish (the retire mechanism's behaviour), and
//   - there is no token-count arithmetic that can underflow the pool. The
//     old "retire" counter is gone entirely; Bug 228's whole class of
//     accounting bug cannot be expressed in this model.
//
// A shrink never revokes a token already held, so `held > effective` is a
// legal transient state after a shrink; it simply admits nobody new until
// enough workers release. All shared mutable state is touched only under mu.

package migcore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ErrCopySlotsExhausted is the sentinel the gate returns when the target
// stayed slot-exhausted past the AIMD give-up bound. Wrapped with the
// concrete numbers; tests assert on it via errors.Is without coupling to
// the message text.
var ErrCopySlotsExhausted = errors.New("pipeline: target connection slots stayed exhausted during parallel copy")

// CopyParallelismGate caps concurrently-open chunk connections and
// applies the Phase 2b AIMD backoff. One gate is shared by the whole run
// (ADR-0123); see the file header for the two token classes.
type CopyParallelismGate struct {
	mu sync.Mutex

	// ceiling is the post-preflight budget — the most connections this run
	// may ever hold open, and the ceiling the additive increase recovers
	// TO. Never changes after construction.
	ceiling int
	// aimdTarget is the AIMD's own view of the cap: halved by a 53300
	// (multiplicative decrease), incremented by a proven-good open
	// (additive increase), clamped to [1, ceiling].
	aimdTarget int
	// effective is the cap actually enforced: max(aimdTarget, baseHeld+1).
	// DERIVED — recomputeEffectiveLocked is its only writer.
	effective int
	// held is every token currently checked out, of both classes;
	// baseHeld is the long-lived subset (see the file header).
	held     int
	baseHeld int

	// notify is closed and replaced whenever a slot may have opened, so
	// blocked acquirers re-evaluate. A fresh channel per generation, so a
	// waiter that captured the old one is always woken exactly once.
	notify chan struct{}

	// attemptsByChunk / waitByChunk track the retry budget PER CHUNK, and
	// that is the whole of roadmap item 126.
	//
	// They used to be two run-wide scalars, so the budget was consumed by
	// however many workers happened to hit 53300 at all rather than by how
	// many times any one chunk had retried. At the shipped 4x4 = 16
	// concurrent, sixteen workers meeting a slot shortage at nearly the
	// same moment burned the whole MaxRetries budget between them: the
	// seventh aborted the entire run while workers 1-6 were still parked
	// in their FIRST backoff, having executed zero retries. The gate's
	// stated purpose — "a transient slot shortage degrades to
	// slower-but-correct" — did not hold at any parallelism >= 7, which is
	// every default multi-core run.
	//
	// The DECREASE stays RUN-WIDE deliberately: many concurrent 53300s
	// SHOULD collapse parallelism hard and fast. It is only the give-up
	// bound and the backoff exponent that are a property of one chunk's
	// own progress.
	//
	// KNOWN LIMITATION, stated rather than implied: the key is the chunk
	// index alone, and under the ADR-0123 run-wide gate that index is NOT
	// unique across tables — table A's chunk 3 and table B's chunk 3 share
	// a budget. The effect is a give-up that can fire earlier than the
	// per-chunk bound advertises. It is bounded and LOUD (an early abort,
	// never a stall), which is why it is documented here rather than
	// bundled into the Bug 228 fix; keying by (table, chunk) is the fix.
	attemptsByChunk map[int]int
	waitByChunk     map[int]time.Duration

	policy CopyBackoffPolicy
}

// NewCopyParallelismGate builds a gate with a budget of `parallelism`
// concurrently-open connections. parallelism is the post-preflight
// effective parallelism (>= 1) and becomes the ceiling the additive
// increase can recover to — never exceed.
func NewCopyParallelismGate(parallelism int, policy CopyBackoffPolicy) *CopyParallelismGate {
	if parallelism < 1 {
		parallelism = 1
	}
	g := &CopyParallelismGate{
		ceiling:         parallelism,
		aimdTarget:      parallelism,
		notify:          make(chan struct{}),
		attemptsByChunk: make(map[int]int),
		waitByChunk:     make(map[int]time.Duration),
		policy:          policy,
	}
	g.recomputeEffectiveLocked()
	return g
}

// Effective returns the cap currently enforced — the post-shrink,
// post-recovery parallelism, floored so a long-lived base holder can
// never own every slot. Thread-safe.
func (g *CopyParallelismGate) Effective() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.effective
}

// Acquire takes one CHUNK token, blocking until a slot is free or ctx is
// done. A worker holds it for the duration of its chunk and returns it via
// [CopyParallelismGate.Release].
func (g *CopyParallelismGate) Acquire(ctx context.Context) error {
	return g.acquire(ctx, false)
}

// AcquireBase takes one BASE token — the long-lived kind a table-pool
// worker holds for a whole table copy while blocked on that table's chunks.
// Declaring the class is what lets the shrink floor keep a chunk slot open;
// see the file header (Bug 228). Released via
// [CopyParallelismGate.ReleaseBase].
func (g *CopyParallelismGate) AcquireBase(ctx context.Context) error {
	return g.acquire(ctx, true)
}

// Release returns a CHUNK token. Called once per successful Acquire.
func (g *CopyParallelismGate) Release() { g.release(false) }

// ReleaseBase returns a BASE token. Called once per successful AcquireBase.
func (g *CopyParallelismGate) ReleaseBase() { g.release(true) }

func (g *CopyParallelismGate) acquire(ctx context.Context, base bool) error {
	// Honour an already-cancelled ctx before admitting, so an errgroup
	// unwinding reports the cancellation rather than opening one more
	// connection it is about to throw away.
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		g.mu.Lock()
		if g.held < g.effective {
			g.held++
			if base {
				g.baseHeld++
				// A new base holder raises the floor, which may open a
				// chunk slot; wake the waiters so they re-evaluate.
				g.recomputeEffectiveLocked()
				g.wakeLocked()
			}
			g.mu.Unlock()
			return nil
		}
		wait := g.notify
		g.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *CopyParallelismGate) release(base bool) {
	g.mu.Lock()
	if g.held > 0 {
		g.held--
	}
	if base && g.baseHeld > 0 {
		g.baseHeld--
	}
	g.recomputeEffectiveLocked()
	g.wakeLocked()
	g.mu.Unlock()
}

// NoteOpenSucceeded is the ADDITIVE INCREASE: the caller proved the target
// seated a brand-new connection, so one unit of the parallelism a shrink
// took away is given back (never past the ceiling).
//
// Recovery is gated on EVIDENCE, which is what keeps a re-probe from
// re-triggering the storm the decrease just quelled: the increase only
// fires on an open that actually succeeded, so a still-saturated target —
// whose opens are still failing with 53300 — grows the cap zero times,
// while any concurrent 53300 halves it. Multiplicative decrease dominates
// additive increase by construction, which is the whole point of AIMD; a
// probe that guesses wrong costs one chunk one backoff, bounded by the
// per-chunk budget.
//
// Without this, Bug 228's other half stands: a transient that CLEARED
// leaves the copy pinned at parallelism 1 for the rest of a multi-hour
// table, because nothing else in the gate can ever raise the cap.
func (g *CopyParallelismGate) NoteOpenSucceeded() {
	g.mu.Lock()
	if g.aimdTarget < g.ceiling {
		g.aimdTarget++
		g.recomputeEffectiveLocked()
		g.wakeLocked()
	}
	g.mu.Unlock()
}

// recomputeEffectiveLocked derives the enforced cap from the AIMD target
// and the live base-token count.
//
// The floor is deliberately NOT clamped to the ceiling, and that is a real
// decision rather than a defensive one: liveness outranks the budget. It
// matters in one reachable case. `budget = tableParallelism × withinParallelism`,
// and a fresh run cannot reach the chunked path at withinParallelism <= 1
// (shouldParallelChunk refuses it), so baseHeld <= ceiling/2 there. But a
// --resume whose RECORDED chunk plan re-engages chunking bypasses that check
// (migrate_parallel.go's hasRecordedChunks branch), so a resumed run with
// withinParallelism 1 gets budget == tableParallelism — a budget the base
// holders alone consume. Clamping the floor to the ceiling there would
// reproduce Bug 228's deadlock exactly, with no 53300 involved at all.
// Spending at most one connection over the measured budget is the cheaper
// error. Pinned by
// TestCopyParallelismGate_BaseHoldersCannotConsumeTheWholeBudget.
func (g *CopyParallelismGate) recomputeEffectiveLocked() {
	e := g.aimdTarget
	if floor := g.baseHeld + 1; e < floor {
		e = floor
	}
	g.effective = e
}

// wakeLocked releases every parked acquirer to re-evaluate admission.
// Broadcast rather than hand-off: the number of newly-admissible slots
// depends on state the waiter must re-read under mu anyway.
func (g *CopyParallelismGate) wakeLocked() {
	close(g.notify)
	g.notify = make(chan struct{})
}

// ShrinkAndBackoff records one slot-exhaustion event: it consults the
// pure AIMD decision, and on a non-give-up verdict halves the effective
// cap and returns the backoff delay the caller should wait before
// retrying its chunk. On a give-up verdict it returns a loud, bounded
// error wrapping ErrCopySlotsExhausted.
//
// The token the failing worker currently holds is NOT returned here — the
// caller keeps it across the wait+retry so the retry re-uses the same slot
// (one fewer connection in flight during the backoff), and returns it via
// Release when the chunk ultimately finishes. A shrink never revokes a
// held token; it only stops NEW admissions until enough workers release.
func (g *CopyParallelismGate) ShrinkAndBackoff(ctx context.Context, chunkIndex int) (time.Duration, error) {
	g.mu.Lock()
	g.attemptsByChunk[chunkIndex]++
	attempt := g.attemptsByChunk[chunkIndex]
	prior := g.waitByChunk[chunkIndex]

	decision := NextCopyBackoff(g.aimdTarget, attempt, prior, g.policy)
	if decision.GiveUp {
		giveUpEffective := g.effective
		g.mu.Unlock()
		return 0, WrapWithHint(PhaseConnect, fmt.Errorf(
			"%w: still SQLSTATE 53300 after %d backoff(s) (parallelism reduced to %d, ~%s waited); "+
				"free connections (close idle / orphaned sessions — see --reap-stale-backends), "+
				"raise max_connections, or lower --bulk-parallelism / --max-target-connections",
			ErrCopySlotsExhausted, attempt-1, giveUpEffective, prior.Round(time.Millisecond),
		))
	}

	prev := g.effective
	if decision.NextParallelism < g.aimdTarget {
		g.aimdTarget = decision.NextParallelism
		g.recomputeEffectiveLocked()
	}
	next := g.effective
	g.waitByChunk[chunkIndex] += decision.Delay
	g.mu.Unlock()

	if prev != next {
		slog.InfoContext(ctx, "reducing copy parallelism after too_many_connections; retrying chunk",
			slog.Int("from", prev),
			slog.Int("to", next),
			slog.Int("chunk", chunkIndex),
			slog.Duration("backoff", decision.Delay),
			slog.Int("attempt", attempt))
	} else {
		slog.InfoContext(ctx, "copy parallelism already at floor; backing off and retrying chunk after too_many_connections",
			slog.Int("parallelism", next),
			slog.Int("chunk", chunkIndex),
			slog.Duration("backoff", decision.Delay),
			slog.Int("attempt", attempt))
	}
	return decision.Delay, nil
}
