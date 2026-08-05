// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared effective-parallelism gate for the connection-resilience
// Phase 2b adaptive backoff. The parallel bulk-copy pool spawns one
// goroutine per chunk; each goroutine acquires a token from this gate
// before opening its target writer / source reader connection. The gate
// caps how many chunk connections are concurrently *open*, and a
// slot-exhaustion event (SQLSTATE 53300) multiplicatively shrinks that
// cap for the rest of the copy.
//
// Concurrency model (the crux — this is a concurrency chunk, CI -race is
// the authoritative gate):
//
//   - tokens is a buffered channel used as a counting semaphore. Its
//     initial capacity is the post-preflight effective parallelism. A
//     worker takes a token before acquiring connections and returns it
//     when the chunk finishes; that bounds concurrently-open connections
//     to the current cap.
//   - "Shrinking" the cap on a 53300 is modelled by *permanently
//     retiring* tokens: under mu, the gate decrements its logical
//     capacity and remembers how many tokens to swallow. A retired token
//     is one the worker holds but does NOT return — so the live token
//     count drops without ever blocking inside the lock. The first
//     workers to finish after a shrink absorb the retirement.
//   - backoff bookkeeping (attempt count + accumulated wait) is guarded
//     by mu and kept PER CHUNK (roadmap item 126). The give-up bound is a
//     property of one chunk's own progress; the SHRINK stays run-wide, so
//     many concurrent 53300s still collapse parallelism hard. Keeping the
//     bound run-wide meant 16 concurrent workers consumed the whole
//     MaxRetries budget between them and aborted the run with zero retries
//     actually executed.
//
// All shared mutable state is touched only under mu or via the channel,
// so there are no data races on the cap or the backoff counters.

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
// applies the Phase 2b AIMD backoff. One gate is shared by all chunk
// goroutines copying a single table.
type CopyParallelismGate struct {
	tokens chan struct{}

	mu sync.Mutex
	// effective is the current logical cap (starts at the post-preflight
	// parallelism, only ever decreases). Guarded by mu.
	effective int
	// retire counts tokens that finishing workers must swallow rather
	// than return, draining the live token pool down to a shrunk cap
	// without blocking under the lock. Guarded by mu.
	retire int
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
	// The shrink stays RUN-WIDE deliberately (see [CopyParallelismGate.effective]):
	// many concurrent 53300s SHOULD collapse parallelism hard and fast.
	// It is only the give-up bound and the backoff exponent that are a
	// property of one chunk's own progress.
	//
	// Keyed by chunkIndex, which is unique within a gate: the gate is
	// constructed per table over that table's chunk set.
	attemptsByChunk map[int]int
	waitByChunk     map[int]time.Duration

	policy CopyBackoffPolicy
}

// NewCopyParallelismGate builds a gate seeded with `parallelism` tokens.
// parallelism is the post-preflight effective parallelism (>= 1).
func NewCopyParallelismGate(parallelism int, policy CopyBackoffPolicy) *CopyParallelismGate {
	if parallelism < 1 {
		parallelism = 1
	}
	tokens := make(chan struct{}, parallelism)
	for i := 0; i < parallelism; i++ {
		tokens <- struct{}{}
	}
	return &CopyParallelismGate{
		tokens:          tokens,
		effective:       parallelism,
		attemptsByChunk: make(map[int]int),
		waitByChunk:     make(map[int]time.Duration),
		policy:          policy,
	}
}

// Effective returns the gate's current logical cap — the effective
// parallelism after any slot-exhaustion shrinks. Thread-safe. Exposed
// for observability + the acquire-retry tests in pipeline-root, which
// assert the multiplicative-decrease shrank the cap; it takes mu so the
// read never races a concurrent ShrinkAndBackoff.
func (g *CopyParallelismGate) Effective() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.effective
}

// acquire takes one token, blocking until one is free or ctx is done.
// A worker holds the token for the duration of its chunk and returns it
// via release (unless the token was retired by a shrink).
func (g *CopyParallelismGate) Acquire(ctx context.Context) error {
	select {
	case <-g.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns a token to the pool — UNLESS a shrink retired it, in
// which case the token is swallowed so the live pool drains down to the
// shrunk cap. Called once per successful acquire, after the chunk's
// connections are closed.
func (g *CopyParallelismGate) Release() {
	g.mu.Lock()
	if g.retire > 0 {
		g.retire--
		g.mu.Unlock()
		return // swallow this token: the cap shrank
	}
	g.mu.Unlock()
	g.tokens <- struct{}{}
}

// shrinkAndBackoff records one slot-exhaustion event: it consults the
// pure AIMD decision, and on a non-give-up verdict halves the effective
// cap (retiring the difference in tokens) and returns the backoff delay
// the caller should wait before retrying its chunk. On a give-up verdict
// it returns a loud, bounded error wrapping ErrCopySlotsExhausted.
//
// The token the failing worker currently holds is NOT returned here —
// the caller keeps it across the wait+retry so the retry re-uses the
// same slot (one fewer connection in flight during the backoff), and
// returns it via release when the chunk ultimately finishes. The retire
// count therefore targets *other* live tokens.
func (g *CopyParallelismGate) ShrinkAndBackoff(ctx context.Context, chunkIndex int) (time.Duration, error) {
	g.mu.Lock()
	g.attemptsByChunk[chunkIndex]++
	attempt := g.attemptsByChunk[chunkIndex]
	current := g.effective
	prior := g.waitByChunk[chunkIndex]

	decision := NextCopyBackoff(current, attempt, prior, g.policy)
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
	next := decision.NextParallelism
	if next < prev {
		// Retire the difference: that many finishing workers will
		// swallow their tokens instead of returning them, draining the
		// live pool to the new cap. Bounded by prev-1 so at least one
		// token (this worker's held one) always survives to make
		// forward progress.
		g.retire += prev - next
		g.effective = next
	}
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
