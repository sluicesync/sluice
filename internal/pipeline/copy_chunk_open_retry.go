// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Cold-copy chunk connection-OPEN reconnect-retry (ADR-0146)
//
// The OPEN-side sibling of ADR-0108's target-WRITE reparent-retry and
// ADR-0109's source-READ reconnect-and-resume. Those two ride a transient
// that strikes AFTER a chunk's connections are open (mid-COPY / mid-write);
// this one closes the remaining hole: a transient connection drop at the
// moment a non-zero chunk OPENS its source reader + target writer.
//
// Before this, [acquireChunkConn] retried ONLY the connection-slot-
// exhaustion class (SQLSTATE 53300); every other open error — including a
// transient `ping: invalid connection` blip — hit the fail-fast branch and
// aborted the WHOLE migrate. A live 49 GB PG→PlanetScale run lost ~45 GB of
// copy progress to a single such blip:
//
//	pipeline: copy table "bench" (parallel): open connections for chunk 24:
//	    mysql: ping: invalid connection
//
// Retry / double-copy safety (the crux — same argument as the 53300 path):
// [openOneChunkConn] fails at reader-open OR writer-open, closes any partial
// (closeIf(rdr)) and returns (nil, nil, err) — i.e. the open fails BEFORE any
// COPY / WriteRows runs, exactly like a 53300. So reconnecting and re-running
// the chunk from its recorded chunk.LastPK cursor (WHERE (pk) > LastPK, the
// existing keyset resume in copy_source_read_retry.go / resumeFromChunkCursor)
// is provably DUP-FREE: no rows were written before the failed open, so the
// resume cannot double-copy. This file only makes the OPEN reconnect instead
// of aborting; the dup-free resume itself is already handled downstream.
//
// Classification is engine-neutral (the pipeline package MUST NOT import an
// engine package): [isRetriableChunkOpenError] honours an engine-classified
// [ir.RetriableError], the stdlib driver.ErrBadConn / io.EOF / net.Error
// timeout surfaces, and a conservative allow-list of driver/OS connection-
// drop text shapes. Unknown shapes stay FATAL (unlike streamer_retry.go's
// default-transient isTransientOpenError) so a real fault is never masked.

package pipeline

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Cold-copy chunk-open retry bounds. Zero-value-safe by construction: these
// are package vars (not config fields), so there is no EnableX-defaulting-true
// trap (the v0.99.51 lesson) — every construction path (CLI, tests, broker,
// future callers) gets the same bounds. They MIRROR ADR-0109's source-read
// reconnect envelope (coldCopySourceRead*): a wall-clock deadline as the real
// terminal bound, a high attempt cap as a runaway backstop, and an
// exponential backoff doubling from base to cap.
//
//	100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s →
//	25.6s → 30s (cap) → 30s → ...
//
// The wall-clock bound matches the read-side's 30 min because a chunk-open
// blip and a mid-read drop are two faces of the SAME class of event (the live
// incident struck during a target storage auto-grow); a run should ride the
// grow/failover identically whichever side the transient lands on. Long enough
// to ride it, short enough that a genuinely-down endpoint surfaces loudly
// rather than hiding for hours.
//
// These are vars (not consts) ONLY so the unit tests can shrink the envelope
// to keep the suite fast — production NEVER mutates them, so the
// zero-value-safe reasoning is unaffected.
var (
	chunkOpenRetryMaxWall     = 30 * time.Minute
	chunkOpenRetryAttempts    = 100000
	chunkOpenRetryBackoffBase = 100 * time.Millisecond
	chunkOpenRetryBackoffCap  = 30 * time.Second
)

// chunkOpenRetryBackoff returns the per-attempt backoff for the chunk-open
// retry loop: exponential doubling from chunkOpenRetryBackoffBase, capped at
// chunkOpenRetryBackoffCap. attempt is 1-based (attempt 1 is the wait BEFORE
// the second try). Mirrors coldCopySourceReadBackoff's shape (ADR-0109).
func chunkOpenRetryBackoff(attempt int) time.Duration {
	b := chunkOpenRetryBackoffBase
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > chunkOpenRetryBackoffCap {
			return chunkOpenRetryBackoffCap
		}
	}
	return b
}

// isRetriableChunkOpenError reports whether err (or anything it wraps) is the
// transient connection-drop class that the chunk-open retry should ride out.
//
// Order matters: PERMANENT faults are checked FIRST so a transient-looking
// substring can never mask a real auth / DSN / permission error (loud-failure
// wins). Then engine-classified transients (ir.RetriableError), then the
// engine-neutral stdlib driver surfaces, then a conservative allow-list of
// driver/OS connection-drop text shapes. Anything else — an unknown shape — is
// NOT retriable: it preserves today's fail-fast behaviour rather than
// defaulting to transient the way streamer_retry.go's isTransientOpenError
// does (that helper guards a stream-start we WANT to keep retrying; a chunk
// open we only retry for a clearly-a-connection-drop shape). nil ⇒ false.
func isRetriableChunkOpenError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())

	// Permanent faults: fail fast + loud. Checked before any transient test.
	for _, p := range []string{
		"access denied",
		"unknown database",
		"invalid dsn",
		"authentication failed",
		"parsedsn",
		"permission denied",
	} {
		if strings.Contains(msg, p) {
			return false
		}
	}

	// Engine-classified transient: honour the reader/writer's own verdict.
	var re ir.RetriableError
	if errors.As(err, &re) && re.Retriable() {
		return true
	}

	// Engine-neutral stdlib driver / EOF surfaces.
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
		return true
	}
	// A timed-out network op (covers the Windows wsarecv WSAETIMEDOUT shape).
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}

	// Driver/OS plain-text connection-drop shapes (case-insensitive on the
	// already-lowercased message).
	for _, s := range []string{
		"invalid connection",
		"connection reset",
		"broken pipe",
		"did not properly respond",
		"connection refused",
		"bad connection",
		"unexpected eof",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}

	// Unknown shape: conservative — preserve today's fatal behaviour.
	return false
}

// openChunkConnWithRetry runs a non-zero chunk's connection-open with both
// the Phase 2b slot-exhaustion backoff (unchanged) AND the ADR-0146 transient
// connection-drop reconnect-retry layered around it. It is the pure retry core
// of [acquireChunkConn]: it manages NO gate token (the caller holds one for
// the chunk's whole lifetime) — it only classifies open errors and decides
// whether to retry, so it is directly unit-testable against a fake open func.
//
//   - open reconnects and re-attempts the chunk's reader+writer pair. On the
//     production path this is a closure over openOneChunkConn(ctx, deps);
//     tests pass a fake.
//   - isSlotExhausted is the engine-supplied 53300 predicate.
//   - slotBackoff is gate.ShrinkAndBackoff — it shrinks parallelism and
//     returns the delay to wait, or a loud give-up error at the AIMD bound.
//
// Error routing per open failure:
//   - slot-exhaustion  → shrink + back off + retry (EXACT existing behaviour).
//   - transient drop   → back off within a bounded wall-clock/attempt budget
//     and retry the open, KEEPING the caller's gate token (same chunk, same
//     budget slot). On budget exhaustion returns a LOUD terminal error wrapping
//     the most recent transient (never silent, never infinite).
//   - anything else    → fail fast + loud, the EXACT existing fatal message.
func openChunkConnWithRetry(
	ctx context.Context,
	chunkIndex int,
	tableName string,
	open func(context.Context) (ir.RowReader, ir.RowWriter, error),
	isSlotExhausted func(error) bool,
	slotBackoff func(context.Context, int) (time.Duration, error),
) (ir.RowReader, ir.RowWriter, error) {
	var (
		transientTry      int
		transientDeadline time.Time
		lastTransient     error
	)
	for {
		rdr, wr, err := open(ctx)
		if err == nil {
			return rdr, wr, nil
		}

		// Slot exhaustion (SQLSTATE 53300): shrink parallelism + back off, then
		// retry the open. Unchanged from the original acquireChunkConn loop.
		if isSlotExhausted(err) {
			delay, giveErr := slotBackoff(ctx, chunkIndex)
			if giveErr != nil {
				return nil, nil, giveErr
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			continue
		}

		// Transient connection drop at OPEN — before any COPY/WriteRows, so a
		// reconnect + resume from chunk.LastPK is provably dup-free (see the
		// file header's safety argument). Reconnect within a bounded budget
		// rather than aborting the whole migrate.
		if isRetriableChunkOpenError(err) {
			lastTransient = err
			if transientTry == 0 {
				transientDeadline = time.Now().Add(chunkOpenRetryMaxWall)
			}
			transientTry++
			if time.Now().After(transientDeadline) || transientTry >= chunkOpenRetryAttempts {
				return nil, nil, fmt.Errorf(
					"open connections for chunk %d of table %q: the connection kept dropping across the "+
						"reconnect window (%s wall-clock, %d attempts; the endpoint may be down, or a prolonged "+
						"target stall keeps refusing new connections): %w",
					chunkIndex, tableName, chunkOpenRetryMaxWall, transientTry, lastTransient,
				)
			}
			backoff := chunkOpenRetryBackoff(transientTry)
			slog.WarnContext(
				ctx, "pipeline: cold-copy hit a transient connection drop opening a chunk; reconnecting and retrying the open",
				slog.String("table", tableName),
				slog.Int("chunk", chunkIndex),
				slog.Int("attempt", transientTry),
				slog.Duration("backoff", backoff),
				slog.Duration("max_wall", chunkOpenRetryMaxWall),
				slog.String("err", err.Error()),
			)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			continue
		}

		// Non-retryable: surface loudly and immediately (EXACT existing path).
		return nil, nil, fmt.Errorf("open connections for chunk %d: %w", chunkIndex, err)
	}
}
