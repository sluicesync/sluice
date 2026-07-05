// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// classifyRetriable inspects err and returns (matchedWrapper, true)
// when err carries an [ir.RetriableError] with `Retriable() == true`.
// Returns (nil, false) otherwise. The matched wrapper is exposed so
// callers can read `RetryHint()` without redoing `errors.As`.
//
// Bug 57 fix (v0.52.2) load-bearing helper: this MUST be checked
// BEFORE any `errors.Is(err, context.DeadlineExceeded)` /
// `context.Canceled` short-circuit. The applier classifier wraps
// `context.DeadlineExceeded` (from `--apply-exec-timeout`) as a
// retriable error; that wrapping preserves the inner error via
// `Unwrap`, so `errors.Is(wrappedErr, context.DeadlineExceeded)`
// returns true and pre-v0.52.2 streamer logic mistook the wrapped
// timeout for a clean shutdown signal — exiting the retry loop with
// zero retry attempts. The fix is to test the wrapper class first
// and only treat unwrapped ctx-termination as clean shutdown.
func classifyRetriable(err error) (ir.RetriableError, bool) {
	var re ir.RetriableError
	if errors.As(err, &re) && re.Retriable() {
		return re, true
	}
	return nil, false
}

// runWithRetry wraps [runOnce] with the ADR-0038 retry loop. Opens
// a side-channel applier to read the persisted CDC position between
// attempts so the consecutive-failure counter can reset whenever an
// attempt made forward progress (a successful batch committed before
// the failure that triggered the retry).
//
// First iteration: respects [Streamer.ResetTargetData] as the caller
// supplied it. Subsequent iterations always warm-resume — the v0.41.0
// pre-CDC anchor write guarantees a persisted position exists by the
// time any retriable apply error fires, so warm-resume is always
// possible. ResetTargetData is cleared after the first iteration so
// a transient applier failure during the retry path does not
// re-trigger the destructive reset.
//
// On clean shutdown, terminal error, ctx cancellation, or budget
// exhaustion, returns the appropriate error (or nil on clean
// shutdown). Budget exhaustion wraps the final transient with a
// "retry budget exhausted" prefix so the operator sees both the
// counter outcome and the underlying cause.
func (s *Streamer) runWithRetry(ctx context.Context, attempts int) error {
	base := s.ApplyRetryBackoffBase
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxBackoff := s.ApplyRetryBackoffCap
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	// Side-channel applier for between-attempt position reads. The
	// inner runOnce owns its own applier with a fresh open/close
	// per iteration; this one stays alive across the whole retry
	// loop so progress is observable.
	posReader, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		// GitHub #17 papercut: when the open failure is a non-
		// retriable startup error (parse error, bad DSN, unreachable
		// target), the retry-policy WARN is noise — the inner
		// runOnce is about to fail with the same error and exit.
		// We skip the WARN for those shapes; for genuinely-
		// transient open failures (network blip mid-startup), the
		// WARN still fires and the single-attempt fall-through is
		// the right behaviour.
		if !isTransientOpenError(err) {
			slog.DebugContext(
				ctx, "applier: retry policy disabled (cannot open position reader); falling through to single-attempt run",
				slog.String("err", err.Error()),
			)
		} else {
			slog.WarnContext(
				ctx, "applier: retry policy disabled (cannot open position reader); falling through to single-attempt run",
				slog.String("err", err.Error()),
			)
		}
		return s.runOnceWithReactiveResnapshot(ctx)
	}
	defer migcore.CloseIf(posReader)

	streamID := s.resolveStreamID()
	var consecutive int
	// ADR-0093: track whether a reactive invalid-position cold-start
	// re-snapshot has already fired this Run, so the recovery is bounded
	// to one (a second consecutive ErrPositionInvalid is terminal).
	resnapshotted := false

	for {
		beforePos, beforeFound, _ := posReader.ReadPosition(ctx, streamID)

		err := s.runOnceCall(ctx)
		if err == nil {
			return nil
		}
		// ADR-0093: a reactive [ir.ErrPositionInvalid] (e.g. a VStream
		// resume from a purged GTID position surfaced via the pump's Recv)
		// is NOT retriable — retrying the same purged position spins
		// forever. Route it to a one-shot cold-start re-snapshot instead.
		// Checked BEFORE classifyRetriable: the position is invalid, not
		// transient, so it must never enter the ADR-0038 backoff loop.
		if s.isReactiveInvalidPosition(err) {
			retry, rerr := s.reactiveResnapshotDecision(ctx, err, resnapshotted)
			if !retry {
				return rerr
			}
			resnapshotted = true
			continue
		}
		// Bug 57 fix (v0.52.2): check the retriable wrapper BEFORE the
		// ctx-Cancel/DeadlineExceeded short-circuit. A wrapped
		// [ir.RetriableError] containing context.DeadlineExceeded (from
		// the applier's `--apply-exec-timeout` watchdog) traverses to
		// DeadlineExceeded via errors.Is's Unwrap walk. Pre-v0.52.2 the
		// check fired on that match and exited the streamer with zero
		// retry — the v0.52.0/v0.52.1 silent-stall fix was inert
		// because the timeout-driven retry never reached the retry loop.
		// The bare-ctx-termination case (operator Ctrl-C, sync stop
		// applyCtx cancel) still needs the early return below; it just
		// has to come AFTER the retriable check now.
		re, retriable := classifyRetriable(err)
		if !retriable {
			// Includes bare context.Canceled / context.DeadlineExceeded
			// (genuine ctx termination) and any non-retriable failure.
			// Returning err preserves the pre-v0.52.2 behaviour for
			// these shapes (callers branch on errors.Is themselves).
			return err
		}

		// Clear ResetTargetData after the first iteration so a
		// transient applier failure during retry does not trigger
		// another destructive reset of dest tables. The reset
		// happens at most once per Run.
		s.ResetTargetData = false

		afterPos, afterFound, _ := posReader.ReadPosition(ctx, streamID)
		progressed := beforeFound && afterFound && afterPos.Token != beforePos.Token

		// ADR-0109 §B — bounded auto-restart of the cold-start after a
		// classified source-read drop DURING the cold-copy phase (the
		// sync-path BACKSTOP, where per-table reconnect (C) is impossible
		// because the snapshot pins one consistent point). The discriminator
		// is afterFound: no cdc-state row exists yet ⇒ the run never reached
		// the post-copy CDC anchor write (coldStartBeginCDC), so the
		// retriable error fired in the cold-copy phase. A naive re-run would
		// take the plain `default:` cold-start branch with forceFresh=false
		// and either dead-end on the populated-target refusal or dup-key
		// (1062) on the partial prior copy (native MySQL, plain INSERT). Set
		// RestartFromScratch so the re-run forces a clean re-establishment:
		// coldStartGatePreflight then makes it land cleanly PER SOURCE — a
		// non-idempotent reader (native MySQL binlog) drops + recreates the
		// in-scope target tables first, an idempotent reader (VStream/PG)
		// re-copies with UPSERT and absorbs the overlap (the v0.99.73
		// forceFresh path). It is bounded + backed-off by the SAME ADR-0038
		// budget below (never an infinite loop) and loud (the retry WARN +
		// the budget-exhaustion terminal both fire). The cdc-state row is
		// untouched (only --reset-target-data clears it).
		//
		// Once a cdc-state row exists (afterFound — the run reached CDC), a
		// retriable error is an apply/CDC-phase transient: warm-resume from
		// the persisted position is correct (and a re-copy would be a wasteful
		// full re-snapshot), so RestartFromScratch is cleared, exactly as the
		// pre-ADR-0109 retry did. A non-retriable cold-start error (the
		// populated-target refusal from a GENUINE operator mistake, a decode
		// fault) never reaches here — classifyRetriable returned false above
		// and the loop already returned it terminal, preserving the
		// v0.99.92-clean terminal behaviour.
		if afterFound {
			// CDC phase: warm-resume from the durable position on the next
			// attempt; do not force a re-copy.
			s.RestartFromScratch = false
		} else {
			// Cold-copy phase: force a clean re-establishment of the copy.
			s.RestartFromScratch = true
		}
		if progressed {
			consecutive = 1
		} else {
			consecutive++
		}

		if consecutive >= attempts {
			return fmt.Errorf("pipeline: apply retry budget exhausted after %d consecutive failures at position %q: %w",
				consecutive, afterPos.Token, err)
		}

		backoff := computeRetryBackoff(consecutive, base, maxBackoff, re.RetryHint())
		slog.InfoContext(
			ctx, "applier: transient error; retrying",
			slog.String("stream_id", streamID),
			slog.Int("attempt", consecutive),
			slog.Int("max_attempts", attempts),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// isTransientOpenError reports whether an applier-open error looks
// like a transient (network blip, brief DNS failure) vs a permanent
// startup failure (DSN parse error, bad credentials, unreachable
// hostname). Permanent failures don't benefit from the retry-policy
// WARN — the inner runOnce will surface the same error and exit;
// the WARN just makes the operator's first stderr line confusing
// (GitHub #17 papercut).
//
// Conservative classification: anything that looks like a parse or
// configuration error is permanent. Network-shape strings are
// transient. Unknown shapes default to transient so the existing
// behaviour (WARN + fall-through) is preserved.
func isTransientOpenError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid DSN"),
		strings.Contains(msg, "DSN must include"),
		strings.Contains(msg, "parseDSN"),
		strings.Contains(msg, "Access denied"),
		strings.Contains(msg, "Unknown database"),
		strings.Contains(msg, "Authentication failed"):
		return false
	}
	return true
}

// computeRetryBackoff returns the per-attempt backoff per ADR-0038:
// exponential doubling from base, capped at max. When the engine's
// classifier provides a non-zero RetryHint, the hint overrides only
// when larger than the computed value (so engines cannot make retries
// fire sooner than the policy's exponential schedule). The hint
// itself is still capped at max so a buggy engine returning an
// unreasonable hint can't unbound the wait.
func computeRetryBackoff(attempt int, base, maxBackoff, hint time.Duration) time.Duration {
	b := base
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > maxBackoff {
			b = maxBackoff
			break
		}
	}
	if hint > b {
		b = hint
	}
	if b > maxBackoff {
		b = maxBackoff
	}
	return b
}
