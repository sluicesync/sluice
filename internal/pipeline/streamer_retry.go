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

// isIdleProgressTimeout reports whether err carries an
// [ir.LivenessProgressTimeoutError] whose IsIdleProgressTimeout() verdict is
// true — the VStream Phase-2 "established, then went idle" progress timeout
// (loose end 2b). Such a retry re-attached and PROVED a serving tablet
// (Phase 2 only arms after the initial attach cleared Phase 1), then observed
// silence, so it is NOT a consecutive failure and must not advance the
// give-up budget.
//
// INVARIANT (loud-failure tenet): a stream that NEVER established — a Phase-1
// liveness timeout ([mysql.vstreamLivenessTimeoutError]) or an open /
// connection error — does NOT satisfy this surface, so it still counts toward
// the budget and fails loudly. Only the established-then-idle case is exempt.
func isIdleProgressTimeout(err error) bool {
	var pe ir.LivenessProgressTimeoutError
	return errors.As(err, &pe) && pe.IsIdleProgressTimeout()
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
	// Bug 202 — the in-memory progress ledger. The `progressed` reset below
	// requires BOTH boundary position reads to succeed, and the read that
	// matters most fails in exactly the outage class the ride-out arc exists
	// for: when the TARGET (which is also the position store) is down at the
	// next failure, the after-read fails, `progressed` stays false, and the
	// counter CARRIED OVER a previous ridden-out outage's attempts — a stream
	// that survived one outage and then applied hours of CDC exited "apply
	// retry budget exhausted" on the SECOND outage's first or second failure.
	// The ledger applies the D0-4 discipline (a FAILED read is never evidence
	// in either direction) to the budget: lastFailureToken records the
	// persisted token best-known as of the previous counted failure, and any
	// SUCCESSFUL anchor-bearing read in the current iteration — the boundary
	// reads or the mid-attempt sentinel's ([observeApplyProgress]) — showing a
	// DIFFERENT token proves the applier durably committed batches since that
	// failure (position writes ride the batch tx, ADR-0007), so the failure
	// starts a fresh budget. With no successful read there is no evidence and
	// the counter keeps accumulating, so a never-reachable target still
	// exhausts the budget in exactly `attempts`, and a stuck batch's frozen
	// token never credits — the loud-failure floor is untouched.
	var lastFailureToken string
	var lastFailureTokenValid bool
	// ADR-0093: track whether a reactive invalid-position cold-start
	// re-snapshot has already fired this Run, so the recovery is bounded
	// to one (a second consecutive ErrPositionInvalid is terminal).
	resnapshotted := false
	// D0-4 (audit 2026-07-23): whether any SUCCESSFUL position read this
	// Run found the cdc-state anchor row. A FAILED read (target down —
	// both engines return found=false on error) proves nothing about the
	// anchor's existence, so it must never drive the RestartFromScratch
	// discriminator below; this flag lets a read failure default to the
	// warm resume the anchor's observed existence justifies.
	anchorSeen := false

	for {
		beforePos, beforeFound, beforeReadErr := posReader.ReadPosition(ctx, streamID)
		if beforeReadErr == nil && beforeFound {
			anchorSeen = true
		}

		// Run the attempt with the committed-progress sentinel alongside it
		// (Bug 202): a bounded-cadence position poll that records the latest
		// SUCCESSFUL observation while the attempt is flowing — the only
		// window the ledger can see progress in when the target is down again
		// by the time the boundary after-read runs. The sentinel is joined
		// (cancel + receive) before any further posReader use, so the side-
		// channel applier is never used concurrently.
		attemptCtx, stopSentinel := context.WithCancel(ctx)
		sentinelObs := make(chan applyProgressObservation, 1)
		go observeApplyProgress(attemptCtx, posReader, streamID, s.applyProgressPollIntervalOrDefault(), sentinelObs)
		err := s.runOnceCall(ctx)
		stopSentinel()
		midObs := <-sentinelObs
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
			// Connect-phase fall-through (the 2026-07-22 scale-soak
			// incident): a retry attempt has to RE-ESTABLISH its
			// connections, and a transient network failure there carries
			// no engine [ir.RetriableError] — pre-fix it returned terminal
			// right here, so a blip that outlived one attempt killed the
			// stream even though the NEXT attempt's warm resume would have
			// succeeded. A [connectPhaseError]-marked failure with a
			// positively-matched transient shape falls through to the SAME
			// budget + backoff below (re stays nil → no engine hint); every
			// other shape returns terminal exactly as before.
			if !isRetriableConnectFailure(err) {
				// Includes bare context.Canceled / context.DeadlineExceeded
				// (genuine ctx termination) and any non-retriable failure.
				// Returning err preserves the pre-v0.52.2 behaviour for
				// these shapes (callers branch on errors.Is themselves).
				return err
			}
		}

		// Clear ResetTargetData after the first iteration so a
		// transient applier failure during retry does not trigger
		// another destructive reset of dest tables. The reset
		// happens at most once per Run.
		s.ResetTargetData = false

		afterPos, afterFound, afterReadErr := posReader.ReadPosition(ctx, streamID)
		if afterReadErr == nil && afterFound {
			anchorSeen = true
		}
		progressed := beforeReadErr == nil && afterReadErr == nil &&
			beforeFound && afterFound && afterPos.Token != beforePos.Token

		// Bug 202: fold this iteration's successful anchor-bearing reads into
		// one freshest observation (position tokens only ever advance, so the
		// latest read subsumes the earlier ones), then compare it against the
		// ledger baseline from the previous counted failure. This credits the
		// deferred evidence `progressed` structurally cannot see — progress
		// whose proving read happened BEFORE the outage that failed this
		// iteration's after-read — while a failed read contributes nothing.
		obsToken, obsValid := "", false
		if beforeReadErr == nil && beforeFound {
			obsToken, obsValid = beforePos.Token, true
		}
		if midObs.valid {
			obsToken, obsValid = midObs.token, true
		}
		if afterReadErr == nil && afterFound {
			obsToken, obsValid = afterPos.Token, true
		}
		committedSinceLastFailure := obsValid && lastFailureTokenValid && obsToken != lastFailureToken

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
		//
		// D0-4 (audit 2026-07-23): the discriminator may only read a
		// SUCCESSFUL position read. A FAILED read (target down during the
		// very outage that classified the apply error retriable) is "could
		// not read the row", NOT "no anchor row" — latching
		// RestartFromScratch on it converted a warm-resumable blip into a
		// destructive forced re-snapshot once the connect-phase retry
		// (v0.99.288) started surviving the outage window: dropped +
		// recreated tables on native MySQL, and on idempotent sources a
		// slot/snapshot recreated at NOW that never replays source DELETEs
		// committed before it (silent divergence). On a failed read the
		// latch keeps its prior value, except that an anchor OBSERVED by
		// any successful read this Run proves warm resume is correct.
		switch {
		case afterReadErr != nil:
			// Read FAILED: no evidence either way. Default to warm resume
			// when the anchor was ever observed this Run; otherwise leave
			// the latch untouched — the next attempt's successful read
			// re-runs this discriminator with real evidence.
			if anchorSeen {
				s.RestartFromScratch = false
			}
		case afterFound:
			// CDC phase: warm-resume from the durable position on the next
			// attempt; do not force a re-copy.
			s.RestartFromScratch = false
		default:
			// Cold-copy phase (PROVEN by a successful read finding no
			// cdc-state row): force a clean re-establishment of the copy.
			s.RestartFromScratch = true
		}
		// Loose end 2b: a VStream Phase-2 "established then went idle"
		// progress timeout (mysql.vstreamProgressTimeoutError, carrying
		// ir.LivenessProgressTimeoutError) is NOT a consecutive failure — the
		// stream re-attached and PROVED a serving tablet (Phase 2 only arms
		// AFTER the initial attach VGTID cleared Phase 1), then the source was
		// simply quiet. Counting it would GUARANTEE a genuinely idle-but-
		// healthy source gives up after ApplyRetryAttempts idle reconnects
		// (the ~6-min idle-PlanetScale give-up over the public vstream
		// endpoint, where no idle heartbeats reach us): each idle reconnect
		// warm-resumes fine but processes zero events, so the position never
		// advances and the progressed-reset never fires. So leave the budget
		// UNTOUCHED for this shape.
		//
		// INVARIANT (loud-failure tenet): ONLY the established-then-idle
		// Phase-2 timeout is exempt. A Phase-1 liveness timeout
		// (mysql.vstreamLivenessTimeoutError — the stream NEVER established:
		// no serving tablet / primary-only wedge) and any connection/open
		// error carry NO marker, so they take the progressed/default branch
		// and STILL exhaust the budget — a stream that can never connect fails
		// loudly, never loops forever. A real failure interspersed with benign
		// idle timeouts also takes the default branch, so it still accumulates
		// toward the cap.
		switch {
		case isIdleProgressTimeout(err):
			// Non-incrementing: leave consecutive as-is (do NOT reset — a
			// real failure between idles must keep its accumulated count).
		case progressed || committedSinceLastFailure:
			// Direct evidence (both boundary reads OK, token advanced across
			// the attempt) or the Bug 202 ledger's deferred evidence (a
			// successful read this iteration shows the token advanced since
			// the previous counted failure). Either proves durable commits
			// happened since the last failure — fresh budget.
			consecutive = 1
		default:
			consecutive++
		}
		// Advance the ledger baseline to the freshest observation on EVERY
		// iteration through this counter — idle-timeout iterations included,
		// so an idle heartbeat's token advance is consumed here and can never
		// falsely credit a later real failure. A read-less iteration (outage)
		// keeps the prior baseline: that carried knowledge is exactly what
		// lets the next successful observation prove cross-outage progress.
		if obsValid {
			lastFailureToken, lastFailureTokenValid = obsToken, true
		}

		if consecutive >= attempts {
			return fmt.Errorf("pipeline: apply retry budget exhausted after %d consecutive failures at position %q: %w",
				consecutive, afterPos.Token, err)
		}

		// re is nil on the connect-transient fall-through (no engine
		// classifier, hence no hint) — the policy's exponential schedule
		// alone owns that backoff.
		var hint time.Duration
		if re != nil {
			hint = re.RetryHint()
		}
		backoff := computeRetryBackoff(consecutive, base, maxBackoff, hint)
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

// defaultApplyProgressPollInterval is the committed-progress sentinel's
// production poll cadence (Bug 202). It matches stopSignalPollInterval — the
// same single-row control-table SELECT the stop poller already issues on the
// same target at the same rate, so the sentinel's cost profile is a known
// quantity. Fast enough that any meaningful healthy stretch between two
// outages yields at least one observation; a stretch shorter than one poll
// AND unluckily bracketed by failed boundary reads simply leaves the counter
// accumulating — a bounded-pessimism residual (loud exit + clean warm
// resume), never silent.
const defaultApplyProgressPollInterval = 5 * time.Second

// applyProgressPollIntervalOrDefault resolves the sentinel cadence:
// the unexported test override when set, else the production default
// (zero-value-safe — every production construction gets the default).
func (s *Streamer) applyProgressPollIntervalOrDefault() time.Duration {
	if s.applyProgressPollInterval > 0 {
		return s.applyProgressPollInterval
	}
	return defaultApplyProgressPollInterval
}

// applyProgressObservation is the committed-progress sentinel's result: the
// latest persisted position token a SUCCESSFUL anchor-bearing read observed
// during the attempt. The zero value means "no successful observation" —
// which the ledger treats as no evidence, never as regression (D0-4).
type applyProgressObservation struct {
	token string
	valid bool
}

// observeApplyProgress is the Bug 202 committed-progress sentinel: while an
// attempt is flowing it polls the persisted position on the side-channel
// applier at a bounded cadence, recording the latest SUCCESSFUL found-row
// observation, and delivers it on out exactly once when ctx is cancelled.
// The caller joins it (cancel + receive) before touching posReader again, so
// the applier is never used from two goroutines at once. Failed reads and
// found=false reads (cold-copy phase — no anchor row yet) record nothing;
// the sentinel never logs, since mid-outage its reads fail every tick and
// that is the expected quiet case.
func observeApplyProgress(ctx context.Context, posReader ir.ChangeApplier, streamID string, interval time.Duration, out chan<- applyProgressObservation) {
	var obs applyProgressObservation
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			out <- obs
			return
		case <-t.C:
		}
		if pos, found, err := posReader.ReadPosition(ctx, streamID); err == nil && found {
			obs = applyProgressObservation{token: pos.Token, valid: true}
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
