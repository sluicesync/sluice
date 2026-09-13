// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"time"
)

// RetriableError is the optional surface an applier error can
// implement to signal that the pipeline's retry policy (ADR-0038)
// should attempt to recover from this error rather than exit the
// stream. Each engine's applier wraps its raw driver errors in a
// classifier that returns a value satisfying this interface for
// the documented transient shapes (Vitess tx-killer / vttablet
// transients on MySQL; SQLSTATE 40001 / 40P01 / 08* / 57P0x on
// Postgres) and the original error verbatim for non-retriable shapes.
//
// The pipeline classifies via errors.As — non-implementing errors
// behave as non-retriable, preserving pre-v0.42.0 fail-on-first
// behaviour for engines that haven't been classified.
//
// Implementations should embed the underlying error (Unwrap) so
// errors.Is / errors.As still find the original cause and any
// existing error-handling chain (e.g. wrapWithHint) continues to
// operate on the wrapped value.
type RetriableError interface {
	error

	// Retriable reports whether the operator-side retry policy
	// should attempt to recover from this error. Implementations
	// typically return true unconditionally — the classifier that
	// produced the wrapper already decided. The interface form
	// keeps the contract explicit at every consumption site.
	Retriable() bool

	// RetryHint optionally returns a minimum-backoff floor that
	// overrides the policy's computed exponential value. Zero
	// means "use the default policy floor"; non-zero is honoured
	// only when larger than the policy's computed backoff so an
	// engine can never make retries fire sooner than the cap
	// allows. No engine emits a non-zero hint today; the field
	// is forward-looking for vttablet ResourceExhausted errors
	// that sometimes carry a retry-after signal.
	RetryHint() time.Duration
}

// TransactionKilledError is the optional surface a [RetriableError]
// can additionally implement to tell the AIMD apply-batch-size
// controller (ADR-0052) that the failure was a server-side
// transaction-killer abort — a Vitess vttablet `code = Aborted ...
// for tx killer rollback` (MySQL Error 1105), or any future engine
// whose server enforces a wall-clock transaction killer
// ([Capabilities.TransactionKiller]).
//
// Such an abort is unambiguous evidence that the batch was too large
// to commit within the target's transaction-timeout window: the whole
// transaction was rolled back server-side. Unlike a generic transient
// (a deadlock victim, a connection blip), repeating the SAME large
// batch will be killed again. The controller treats this as a STRONG,
// IMMEDIATE multiplicative-decrease signal (halve the next batch at
// once, bypassing the generic retry-rate accumulator's threshold) so
// successive attempts converge on a batch small enough to commit
// before the killer fires — instead of exhausting the ADR-0038 retry
// budget at a fixed large size and dying (the v0.99.69 live finding).
//
// The signal is engine-neutral: the controller checks for this surface
// via [errors.As] without importing any engine package, mirroring the
// way it checks [RetriableError]. An error that implements this MUST
// also implement [RetriableError] with Retriable()==true — a tx-killer
// abort is always retriable (the rollback already happened, so the
// re-apply is clean and idempotent per ADR-0010).
type TransactionKilledError interface {
	RetriableError

	// TransactionKilled reports whether this error is a server-side
	// transaction-killer abort. Implementations that set the flag
	// conditionally (only some Error 1105 shapes are tx-killer aborts)
	// return the per-error verdict; the bare presence of the method
	// is not itself the signal.
	TransactionKilled() bool
}

// LivenessProgressTimeoutError is the optional surface a [RetriableError]
// can additionally implement to tell the pipeline's retry loop (ADR-0038)
// that the failure was a source-stream "established, then went idle"
// progress timeout — a CDC stream that PROVED liveness (its reader's
// first-event / serving-tablet gate cleared) and THEN received no events for
// the progress window — as distinct from a stream that FAILED TO ESTABLISH
// at all.
//
// The distinction is load-bearing for the give-up budget (loose end 2b). The
// streamer's bounded-retry loop (ADR-0038) resets its consecutive-failure
// counter only when the persisted CDC position advances. A genuinely
// idle-but-healthy source re-attaches, warm-resumes, processes ZERO events,
// and so NEVER advances the position — its counter would climb to
// ApplyRetryAttempts and the sync would give up even though nothing is wrong
// (the idle-PlanetScale ~6-min give-up over the public vstream endpoint,
// where no idle heartbeats reach us). An error carrying this marker with
// IsIdleProgressTimeout()==true is treated as NON-incrementing so a truly
// idle source survives indefinitely.
//
// INVARIANT (loud-failure tenet): implementations MUST set the flag ONLY for
// the "established then idle" case. An error for a stream that NEVER
// established — a first-event/liveness timeout, an open/connection failure —
// MUST NOT carry the flag, so it still exhausts the budget and fails LOUDLY:
// a genuinely dead / no-tablet stream must never loop forever.
//
// Engine-neutral: the streamer checks for this surface via [errors.As]
// without importing any engine package, mirroring [RetriableError] and
// [TransactionKilledError]. An error that implements this MUST also implement
// [RetriableError] with Retriable()==true — an idle-progress timeout is
// always retriable (reconnecting from the last position is the correct
// recovery).
type LivenessProgressTimeoutError interface {
	RetriableError

	// IsIdleProgressTimeout reports whether this error is the
	// "established then went idle" progress timeout (exempt from the
	// give-up budget). Implementations that set the flag conditionally
	// return the per-error verdict; the bare presence of the method is
	// not itself the signal.
	IsIdleProgressTimeout() bool
}

// TerminalError is the INVERSE of [RetriableError]: an error whose producer
// knows recovery is impossible, asserted strongly enough to override every
// heuristic a classifier would otherwise apply.
//
// # Why this exists, and why RetriableError alone was not enough
//
// The retry classifiers are layered and heuristic by design. They honour an
// engine's RetriableError verdict, then fall back to stdlib surfaces
// (driver.ErrBadConn, io.EOF, net.Error timeouts) and finally to a text
// allow-list of driver/OS connection-drop shapes. That layering is what lets a
// new transport failure be ridden out without teaching every engine about it.
//
// It also means an engine CANNOT reliably say "no". A terminal error that
// merely lacks a RetriableError wrapper is still judged by the heuristics —
// and if it WRAPS its cause (which good errors do), the cause's text and
// sentinels are still in the chain and can make the whole thing read as
// transient.
//
// Measured 2026-09-12: the Postgres engine refuses a copy whose exported
// SNAPSHOT-PINNED connection has died, because the snapshot lives in a
// transaction on that connection and no replay can ever succeed. The refusal
// deliberately carried no RetriableError. The chunk retry replayed it 26 times
// anyway, because the refusal wrapped its cause with %w and the cause was an
// EOF — so errors.Is(err, io.EOF) was true and the heuristic said "transient".
// The engine knew; it had no way to be heard.
//
// Consumers MUST check this BEFORE any transient test, including before
// honouring RetriableError, so that an error which somehow satisfies both is
// treated as terminal. Refusing to retry is the safe direction: the cost of a
// wrong terminal verdict is one loud failure, and the cost of a wrong
// retriable verdict is a run that spins until its budget expires and fails
// anyway — with the operator having waited for it.
type TerminalError interface {
	error

	// Terminal reports that retrying cannot succeed. Implementations
	// return true unconditionally; the producer already decided.
	Terminal() bool
}

// IsTerminal reports whether err, or anything it wraps, asserts itself as
// [TerminalError]. The single predicate every classifier should consult first.
func IsTerminal(err error) bool {
	var te TerminalError
	return errors.As(err, &te) && te.Terminal()
}
