// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "time"

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
