// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/nettransient"
)

// # Transient-error classification for the trigger-CDC engines
//
// The trigger-CDC readers (pgtrigger, sqlite-trigger, and its d1-trigger
// transport sibling) poll a change-log table in a loop. Until v0.99.286 a poll
// error was surfaced RAW — `setErr(fmt.Errorf("<engine>: poll: %w", err))` —
// with no [ir.RetriableError] wrapper anywhere on the chain. The pipeline's
// retry loop classifies purely by interface (`errors.As` for
// [ir.RetriableError]; see pipeline.classifyRetriable), so an unwrapped
// transient never matched: ANY blip terminated the stream.
//
// # Ground truth (2026-07-22)
//
// A multi-day soak of the two d1-trigger streams died on routine transients,
// hours apart, each exiting the process:
//
//	d1: query database "…": Post "…": net/http: TLS handshake timeout
//	d1: query database "…" failed: HTTP 500: {"errors":[{"code":7500,…}]}
//
// Neither is a fault in the change-log or the data — the network hiccuped and
// Cloudflare returned one 500. Both are exactly what a days-long poll against a
// managed HTTP API will hit. Data was never at risk (the `{"last_id":N}`
// position is durable and every restart warm-resumed cleanly), but a sync that
// dies on each blip is not operationally usable.
//
// Classifying these lets the EXISTING ADR-0038 pipeline retry loop reopen the
// pump in process, with its bounded budget still providing the loud-failure
// floor: a genuinely persistent outage exhausts the budget and fails loudly
// rather than spinning forever.
//
// The classification is deliberately NARROW — only shapes that are transient by
// construction. A wrong DSN, a bad token, a missing change-log table, or a
// decode fault stays TERMINAL, because retrying those masks an operator error.

// retriableTriggerError satisfies [ir.RetriableError] for a classified
// trigger-CDC transient. It carries no retry hint — the pipeline's exponential
// policy owns the backoff; these transports have no server-supplied
// retry-after that sluice consumes today.
type retriableTriggerError struct{ err error }

func (e *retriableTriggerError) Error() string            { return e.err.Error() }
func (e *retriableTriggerError) Unwrap() error            { return e.err }
func (e *retriableTriggerError) Retriable() bool          { return true }
func (e *retriableTriggerError) RetryHint() time.Duration { return 0 }

// ClassifyTransient wraps err in an [ir.RetriableError] when it matches one of
// the documented transient transport shapes; returns err unchanged otherwise.
// nil in → nil out.
//
// Callers wrap at the point the transport error is produced (e.g. the D1 HTTP
// client), NOT at the reader's `setErr`: the reader's `poll: %w` chain keeps the
// wrapper reachable via `errors.As`, so one classification site serves every
// caller of that transport.
func ClassifyTransient(err error) error {
	if err == nil {
		return nil
	}
	if IsTransientTransportError(err) {
		return &retriableTriggerError{err: err}
	}
	return err
}

// AsTransient wraps err as retriable UNCONDITIONALLY. It is for callers that
// have already established transience from STRUCTURED information the error
// text does not carry — today: an HTTP status code judged by
// [RetriableHTTPStatus]. nil in → nil out.
//
// Prefer [ClassifyTransient] when the decision must be derived from the error
// itself; use this only where the caller holds the structured signal.
func AsTransient(err error) error {
	if err == nil {
		return nil
	}
	return &retriableTriggerError{err: err}
}

// IsTransientTransportError reports whether err is a network/transport shape
// that is transient by construction — the connection or TLS handshake failed,
// timed out, or was reset mid-flight.
//
// The shape vocabulary lives in [nettransient.IsTransientShape] — the ONE
// shared matcher (audit 2026-07-23 QUAL-1/G-9). This site used to carry its
// own copy, and it drifted one release after Bug 199: the Windows dial
// wordings (`connectex:` / "actively refused" / "connection timed out")
// reached the pipeline's connect-phase retry and both applier classifiers but
// never this list, so a pgtrigger-source sync on Windows exited terminally on
// a routine managed-PG restart. Delegating closes the class: the corpus is
// pinned once in the shared package (structured checks first, text fallback
// for shapes like `net/http: TLS handshake timeout` that carry no structured
// form through the stdlib HTTP stack; "no such host" / auth / malformed-
// request / decode faults stay terminal-by-design), and
// [TestIsTransientTransportError_CorpusParity] fails if this site ever drifts
// from it again.
//
// Kept as a named exported helper so [TestIsTransientTransportError] pins the
// observed trigger-CDC shapes end-to-end — widening or narrowing the retry
// surface then fails a pin rather than slipping in silently.
func IsTransientTransportError(err error) bool {
	return nettransient.IsTransientShape(err)
}

// RetriableHTTPStatus reports whether an HTTP status code from a trigger-CDC
// REST transport (today: the Cloudflare D1 query API) is a transient worth
// retrying.
//
// The set is the standard "server-side hiccup / back off and retry" family:
// 408 request-timeout, 429 rate-limited, and the 5xx gateway/availability
// codes. Notably 500 is included — the observed D1 failure was a plain
// `HTTP 500 internal error; reference = …`, which is Cloudflare-side and clears
// on retry.
//
// Every other status stays TERMINAL: 4xx (other than 408/429) means the request
// itself is wrong — a bad token (401/403), a missing database (404), a malformed
// statement (400) — and retrying masks an operator error. The pipeline's bounded
// budget still fails loudly if a "transient" 5xx never clears.
//
// Kept as a named exported helper so [TestRetriableHTTPStatus] pins the exact
// code set.
func RetriableHTTPStatus(code int) bool {
	switch code {
	case 408, // request timeout
		429, // too many requests (rate limited)
		500, // internal server error (observed on D1)
		502, // bad gateway
		503, // service unavailable
		504: // gateway timeout
		return true
	default:
		return false
	}
}

// compile-time assertion: the wrapper really satisfies the engine-neutral
// retry surface the pipeline classifies on. A signature drift in
// [ir.RetriableError] then fails the build here rather than silently making
// every trigger-CDC transient terminal again.
var _ ir.RetriableError = (*retriableTriggerError)(nil)
