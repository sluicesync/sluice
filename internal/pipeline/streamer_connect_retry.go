// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"

	"sluicesync.dev/sluice/internal/nettransient"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// # Transient connect-phase failures must not kill the retry loop
//
// The ADR-0038 retry loop classifies purely by interface
// ([classifyRetriable] → [ir.RetriableError]), which the engines attach to
// errors produced INSIDE a flowing attempt (apply writes, CDC reads). But a
// retry attempt first has to RE-ESTABLISH its connections — runOnce opens
// the target applier, the source readers, and the schema surfaces fresh
// each iteration — and an error there carries no wrapper, so it returned
// terminal from [runWithRetry].
//
// # Ground truth (2026-07-22, the scale-soak incident)
//
// A ~30s network blip broke both legs of a live PlanetScale↔PlanetScale
// filtered sync at once. The VStream read error WAS classified and the
// retry loop engaged ("applier: transient error; retrying" attempt=1) — but
// the reopen then died at
//
//	pipeline: open target change applier: mysql: ping: invalid connection
//
// and the process exited. The blip outlived one attempt but not the budget:
// the same warm resume, run 30 minutes later, drained the backlog in under
// two minutes. This is the connect-phase sibling of the v0.99.286
// trigger-CDC classification gap (internal/engines/internal/triggercdc);
// the class is "classification coverage, not missing retry machinery".
//
// # The fix
//
// The sync path's DB-touching setup sites wrap their failures in a
// [connectPhaseError] marker (via [connectHint], which composes the
// existing PhaseConnect operator hint). [runWithRetry] treats a marked
// failure as retriable ONLY when it also has a positively-matched transient
// network shape ([isTransientNetworkShape] — narrow by design, mirroring
// triggercdc.IsTransientTransportError). Everything else stays terminal:
// a DSN parse error, a bad credential, a coded refusal
// (SLUICE-E-…), or an unknown shape fails as loudly as before. The
// existing consecutive-failure budget bounds the new path, so a target
// that can NEVER be reached still exhausts the budget and fails loudly —
// the loud-failure floor is the budget, not the classification.

// connectPhaseError marks an error raised while (re)establishing the
// pipeline's DB connections/setup in runOnce — before any stream was
// flowing. Transparent: Error() and Unwrap() delegate, so operator-facing
// text, errors.Is/As chains, and the CLI's coded-error extraction are
// unchanged. Its only consumer is [runWithRetry]'s connect-transient
// fall-through.
type connectPhaseError struct{ err error }

func (e *connectPhaseError) Error() string { return e.err.Error() }
func (e *connectPhaseError) Unwrap() error { return e.err }

// connectHint wraps a connect/setup-phase failure with the PhaseConnect
// operator hint (exactly what the call sites did before) plus the
// [connectPhaseError] retry marker. nil in → nil out.
func connectHint(err error) error {
	if err == nil {
		return nil
	}
	return &connectPhaseError{err: migcore.WrapWithHint(migcore.PhaseConnect, err)}
}

// isRetriableConnectFailure reports whether err is a connect-phase-marked
// failure with a positively-transient network shape — the only combination
// [runWithRetry] retries without an [ir.RetriableError] wrapper. Both legs
// are required: an unmarked error may come from a phase whose retry
// semantics the engines own, and a marked-but-unmatched error (refusal,
// credential, parse) must stay terminal.
func isRetriableConnectFailure(err error) bool {
	var cp *connectPhaseError
	return errors.As(err, &cp) && isTransientNetworkShape(err)
}

// isTransientNetworkShape reports whether err is a network/transport shape
// that is transient by construction. Positive-match only — the DEFAULT is
// "not transient", the opposite posture from [isTransientOpenError] (whose
// job is merely picking a log level and defaults transient).
//
// The shape vocabulary lives in [nettransient.IsTransientShape] — the ONE
// shared matcher (audit 2026-07-23 QUAL-1/G-9). This site used to carry its
// own copy ("mirrors triggercdc.IsTransientTransportError, which pipeline
// cannot import") plus the pool-facing additions ground-truthed in the
// scale-soak incident ("invalid connection", the Windows winsock wordings,
// the Bug 199a `connectex:` dial refusal); those and the exclusions
// ("no such host", auth, DSN parse, coded refusals — all terminal-by-design)
// are now documented and pinned in the shared package, and
// [TestIsTransientNetworkShape_CorpusParity] fails if this site ever drifts
// from the corpus again.
func isTransientNetworkShape(err error) bool {
	return nettransient.IsTransientShape(err)
}
