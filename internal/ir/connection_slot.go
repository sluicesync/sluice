// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// ConnectionSlotClassifier is the optional surface a target engine
// implements to tell the orchestrator whether a connection-open (or
// first-statement) failure is the *connection-slot-exhaustion* class —
// the only error the parallel bulk-copy pool treats as backpressure and
// retries under AIMD (connection-resilience Phase 2b).
//
// The contract is deliberately narrow: IsConnectionSlotExhausted MUST
// return true *only* for the slot-exhaustion class (Postgres SQLSTATE
// 53300 — `too_many_connections`, which also covers the
// superuser-reserved-slots FATAL `remaining connection slots are
// reserved for roles with the SUPERUSER attribute`). Every other error
// — a bad DSN, permission denied, a real COPY failure — MUST return
// false so it fails loudly and immediately. Masking a genuine error as
// backpressure would spin the pool down to parallelism 1 and then give
// up with a slot-exhaustion message that hides the real cause; that is
// the failure mode this classifier exists to prevent, so precision here
// is a correctness property, not a nicety.
//
// The orchestrator discovers the surface structurally with a type
// assertion (mirroring [TargetConnectionBudgetProber]); engines that do
// not implement it — or implement it returning false — make every open
// error non-retryable, preserving the pre-Phase-2b fail-fast behaviour.
// Engines with no connection-slot model (today: MySQL) simply omit it.
//
// nil in → false (a nil error is not a slot exhaustion).
type ConnectionSlotClassifier interface {
	IsConnectionSlotExhausted(err error) bool
}
