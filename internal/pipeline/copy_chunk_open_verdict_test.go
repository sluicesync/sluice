// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// engineVerdictErr is a stand-in for an engine-classified transient. The
// engines' own classifier types are unexported, and this test deliberately does
// NOT reach for one: what it grades is that the chunk-open retry honours the
// ir.RetriableError CONTRACT, whoever implements it.
type engineVerdictErr struct{ err error }

func (e *engineVerdictErr) Error() string   { return e.err.Error() }
func (e *engineVerdictErr) Unwrap() error   { return e.err }
func (e *engineVerdictErr) Retriable() bool { return true }

// RetryHint completes the ir.RetriableError contract. Zero means "use the
// policy's own floor", which is what every engine emits today.
func (e *engineVerdictErr) RetryHint() time.Duration { return 0 }

// TestChunkOpenRetryHonoursEngineVerdict is the pipeline half of a two-package
// pin, and it exists because the failure it guards against was invisible to
// both halves alone.
//
// MEASURED 2026-09-13 on a fresh PS-10 Neki whose 10 GiB volume filled mid-copy.
// The shard went read-only, its sidecars went unhealthy, and the engine's
// after-connect spatial-OID probe — which runs on EVERY per-chunk writer
// connection — got `NK205 no healthy sidecars available`. NK205 is classified
// transient on purpose, so the copy should have backed off and continued. It
// did not: the engine returned that error UNCLASSIFIED, so by the time it
// reached this predicate there was no verdict to honour, and a transient
// platform condition failed the table after 3m40s.
//
// The engine half (postgres.TestSpatialOIDProbeErrorIsClassified) grades that
// the verdict is now attached. THIS half grades that attaching it is worth
// anything — that the chunk-open retry reads it. Either test passes while the
// other's side is broken, which is precisely how the live defect survived.
func TestChunkOpenRetryHonoursEngineVerdict(t *testing.T) {
	t.Parallel()

	// The shape the engine now hands over: a platform error the pipeline has
	// no business decoding itself, carrying the engine's verdict.
	classified := &engineVerdictErr{err: fmt.Errorf(
		"postgres: lookup spatial type OIDs: no healthy sidecars available for shard sh5 (SQLSTATE NK205)",
	)}

	if !isRetriableChunkOpenError(classified) {
		t.Fatal("the chunk-open retry refuses an error carrying an engine RetriableError verdict. The " +
			"pipeline cannot classify platform SQLSTATEs itself — honouring the engine's verdict is the " +
			"whole mechanism — so refusing it means a transient sidecar or grow window fails the table")
	}

	// Anti-vacuity: the identical text WITHOUT the verdict must be refused.
	// If the predicate accepted this too, it would be matching on message
	// shape and the engine's classification would be decorative.
	bare := errors.New(
		"postgres: lookup spatial type OIDs: no healthy sidecars available for shard sh5 (SQLSTATE NK205)",
	)
	if isRetriableChunkOpenError(bare) {
		t.Fatal("the chunk-open retry accepts the same message with NO engine verdict attached — so this " +
			"test cannot tell a classified error from an unclassified one, and the engine-side fix it " +
			"is paired with would be unfalsifiable here")
	}

	// And a TERMINAL verdict must still win, so the fix above cannot be read
	// as "anything the engine hands over gets retried".
	terminal := &pipelineTerminalErr{err: errors.New("postgres: something the engine says is fatal")}
	if isRetriableChunkOpenError(terminal) {
		t.Fatal("a TERMINAL engine verdict is being retried by the chunk-open path")
	}
}

// pipelineTerminalErr is the terminal counterpart of engineVerdictErr.
type pipelineTerminalErr struct{ err error }

func (e *pipelineTerminalErr) Error() string  { return e.err.Error() }
func (e *pipelineTerminalErr) Unwrap() error  { return e.err }
func (e *pipelineTerminalErr) Terminal() bool { return true }

var (
	_ ir.RetriableError = (*engineVerdictErr)(nil)
	_ ir.TerminalError  = (*pipelineTerminalErr)(nil)
)
