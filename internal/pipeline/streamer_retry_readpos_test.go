// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin the M0-5 fix for the 2026-07-23 audit's D0-4 (HIGH):
// runWithRetry's between-attempt position reads used to DISCARD the
// ReadPosition error, so a failed read (target down — both engines return
// found=false on error) was indistinguishable from "no anchor row exists",
// and the ADR-0109 §B discriminator latched RestartFromScratch=true. With
// the v0.99.288 connect-phase retry keeping the process alive through the
// outage, the first successful reconnect then executed a DESTRUCTIVE forced
// re-snapshot instead of the correct warm resume — and on idempotent
// sources a re-snapshot never replays source DELETEs committed before the
// new snapshot (silent divergence). The contract pinned here:
//
//   - a FAILED read leaves the latch at its prior value, and defaults to
//     warm-resume when a successful read found the anchor at any point
//     this Run;
//   - only a SUCCESSFUL read with found=false may latch RestartFromScratch.
//
// Pure stubbed-applier unit territory (audit G-4) — no containers.

// scriptedPosRead is one ReadPosition outcome in a scripted sequence.
type scriptedPosRead struct {
	pos   ir.Position
	found bool
	err   error
}

// scriptedPosApplier replays a fixed ReadPosition script in call order,
// repeating the final entry once exhausted. Models a target that is up for
// some reads and down (read ERRORS, not "row absent") for others.
type scriptedPosApplier struct {
	reads []scriptedPosRead
	calls int
}

func (a *scriptedPosApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	i := a.calls
	if i >= len(a.reads) {
		i = len(a.reads) - 1
	}
	a.calls++
	r := a.reads[i]
	return r.pos, r.found, r.err
}

func (*scriptedPosApplier) EnsureControlTable(context.Context) error               { return nil }
func (*scriptedPosApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (*scriptedPosApplier) RequestStop(context.Context, string) error              { return nil }
func (*scriptedPosApplier) ClearStopRequested(context.Context, string) error       { return nil }
func (*scriptedPosApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }

// scriptedPosEngine hands runWithRetry the scripted applier as its
// side-channel position reader; every other engine surface is unused by
// the retry seam.
type scriptedPosEngine struct {
	applier *scriptedPosApplier
}

func (scriptedPosEngine) Name() string                  { return "postgres" }
func (scriptedPosEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (scriptedPosEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("unused")
}

func (scriptedPosEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("unused")
}

func (scriptedPosEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("unused")
}

func (scriptedPosEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("unused")
}

func (scriptedPosEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("unused")
}

func (e scriptedPosEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return e.applier, nil
}

func (scriptedPosEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("unused")
}

// scriptedRetryStreamer wires runWithRetry onto the scripted position
// reader with a negligible backoff, mirroring fastRetryStreamer.
func scriptedRetryStreamer(reads []scriptedPosRead, attempts int) *Streamer {
	return &Streamer{
		StreamID:              "test-stream",
		Target:                scriptedPosEngine{applier: &scriptedPosApplier{reads: reads}},
		TargetDSN:             "tgt",
		ApplyRetryAttempts:    attempts,
		ApplyRetryBackoffBase: 1,
		ApplyRetryBackoffCap:  1,
	}
}

// errPosReadDown is the shape a target-down position read surfaces: the
// read FAILED — it did not observe "no anchor row".
var errPosReadDown = errors.New("read position: dial tcp 127.0.0.1:5432: connect: connection refused")

// TestRetryLoop_FailedPositionReadKeepsWarmResume is the D0-4 core pin
// (RED pre-fix): established CDC phase — the anchor row exists and the
// attempt's BEFORE-read observed it — then the target restarts, the apply
// error classifies retriable, and the between-attempt AFTER-read FAILS
// (target still down). The next attempt MUST warm-resume: latching
// RestartFromScratch here converts a warm-resumable blip into a forced
// re-snapshot that (on idempotent sources) never replays pre-snapshot
// source DELETEs.
func TestRetryLoop_FailedPositionReadKeepsWarmResume(t *testing.T) {
	anchor := ir.Position{Engine: "postgres", Token: "cdc-anchor"}
	s := scriptedRetryStreamer([]scriptedPosRead{
		{pos: anchor, found: true}, // before-read, attempt 1: target up, anchor exists
		{err: errPosReadDown},      // after-read, attempt 1: target DOWN — read FAILED
		{err: errPosReadDown},      // before-read, attempt 2: still down at read time
	}, 8)

	var calls int
	restartOnSecond := true
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls == 1 {
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		}
		restartOnSecond = s.RestartFromScratch
		return nil
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want clean warm-resume recovery: %v", err)
	}
	if calls != 2 {
		t.Fatalf("runOnce called %d times; want 2", calls)
	}
	if restartOnSecond {
		t.Error("RestartFromScratch latched on a FAILED position read (D0-4): the anchor was observed this Run, so the retry must warm-resume — a forced re-snapshot here is destructive (dropped tables on native MySQL; never-replayed source DELETEs on idempotent sources)")
	}
}

// TestRetryLoop_FailedPositionReadNeverLatches pins the other half of the
// D0-4 contract (RED pre-fix): even when NO successful read ever found the
// anchor, a FAILED read may not latch — the latch keeps its prior value
// (here: the zero value, false). Only a SUCCESSFUL read proving "no anchor
// row" may force the destructive re-establishment; "could not read" proves
// nothing.
func TestRetryLoop_FailedPositionReadNeverLatches(t *testing.T) {
	s := scriptedRetryStreamer([]scriptedPosRead{
		{err: errPosReadDown}, // every read fails: target down for the whole window
	}, 8)

	var calls int
	restartOnSecond := true
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls == 1 {
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		}
		restartOnSecond = s.RestartFromScratch
		return nil
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want recovery without a forced restart: %v", err)
	}
	if restartOnSecond {
		t.Error("RestartFromScratch latched with zero successful position reads — 'could not read the row' was conflated with 'no anchor row'")
	}
}

// TestRetryLoop_SuccessfulReadNoRowStillLatches pins the ADR-0109 §B
// behaviour that MUST survive the D0-4 fix (green pre- and post-fix,
// sibling of TestColdCopyAutoRestart_PlainPath): a genuine pre-anchor
// cold-copy failure — the read SUCCEEDS and proves no cdc-state row exists
// — still latches RestartFromScratch so the re-run forces a clean
// re-establishment instead of dup-keying on the partial prior copy.
func TestRetryLoop_SuccessfulReadNoRowStillLatches(t *testing.T) {
	s := scriptedRetryStreamer([]scriptedPosRead{
		{found: false}, // reads succeed; the anchor genuinely does not exist
	}, 8)

	var calls int
	var restartOnSecond bool
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls == 1 {
			return &retriableWrapper{err: errors.New("pipeline: copy table: source row stream failed")}
		}
		restartOnSecond = s.RestartFromScratch
		return nil
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want bounded auto-restart recovery: %v", err)
	}
	if !restartOnSecond {
		t.Error("a SUCCESSFUL read proving no anchor row must still latch RestartFromScratch (ADR-0109 §B) — the D0-4 fix must not weaken the genuine cold-copy discriminator")
	}
}
