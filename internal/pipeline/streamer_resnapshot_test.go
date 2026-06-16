// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// invalidPositionErr is the shape the engine classifier produces for a
// reactive purged/invalid resume position (ADR-0093 / mysql
// classifyReaderError): an error wrapping [ir.ErrPositionInvalid]. The
// pipeline package can't import the engine, so this mirrors the wrapped
// shape directly.
func invalidPositionErr() error {
	return fmt.Errorf("source vstream cannot resume (gtid_purged advanced past it): %w", ir.ErrPositionInvalid)
}

// resnapshotApplier is a minimal [ir.ChangeApplier] for the runWithRetry
// side-channel position reader: ReadPosition always reports "no progress"
// (found=false) so the retry budget counts consecutive failures, and
// every other method no-ops. Reuses the same shape as
// resumeDispatchApplier but local so the ADR-0093 pins are self-contained.
type resnapshotApplier struct{}

func (resnapshotApplier) EnsureControlTable(context.Context) error { return nil }
func (resnapshotApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}
func (resnapshotApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (resnapshotApplier) RequestStop(context.Context, string) error              { return nil }
func (resnapshotApplier) ClearStopRequested(context.Context, string) error       { return nil }
func (resnapshotApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }

// resnapshotTargetEngine is a target whose OpenChangeApplier hands back a
// resnapshotApplier so runWithRetry can open its side-channel position
// reader. Every other engine method errors (unused by the seam path).
type resnapshotTargetEngine struct{}

func (resnapshotTargetEngine) Name() string                  { return "mysql" }
func (resnapshotTargetEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (resnapshotTargetEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("unused")
}

func (resnapshotTargetEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("unused")
}

func (resnapshotTargetEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("unused")
}

func (resnapshotTargetEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("unused")
}

func (resnapshotTargetEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("unused")
}

func (resnapshotTargetEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return resnapshotApplier{}, nil
}

func (resnapshotTargetEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("unused")
}

// TestReactiveResnapshot_SingleAttempt pins the ADR-0093 reactive
// cold-start recovery on the single-attempt Run path (retry disabled —
// the default when --apply-retry-attempts is unset). A runOnce that
// returns a reactive ir.ErrPositionInvalid on the first call must, by
// default, set RestartFromScratch and re-run once (the second runOnce
// observes the forced cold-start flag); with the opt-out it must fail
// loudly without re-running.
func TestReactiveResnapshot_SingleAttempt(t *testing.T) {
	t.Run("default: re-runs once in forced cold-start", func(t *testing.T) {
		var calls int
		var sawColdStartOnSecond bool
		s := &Streamer{
			StreamID:                        "test-stream",
			AutoResnapshotOnInvalidPosition: true,
		}
		s.runOnceFn = func(context.Context) error {
			calls++
			switch calls {
			case 1:
				if s.RestartFromScratch {
					t.Errorf("RestartFromScratch set on the FIRST attempt; the cold-start flag must only be raised by the recovery")
				}
				return invalidPositionErr()
			default:
				sawColdStartOnSecond = s.RestartFromScratch
				return nil // re-snapshot succeeds
			}
		}

		if err := s.Run(context.Background()); err != nil {
			t.Fatalf("Run returned error; want clean recovery: %v", err)
		}
		if calls != 2 {
			t.Fatalf("runOnce called %d times; want exactly 2 (original + one re-snapshot)", calls)
		}
		if !sawColdStartOnSecond {
			t.Error("the re-snapshot attempt did NOT see RestartFromScratch=true; recovery must force a cold-start")
		}
	})

	t.Run("opt-out: loud terminal error, no re-run", func(t *testing.T) {
		var calls int
		s := &Streamer{
			StreamID:                        "test-stream",
			AutoResnapshotOnInvalidPosition: false,
		}
		s.runOnceFn = func(context.Context) error {
			calls++
			return invalidPositionErr()
		}

		err := s.Run(context.Background())
		if err == nil {
			t.Fatal("Run returned nil; --no-auto-resnapshot must surface a loud terminal error")
		}
		if calls != 1 {
			t.Fatalf("runOnce called %d times; opt-out must NOT re-run (want 1)", calls)
		}
		// The underlying invalid-position must remain reachable, and the
		// message must name the recovery commands.
		if !errors.Is(err, ir.ErrPositionInvalid) {
			t.Errorf("opt-out error lost ErrPositionInvalid from the chain: %v", err)
		}
		for _, want := range []string{"--restart-from-scratch", "--reset-target-data", "--no-auto-resnapshot"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("opt-out error does not name %q (must be actionable): %v", want, err)
			}
		}
	})

	t.Run("bounded: second consecutive invalid position is terminal", func(t *testing.T) {
		var calls int
		s := &Streamer{
			StreamID:                        "test-stream",
			AutoResnapshotOnInvalidPosition: true,
		}
		s.runOnceFn = func(context.Context) error {
			calls++
			return invalidPositionErr() // always invalid — source purging faster than snapshot
		}

		err := s.Run(context.Background())
		if err == nil {
			t.Fatal("Run returned nil; a second consecutive invalid position must be terminal (no infinite re-snapshot loop)")
		}
		if calls != 2 {
			t.Fatalf("runOnce called %d times; the one-shot recovery must be bounded to 2 total (want 2)", calls)
		}
		if !errors.Is(err, ir.ErrPositionInvalid) {
			t.Errorf("bounded-terminal error lost ErrPositionInvalid from the chain: %v", err)
		}
	})
}

// TestReactiveResnapshot_RetryPath pins the same ADR-0093 recovery on the
// runWithRetry path (--apply-retry-attempts > 1). The reactive
// ir.ErrPositionInvalid must be routed to the one-shot cold-start
// re-snapshot, NOT into the ADR-0038 backoff loop (retrying the same
// purged position spins forever). The same bounded + opt-out semantics
// apply.
func TestReactiveResnapshot_RetryPath(t *testing.T) {
	newStreamer := func(auto bool) *Streamer {
		return &Streamer{
			StreamID:                        "test-stream",
			Target:                          resnapshotTargetEngine{},
			TargetDSN:                       "tgt",
			ApplyRetryAttempts:              5,
			AutoResnapshotOnInvalidPosition: auto,
		}
	}

	t.Run("default: re-runs once in forced cold-start", func(t *testing.T) {
		s := newStreamer(true)
		var calls int
		var sawColdStartOnSecond bool
		s.runOnceFn = func(context.Context) error {
			calls++
			if calls == 1 {
				return invalidPositionErr()
			}
			sawColdStartOnSecond = s.RestartFromScratch
			return nil
		}
		if err := s.Run(context.Background()); err != nil {
			t.Fatalf("Run returned error; want clean recovery: %v", err)
		}
		if calls != 2 {
			t.Fatalf("runOnce called %d times; want exactly 2 (original + one re-snapshot)", calls)
		}
		if !sawColdStartOnSecond {
			t.Error("the re-snapshot attempt did NOT see RestartFromScratch=true")
		}
	})

	t.Run("bounded: second consecutive invalid position is terminal (not a backoff spin)", func(t *testing.T) {
		s := newStreamer(true)
		var calls int
		s.runOnceFn = func(context.Context) error {
			calls++
			return invalidPositionErr()
		}
		err := s.Run(context.Background())
		if err == nil {
			t.Fatal("Run returned nil; a second consecutive invalid position must be terminal")
		}
		// Critically: bounded to 2, NOT ApplyRetryAttempts (5) — the
		// invalid position must never enter the retry budget loop.
		if calls != 2 {
			t.Fatalf("runOnce called %d times; want 2 (the invalid position must NOT consume the ADR-0038 retry budget)", calls)
		}
		if !errors.Is(err, ir.ErrPositionInvalid) {
			t.Errorf("bounded-terminal error lost ErrPositionInvalid: %v", err)
		}
	})

	t.Run("opt-out: loud terminal error, no re-run", func(t *testing.T) {
		s := newStreamer(false)
		var calls int
		s.runOnceFn = func(context.Context) error {
			calls++
			return invalidPositionErr()
		}
		err := s.Run(context.Background())
		if err == nil {
			t.Fatal("Run returned nil; --no-auto-resnapshot must surface a loud terminal error")
		}
		if calls != 1 {
			t.Fatalf("runOnce called %d times; opt-out must NOT re-run (want 1)", calls)
		}
		if !errors.Is(err, ir.ErrPositionInvalid) {
			t.Errorf("opt-out error lost ErrPositionInvalid: %v", err)
		}
	})
}

// TestReactiveResnapshot_IgnoresCtxCancel pins that a bare ctx
// cancellation is NOT mistaken for an invalid-position recovery trigger:
// the recovery must only fire on a genuine ir.ErrPositionInvalid.
func TestReactiveResnapshot_IgnoresCtxCancel(t *testing.T) {
	s := &Streamer{StreamID: "test-stream", AutoResnapshotOnInvalidPosition: true}
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		return context.Canceled
	}
	err := s.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run should return ctx.Canceled verbatim; got %v", err)
	}
	if calls != 1 {
		t.Fatalf("runOnce called %d times; a ctx cancel must not trigger a re-snapshot (want 1)", calls)
	}
}
