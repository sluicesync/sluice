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

// coldCopyDropErr is the shape the MySQL reader's classifyApplierError
// produces for a connection-drop class source-read error during cold-copy
// (ADR-0109 §C reader-classification → ir.RetriableError). It propagates up
// through readerStreamErr → coldStartRunCopy → coldStart → runOnce to
// runWithRetry. Built with the package-local retriableWrapper test double so
// the pipeline tests don't import the engine package.
func coldCopyDropErr() error {
	inner := fmt.Errorf("pipeline: copy table %q: source row stream for table %q failed: mysql: rows iteration: invalid connection", "documents", "documents")
	return &retriableWrapper{err: inner}
}

// posControlApplier is a runWithRetry side-channel position reader whose
// ReadPosition return is controlled by foundPos: false models the cold-copy
// phase (no cdc-state row persisted yet — coldStartBeginCDC hasn't run), true
// models the CDC phase (the post-copy anchor has been written). This is the
// exact discriminator ADR-0109 §B uses to decide whether a retriable error
// triggers a cold-start auto-restart (cold-copy phase) or a warm-resume (CDC
// phase).
type posControlApplier struct {
	foundPos bool
}

func (posControlApplier) EnsureControlTable(context.Context) error { return nil }
func (a posControlApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	if a.foundPos {
		// A stable, non-empty token: present but unchanging between attempts
		// so "progress" never resets the consecutive-failure budget.
		return ir.Position{Engine: "mysql", Token: "cdc-anchor"}, true, nil
	}
	return ir.Position{}, false, nil
}
func (posControlApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (posControlApplier) RequestStop(context.Context, string) error              { return nil }
func (posControlApplier) ClearStopRequested(context.Context, string) error       { return nil }
func (posControlApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }

// posControlTargetEngine hands runWithRetry a posControlApplier as its
// side-channel position reader. Every other engine method is unused by the
// retry seam.
type posControlTargetEngine struct {
	foundPos bool
}

func (posControlTargetEngine) Name() string                  { return "mysql" }
func (posControlTargetEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (posControlTargetEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("unused")
}

func (posControlTargetEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("unused")
}

func (posControlTargetEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("unused")
}

func (posControlTargetEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("unused")
}

func (posControlTargetEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("unused")
}

func (e posControlTargetEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return posControlApplier(e), nil
}

func (posControlTargetEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("unused")
}

// fastRetryStreamer builds a Streamer wired onto runWithRetry with a tiny
// backoff base/cap so the consecutive-failure budget is exercised without
// real waits. foundPos selects the cold-copy (false) vs CDC (true) phase via
// the side-channel position reader.
func fastRetryStreamer(t *testing.T, foundPos bool, attempts int) *Streamer {
	t.Helper()
	return &Streamer{
		StreamID:              "test-stream",
		Target:                posControlTargetEngine{foundPos: foundPos},
		TargetDSN:             "tgt",
		ApplyRetryAttempts:    attempts,
		ApplyRetryBackoffBase: 1, // 1ns base → effectively no wait
		ApplyRetryBackoffCap:  1,
	}
}

// TestColdCopyAutoRestart_PlainPath is the ADR-0109 §B core pin: a
// classified-retriable SOURCE-read drop during the cold-COPY phase (no
// cdc-state row exists yet → foundPos=false) must force RestartFromScratch on
// the re-run — so the plain native-MySQL cold-copy re-establishes a CLEAN
// copy (coldStartGatePreflight drops + recreates the in-scope target tables)
// rather than dead-ending on the populated-target refusal or dup-keying
// (Error 1062) on the partial prior copy. The re-run then converges.
func TestColdCopyAutoRestart_PlainPath(t *testing.T) {
	s := fastRetryStreamer(t, false /* cold-copy phase */, 8)
	var calls int
	var sawForceFreshOnSecond bool
	s.runOnceFn = func(context.Context) error {
		calls++
		switch calls {
		case 1:
			if s.RestartFromScratch {
				t.Errorf("RestartFromScratch set on the FIRST attempt; the force-fresh must only be raised by the §B recovery")
			}
			return coldCopyDropErr()
		default:
			// The re-run MUST see the forced clean re-establishment so the
			// plain-INSERT copy doesn't dup-key on the partial prior copy.
			sawForceFreshOnSecond = s.RestartFromScratch
			return nil // the clean re-copy converges (src==dst)
		}
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want a clean bounded auto-restart recovery: %v", err)
	}
	if calls != 2 {
		t.Fatalf("runOnce called %d times; want exactly 2 (original cold-copy + one clean re-copy)", calls)
	}
	if !sawForceFreshOnSecond {
		t.Error("the re-copy attempt did NOT see RestartFromScratch=true — a plain native-MySQL re-copy would then dup-key (1062) on the leftover partial copy")
	}
}

// TestColdCopyAutoRestart_CDCPhaseWarmResumes pins the inverse: once a
// cdc-state row exists (foundPos=true — the run reached the post-copy CDC
// anchor write), a retriable error is a CDC/apply-phase transient. The
// re-run must WARM-RESUME from the durable position (RestartFromScratch
// cleared), NOT trigger a wasteful full re-snapshot. This preserves the
// pre-ADR-0109 apply-retry behaviour exactly.
func TestColdCopyAutoRestart_CDCPhaseWarmResumes(t *testing.T) {
	s := fastRetryStreamer(t, true /* CDC phase: cdc-state row exists */, 8)
	var calls int
	var forceFreshOnSecond bool
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls == 1 {
			return coldCopyDropErr()
		}
		forceFreshOnSecond = s.RestartFromScratch
		return nil
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; want clean warm-resume recovery: %v", err)
	}
	if calls != 2 {
		t.Fatalf("runOnce called %d times; want 2", calls)
	}
	if forceFreshOnSecond {
		t.Error("RestartFromScratch was forced on a CDC-phase retry — a durable position exists, so it must warm-resume, not re-copy")
	}
}

// TestColdCopyAutoRestart_NonRetriableStaysTerminal pins the v0.99.92-clean
// preservation: a NON-retriable cold-start error (e.g. the populated-target
// refusal from a genuine operator mistake, or a decode fault) must stay a
// clean terminal exit — no re-run, no dup-key, no loop. classifyRetriable
// returns false for it and runWithRetry returns it verbatim.
func TestColdCopyAutoRestart_NonRetriableStaysTerminal(t *testing.T) {
	s := fastRetryStreamer(t, false, 8)
	var calls int
	terminal := errors.New("pipeline: cold-start refused: target table \"documents\" already contains data")
	s.runOnceFn = func(context.Context) error {
		calls++
		return terminal
	}

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; a non-retriable cold-start error must stay terminal")
	}
	if !errors.Is(err, terminal) {
		t.Errorf("terminal error not returned verbatim; got %v", err)
	}
	if calls != 1 {
		t.Fatalf("runOnce called %d times; a non-retriable error must NOT re-run (want 1)", calls)
	}
}

// TestColdCopyAutoRestart_BudgetBoundsRestarts pins that the auto-restart is
// BOUNDED by the ADR-0038 retry budget — a source that keeps dropping the
// cold-copy read (never converges) must exhaust the budget and surface a LOUD
// terminal error, never an infinite restart loop. With foundPos=false the
// position never advances, so every attempt counts against the consecutive
// budget.
func TestColdCopyAutoRestart_BudgetBoundsRestarts(t *testing.T) {
	const attempts = 4
	s := fastRetryStreamer(t, false, attempts)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		return coldCopyDropErr() // never converges
	}

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; a perpetually-dropping cold-copy must surface a loud terminal error after the budget")
	}
	if calls != attempts {
		t.Fatalf("runOnce called %d times; the auto-restart must be bounded to the %d-attempt budget", calls, attempts)
	}
	if !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Errorf("budget-exhaustion error must be loud + name the budget; got %v", err)
	}
	// The underlying transient must remain reachable for diagnostics.
	if !strings.Contains(err.Error(), "invalid connection") {
		t.Errorf("budget-exhaustion error lost the underlying transient cause: %v", err)
	}
}
