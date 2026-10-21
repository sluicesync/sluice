// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Routing pins for a persisted position. One carrying a mid-COPY TablePKs
// cursor (an INTERRUPTED cold start, written by a release before v0.163.3)
// is refused VSTREAM-COPY-RESUME-WITHDRAWN: it neither resumes the copy
// from the cursor, which kept the rows the source deleted while stopped,
// nor reaches warmResume's plain CDC reader, which would leave the rest of
// the copy missing. --restart-from-scratch, the refusal's remedy, still
// runs. A COMPLETED cold start (cursor-less position) stays on the
// plain-CDC warmResume path.
//
// These tests assert the runOnce dispatch decision in isolation: a
// recording source engine reports whether the cursor was present and
// records WHICH open path the streamer took.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestStreamer_InterruptedColdStart_RefusesTheCursor asserts that a
// persisted position carrying a mid-COPY cursor is refused
// VSTREAM-COPY-RESUME-WITHDRAWN under the supervisor's no-restart marker,
// before any open: neither the seeded snapshot (the withdrawn resume) nor
// the plain CDC reader (a warm resume that copies nothing) is reached.
func TestStreamer_InterruptedColdStart_RefusesTheCursor(t *testing.T) {
	const token = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100","table_p_ks":[{"table_name":"widgets","lastpk":"AAAA"}]}]`
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{
		name:           "planetscale",
		caps:           ir.Capabilities{CDC: ir.CDCBinlog},
		cdcReader:      cdc,
		carriesCursor:  true,
		schemaOneTable: true,
	}
	target := &copyResumeEngine{name: "postgres"}
	applier := &resumeDispatchApplier{
		stored: ir.Position{Engine: "postgres", Token: token},
		found:  true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := &Streamer{
		Source:        source,
		Target:        target,
		SourceDSN:     "src",
		TargetDSN:     "tgt",
		StreamID:      "test-stream",
		SchemaChanges: "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:       applier,
	}
	err := s.Run(ctx)

	if err == nil || !strings.Contains(err.Error(), copyResumeWithdrawnMarker) || !strings.Contains(err.Error(), "--restart-from-scratch") {
		t.Fatalf("Run = %v; want the %s refusal naming --restart-from-scratch", err, copyResumeWithdrawnMarker)
	}
	if refusalARestartRepeats(err) == nil {
		t.Errorf("the refusal carries no sentinel in refusalsARestartRepeats, so the supervisor would retry it: %v", err)
	}
	if n := source.resumeOpenCalls.Load(); n != 0 {
		t.Errorf("OpenSnapshotStreamFromPosition called %d times; want 0 (the cursor resume is withdrawn)", n)
	}
	if cdc.once || source.cdcOpenCalls.Load() != 0 {
		t.Errorf("the plain CDC reader was opened (%d) — a warm resume from a mid-COPY cursor copies nothing more", source.cdcOpenCalls.Load())
	}
}

// TestStreamer_InterruptedColdStart_RestartFromScratchStillRuns asserts the
// refusal's remedy is reachable: with --restart-from-scratch, a persisted
// mid-COPY cursor is ignored and a fresh cold start runs (it opens the
// scoped snapshot, not the seeded one, and unwinds at the unconfigured test
// target).
func TestStreamer_InterruptedColdStart_RestartFromScratchStillRuns(t *testing.T) {
	const token = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100","table_p_ks":[{"table_name":"widgets","lastpk":"AAAA"}]}]`
	source := &copyResumeEngine{
		name:           "planetscale",
		caps:           ir.Capabilities{CDC: ir.CDCBinlog},
		cdcReader:      &capturingCDCReader{captured: make(chan struct{})},
		carriesCursor:  true,
		schemaOneTable: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s := &Streamer{
		Source:             source,
		Target:             &copyResumeEngine{name: "postgres"},
		SourceDSN:          "src",
		TargetDSN:          "tgt",
		StreamID:           "test-stream",
		SchemaChanges:      "refuse",
		Applier:            &resumeDispatchApplier{stored: ir.Position{Engine: "postgres", Token: token}, found: true},
		RestartFromScratch: true,
	}
	err := s.Run(ctx)
	if err != nil && strings.Contains(err.Error(), copyResumeWithdrawnMarker) {
		t.Fatalf("--restart-from-scratch was refused %s; the remedy must run: %v", copyResumeWithdrawnMarker, err)
	}
	if n := source.snapshotOpenCalls.Load(); n != 1 {
		t.Errorf("OpenSnapshotStream called %d times; want 1 (the fresh cold start)", n)
	}
	if n := source.resumeOpenCalls.Load(); n != 0 {
		t.Errorf("OpenSnapshotStreamFromPosition called %d times; want 0", n)
	}
}

// TestStreamer_CompletedColdStart_StaysOnPlainCDC asserts that a
// cursor-less persisted position keeps the fast plain-CDC warm-resume
// path: StreamChanges IS called, OpenSnapshotStreamFromPosition is NOT.
func TestStreamer_CompletedColdStart_StaysOnPlainCDC(t *testing.T) {
	const token = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-200"}]`
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{
		name:          "planetscale",
		caps:          ir.Capabilities{CDC: ir.CDCBinlog},
		cdcReader:     cdc,
		carriesCursor: false, // cursor-less → completed cold-start
	}
	target := &copyResumeEngine{name: "postgres"}
	applier := &resumeDispatchApplier{
		stored: ir.Position{Engine: "postgres", Token: token},
		found:  true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := &Streamer{
		Source:        source,
		Target:        target,
		SourceDSN:     "src",
		TargetDSN:     "tgt",
		StreamID:      "test-stream",
		SchemaChanges: "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:       applier,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	select {
	case <-cdc.captured:
	case <-time.After(2 * time.Second):
		cancel()
		<-runErr
		t.Fatal("plain CDC StreamChanges not called within 2s; cursor-less resume did not take the fast warm-resume path")
	}
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if source.resumeOpenCalls.Load() != 0 {
		t.Errorf("OpenSnapshotStreamFromPosition called %d times; want 0 (completed cold-start must stay on the plain CDC path)",
			source.resumeOpenCalls.Load())
	}
}

// TestStreamer_RestartFromScratch_ForcesColdStart pins F2: a cursor-less
// persisted position would normally warm-resume via the plain CDC reader
// (see TestStreamer_CompletedColdStart_StaysOnPlainCDC), but with
// RestartFromScratch the streamer must IGNORE the persisted position and force
// a fresh cold-start — never touching the warm-resume plain-CDC path. The
// cold-start path opens a snapshot (not OpenCDCReader) and here unwinds at the
// test target's schema writer; the load-bearing assertion is the PATH.
func TestStreamer_RestartFromScratch_ForcesColdStart(t *testing.T) {
	const token = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-200"}]`
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{
		name:           "planetscale",
		caps:           ir.Capabilities{CDC: ir.CDCBinlog},
		cdcReader:      cdc,
		carriesCursor:  false, // cursor-less: would warm-resume WITHOUT the flag
		schemaOneTable: true,  // proceed past the empty-schema short-circuit
	}
	target := &copyResumeEngine{name: "postgres"}
	applier := &resumeDispatchApplier{
		stored: ir.Position{Engine: "postgres", Token: token},
		found:  true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := &Streamer{
		Source:             source,
		Target:             target,
		SourceDSN:          "src",
		TargetDSN:          "tgt",
		StreamID:           "test-stream",
		SchemaChanges:      "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:            applier,
		RestartFromScratch: true,
	}
	err := s.Run(ctx)

	if cdc.once {
		t.Error("plain CDC StreamChanges was called — --restart-from-scratch wrongly warm-resumed instead of forcing a fresh cold-start")
	}
	if source.cdcOpenCalls.Load() != 0 {
		t.Errorf("OpenCDCReader called %d times; want 0 (restart-from-scratch must not take the warm-resume plain-CDC path)", source.cdcOpenCalls.Load())
	}
	if source.resumeOpenCalls.Load() != 0 {
		t.Errorf("OpenSnapshotStreamFromPosition called %d times; want 0 (a cursor-less restart is a fresh cold-start, not a resume)", source.resumeOpenCalls.Load())
	}
	if err == nil {
		t.Error("Run returned nil; want the forced cold-start path to be taken (and unwind at the unconfigured test target)")
	}
}

// copyResumeEngine is a recording ir.Engine that optionally implements
// ir.SnapshotStreamResumer. It records which open path the streamer takes
// so the routing tests can assert bulk-resume vs plain-CDC dispatch.
type copyResumeEngine struct {
	name      string
	caps      ir.Capabilities
	cdcReader *capturingCDCReader

	// carriesCursor is what PositionCarriesCopyCursor reports. When false,
	// the engine still implements the resumer surface (so the type-assert
	// in runOnce succeeds) but reports "no cursor", exercising the
	// cursor-less → plain-CDC branch.
	carriesCursor bool

	// schemaOneTable makes OpenSchemaReader return a one-table schema so
	// coldStart proceeds past the empty-schema short-circuit to the
	// snapshot open (where the routing decision is observable).
	schemaOneTable bool

	resumeOpenCalls   atomic.Int32
	snapshotOpenCalls atomic.Int32
	cdcOpenCalls      atomic.Int32
}

func (e *copyResumeEngine) Name() string                  { return e.name }
func (e *copyResumeEngine) Capabilities() ir.Capabilities { return e.caps }

func (e *copyResumeEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &copyResumeSchemaReader{oneTable: e.schemaOneTable}, nil
}

func (e *copyResumeEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("not used in copy-resume dispatch test")
}

func (e *copyResumeEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not used in copy-resume dispatch test")
}

func (e *copyResumeEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not used in copy-resume dispatch test")
}

func (e *copyResumeEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	e.cdcOpenCalls.Add(1)
	if e.cdcReader == nil {
		return nil, errors.New("no CDC reader configured")
	}
	return e.cdcReader, nil
}

func (e *copyResumeEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used in copy-resume dispatch test")
}

func (e *copyResumeEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	e.snapshotOpenCalls.Add(1)
	return nil, errors.New("not used in copy-resume dispatch test")
}

// PositionCarriesCopyCursor + OpenSnapshotStreamFromPosition make
// copyResumeEngine an ir.SnapshotStreamResumer.
func (e *copyResumeEngine) PositionCarriesCopyCursor(ir.Position) bool {
	return e.carriesCursor
}

func (e *copyResumeEngine) OpenSnapshotStreamFromPosition(
	context.Context, string, ir.Position, []string,
) (*ir.SnapshotStream, error) {
	e.resumeOpenCalls.Add(1)
	return nil, errors.New("copy-resume dispatch test: no snapshot produced")
}

type copyResumeSchemaReader struct{ oneTable bool }

func (r *copyResumeSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	if !r.oneTable {
		return &ir.Schema{}, nil
	}
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
	}}}, nil
}
