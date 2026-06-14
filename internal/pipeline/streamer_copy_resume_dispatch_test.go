// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// v0.99.8 SILENT-DEGRADE fix — routing pin. An INTERRUPTED cold-start
// COPY (persisted position carries a mid-COPY TablePKs cursor) must route
// through coldStart's bulk snapshot resume path
// (SnapshotStreamResumer.OpenSnapshotStreamFromPosition → batched
// bulk-COPY writer), NOT through warmResume's plain CDC reader (per-row
// apply, ~10 rows/sec). A COMPLETED cold-start (cursor-less position)
// must stay on the fast plain-CDC warmResume path.
//
// These tests assert the runOnce dispatch decision in isolation: a
// recording source engine reports whether the cursor was present and
// records WHICH open path the streamer took. coldStart is short-circuited
// right after the routing decision by having the seeded snapshot open
// return an error, so the test exercises the branch without booting a
// real snapshot.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// errResumeRouted is the sentinel the resumer source returns from the
// seeded snapshot open so coldStart unwinds immediately after the routing
// decision (we only care which path was taken, not the full cold-start).
var errResumeRouted = errors.New("resume routed (test short-circuit)")

// TestStreamer_InterruptedColdStart_RoutesToBulkResume asserts that a
// persisted position carrying a mid-COPY cursor drives the bulk snapshot
// resume path — OpenSnapshotStreamFromPosition is called with that exact
// position, and the plain CDC reader's StreamChanges is NEVER reached.
func TestStreamer_InterruptedColdStart_RoutesToBulkResume(t *testing.T) {
	const token = `[{"keyspace":"main","shard":"-","gtid":"MySQL56/abcd:1-100","table_p_ks":[{"table_name":"widgets","lastpk":"AAAA"}]}]`
	cdc := &capturingCDCReader{captured: make(chan struct{})}
	source := &copyResumeEngine{
		name:           "planetscale",
		caps:           ir.Capabilities{CDC: ir.CDCBinlog},
		cdcReader:      cdc,
		carriesCursor:  true,
		resumeOpenErr:  errResumeRouted,
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

	// coldStart's seeded snapshot open returned errResumeRouted; Run wraps
	// it. The load-bearing assertion is the path, not the error text.
	if source.resumeOpenCalls.Load() != 1 {
		t.Errorf("OpenSnapshotStreamFromPosition called %d times; want 1 (interrupted cold-start must take the bulk resume path)",
			source.resumeOpenCalls.Load())
	}
	if cdc.once {
		t.Error("plain CDC StreamChanges was called — interrupted cold-start wrongly routed to the per-row warm-resume path (the ~10 rows/sec silent-degrade bug)")
	}
	if source.cdcOpenCalls.Load() != 0 {
		t.Errorf("OpenCDCReader called %d times; want 0 (bulk resume must not open the plain CDC reader)", source.cdcOpenCalls.Load())
	}
	if gotPos := source.resumeOpenPos.Load(); gotPos == nil || gotPos.Token != token {
		t.Errorf("OpenSnapshotStreamFromPosition got position %v; want token %q", gotPos, token)
	}
	// The resumed COPY must be scoped to the SAME filtered table allowlist a
	// fresh cold-start uses (v0.99.12 table-scope follow-up for the resume
	// path). The one-table schema yields exactly ["widgets"].
	if gotTables := source.resumeOpenTables.Load(); gotTables == nil ||
		len(*gotTables) != 1 || (*gotTables)[0] != "widgets" {
		t.Errorf("OpenSnapshotStreamFromPosition got tables %v; want [\"widgets\"] (resume must carry the filtered allowlist)", gotTables)
	}
	if err == nil {
		t.Error("Run returned nil; want the routed-snapshot error to surface (loud, not silent)")
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

	// resumeOpenErr is returned from OpenSnapshotStreamFromPosition so
	// coldStart unwinds immediately after the routing decision.
	resumeOpenErr error

	// schemaOneTable makes OpenSchemaReader return a one-table schema so
	// coldStart proceeds past the empty-schema short-circuit to the
	// snapshot open (where the routing decision is observable).
	schemaOneTable bool

	resumeOpenCalls  atomic.Int32
	cdcOpenCalls     atomic.Int32
	resumeOpenPos    atomic.Pointer[ir.Position]
	resumeOpenTables atomic.Pointer[[]string]
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
	return nil, errors.New("not used in copy-resume dispatch test")
}

// PositionCarriesCopyCursor + OpenSnapshotStreamFromPosition make
// copyResumeEngine an ir.SnapshotStreamResumer.
func (e *copyResumeEngine) PositionCarriesCopyCursor(ir.Position) bool {
	return e.carriesCursor
}

func (e *copyResumeEngine) OpenSnapshotStreamFromPosition(
	_ context.Context, _ string, from ir.Position, tables []string,
) (*ir.SnapshotStream, error) {
	e.resumeOpenCalls.Add(1)
	p := from
	e.resumeOpenPos.Store(&p)
	tbls := tables
	e.resumeOpenTables.Store(&tbls)
	if e.resumeOpenErr != nil {
		return nil, e.resumeOpenErr
	}
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
