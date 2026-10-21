// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// checkpointRows is a snapshot row reader that opts into COPY checkpoints
// and records whether a sink was ever handed to it.
type checkpointRows struct {
	ir.RowReader
	wired bool
}

func (r *checkpointRows) SetCopyCheckpoint(ir.CopyCheckpointFunc) { r.wired = true }

var _ ir.CopyCheckpointer = (*checkpointRows)(nil)

// checkpointingSnapshotSource opens a snapshot whose row reader accepts a
// COPY checkpoint sink, as the VStream cold-start reader does.
type checkpointingSnapshotSource struct {
	stubEngine
	rows *checkpointRows
}

func (c checkpointingSnapshotSource) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return &ir.SnapshotStream{Rows: c.rows}, nil
}

// TestColdStartOpenSnapshot_WiresNoCopyCheckpoint pins the withdrawal of
// the VStream mid-COPY resume (VSTREAM-COPY-RESUME-WITHDRAWN): a cold start
// hands no checkpoint sink to a snapshot reader that would accept one, so no
// mid-COPY position is ever persisted. A persisted cursor is what a resume
// started from, and a resumed copy kept every row the source deleted while
// the stream was stopped; the refusal of one written by an older release is
// pinned by TestStreamer_InterruptedColdStart_RefusesTheCursor.
func TestColdStartOpenSnapshot_WiresNoCopyCheckpoint(t *testing.T) {
	rows := &checkpointRows{}
	s := &Streamer{Source: checkpointingSnapshotSource{rows: rows}, SourceDSN: "src"}
	stream, err := s.coldStartOpenSnapshot(context.Background(), nil)
	if err != nil {
		t.Fatalf("coldStartOpenSnapshot: %v", err)
	}
	if stream.Rows != rows {
		t.Fatal("the cold start did not open the source's snapshot, so this test checks nothing")
	}
	if rows.wired {
		t.Fatal("the cold start wired a COPY checkpoint sink: a mid-COPY position would be persisted, and the next start would resume from it")
	}
}

// populatedTarget is a target whose tables all hold rows.
type populatedTarget struct{ emptyCheckingDropper }

func (populatedTarget) IsTableEmpty(context.Context, *ir.Table) (bool, error) { return false, nil }

// TestColdStartGate_RestartFromScratchSchemaAlreadyApplied_RefusesAPopulatedTarget
// pins RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED: under --schema-already-applied
// the clear --restart-from-scratch promises is skipped, so on an upsert
// reader with rows on the target the re-copy would keep every row the source
// deleted; it refuses, drops nothing, and lets an emptied target through.
func TestColdStartGate_RestartFromScratchSchemaAlreadyApplied_RefusesAPopulatedTarget(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:               &copyResumeEngine{name: "planetscale"},
			Target:               newRecordingEngine("planetscale"),
			RestartFromScratch:   true,
			SchemaAlreadyApplied: true,
		}
	}

	populated := &populatedTarget{}
	_, err := newStreamer().coldStartGatePreflight(context.Background(), schema, nil, populated,
		gateStream(idempotentReader{}), &stubChangeApplier{}, "stream-1", false, freshCopyOperatorRestart)
	if err == nil || !strings.Contains(err.Error(), restartUnclearedMarker) {
		t.Fatalf("populated target: err = %v; want the %s refusal", err, restartUnclearedMarker)
	}
	if len(populated.dropped) != 0 {
		t.Errorf("the refusal dropped %v; --schema-already-applied tables must never be dropped", populated.dropped)
	}

	emptied := &emptyCheckingDropper{}
	if _, err := newStreamer().coldStartGatePreflight(context.Background(), schema, nil, emptied,
		gateStream(idempotentReader{}), &stubChangeApplier{}, "stream-1", false, freshCopyOperatorRestart); err != nil &&
		strings.Contains(err.Error(), restartUnclearedMarker) {
		t.Fatalf("an emptied target was refused: %v", err)
	}
}
