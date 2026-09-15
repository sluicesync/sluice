// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// positionEngine is an engine with a CDC capability on paper whose schema
// reader implements PositionCapturer and answers whatever the test says —
// the shape of the postgres engine talking to a PlanetScale Neki router.
type positionEngine struct {
	capsEngine
	capture func(ctx context.Context, slot string) (ir.Position, error)
}

func (e positionEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &capturingSchemaReader{capture: e.capture}, nil
}

type capturingSchemaReader struct {
	recordingSchemaReader
	capture func(ctx context.Context, slot string) (ir.Position, error)
}

func (r *capturingSchemaReader) CaptureBackupPosition(ctx context.Context, slot string) (ir.Position, error) {
	return r.capture(ctx, slot)
}

var _ irbackup.PositionCapturer = (*capturingSchemaReader)(nil)

func newPositionEngine(capture func(ctx context.Context, slot string) (ir.Position, error)) positionEngine {
	return positionEngine{
		capsEngine: capsEngine{name: "postgres", caps: ir.Capabilities{CDC: ir.CDCLogicalReplication}},
		capture:    capture,
	}
}

// TestRecordEndPosition_UnavailablePositionLeavesEndPositionEmpty pins the
// Neki-source shape: the capturer EXISTS (the engine declares CDC) but this
// server has no position to give, and says so with ErrPositionUnavailable.
// The backup must complete with an empty EndPosition — the same outcome as an
// engine without a capturer — not die at the finalize phase, which is what a
// `backup full` FROM a sharded Neki did on 2026-09-15 (run 34926553073) after
// copying every row.
func TestRecordEndPosition_UnavailablePositionLeavesEndPositionEmpty(t *testing.T) {
	calls := 0
	b := &Backup{Source: newPositionEngine(func(context.Context, string) (ir.Position, error) {
		calls++
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: no WAL position on a router: %w",
			irbackup.ErrPositionUnavailable)
	})}
	manifest := &irbackup.Manifest{SourceEngine: "postgres"}

	// No snapshot, not adopting: the v0.17.x fallback path, which is the one
	// a Neki source takes (its replication connect is refused, so the
	// snapshot opener falls back).
	if err := b.recordEndPosition(context.Background(), manifest, false, ir.Position{}, nil, nil); err != nil {
		t.Fatalf("recordEndPosition must tolerate an UNAVAILABLE position; got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("capturer called %d times; want exactly 1", calls)
	}
	if manifest.EndPosition != (ir.Position{}) {
		t.Fatalf("EndPosition = %+v; want empty when the source has no position", manifest.EndPosition)
	}
}

// TestRecordEndPosition_OtherCaptureErrorsStayLoud is the other direction:
// a capture failure that is NOT the unavailable sentinel must still refuse.
// A lost position on a source that has one is the silent half — a full whose
// EndPosition quietly went empty would anchor the next incremental at "CDC's
// current position" and skip everything in between.
func TestRecordEndPosition_OtherCaptureErrorsStayLoud(t *testing.T) {
	boom := errors.New("connection reset by peer")
	b := &Backup{Source: newPositionEngine(func(context.Context, string) (ir.Position, error) {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: %w", boom)
	})}
	manifest := &irbackup.Manifest{SourceEngine: "postgres"}

	err := b.recordEndPosition(context.Background(), manifest, false, ir.Position{}, nil, nil)
	if err == nil {
		t.Fatal("a capture failure that is not ErrPositionUnavailable was swallowed into an empty EndPosition")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the underlying failure must stay reachable: %v", err)
	}
	if manifest.EndPosition != (ir.Position{}) {
		t.Fatalf("EndPosition = %+v on a failed capture; want untouched", manifest.EndPosition)
	}
}
