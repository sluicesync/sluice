// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// checkpointRows is a snapshot row reader that opts into COPY checkpoints
// and keeps the sink it was handed.
type checkpointRows struct {
	ir.RowReader
	fn ir.CopyCheckpointFunc
}

func (r *checkpointRows) SetCopyCheckpoint(fn ir.CopyCheckpointFunc) { r.fn = fn }

// positionRecorder is an applier that records every position written.
type positionRecorder struct {
	ir.ChangeApplier
	written []ir.Position
}

func (p *positionRecorder) WritePosition(_ context.Context, _ string, pos ir.Position) error {
	p.written = append(p.written, pos)
	return nil
}

// cursorSource is a resumer whose positions carry a copy cursor when their
// token is "cursor".
type cursorSource struct{ stubEngine }

func (cursorSource) PositionCarriesCopyCursor(from ir.Position) bool { return from.Token == "cursor" }

func (cursorSource) OpenSnapshotStreamFromPosition(context.Context, string, ir.Position, []string) (*ir.SnapshotStream, error) {
	panic("not called")
}

var _ ir.SnapshotStreamResumer = cursorSource{}

// TestApplyCopyCheckpoint_PersistsOnlyAPositionTheNextStartResumes pins
// VSTREAM-COPY-CURSORLESS-CHECKPOINT-WARM-RESUMES: a mid-COPY checkpoint
// whose position carries no copy cursor is the shape a COMPLETED copy
// leaves, so the next start would warm-resume from it and never copy the
// rest. It is not persisted; one with a cursor is. A source that cannot
// resume a COPY gets no sink at all.
func TestApplyCopyCheckpoint_PersistsOnlyAPositionTheNextStartResumes(t *testing.T) {
	ctx := context.Background()
	rows, applier := &checkpointRows{}, &positionRecorder{}
	applyCopyCheckpoint(rows, applier, cursorSource{}, "s1")
	if rows.fn == nil {
		t.Fatal("a resumer source got no checkpoint sink")
	}
	cursorless := ir.Position{Engine: "mysql", Token: "no-cursor"}
	withCursor := ir.Position{Engine: "mysql", Token: "cursor"}
	for _, pos := range []ir.Position{cursorless, withCursor, cursorless} {
		if err := rows.fn(ctx, pos); err != nil {
			t.Fatalf("checkpoint %v: %v", pos, err)
		}
	}
	if len(applier.written) != 1 || applier.written[0] != withCursor {
		t.Fatalf("persisted %v; want only the position that carries a copy cursor", applier.written)
	}

	plain := &checkpointRows{}
	applyCopyCheckpoint(plain, &positionRecorder{}, stubEngine{}, "s1")
	if plain.fn != nil {
		t.Fatal("a source that cannot resume a COPY got a checkpoint sink; any mid-COPY position it persisted would read as a completed copy")
	}
}
