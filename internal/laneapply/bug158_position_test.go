// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingSeam is a no-DB [LaneApplier] for the Bug-158 orchestrator pins: it
// applies lane/barrier changes in-memory (always succeeding) and RECORDS every
// WriteCheckpoint position + every ApplyBarrierChange the orchestrator drives,
// so a test can assert WHICH position the frontier coordinator persists. It
// must NOT expose an InvalidateMetadataCaches method: that the orchestrator no
// longer calls one (the Bug-158 over-invalidation) is enforced structurally by
// the interface — recordingSeam compiling against [LaneApplier] is itself the
// guard.
type recordingSeam struct {
	mu          sync.Mutex
	checkpoints []string    // tokens passed to WriteCheckpoint, in order
	barriers    []ir.Change // changes passed to ApplyBarrierChange, in order
}

func (s *recordingSeam) PKValuesForRouting(_ context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error) {
	// Route every Insert/Update/Delete to a single lane by a fixed key so the
	// stream's ordering is deterministic; non-row events go to the barrier.
	switch c.(type) {
	case ir.Insert, ir.Update, ir.Delete:
		return "ks.t", []any{int64(1)}, true, nil
	}
	return "", nil, false, nil
}

func (s *recordingSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change) (int, error) {
	return len(batch), nil
}

func (s *recordingSeam) ClassifyError(err error) error { return err }

func (s *recordingSeam) WriteCheckpoint(_ context.Context, pos ir.Position, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints = append(s.checkpoints, pos.Token)
	return nil
}

func (s *recordingSeam) ApplyBarrierChange(_ context.Context, c ir.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.barriers = append(s.barriers, c)
	return nil
}

func (s *recordingSeam) lastCheckpoint() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.checkpoints) == 0 {
		return "", false
	}
	return s.checkpoints[len(s.checkpoints)-1], true
}

// TestOrchestrator_FirstBoundarySchemaSnapshotNeverBecomesResumePosition is
// the Bug-158 orchestrator-level pin (the position half). A stream whose first
// event is a SchemaSnapshot carrying a metadata-anchored token (pgoutput's
// first-touch RelationMessage WAL position 0/0) followed by real row+Tx events
// must NEVER persist that 0/0 token as the resume position: the SchemaSnapshot
// is excluded from boundary tracking and the concurrent barrier applies
// position-free (the frontier checkpoint owns the resume position). The
// persisted position must be the real Tx-boundary token of the surrounding
// rows.
//
// Pre-fix, the barrier wrote `lastWrittenTok = snapshot.Pos().Token` (0/0) and
// recorded a (seq, 0/0) tx boundary that CheckpointPosition returned, pinning
// the persisted position at 0/0 forever — warm-resume could never reach the
// right LSN (the operational half of Bug 158).
func TestOrchestrator_FirstBoundarySchemaSnapshotNeverBecomesResumePosition(t *testing.T) {
	const zeroTok = `{"slot":"s","lsn":"0/0"}`
	const txTok = `{"slot":"s","lsn":"4/100"}`

	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres", Token: tok} }

	// The Bug-158 stream shape: SchemaSnapshot (0/0) barrier FIRST, then a real
	// transaction (TxBegin → Insert → TxCommit) carrying a real position. Pre-fix,
	// the orchestrator's noteBoundary ran for the SchemaSnapshot, so the
	// 0/0→txTok position change recorded a spurious (snapshotSeq, 0/0) tx
	// boundary that CheckpointPosition could then return as the resume point;
	// excluding the SchemaSnapshot from boundary tracking removes that 0/0
	// boundary entirely.
	changes := make(chan ir.Change, 8)
	changes <- ir.SchemaSnapshot{Position: pos(zeroTok), Schema: "ks", Table: "t", IR: &ir.Table{Name: "t"}}
	changes <- ir.TxBegin{Position: pos(txTok)}
	changes <- ir.Insert{Position: pos(txTok), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}
	changes <- ir.TxCommit{Position: pos(txTok)}
	close(changes)

	if err := orch.Run(context.Background(), changes); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The SchemaSnapshot reached the barrier (it was applied).
	if len(seam.barriers) != 1 {
		t.Fatalf("ApplyBarrierChange calls = %d; want 1 (the SchemaSnapshot)", len(seam.barriers))
	}
	if _, ok := seam.barriers[0].(ir.SchemaSnapshot); !ok {
		t.Fatalf("barrier change = %T; want ir.SchemaSnapshot", seam.barriers[0])
	}

	// No persisted checkpoint may carry the SchemaSnapshot's 0/0 token.
	for i, tok := range seam.checkpoints {
		if tok == zeroTok {
			t.Fatalf("checkpoint[%d] persisted the SchemaSnapshot's 0/0 metadata token — "+
				"a barrier's metadata-anchored position must never become the resume position (Bug 158)", i)
		}
	}

	// The final persisted position must be the real Tx-boundary token.
	last, ok := seam.lastCheckpoint()
	if !ok {
		t.Fatal("no checkpoint persisted; want the real Tx-boundary token (the frontier must advance past the 0/0 baseline)")
	}
	if last != txTok {
		t.Errorf("final persisted position = %q; want the real Tx token %q", last, txTok)
	}
}
