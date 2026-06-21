// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// These pins guard the concurrent-apply checkpoint-boundary fix (the
// counterpart of the serial item-29 / v0.99.89 fix). Found live on a
// native-MySQL file/pos run: the orchestrator recorded a checkpoint boundary on
// every position-TOKEN change, which on file/pos (where every binlog event has a
// distinct LogPos) recorded mid-transaction ROW positions as boundaries — and
// CheckpointPosition then persisted one. Warm-resume from a mid-tx file/pos
// position fails with "no corresponding table map event" (go-mysql reads a ROWS
// event whose TABLE_MAP was earlier in the same tx), crash-looping the stream.
//
// The fix: on a stream that emits Tx markers (binlog-MySQL, Postgres), record a
// boundary ONLY at TxCommit (and the DDL-boundary Truncate); marker-LESS streams
// (VStream, whose VGTID token is tx-stable) keep the position-run heuristic.

// TestOrchestrator_MarkerStream_InterruptedMidTx_NeverPersistsMidTxPosition is
// THE regression pin — it reproduces the live trigger. A first transaction
// commits (establishing a good TxCommit boundary), then a SECOND transaction is
// INTERRUPTED mid-way (TxBegin → rows, but the stream ends with NO TxCommit, as a
// crash / watchdog-bounce / periodic-checkpoint-mid-tx would leave it). On a
// file/pos stream (distinct LogPos per event) the persisted checkpoint must stay
// at the first tx's COMMIT boundary — never a mid-transaction row position of the
// interrupted tx (which is unresumable: warm-resume from it crash-loops with "no
// corresponding table map event"; the interrupted tx is instead re-read +
// idempotently re-applied from the prior boundary).
//
// Without the fix the orchestrator's end-of-stream record (and the position-run
// heuristic) recorded the last mid-tx ROW position as the boundary, and
// CheckpointPosition returned it — the exact crash-loop position seen live.
func TestOrchestrator_MarkerStream_InterruptedMidTx_NeverPersistsMidTxPosition(t *testing.T) {
	// file/pos tokens: a distinct LogPos per binlog event (the shape that broke).
	const (
		pBegin1  = `{"mode":"file_pos","file":"b.204","pos":131340000}`
		pRow1    = `{"mode":"file_pos","file":"b.204","pos":131345000}`
		pCommit1 = `{"mode":"file_pos","file":"b.204","pos":131349000}` // tx1 boundary — the safe resume point
		pBegin2  = `{"mode":"file_pos","file":"b.204","pos":131350000}`
		pRow2a   = `{"mode":"file_pos","file":"b.204","pos":131350080}` // mid-tx (the live crash position shape)
		pRow2b   = `{"mode":"file_pos","file":"b.204","pos":131358276}` // mid-tx — last event before the interrupt
	)
	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)

	changes := make(chan ir.Change, 8)
	changes <- ir.TxBegin{Position: pos(pBegin1)}
	changes <- ir.Insert{Position: pos(pRow1), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}
	changes <- ir.TxCommit{Position: pos(pCommit1)}
	changes <- ir.TxBegin{Position: pos(pBegin2)}
	changes <- ir.Insert{Position: pos(pRow2a), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(2)}}
	changes <- ir.Insert{Position: pos(pRow2b), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(3)}}
	close(changes) // interrupted mid-tx2: no TxCommit

	if err := orch.Run(context.Background(), changes); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// No checkpoint may EVER carry a mid-transaction position — those are
	// unresumable on MySQL file/pos (the warm-resume crash-loop).
	for i, tok := range seam.checkpoints {
		switch tok {
		case pRow1, pRow2a, pRow2b:
			t.Fatalf("checkpoint[%d] persisted a MID-TRANSACTION row position %q — "+
				"unresumable on MySQL file/pos (the warm-resume crash-loop bug)", i, tok)
		case pBegin1, pBegin2:
			t.Fatalf("checkpoint[%d] persisted a TxBegin position %q; only TxCommit is the resume point", i, tok)
		}
	}

	// The persisted position must be tx1's COMMIT boundary — the interrupted tx2
	// is replayed (idempotently) from there on warm-resume.
	last, ok := seam.lastCheckpoint()
	if !ok {
		t.Fatal("no checkpoint persisted; want tx1's TxCommit boundary")
	}
	if last != pCommit1 {
		t.Errorf("final persisted position = %q; want tx1's TxCommit boundary %q (never a mid-tx2 row)", last, pCommit1)
	}
}

// TestOrchestrator_MarkerStream_TruncateIsABoundary pins that a Truncate (a DDL
// statement boundary, auto-committed and not wrapped in BEGIN/XID) IS a valid
// resume point on a marker stream: its position is recorded so the checkpoint
// can advance to it.
func TestOrchestrator_MarkerStream_TruncateIsABoundary(t *testing.T) {
	const (
		pBegin  = `{"mode":"file_pos","file":"b.1","pos":100}`
		pRow    = `{"mode":"file_pos","file":"b.1","pos":200}`
		pCommit = `{"mode":"file_pos","file":"b.1","pos":300}`
		pTrunc  = `{"mode":"file_pos","file":"b.1","pos":400}`
	)
	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)

	changes := make(chan ir.Change, 8)
	changes <- ir.TxBegin{Position: pos(pBegin)}
	changes <- ir.Insert{Position: pos(pRow), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}
	changes <- ir.TxCommit{Position: pos(pCommit)}
	changes <- ir.Truncate{Position: pos(pTrunc), Schema: "ks", Table: "t"}
	close(changes)

	if err := orch.Run(context.Background(), changes); err != nil {
		t.Fatalf("Run: %v", err)
	}
	last, ok := seam.lastCheckpoint()
	if !ok {
		t.Fatal("no checkpoint persisted")
	}
	if last != pTrunc {
		t.Errorf("final persisted position = %q; want the Truncate DDL boundary %q", last, pTrunc)
	}
	for i, tok := range seam.checkpoints {
		if tok == pRow {
			t.Fatalf("checkpoint[%d] persisted a mid-tx row position %q", i, tok)
		}
	}
}

// TestOrchestrator_MarkerLessStream_TokenRunUnchanged is the VStream regression
// guard: a marker-LESS stream (no TxBegin/TxCommit) whose position token is
// STABLE within a source transaction and changes only at the boundary must keep
// using the position-run heuristic — the last change of each run is the boundary.
// This is the behavior VStream relies on; the fix must not change it.
func TestOrchestrator_MarkerLessStream_TokenRunUnchanged(t *testing.T) {
	// VGTID-style tokens: stable within a tx, change at the tx boundary.
	const (
		pTx1 = `[{"keyspace":"c","shard":"-80","gtid":"MySQL56/uuid:1-100"}]`
		pTx2 = `[{"keyspace":"c","shard":"-80","gtid":"MySQL56/uuid:1-101"}]`
	)
	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)

	// Two transactions, each two rows, NO Tx markers (VStream shape). Rows in a
	// tx share the VGTID token; the token changes at the next tx.
	changes := make(chan ir.Change, 8)
	changes <- ir.Insert{Position: pos(pTx1), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}
	changes <- ir.Insert{Position: pos(pTx1), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(2)}}
	changes <- ir.Insert{Position: pos(pTx2), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(3)}}
	changes <- ir.Insert{Position: pos(pTx2), Schema: "ks", Table: "t", Row: ir.Row{"id": int64(4)}}
	close(changes)

	if err := orch.Run(context.Background(), changes); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The token-run heuristic records pTx1 (when the token changes to pTx2) and
	// pTx2 (at end-of-stream); the final persisted position is the last run's.
	last, ok := seam.lastCheckpoint()
	if !ok {
		t.Fatal("no checkpoint persisted on the marker-less path (the token-run heuristic must still work for VStream)")
	}
	if last != pTx2 {
		t.Errorf("final persisted position = %q; want the last tx-stable token %q (VStream token-run unchanged)", last, pTx2)
	}
}
