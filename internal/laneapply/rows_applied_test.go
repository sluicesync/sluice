// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// countingSeam is a no-DB [LaneApplier] that routes row changes to lanes BY
// the row's "id" (so distinct keys spread across the W lanes, exercising the
// cross-lane aggregation) and records every rowsApplied delta the coordinator
// passes to WriteCheckpoint. A row with no "id" routes to the barrier
// (keyless), proving a barriered DML change is still counted. Every apply
// succeeds, so the run always drains and the final checkpoint fires.
type countingSeam struct {
	mu         sync.Mutex
	rowsDeltas []int64
}

func (s *countingSeam) PKValuesForRouting(_ context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error) {
	var row ir.Row
	switch v := c.(type) {
	case ir.Insert:
		row = v.Row
	case ir.Update:
		row = v.After
	case ir.Delete:
		row = v.Before
	default:
		return "", nil, false, nil
	}
	id, has := row["id"]
	if !has {
		// Keyless → barrier (still counted as DML in handle).
		return "", nil, false, nil
	}
	return "ks.t", []any{id}, true, nil
}

func (s *countingSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change) (int, error) {
	return len(batch), nil
}

func (s *countingSeam) ClassifyError(err error) error { return err }

func (s *countingSeam) WriteCheckpoint(_ context.Context, _ ir.Position, rowsApplied int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsDeltas = append(s.rowsDeltas, rowsApplied)
	return nil
}

func (s *countingSeam) ApplyBarrierChange(context.Context, ir.Change) error { return nil }

func (s *countingSeam) total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, d := range s.rowsDeltas {
		total += d
	}
	return total
}

func (s *countingSeam) anyNegative() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.rowsDeltas {
		if d < 0 {
			return true
		}
	}
	return false
}

// TestOrchestrator_RowsApplied_ExactUnderLaneApply is the load-bearing
// concurrent-aggregation pin (ADR-0156 phase 2). Across W in-order lanes
// committing independently, the cumulative rows_applied written at the
// durable-across-lanes checkpoint boundaries must equal EXACTLY the number of
// row-level DML changes applied — no double-count, no under-count — with
// TRUNCATE / Tx markers excluded and a keyless (barriered) DML change still
// counted. The count is realized only at frontier boundaries, so it can never
// count a change the lanes didn't commit.
func TestOrchestrator_RowsApplied_ExactUnderLaneApply(t *testing.T) {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	ins := func(p, id string) ir.Change {
		return ir.Insert{Position: tok(p), Schema: "ks", Table: "t", Row: ir.Row{"id": id}}
	}
	upd := func(p, id string) ir.Change {
		return ir.Update{Position: tok(p), Schema: "ks", Table: "t", Before: ir.Row{"id": id}, After: ir.Row{"id": id}}
	}
	del := func(p, id string) ir.Change {
		return ir.Delete{Position: tok(p), Schema: "ks", Table: "t", Before: ir.Row{"id": id}}
	}
	keylessIns := func(p string) ir.Change {
		return ir.Insert{Position: tok(p), Schema: "ks", Table: "t", Row: ir.Row{"note": "no-id"}}
	}

	// The stream: three transactions (marker stream) with mixed DML across
	// distinct keys (spread over the lanes), a TRUNCATE barrier between them
	// (not counted), and one keyless insert (barriered, still counted). Total
	// row-level DML = 3 + 2 + 2 = 7.
	changes := []ir.Change{
		// Tx1: 3 DML across distinct keys.
		ir.TxBegin{Position: tok("t1")},
		ins("t1", "1"), ins("t1", "2"), upd("t1", "3"),
		ir.TxCommit{Position: tok("t1c")},
		// Tx2: 2 DML.
		ir.TxBegin{Position: tok("t2")},
		del("t2", "4"), ins("t2", "5"),
		ir.TxCommit{Position: tok("t2c")},
		// A TRUNCATE barrier (NOT row-level DML — excluded).
		ir.Truncate{Position: tok("trunc"), Schema: "ks", Table: "t"},
		// Tx3: a keyless insert (barriered) + a keyed insert = 2 DML.
		ir.TxBegin{Position: tok("t3")},
		keylessIns("t3"), ins("t3", "6"),
		ir.TxCommit{Position: tok("t3c")},
	}
	const wantDML = 7

	// Run across several lane counts (serial through W>DML) — the aggregated
	// total must be identical regardless of how the keys shard across lanes.
	for _, lanes := range []int{1, 2, 4, 8} {
		seam := &countingSeam{}
		orch := NewOrchestrator(Config{Lanes: lanes, MaxBatchSize: 4}, seam)
		ch := make(chan ir.Change, len(changes))
		for _, c := range changes {
			ch <- c
		}
		close(ch)
		if err := orch.Run(context.Background(), ch); err != nil {
			t.Fatalf("lanes=%d: Run: %v", lanes, err)
		}
		if seam.anyNegative() {
			t.Fatalf("lanes=%d: a checkpoint carried a NEGATIVE rows delta: %v", lanes, seam.rowsDeltas)
		}
		if got := seam.total(); got != wantDML {
			t.Fatalf("lanes=%d: cumulative rows_applied = %d; want %d (exact — no double/under-count)\ndeltas: %v",
				lanes, got, wantDML, seam.rowsDeltas)
		}
	}
}

// TestOrchestrator_RowsApplied_MarkerlessStream pins the count on a
// marker-LESS stream (VStream shape): no Tx* markers, each row carries a
// distinct monotone position token, and the position-run heuristic settles
// boundaries. The cumulative rows_applied must still equal the exact DML
// count.
func TestOrchestrator_RowsApplied_MarkerlessStream(t *testing.T) {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	ins := func(p, id string) ir.Change {
		return ir.Insert{Position: tok(p), Schema: "ks", Table: "t", Row: ir.Row{"id": id}}
	}

	// Five row changes, each with its own position token (VStream advances the
	// VGTID per source tx; here each is a distinct boundary).
	changes := []ir.Change{
		ins("p1", "1"), ins("p2", "2"), ins("p3", "3"), ins("p4", "4"), ins("p5", "5"),
	}
	for _, lanes := range []int{1, 3} {
		seam := &countingSeam{}
		orch := NewOrchestrator(Config{Lanes: lanes, MaxBatchSize: 2}, seam)
		ch := make(chan ir.Change, len(changes))
		for _, c := range changes {
			ch <- c
		}
		close(ch)
		if err := orch.Run(context.Background(), ch); err != nil {
			t.Fatalf("lanes=%d: Run: %v", lanes, err)
		}
		if got := seam.total(); got != int64(len(changes)) {
			t.Fatalf("lanes=%d: cumulative rows_applied = %d; want %d\ndeltas: %v",
				lanes, got, len(changes), seam.rowsDeltas)
		}
	}
}
