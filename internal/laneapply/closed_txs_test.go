// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// closedTxSeam is countingSeam recording the closed transactions each
// checkpoint hands to WriteCheckpoint — the ADR-0190 garbage-collection feed.
type closedTxSeam struct {
	countingSeam
	mu2    sync.Mutex
	closed [][]string
}

func (s *closedTxSeam) WriteCheckpoint(_ context.Context, _ ir.Position, _ int64, closedTxs []string) error {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.closed = append(s.closed, append([]string(nil), closedTxs...))
	return nil
}

// TestOrchestrator_CheckpointHandsEveryClosedTransactionOnce pins the lane
// path's apply-mark GC feed: every identity-carrying transaction whose commit a
// persisted checkpoint passes is handed to WriteCheckpoint exactly once, in
// source order, and a transaction with no identity never is.
func TestOrchestrator_CheckpointHandsEveryClosedTransactionOnce(t *testing.T) {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	ins := func(p, id, tx string, seq uint64) ir.Change {
		return ir.Insert{
			Position: tok(p), Schema: "ks", Table: "t", Row: ir.Row{"id": id},
			ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	stream := []ir.Change{
		ir.TxBegin{Position: tok("p0")},
		ins("p0", "a", "tx1", 1), ins("p0", "b", "tx1", 2),
		ir.TxCommit{Position: tok("c1")},
		ir.TxBegin{Position: tok("c1")},
		ins("c1", "c", "", 0),
		ir.TxCommit{Position: tok("c2")}, // no identity
		ir.TxBegin{Position: tok("c2")},
		ins("c2", "a", "tx3", 1),
		ir.TxCommit{Position: tok("c3")},
	}
	seam := &closedTxSeam{}
	o := NewOrchestrator(Config{Lanes: 3, MaxBatchSize: 4}, seam)
	ch := make(chan ir.Change, len(stream))
	for _, c := range stream {
		ch <- c
	}
	close(ch)
	if err := o.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var all []string
	for _, c := range seam.closed {
		all = append(all, c...)
	}
	if len(all) != 2 || all[0] != "tx1" || all[1] != "tx3" {
		t.Fatalf("checkpoints handed closed transactions %v (per checkpoint %v); want [tx1 tx3], each once, in order", all, seam.closed)
	}
}
