// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fenceSeam is countingSeam whose table "m" is a marked (non-idempotent)
// class: ApplyMarkTx names the change's transaction for it. It logs, in
// order, every checkpoint write and every fence, with the rows the lanes had
// committed when the fence cleared.
type fenceSeam struct {
	countingSeam
	mu2     sync.Mutex
	applied []string
	events  []string
}

func (s *fenceSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change) (int, error) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	for _, c := range batch {
		s.applied = append(s.applied, fmt.Sprint(c.(ir.Insert).Row["id"]))
	}
	return len(batch), nil
}

func (s *fenceSeam) WriteCheckpoint(_ context.Context, pos ir.Position, _ int64, _ []string) error {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.events = append(s.events, "ckpt:"+pos.Token)
	return nil
}

func (s *fenceSeam) ApplyMarkTx(_ context.Context, c ir.Change) string {
	if _, table := RowChangeSchemaTable(c); table == "m" {
		return ir.ApplyIDOf(c).TxID
	}
	return ""
}

func (s *fenceSeam) ApplyMarksFenced(txID string) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	committed := slices.Clone(s.applied)
	slices.Sort(committed)
	s.events = append(s.events, "fence:"+txID+" committed="+strings.Join(committed, ","))
}

// TestOrchestrator_MarkFence pins ADR-0190 amendment A on the orchestrator:
// the first marked change of a transaction is routed only after every lane
// has committed everything before it and the checkpoint has persisted the
// transaction's start; a second marked change of the same transaction does
// not fence again; an idempotent-only transaction never fences.
func TestOrchestrator_MarkFence(t *testing.T) {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	ins := func(p, table, id, tx string, seq uint64) ir.Change {
		return ir.Insert{
			Position: tok(p), Schema: "ks", Table: table, Row: ir.Row{"id": id},
			ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	stream := []ir.Change{
		ir.TxBegin{Position: tok("p0")},
		ins("p0", "t", "a", "tx1", 1), ins("p0", "m", "b", "tx1", 1), ins("p0", "m", "c", "tx1", 2),
		ir.TxCommit{Position: tok("c1")},
		ir.TxBegin{Position: tok("c1")},
		ins("c1", "t", "d", "tx2", 1),
		ir.TxCommit{Position: tok("c2")},
		ir.TxBegin{Position: tok("c2")},
		ins("c2", "t", "e", "tx3", 1), ins("c2", "m", "f", "tx3", 1),
		ir.TxCommit{Position: tok("c3")},
	}
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		o := NewOrchestrator(Config{Lanes: lanes, MaxBatchSize: 8}, seam)
		ch := make(chan ir.Change, len(stream))
		for _, c := range stream {
			ch <- c
		}
		close(ch)
		if err := o.Run(context.Background(), ch); err != nil {
			t.Fatalf("lanes=%d: Run: %v", lanes, err)
		}
		var fences []string
		for i, e := range seam.events {
			if !strings.HasPrefix(e, "fence:") {
				continue
			}
			fences = append(fences, e)
			// tx3's fence must follow the checkpoint of tx2's commit — the
			// persisted position sits at tx3's start.
			if strings.HasPrefix(e, "fence:tx3") && (i == 0 || seam.events[i-1] != "ckpt:c2") {
				t.Errorf("lanes=%d: tx3's fence was not preceded by the checkpoint at its start (ckpt:c2): %v", lanes, seam.events)
			}
		}
		want := []string{"fence:tx1 committed=a", "fence:tx3 committed=a,b,c,d,e"}
		if !slices.Equal(fences, want) {
			t.Errorf("lanes=%d: fences %v; want %v (once per marked transaction, after every earlier change committed)\nevents: %v",
				lanes, fences, want, seam.events)
		}
	}
}
