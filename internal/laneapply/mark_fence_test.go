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
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fenceSeam is countingSeam whose table "m" is a marked (non-idempotent)
// class: ApplyMarkTx names the change's transaction for it. It logs, in
// order, every checkpoint write and every fence, with the rows the lanes had
// committed when the fence cleared.
type fenceSeam struct {
	countingSeam
	mu2         sync.Mutex
	applied     []string
	events      []string
	markTxAsked int
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
	s.mu2.Lock()
	s.markTxAsked++
	s.mu2.Unlock()
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

// markFenceStream is three transactions: tx1 and tx3 send marked changes
// (table "m") to the lanes, tx2 only idempotent ones.
func markFenceStream() []ir.Change {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	ins := func(p, table, id, tx string, seq uint64) ir.Change {
		return ir.Insert{
			Position: tok(p), Schema: "ks", Table: table, Row: ir.Row{"id": id},
			ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	return []ir.Change{
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
}

// runFenceStream runs stream through a fresh orchestrator over seam and
// reports how long Run took.
func runFenceStream(t *testing.T, cfg Config, seam LaneApplier, stream []ir.Change) time.Duration {
	t.Helper()
	o := NewOrchestrator(cfg, seam)
	ch := make(chan ir.Change, len(stream))
	for _, c := range stream {
		ch <- c
	}
	close(ch)
	start := time.Now()
	if err := o.Run(context.Background(), ch); err != nil {
		t.Fatalf("lanes=%d: Run: %v", cfg.Lanes, err)
	}
	return time.Since(start)
}

// TestOrchestrator_MarkFence pins ADR-0190 amendment A on the orchestrator,
// with --exactly-once-lanes: the first marked change of a transaction is
// routed only after every lane has committed everything before it and the
// checkpoint has persisted the transaction's start; a second marked change of
// the same transaction does not fence again; an idempotent-only transaction
// never fences.
func TestOrchestrator_MarkFence(t *testing.T) {
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: true}, seam, markFenceStream())
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

// TestOrchestrator_MarkFence_OffByDefault pins ADR-0190 amendment C: without
// ExactlyOnceLanes (the zero value, the default) the coordinator never asks
// which transaction a lane change would mark and never fences — the lane path
// runs as it did before ADR-0190 — and every change still reaches a lane.
func TestOrchestrator_MarkFence_OffByDefault(t *testing.T) {
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8}, seam, markFenceStream())
		seam.mu2.Lock()
		asked, events, applied := seam.markTxAsked, slices.Clone(seam.events), len(seam.applied)
		seam.mu2.Unlock()
		if asked != 0 {
			t.Errorf("lanes=%d: the coordinator asked ApplyMarkTx %d times with exactly-once lanes off; want 0", lanes, asked)
		}
		for _, e := range events {
			if strings.HasPrefix(e, "fence:") {
				t.Errorf("lanes=%d: fenced (%s) with exactly-once lanes off: %v", lanes, e, events)
			}
		}
		if applied != 6 {
			t.Errorf("lanes=%d: %d changes reached a lane; want 6", lanes, applied)
		}
	}
}

// TestOrchestrator_DrainDoesNotWaitOutTheIdleGrace pins the drain sentinel
// (drainLanes): a mark fence or a barrier must not wait for a lane holding a
// partial batch to hit its idle-flush grace. The grace here is 5 s; before
// the sentinel each drain whose predecessor sat in a partial lane batch cost
// one full grace (the 2026-09-29 benchmark's ~100 ms per marked transaction),
// so each stream below — two fences, or two barriers — took 10 s or more.
func TestOrchestrator_DrainDoesNotWaitOutTheIdleGrace(t *testing.T) {
	const grace = 5 * time.Second
	pos := func(n string) ir.Position { return ir.Position{Engine: "test", Token: n} }
	var barrierStream []ir.Change
	for i, id := range []string{"a", "b", "c", "d"} {
		barrierStream = append(barrierStream, ir.Insert{Position: pos(id), Schema: "ks", Table: "t", Row: ir.Row{"id": id}})
		if i%2 == 1 { // a row with no key: the barrier path
			barrierStream = append(barrierStream, ir.Insert{Position: pos(id + "-kl"), Schema: "ks", Table: "kl", Row: ir.Row{"v": id}})
		}
	}
	for _, lanes := range []int{1, 3} {
		for _, tc := range []struct {
			name   string
			cfg    Config
			stream []ir.Change
		}{
			{"mark fence", Config{Lanes: lanes, MaxBatchSize: 8, IdleFlushPeriod: grace, ExactlyOnceLanes: true}, markFenceStream()},
			{"barrier", Config{Lanes: lanes, MaxBatchSize: 8, IdleFlushPeriod: grace}, barrierStream},
		} {
			if took := runFenceStream(t, tc.cfg, &fenceSeam{}, tc.stream); took >= grace {
				t.Errorf("lanes=%d, %s: Run took %v; a drain waited out the %v idle grace instead of flushing the lanes",
					lanes, tc.name, took, grace)
			}
		}
	}
}
