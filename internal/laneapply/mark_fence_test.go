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
// order, every checkpoint write, every fence (with the rows the lanes had
// committed when it cleared), and every fold a lane batch carried (with the
// rows of that batch), and sums the rows_applied increments of both writers.
type fenceSeam struct {
	countingSeam
	mu2         sync.Mutex
	applied     []string
	events      []string
	markTxAsked int
	rowsTotal   int64
}

func (s *fenceSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change, fold *FoldTicket) (int, error) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	ids := make([]string, 0, len(batch))
	for _, c := range batch {
		id := fmt.Sprint(c.(ir.Insert).Row["id"])
		ids = append(ids, id)
		s.applied = append(s.applied, id)
	}
	if fold != nil {
		s.events = append(s.events, fmt.Sprintf("fold:%s pos=%s rows=%d closed=%s batch=%s",
			fold.Tx, fold.Pos.Token, fold.RowsApplied, strings.Join(fold.ClosedTxs, ","), strings.Join(ids, ",")))
		s.rowsTotal += fold.RowsApplied
	}
	return len(batch), nil
}

func (s *fenceSeam) WriteCheckpoint(_ context.Context, pos ir.Position, rowsApplied int64, _ []string) error {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.events = append(s.events, "ckpt:"+pos.Token)
	s.rowsTotal += rowsApplied
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

func (s *fenceSeam) ApplyMarksFenced(txID string, anchored bool) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	committed := slices.Clone(s.applied)
	slices.Sort(committed)
	s.events = append(s.events, fmt.Sprintf("fence:%s anchored=%v committed=%s", txID, anchored, strings.Join(committed, ",")))
}

func (s *fenceSeam) ApplyBarrierChange(context.Context, ir.Change) error {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	committed := slices.Clone(s.applied)
	slices.Sort(committed)
	s.events = append(s.events, "barrier: committed="+strings.Join(committed, ","))
	return nil
}

// eventsWith returns the seam's events carrying prefix, in order.
func (s *fenceSeam) eventsWith(prefix string) []string {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	var out []string
	for _, e := range s.events {
		if strings.HasPrefix(e, prefix) {
			out = append(out, e)
		}
	}
	return out
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

// TestOrchestrator_FoldTicket pins ADR-0190 amendments A and D on the
// orchestrator, with --exactly-once-lanes: the first marked change of a
// transaction is routed only after every lane has committed everything before
// it; a second marked change of the same transaction does not fence again; an
// idempotent-only transaction never fences. The run's first transaction is
// anchored at its fence (its start is the run's persisted start position), so
// it carries no ticket; every later one carries exactly one, on the envelope
// of its first marked change, naming the transaction's start (the previous
// commit), the transactions that start closes, and the rows_applied
// increment — and the coordinator never writes that position itself. Across
// fold and checkpoint, every DML change is counted exactly once.
func TestOrchestrator_FoldTicket(t *testing.T) {
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: true}, seam, markFenceStream())
		fences := seam.eventsWith("fence:")
		want := []string{"fence:tx1 anchored=true committed=a", "fence:tx3 anchored=false committed=a,b,c,d,e"}
		if !slices.Equal(fences, want) {
			t.Errorf("lanes=%d: fences %v; want %v (once per marked transaction, after every earlier change committed)\nevents: %v",
				lanes, fences, want, seam.events)
		}
		folds := seam.eventsWith("fold:")
		if wantFold := []string{"fold:tx3 pos=c2 rows=4 closed=tx1,tx2 batch=f"}; !slices.Equal(folds, wantFold) {
			t.Errorf("lanes=%d: folds %v; want %v (one ticket, on tx3's first marked change, anchored at tx3's start)\nevents: %v",
				lanes, folds, wantFold, seam.events)
		}
		for _, ck := range seam.eventsWith("ckpt:") {
			if ck != "ckpt:c3" {
				t.Errorf("lanes=%d: the coordinator wrote checkpoint %q; the fold owns tx3's start and only the end of the run "+
					"(c3) is the coordinator's\nevents: %v", lanes, ck, seam.events)
			}
		}
		if seam.rowsTotal != 6 {
			t.Errorf("lanes=%d: fold + checkpoint rows_applied = %d; want 6 (every DML change counted exactly once)", lanes, seam.rowsTotal)
		}
	}
}

// TestOrchestrator_FenceIssuesNoSynchronousCheckpoint is amendment D's
// performance claim as a deterministic gate — the one property a correctness
// suite cannot see: on a stream of marked transactions the fences write no
// position of their own. Every marked transaction but the first gets a fold
// ticket, and the coordinator's WriteCheckpoint runs only for the end of the
// run (plus, at most, an idle tick per elapsed second — a fence-driven write
// would be one per transaction).
func TestOrchestrator_FenceIssuesNoSynchronousCheckpoint(t *testing.T) {
	const n = 60
	tok := func(i int) ir.Position { return ir.Position{Engine: "mysql", Token: fmt.Sprintf("c%03d", i)} }
	var stream []ir.Change
	for i := 1; i <= n; i++ {
		tx := fmt.Sprintf("tx%03d", i)
		stream = append(
			stream,
			ir.TxBegin{Position: tok(i - 1)},
			ir.Insert{Position: tok(i - 1), Schema: "ks", Table: "t", Row: ir.Row{"id": "t" + tx}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}},
			ir.Insert{Position: tok(i - 1), Schema: "ks", Table: "m", Row: ir.Row{"id": "m" + tx}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}},
			ir.TxCommit{Position: tok(i)},
		)
	}
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		took := runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: true}, seam, stream)
		folds, ckpts := len(seam.eventsWith("fold:")), len(seam.eventsWith("ckpt:"))
		if folds != n-1 {
			t.Errorf("lanes=%d: %d fold tickets for %d marked transactions; want %d (all but the run's first)", lanes, folds, n, n-1)
		}
		if allowed := 1 + int(took/checkpointIdlePeriod) + 1; ckpts > allowed {
			t.Errorf("lanes=%d: the coordinator wrote %d checkpoints in %v; the fences must write none (allowed: the end of "+
				"the run and the idle ticks, %d)", lanes, ckpts, took, allowed)
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

// barrierFoldStream is tx1 (an unmarked lane change), then tx2 holding a
// marked lane change (table "m") and a barrier (a keyless row) in the order
// given: "fold-first" or "barrier-first".
func barrierFoldStream(order string) []ir.Change {
	tok := func(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }
	marked := ir.Insert{Position: tok("c1"), Schema: "ks", Table: "m", Row: ir.Row{"id": "b"}, ApplyID: ir.ApplyID{TxID: "tx2", Seq: 1}}
	barrier := ir.Insert{Position: tok("c1"), Schema: "ks", Table: "k", Row: ir.Row{"v": "kl"}, ApplyID: ir.ApplyID{TxID: "tx2", Seq: 1}}
	first, second := marked, barrier
	if order == "barrier-first" {
		first, second = barrier, marked
	}
	return []ir.Change{
		ir.TxBegin{Position: tok("p0")},
		ir.Insert{Position: tok("p0"), Schema: "ks", Table: "t", Row: ir.Row{"id": "a"}, ApplyID: ir.ApplyID{TxID: "tx1", Seq: 1}},
		ir.TxCommit{Position: tok("c1")},
		ir.TxBegin{Position: tok("c1")},
		first, second,
		ir.TxCommit{Position: tok("c2")},
	}
}

// TestOrchestrator_BarrierAndFoldInOneTransaction pins amendment D's two
// mixed orders (§D.3). Fold first: the barrier's drain waits for the fold
// (it sees the fold's change committed), and its pre-apply checkpoint writes
// nothing — the fold claimed the transaction's start. Barrier first: the
// barrier's pre-apply checkpoint persists the transaction's start
// synchronously, so the fence that follows is anchored and issues NO ticket.
func TestOrchestrator_BarrierAndFoldInOneTransaction(t *testing.T) {
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: true}, seam, barrierFoldStream("fold-first"))
		if folds := seam.eventsWith("fold:"); !slices.Equal(folds, []string{"fold:tx2 pos=c1 rows=1 closed=tx1 batch=b"}) {
			t.Errorf("lanes=%d, fold first: folds %v; want tx2's ticket at its start (c1)\nevents: %v", lanes, folds, seam.events)
		}
		if b := seam.eventsWith("barrier:"); !slices.Equal(b, []string{"barrier: committed=a,b"}) {
			t.Errorf("lanes=%d, fold first: barrier %v ran before the fold's batch committed\nevents: %v", lanes, b, seam.events)
		}
		if ck := seam.eventsWith("ckpt:"); !slices.Equal(ck, []string{"ckpt:c2"}) {
			t.Errorf("lanes=%d, fold first: checkpoints %v; the barrier's pre-apply checkpoint must find the start already "+
				"claimed by the fold (only the end of the run, c2, is written)\nevents: %v", lanes, ck, seam.events)
		}

		seam = &fenceSeam{}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: true}, seam, barrierFoldStream("barrier-first"))
		if folds := seam.eventsWith("fold:"); len(folds) != 0 {
			t.Errorf("lanes=%d, barrier first: folds %v; the barrier already persisted the transaction's start, so the fence "+
				"must issue no ticket\nevents: %v", lanes, folds, seam.events)
		}
		if f := seam.eventsWith("fence:"); !slices.Equal(f, []string{"fence:tx2 anchored=true committed=a"}) {
			t.Errorf("lanes=%d, barrier first: fences %v; want tx2 fenced anchored\nevents: %v", lanes, f, seam.events)
		}
		if ck := seam.eventsWith("ckpt:"); len(ck) == 0 || ck[0] != "ckpt:c1" {
			t.Errorf("lanes=%d, barrier first: checkpoints %v; the barrier's pre-apply checkpoint must write the start (c1)\nevents: %v",
				lanes, ck, seam.events)
		}
	}
}
