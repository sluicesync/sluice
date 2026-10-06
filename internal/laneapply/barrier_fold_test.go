// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0190 amendment E: the lane barrier's pre-apply checkpoint rides the
// barrier's own target transaction. These pins grade the coordinator's half
// (which checkpoint, when it is claimed, what a declined kind keeps); the
// engines' half — one transaction, position last, the skip path — is graded
// against real servers by TestLaneBarrier_* in each engine package.

// barrierFoldTok is a position token in the barrier-fold streams.
func barrierFoldTok(n string) ir.Position { return ir.Position{Engine: "mysql", Token: n} }

// barrierStreamTx is source transaction n of a marker stream: one lane row
// "a<n>" and one keyless row (a barrier) whose OWN position "r<n>" is
// mid-transaction — never T's start (c<n-1>) — so a fold that wrote it is
// visible. Both rows carry the transaction's identity, so its commit closes
// tx<n> (the ClosedTxs a later checkpoint hands over).
func barrierStreamTx(n int) []ir.Change {
	start, tx := barrierFoldTok(fmt.Sprintf("c%d", n-1)), fmt.Sprintf("tx%d", n)
	return []ir.Change{
		ir.TxBegin{Position: start},
		ir.Insert{Position: start, Schema: "ks", Table: "t", Row: ir.Row{"id": fmt.Sprintf("a%d", n)}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}},
		ir.Insert{Position: barrierFoldTok(fmt.Sprintf("r%d", n)), Schema: "ks", Table: "k", Row: ir.Row{"v": n}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}},
		ir.TxCommit{Position: barrierFoldTok(fmt.Sprintf("c%d", n))},
	}
}

// TestOrchestrator_BarrierIssuesNoSynchronousCheckpoint is amendment E's
// performance claim as a deterministic gate, the one property a correctness
// suite cannot see (the counterpart of amendment D's
// TestOrchestrator_FenceIssuesNoSynchronousCheckpoint): on a stream of N
// transactions each carrying a foldable barrier, no barrier makes the
// coordinator call WriteCheckpoint — every barrier but the run's first hands
// its pre-apply checkpoint to ApplyBarrierChange instead, as T−1's commit
// token, T−1 closed, and T−1's DML. The coordinator writes only the end of
// the run and, at most, an idle tick per elapsed second. The declined arm is
// the reverse direction: the same stream with FoldsBarrierCheckpoint=false
// must show one coordinator checkpoint per barrier but the first, proving
// this gate counts them.
func TestOrchestrator_BarrierIssuesNoSynchronousCheckpoint(t *testing.T) {
	const n = 60
	var stream []ir.Change
	for i := 1; i <= n; i++ {
		stream = append(stream, barrierStreamTx(i)...)
	}
	for _, lanes := range []int{1, 3} {
		seam := &fenceSeam{}
		took := runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8}, seam, stream)
		barriers, ckpts := seam.eventsWith("barrier:"), seam.eventsWith("ckpt:")
		if len(barriers) != n {
			t.Fatalf("lanes=%d: %d barriers; want %d", lanes, len(barriers), n)
		}
		for i, b := range barriers {
			want := "barrier: committed=" + strings.Join(committedThrough(i+1), ",")
			if i > 0 {
				want += fmt.Sprintf(" at=c%d rows=2 closed=tx%d", i, i)
			}
			if b != want {
				t.Errorf("lanes=%d: barrier %d was %q; want %q (T−1's commit, T−1 closed, T−1's two rows)", lanes, i+1, b, want)
			}
		}
		if allowed := 1 + int(took/checkpointIdlePeriod) + 1; len(ckpts) > allowed {
			t.Errorf("lanes=%d: the coordinator wrote %d checkpoints in %v; the barriers must write none of their own "+
				"(allowed: the end of the run and the idle ticks, %d): %v", lanes, len(ckpts), took, allowed, ckpts)
		}

		declined := &fenceSeam{declineBarrierFolds: true}
		runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8}, declined, stream)
		if got := len(declined.eventsWith("ckpt:")); got < n-1 {
			t.Errorf("lanes=%d, declined: %d coordinator checkpoints; want at least %d, one per barrier but the run's "+
				"first — the gate is not counting pre-apply checkpoints", lanes, got, n-1)
		}
		for _, b := range declined.eventsWith("barrier:") {
			if strings.Contains(b, " at=") {
				t.Errorf("lanes=%d, declined: a barrier was handed a checkpoint (%s) its engine does not fold", lanes, b)
			}
		}
	}
}

// committedThrough lists, sorted as fenceSeam logs them, the lane rows of
// transactions 1..n ("a1".."an").
func committedThrough(n int) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, fmt.Sprintf("a%d", i))
	}
	slices.Sort(out)
	return out
}

// TestOrchestrator_BarrierFoldAnchorsAtTransactionStart pins WHICH position a
// barrier folds: exactly what writeCheckpoint would have written — the
// barrier's transaction's start, on every stream shape — or nothing when that
// is already durable. The barrier's own position is never folded (each row
// barrier below carries a distinct "r…" token, and the SchemaSnapshot
// pgoutput's metadata-anchored 0/0).
func TestOrchestrator_BarrierFoldAnchorsAtTransactionStart(t *testing.T) {
	tok := barrierFoldTok
	row := func(p, id, tx string) ir.Change {
		return ir.Insert{Position: tok(p), Schema: "ks", Table: "t", Row: ir.Row{"id": id}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}}
	}
	keyless := func(p, tx string) ir.Change {
		return ir.Insert{Position: tok(p), Schema: "ks", Table: "k", Row: ir.Row{"v": p}, ApplyID: ir.ApplyID{TxID: tx, Seq: 1}}
	}
	marked := ir.Insert{Position: tok("c1"), Schema: "ks", Table: "m", Row: ir.Row{"id": "b"}, ApplyID: ir.ApplyID{TxID: "tx2", Seq: 1}}
	snapshot := ir.SchemaSnapshot{Position: tok(`{"lsn":"0/0"}`), Schema: "ks", Table: "t", IR: &ir.Table{Name: "t"}}

	cases := []struct {
		name         string
		exactlyOnce  bool
		stream       []ir.Change
		wantBarriers []string
	}{{
		name: "marker stream: T−1's TxCommit",
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")},
			row("c0", "a", "tx1"),
			ir.TxCommit{Position: tok("c1")},
			ir.TxBegin{Position: tok("c1")},
			row("c1", "b", "tx2"), keyless("r2", "tx2"),
			ir.TxCommit{Position: tok("c2")},
		},
		wantBarriers: []string{"barrier: committed=a,b at=c1 rows=1 closed=tx1"},
	}, {
		name: "marker stream: a keepalive boundary after T−1",
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")},
			row("c0", "a", "tx1"),
			ir.TxCommit{Position: tok("c1")},
			ir.TxBegin{Position: tok("k1")},
			ir.TxCommit{Position: tok("k1")},
			ir.TxBegin{Position: tok("k1")},
			keyless("r2", "tx2"),
			ir.TxCommit{Position: tok("c2")},
		},
		wantBarriers: []string{"barrier: committed=a at=k1 rows=1 closed=tx1"},
	}, {
		name: "marker-less stream: the change at seq−1",
		stream: []ir.Change{
			row("p1", "a", "trg:1"), row("p2", "b", "trg:2"), keyless("p3", "trg:3"), row("p4", "c", "trg:4"),
		},
		wantBarriers: []string{"barrier: committed=a,b at=p2 rows=2 closed=trg:1,trg:2"},
	}, {
		name: "the run's first transaction: nothing to fold",
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")}, row("c0", "a", "tx1"), keyless("r1", "tx1"), ir.TxCommit{Position: tok("c1")},
		},
		wantBarriers: []string{"barrier: committed=a"},
	}, {
		name: "an anchor already written: the second barrier of T folds nothing",
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")},
			row("c0", "a", "tx1"),
			ir.TxCommit{Position: tok("c1")},
			ir.TxBegin{Position: tok("c1")},
			keyless("r2", "tx2"), keyless("r3", "tx2"),
			ir.TxCommit{Position: tok("c2")},
		},
		wantBarriers: []string{"barrier: committed=a at=c1 rows=1 closed=tx1", "barrier: committed=a"},
	}, {
		name:        "a lane fold already claimed T's start",
		exactlyOnce: true,
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")},
			row("c0", "a", "tx1"),
			ir.TxCommit{Position: tok("c1")},
			ir.TxBegin{Position: tok("c1")},
			marked, keyless("r2", "tx2"),
			ir.TxCommit{Position: tok("c2")},
		},
		wantBarriers: []string{"barrier: committed=a,b"},
	}, {
		name: "a SchemaSnapshot (0/0): T's start, never its own token",
		stream: []ir.Change{
			ir.TxBegin{Position: tok("c0")},
			row("c0", "a", "tx1"),
			ir.TxCommit{Position: tok("c1")},
			ir.TxBegin{Position: tok("c1")},
			snapshot, row("c1", "b", "tx2"),
			ir.TxCommit{Position: tok("c2")},
		},
		wantBarriers: []string{"barrier: committed=a at=c1 rows=1 closed=tx1"},
	}}
	for _, tc := range cases {
		for _, lanes := range []int{1, 3} {
			seam := &fenceSeam{}
			runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8, ExactlyOnceLanes: tc.exactlyOnce}, seam, tc.stream)
			if got := seam.eventsWith("barrier:"); !slices.Equal(got, tc.wantBarriers) {
				t.Errorf("%s, lanes=%d: barriers %q; want %q\nevents: %v", tc.name, lanes, got, tc.wantBarriers, seam.events)
			}
		}
	}

	// The Bug 158 recordingSeam: a run whose FIRST event is the 0/0
	// SchemaSnapshot inside a transaction after T−1 — nothing it persisted, by
	// either writer, may be the snapshot's token, and it folded T's start.
	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)
	ch := make(chan ir.Change, 8)
	for _, c := range []ir.Change{
		ir.TxBegin{Position: tok("c0")},
		row("c0", "a", ""),
		ir.TxCommit{Position: tok("c1")},
		ir.TxBegin{Position: tok("c1")},
		snapshot,
		ir.TxCommit{Position: tok("c2")},
	} {
		ch <- c
	}
	close(ch)
	if err := orch.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !slices.Equal(seam.folded, []string{"c1"}) {
		t.Errorf("recordingSeam: folded %q; want [c1] (the SchemaSnapshot's transaction's start)", seam.folded)
	}
	if slices.Contains(seam.checkpoints, snapshot.Position.Token) {
		t.Errorf("recordingSeam: persisted the SchemaSnapshot's 0/0 token (%q) — Bug 158", seam.checkpoints)
	}
}

// TestOrchestrator_DeclinedBarrierKeepsTwoCommits pins the decline: an engine
// answering FoldsBarrierCheckpoint=false (a MySQL-family Truncate) gets
// exactly the sequence that predates amendment E — the coordinator's
// checkpoint, the change with a nil checkpoint, and the post-apply
// checkpoint at the Truncate's own boundary. The folding arm of the same
// stream is the reverse direction: one barrier call carrying the start, and
// only the post-apply checkpoint from the coordinator.
func TestOrchestrator_DeclinedBarrierKeepsTwoCommits(t *testing.T) {
	tok := barrierFoldTok
	stream := []ir.Change{
		ir.TxBegin{Position: tok("c0")},
		ir.Insert{Position: tok("c0"), Schema: "ks", Table: "t", Row: ir.Row{"id": "a"}, ApplyID: ir.ApplyID{TxID: "tx1", Seq: 1}},
		ir.TxCommit{Position: tok("c1")},
		ir.Truncate{Position: tok("tr1"), Schema: "ks", Table: "t"},
	}
	for _, tc := range []struct {
		decline bool
		want    []string
	}{
		{true, []string{"ckpt:c1", "barrier: committed=a", "ckpt:tr1"}},
		{false, []string{"barrier: committed=a at=c1 rows=1 closed=tx1", "ckpt:tr1"}},
	} {
		for _, lanes := range []int{1, 3} {
			seam := &fenceSeam{declineBarrierFolds: tc.decline}
			runFenceStream(t, Config{Lanes: lanes, MaxBatchSize: 8}, seam, stream)
			if got := seam.eventsWith(""); !slices.Equal(got, tc.want) {
				t.Errorf("decline=%v, lanes=%d: events %q; want %q", tc.decline, lanes, got, tc.want)
			}
		}
	}
}

// failingBarrierSeam is fenceSeam whose ApplyBarrierChange fails whenever it
// is handed a checkpoint to fold: the barrier's transaction rolled back.
type failingBarrierSeam struct {
	fenceSeam
}

var errBarrierRolledBack = errors.New("barrier transaction rolled back")

func (s *failingBarrierSeam) ApplyBarrierChange(ctx context.Context, c ir.Change, at *BarrierCheckpoint) error {
	if at != nil {
		return errBarrierRolledBack
	}
	return s.fenceSeam.ApplyBarrierChange(ctx, c, at)
}

// TestOrchestrator_BarrierFoldFailureClaimsNothing pins that the coordinator
// advances its persisted-checkpoint bookkeeping only after the barrier's
// transaction committed: a failed fold leaves lastWrittenSeq, lastWrittenCum
// and the unclosed transactions exactly as they were, and the run fails with
// the barrier's error — so nothing the engine rolled back is ever treated as
// written (no later checkpoint skips the boundary, under-counts rows_applied
// or forgets to delete tx1's marks). The reverse direction is the same stream
// succeeding, which advances all three.
func TestOrchestrator_BarrierFoldFailureClaimsNothing(t *testing.T) {
	tok := barrierFoldTok
	stream := []ir.Change{
		ir.TxBegin{Position: tok("c0")},
		ir.Insert{Position: tok("c0"), Schema: "ks", Table: "t", Row: ir.Row{"id": "a"}, ApplyID: ir.ApplyID{TxID: "tx1", Seq: 1}},
		ir.TxCommit{Position: tok("c1")},
		ir.TxBegin{Position: tok("c1")},
		ir.Insert{Position: tok("r2"), Schema: "ks", Table: "k", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "tx2", Seq: 1}},
	}
	run := func(seam LaneApplier) (*Orchestrator, error) {
		o := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 8}, seam)
		ch := make(chan ir.Change, len(stream))
		for _, c := range stream {
			ch <- c
		}
		close(ch)
		err := o.Run(context.Background(), ch)
		return o, err
	}

	o, err := run(&failingBarrierSeam{})
	if !errors.Is(err, errBarrierRolledBack) {
		t.Fatalf("Run = %v; want the barrier's error", err)
	}
	if o.lastWrittenSeq != 0 || o.lastWrittenCum != 0 || len(o.closedTx) != 1 {
		t.Errorf("after a failed fold: lastWrittenSeq=%d lastWrittenCum=%d closedTx=%v; want 0, 0 and tx1 still unclosed — "+
			"the coordinator claimed a checkpoint the barrier's rollback discarded", o.lastWrittenSeq, o.lastWrittenCum, o.closedTx)
	}

	o, err = run(&fenceSeam{})
	if err != nil {
		t.Fatalf("Run (success): %v", err)
	}
	if o.lastWrittenSeq == 0 || o.lastWrittenCum != 1 || len(o.closedTx) != 0 {
		t.Errorf("after a committed fold: lastWrittenSeq=%d lastWrittenCum=%d closedTx=%v; want the c1 boundary, 1 and none — "+
			"the reverse direction does not advance them", o.lastWrittenSeq, o.lastWrittenCum, o.closedTx)
	}
}
