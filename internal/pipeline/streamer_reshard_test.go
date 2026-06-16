// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0094 reshard auto-follow: unit-pins the Streamer's
// [applyWithReshardFollow] loop with a fake reader + applier so the
// channel-swap concurrency runs under `-race` in the standard unit job
// (the real end-to-end reshard needs a vitess/lite cluster and lives in
// the scheduled extended suite). The reader-side seam correctness
// (exactly-once across the journal) is proven separately by the
// vitessreshard chaos test; this pins the ORCHESTRATION: every event on
// both sides of a reopen is applied exactly once and in order, a reopen
// failure / budget exhaustion fails loud, a non-reshard close settles
// normally, and Shape-A is NOT auto-followed (deferred).

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// reshardRecordingApplier drains the change channel and records each
// change's id in arrival order, so the test can assert exactly-once,
// in-order delivery across a reopen seam.
type reshardRecordingApplier struct {
	stubChangeApplier
	mu      sync.Mutex
	applied []int64
}

func (a *reshardRecordingApplier) Apply(_ context.Context, _ string, changes <-chan ir.Change) error {
	for c := range changes {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		id, _ := ins.Row["id"].(int64)
		a.mu.Lock()
		a.applied = append(a.applied, id)
		a.mu.Unlock()
	}
	return nil
}

func (a *reshardRecordingApplier) ids() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]int64, len(a.applied))
	copy(out, a.applied)
	return out
}

// fakeReshardReader hands out a pre-seeded sequence of post-reshard
// channels on successive ReopenAfterReshard calls; when exhausted it
// reports "not a reshard" (a clean end). An err / alwaysNew override the
// behaviour for the failure / budget cases.
type fakeReshardReader struct {
	mu        sync.Mutex
	next      []<-chan ir.Change
	calls     int
	err       error // when set: every call returns (nil, true, err)
	alwaysNew func() <-chan ir.Change
}

func (r *fakeReshardReader) ReopenAfterReshard(context.Context) (changes <-chan ir.Change, wasReshard bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return nil, true, r.err
	}
	if r.alwaysNew != nil {
		return r.alwaysNew(), true, nil
	}
	if r.calls-1 >= len(r.next) {
		return nil, false, nil // no more reshards → clean end
	}
	return r.next[r.calls-1], true, nil
}

func chanOf(inserts ...int64) <-chan ir.Change {
	ch := make(chan ir.Change, len(inserts))
	for _, id := range inserts {
		ch <- ir.Insert{Table: "t", Row: map[string]any{"id": id}}
	}
	close(ch)
	return ch
}

func newReshardStreamer(reader ir.ReshardReopener) *Streamer {
	return &Streamer{ApplyBatchSize: 1, sourceReshard: reader}
}

func runReshardFollow(t *testing.T, s *Streamer, applier ir.ChangeApplier, first <-chan ir.Change) error {
	t.Helper()
	var stopObserved atomic.Bool
	ctx := context.Background()
	return s.applyWithReshardFollow(ctx, ctx, applier, "sid", first, &liveAddedFilter{}, &stopObserved)
}

func TestApplyWithReshardFollow_FollowsReshardExactlyOnce(t *testing.T) {
	applier := &reshardRecordingApplier{}
	reader := &fakeReshardReader{next: []<-chan ir.Change{chanOf(3, 4)}}
	s := newReshardStreamer(reader)

	// First channel carries {1,2}; after its clean close the reader
	// reshards once and hands {3,4}; after that, a clean end.
	if err := runReshardFollow(t, s, applier, chanOf(1, 2)); err != nil {
		t.Fatalf("applyWithReshardFollow: %v", err)
	}
	got := applier.ids()
	want := []int64{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("applied = %v; want %v (every event on both sides of the seam, exactly once)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("applied = %v; want %v (order preserved across the seam)", got, want)
		}
	}
	if reader.calls != 2 {
		t.Errorf("ReopenAfterReshard calls = %d; want 2 (one reshard, one clean-end probe)", reader.calls)
	}
}

func TestApplyWithReshardFollow_NonReshardCloseSettles(t *testing.T) {
	applier := &reshardRecordingApplier{}
	reader := &fakeReshardReader{} // no next → first probe says "not a reshard"
	s := newReshardStreamer(reader)

	if err := runReshardFollow(t, s, applier, chanOf(1, 2)); err != nil {
		t.Fatalf("applyWithReshardFollow: %v", err)
	}
	if got := applier.ids(); len(got) != 2 {
		t.Errorf("applied = %v; want [1 2] (clean end, no reopen)", got)
	}
}

func TestApplyWithReshardFollow_ReopenErrorIsLoud(t *testing.T) {
	applier := &reshardRecordingApplier{}
	reader := &fakeReshardReader{err: errors.New("vtgate gone")}
	s := newReshardStreamer(reader)

	err := runReshardFollow(t, s, applier, chanOf(1))
	if err == nil || !strings.Contains(err.Error(), "reshard reopen") {
		t.Fatalf("err = %v; want a loud 'reshard reopen' failure", err)
	}
}

func TestApplyWithReshardFollow_BudgetExhaustionIsLoud(t *testing.T) {
	applier := &reshardRecordingApplier{}
	// Every reopen succeeds with a fresh EMPTY channel → an immediate
	// clean close → another reopen … until the budget trips loudly.
	reader := &fakeReshardReader{alwaysNew: func() <-chan ir.Change { return chanOf() }}
	s := newReshardStreamer(reader)

	err := runReshardFollow(t, s, applier, chanOf())
	if err == nil || !strings.Contains(err.Error(), "budget exhausted") {
		t.Fatalf("err = %v; want a loud 'budget exhausted' failure", err)
	}
	if reader.calls <= maxReshardReopensPerRun {
		t.Errorf("ReopenAfterReshard calls = %d; want > %d (loop ran to the budget)", reader.calls, maxReshardReopensPerRun)
	}
}

func TestApplyWithReshardFollow_ShapeAIsNotFollowed(t *testing.T) {
	applier := &reshardRecordingApplier{}
	reader := &fakeReshardReader{next: []<-chan ir.Change{chanOf(3, 4)}}
	s := newReshardStreamer(reader)
	// Shape A engaged → reshard auto-follow is deferred; the loop must
	// settle after the first channel without probing the reopener.
	s.InjectShardColumn = ShardColumnSpec{Name: "src_shard", Value: "shard_a"}

	if err := runReshardFollow(t, s, applier, chanOf(1, 2)); err != nil {
		t.Fatalf("applyWithReshardFollow: %v", err)
	}
	if got := applier.ids(); len(got) != 2 {
		t.Errorf("applied = %v; want [1 2] (Shape-A reshard is deferred — no follow)", got)
	}
	if reader.calls != 0 {
		t.Errorf("ReopenAfterReshard calls = %d; want 0 (Shape-A must not auto-follow)", reader.calls)
	}
}
