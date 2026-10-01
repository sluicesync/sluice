// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// foldSeam is testSeam recording every ApplyLaneBatch attempt — the batch's
// row ids and the fold ticket it was handed — and answering each with
// result(batch, fold).
type foldSeam struct {
	testSeam
	result func(batch []ir.Change, fold *FoldTicket) error

	mu       sync.Mutex
	attempts []foldAttempt
}

type foldAttempt struct {
	ids  string
	fold string
	err  error
}

func (s *foldSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change, fold *FoldTicket) (int, error) {
	ids := make([]string, 0, len(batch))
	for _, c := range batch {
		ids = append(ids, fmt.Sprint(c.(ir.Insert).Row["id"]))
	}
	err := s.result(batch, fold)
	a := foldAttempt{ids: strings.Join(ids, ","), err: err}
	if fold != nil {
		a.fold = fold.Tx
	}
	s.mu.Lock()
	s.attempts = append(s.attempts, a)
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return len(batch), nil
}

// foldBuf is a lane buffer of rows ids, the one at ticketAt carrying a fold
// ticket for tx "T" (none when ticketAt < 0).
func foldBuf(ticketAt int, ids ...string) []LaneChange {
	buf := make([]LaneChange, len(ids))
	for i, id := range ids {
		buf[i] = LaneChange{Seq: uint64(i + 1), Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": id}}}
		if i == ticketAt {
			buf[i].fold = &FoldTicket{Tx: "T", Pos: ir.Position{Token: "anchor"}}
		}
	}
	return buf
}

// TestOrchestrator_FoldRidesTheSplitSubBatch pins amendment D's split rule:
// when a lane batch is split to converge in-lane, the fold ticket rides the
// sub-batch that holds the ticket's envelope — and no other. The ticket here
// sits in the SECOND half on purpose (the algorithm does not rely on the
// fenced change opening its batch, though it always does): a split that
// handed the parent's ticket to both halves, or to the first, would write the
// anchor in a transaction without the fenced change's marks.
func TestOrchestrator_FoldRidesTheSplitSubBatch(t *testing.T) {
	seam := &foldSeam{result: func(batch []ir.Change, _ *FoldTicket) error {
		if len(batch) > 1 {
			return errTxKiller // persistent at any size above one: split to singles
		}
		return nil
	}}
	o := NewOrchestrator(Config{Lanes: 1, MaxBatchSize: 8}, seam)
	o.cancel = func() {}
	if _, err := o.applyLaneBatch(context.Background(), 0, nil, foldBuf(2, "a", "b", "c", "d")); err != nil {
		t.Fatalf("applyLaneBatch: %v", err)
	}
	carried := 0
	for _, a := range seam.attempts {
		holdsC := strings.Contains(a.ids, "c")
		if holdsC != (a.fold == "T") {
			t.Errorf("attempt on [%s] carried fold %q; the ticket must ride exactly the (sub-)batches holding its envelope (c)", a.ids, a.fold)
		}
		if a.err == nil && a.fold == "T" {
			carried++
		}
	}
	if carried != 1 {
		t.Errorf("the ticket was committed by %d batches; want exactly 1\nattempts: %+v", carried, seam.attempts)
	}
	if got := o.frontier.FrontierSeq(); got != 4 {
		t.Errorf("frontier = %d after every sub-batch committed; want 4", got)
	}
}

// TestLaneApplyBatch_FoldCommitOutcomeUnknownIsFatal pins amendment D's one
// retry-semantics change: a COMMIT-step error on a fold batch has an unknown
// outcome — retrying in place after a commit that actually landed would count
// its rows twice — so the lane does not retry it, even when its cause is
// retriable, and the run fails (the streamer's re-entry re-reads the truth).
// The same error on a batch with NO ticket, and a pre-commit error on a fold
// batch, keep the ordinary in-lane retry. The failure stays classified, so
// the streamer still sees a transient as transient.
func TestLaneApplyBatch_FoldCommitOutcomeUnknownIsFatal(t *testing.T) {
	failFirst := func(wrap func(error) error) func([]ir.Change, *FoldTicket) error {
		failed := false
		return func([]ir.Change, *FoldTicket) error {
			if failed {
				return nil
			}
			failed = true
			return wrap(errTxKiller)
		}
	}
	for _, tc := range []struct {
		name      string
		ticketAt  int
		wrap      func(error) error
		wantFatal bool
	}{
		{"fold batch, COMMIT outcome unknown", 0, CommitOutcomeUnknown, true},
		{"fold batch, pre-commit abort", 0, func(err error) error { return err }, false},
		{"no ticket, COMMIT-step error", -1, CommitOutcomeUnknown, false},
	} {
		for _, ids := range [][]string{{"a"}, {"a", "b", "c"}} { // the single-change and multi-change retry loops
			t.Run(fmt.Sprintf("%s/%d", tc.name, len(ids)), func(t *testing.T) {
				seam := &foldSeam{result: failFirst(tc.wrap)}
				o := NewOrchestrator(Config{Lanes: 1, MaxBatchSize: 8}, seam)
				o.cancel = func() {}
				_, err := o.applyLaneBatch(context.Background(), 0, nil, foldBuf(tc.ticketAt, ids...))
				if !tc.wantFatal {
					if err != nil || len(seam.attempts) != 2 {
						t.Fatalf("err %v after %d attempts; want the ordinary in-lane retry to commit on attempt 2", err, len(seam.attempts))
					}
					return
				}
				if err == nil {
					t.Fatal("a fold batch whose COMMIT outcome is unknown was retried in place and committed")
				}
				if len(seam.attempts) != 1 {
					t.Errorf("%d attempts; want exactly 1 — no in-place retry", len(seam.attempts))
				}
				if !strings.Contains(err.Error(), "unknown outcome") {
					t.Errorf("the run error does not name the unknown commit outcome: %v", err)
				}
				var re ir.RetriableError
				if !errors.As(err, &re) || !re.Retriable() {
					t.Errorf("the run error lost the cause's classification (the streamer's re-entry needs it): %v", err)
				}
				if got := o.frontier.FrontierSeq(); got != 0 {
					t.Errorf("frontier = %d; a batch that did not commit must not advance it", got)
				}
			})
		}
	}
}

// TestLaneApplyBatch_DuplicateFoldTicketRefuses pins FOLD-TICKET-DUPLICATE:
// one lane batch carrying two tickets is a coordinator defect, refused
// before anything is applied rather than folding one and dropping the other.
func TestLaneApplyBatch_DuplicateFoldTicketRefuses(t *testing.T) {
	seam := &foldSeam{result: func([]ir.Change, *FoldTicket) error { return nil }}
	o := NewOrchestrator(Config{Lanes: 1, MaxBatchSize: 8}, seam)
	o.cancel = func() {}
	buf := foldBuf(0, "a", "b")
	buf[1].fold = &FoldTicket{Tx: "U"}
	_, err := o.applyLaneBatch(context.Background(), 0, nil, buf)
	if err == nil || !strings.Contains(err.Error(), FoldTicketDuplicateMarker) {
		t.Fatalf("err = %v; want a %s refusal", err, FoldTicketDuplicateMarker)
	}
	if len(seam.attempts) != 0 {
		t.Errorf("%d apply attempts; a batch carrying two tickets must apply nothing", len(seam.attempts))
	}
}
