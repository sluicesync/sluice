// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// splitNote is the sentence splitNoterErr hands [ir.NoteSourceTxSplit].
const splitNote = "SPLIT-NOTED"

// splitNoterErr stands in for the GC-42 refusal: an [ir.SourceTxSplitNoter].
type splitNoterErr struct{}

func (splitNoterErr) Error() string             { return "refused" }
func (splitNoterErr) SourceTxSplitNote() string { return splitNote }

// splitSeam routes a row change by its key unless it changes the key (an
// UPDATE whose before and after ids differ goes to the barrier, as the real
// engines route a key change), and refuses the change whose before-image id
// is refuseID — wherever it is applied, so the test covers a lane refusal and
// a barrier refusal through the same rule.
type splitSeam struct{ refuseID int64 }

func (s *splitSeam) RouteForChange(_ context.Context, c ir.Change) (Route, bool, error) {
	if u, ok := c.(ir.Update); ok && u.Before["id"] != u.After["id"] {
		return Route{}, false, nil
	}
	return Route{Qualified: "s.t", PKVals: []any{splitKey(c)}, Scope: RouteScopeKey}, true, nil
}

func (s *splitSeam) refuses(c ir.Change) bool {
	switch v := c.(type) {
	case ir.Update:
		return v.Before["id"] == s.refuseID
	case ir.Delete:
		return v.Before["id"] == s.refuseID
	}
	return false
}

func (s *splitSeam) ApplyLaneBatch(_ context.Context, _ int, batch []ir.Change, _ *FoldTicket) (int, error) {
	for _, c := range batch {
		if s.refuses(c) {
			return 0, splitNoterErr{}
		}
	}
	return len(batch), nil
}

func (s *splitSeam) ApplyBarrierChange(_ context.Context, c ir.Change) error {
	if s.refuses(c) {
		return splitNoterErr{}
	}
	return nil
}

func (s *splitSeam) ClassifyError(err error) error { return err }

func (s *splitSeam) WriteCheckpoint(context.Context, ir.Position, int64, []string) error { return nil }

// SkipsRowChange: a row of table "gone" has no target table, so it writes
// nothing (the PG-2 skip).
func (s *splitSeam) SkipsRowChange(_ context.Context, c ir.Change) bool {
	if !ir.IsRowDMLChange(c) {
		return false
	}
	_, table := RowChangeSchemaTable(c)
	return table == "gone"
}

func (s *splitSeam) ApplyMarkTx(context.Context, ir.Change) string { return "" }
func (s *splitSeam) ApplyMarksFenced(string, bool)                 {}

func splitKey(c ir.Change) any {
	switch v := c.(type) {
	case ir.Insert:
		return v.Row["id"]
	case ir.Update:
		return v.Before["id"]
	case ir.Delete:
		return v.Before["id"]
	}
	return nil
}

// TestOrchestrator_KeyScopedRefusalNamesTheSplit pins Bug 294 on the lane
// path: a refusal says its source transaction WAS split only when the
// coordinator knows part of it is durable — a barrier inside the transaction
// drained the lanes after one of its rows, or committed one of its statements
// itself — and keeps its own "may" when the only split is across lanes, whose
// commits it cannot know. Both refusal sites are covered: a lane batch and a
// barrier. The filed repro (a key change, then a DELETE that matches the key
// it moved to) is the first case.
func TestOrchestrator_KeyScopedRefusalNamesTheSplit(t *testing.T) {
	upd := func(from, to int64) ir.Change {
		return ir.Update{Schema: "s", Table: "t", Before: ir.Row{"id": from}, After: ir.Row{"id": to}}
	}
	del := func(id int64) ir.Change { return ir.Delete{Schema: "s", Table: "t", Before: ir.Row{"id": id}} }
	ins := func(id int64) ir.Change { return ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": id}} }
	begin, commit := ir.TxBegin{}, ir.TxCommit{}
	snap := ir.SchemaSnapshot{Schema: "s", Table: "t"}
	insGone := ir.Insert{Schema: "s", Table: "gone", Row: ir.Row{"id": int64(5)}}
	updGone := ir.Update{Schema: "s", Table: "gone", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(4)}}
	cases := []struct {
		name      string
		changes   []ir.Change
		wantSplit bool
	}{
		{"lane refusal after a committed key-change barrier (the repro)", []ir.Change{begin, upd(1, 2), del(2)}, true},
		{"lane refusal, the transaction split only across lanes", []ir.Change{begin, ins(5), del(2)}, false},
		{"barrier refusal after a routed row the drain committed", []ir.Change{begin, ins(5), upd(2, 3)}, true},
		{"barrier refusal as the transaction's first change", []ir.Change{begin, upd(2, 3)}, false},
		{"the previous transaction had the barrier, this one does not", []ir.Change{begin, upd(1, 4), commit, begin, del(2)}, false},
		// Bug 295: a barrier that wrote nothing of the transaction did not
		// commit part of it — the lazily emitted SchemaSnapshot at a table's
		// first row, and a row skipped for an absent target, whether routed
		// to a lane before the drain or applied as the barrier itself. Each
		// against a refusal at both sites; the snapshot draining a routed
		// row that DID write still names the split.
		{"lane refusal after a schema snapshot barrier (Bug 295)", []ir.Change{begin, snap, del(2)}, false},
		{"barrier refusal after a schema snapshot barrier", []ir.Change{begin, snap, upd(2, 3)}, false},
		{"a schema snapshot barrier drained a routed row that wrote", []ir.Change{begin, ins(5), snap, del(2)}, true},
		{"barrier refusal after a drained row skipped for an absent target", []ir.Change{begin, insGone, upd(2, 3)}, false},
		{"lane refusal after a key-change barrier skipped for an absent target", []ir.Change{begin, updGone, del(2)}, false},
		// A committed TRUNCATE leaves the target in a state the source never
		// had: a split, as the batch and per-change paths also say.
		{"lane refusal after a Truncate barrier", []ir.Change{begin, ir.Truncate{Schema: "s", Table: "t"}, del(2)}, true},
		{"barrier refusal after a Truncate barrier", []ir.Change{begin, ir.Truncate{Schema: "s", Table: "t"}, upd(2, 3)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, &splitSeam{refuseID: 2})
			ch := make(chan ir.Change, len(tc.changes))
			for i, c := range tc.changes {
				ch <- withPos(c, i+1)
			}
			close(ch)
			err := orch.Run(context.Background(), ch)
			var refusal splitNoterErr
			if !errors.As(err, &refusal) {
				t.Fatalf("want the refusal; got %v", err)
			}
			if got := strings.Contains(err.Error(), splitNote); got != tc.wantSplit {
				t.Errorf("names the split = %v, want %v: %v", got, tc.wantSplit, err)
			}
		})
	}
}

// withPos stamps a distinct position on a change.
func withPos(c ir.Change, n int) ir.Change {
	p := ir.Position{Token: strings.Repeat("x", n)}
	switch v := c.(type) {
	case ir.TxBegin:
		v.Position = p
		return v
	case ir.TxCommit:
		v.Position = p
		return v
	case ir.Insert:
		v.Position = p
		return v
	case ir.Update:
		v.Position = p
		return v
	case ir.Delete:
		v.Position = p
		return v
	case ir.SchemaSnapshot:
		v.Position = p
		return v
	case ir.Truncate:
		v.Position = p
		return v
	}
	return c
}
