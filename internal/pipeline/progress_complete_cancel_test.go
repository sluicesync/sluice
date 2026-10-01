// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTableCompleteRowSurvivesAStop pins the fix for the CI-red
// TestStreamer_WarmResume_PG_FullSlots_NeverProbesRefuses (run 36778297813):
// a stop landing between a table's last committed chunk and its COMPLETE
// progress row lost the row, so the stopped-cold-start resume could not
// prove the copy finished and fell through to a fresh cold start — on a
// source at its slot ceiling, the REPLICATION-HEADROOM refusal; elsewhere a
// needless full re-copy (and `migrate --resume` re-copied the table too).
//
// Both lanes' writers are driven with an already-cancelled context: the
// fast lane / migrate through setTableProgressAndWrite, the serial cold
// start through the tableProgressRecorder. The COMPLETE row must land; an
// IN-PROGRESS row on the same cancelled context must NOT — that control is
// what shows the store really honours cancellation, so the green is not
// the fake ignoring its context.
//
// This detach is safe only on top of GC-41 (i)'s source-end verdict: it
// first shipped without it and was reverted, because a copy a stop cut
// short could return nil and this write then made the partial table's
// COMPLETE row durable. TestStopMidCopyRecordsNoCompleteRow holds that
// precondition — the two pins are a pair, and neither is enough alone.
func TestTableCompleteRowSurvivesAStop(t *testing.T) {
	stopped, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("setTableProgressAndWrite", func(t *testing.T) {
		store := ctxHonouringStateStore{newFakeStateStore()}
		rc := resumeContext{store: store, migrationID: "m", enabled: true}
		state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
		var mu sync.Mutex
		setTableProgressAndWrite(stopped, rc, state, &mu, "done_t", ir.TableProgress{State: ir.TableProgressComplete, RowsCopied: 50})
		setTableProgressAndWrite(stopped, rc, state, &mu, "busy_t", ir.TableProgress{State: ir.TableProgressInProgress, RowsCopied: 7})
		got, _ := store.get("m")
		if got.TableProgress["done_t"].State != ir.TableProgressComplete {
			t.Fatalf("the COMPLETE row of a table whose rows had committed was lost to the stop: %+v", got.TableProgress)
		}
		if _, wrote := got.TableProgress["busy_t"]; wrote {
			t.Fatal("control: an in-progress write on a cancelled context landed — the store is not honouring cancellation, so the complete-row assertion proves nothing")
		}
	})

	t.Run("tableProgressRecorder", func(t *testing.T) {
		store := ctxHonouringStateStore{newFakeStateStore()}
		rec := newTableProgressRecorder(resumeContext{store: store, migrationID: "s", enabled: true, noResume: true}, "")
		table := &ir.Table{Name: "t"}
		rec.expectItems(table, 2)
		rec.completed(stopped, table, 10) // chunk 1 of 2: in progress
		if got, _ := store.get("s"); len(got.TableProgress) != 0 {
			t.Fatalf("control: an in-progress write on a cancelled context landed: %+v", got.TableProgress)
		}
		rec.completed(stopped, table, 10) // chunk 2 of 2: complete
		got, _ := store.get("s")
		if e := got.TableProgress["t"]; e.State != ir.TableProgressComplete || e.RowsCopied != 20 {
			t.Fatalf("the serial lane's COMPLETE row was lost to the stop: %+v", e)
		}
	})
}
