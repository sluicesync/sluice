// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the cross-cycle backfill-state semantics (live-caught
// 2026-07-15, first psverify CI dispatch, on a reused database): a
// prior cycle's COMPLETED marker for the same table+sets+where must
// not no-op a NEW cycle whose expand leg just re-created the column —
// the orchestrator restarts the walk when THIS run's expand leg
// actually deployed. The in-cycle resume contract (--resume-from
// migrate honors the persisted cursor / completed marker) and
// standalone `sluice backfill` are unchanged.

package expandcontract

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// seedBackfillState plants a persisted state row for the
// orchestrator's exact backfill spec in the fake engine's store —
// the residue a prior cycle (or a crashed walk) leaves in the SAME
// database the next cycle reuses.
func seedBackfillState(o *Orchestrator, eng *ecFakeEngine, phase ir.MigrationPhase, progress *ir.TableProgress) string {
	id := pipeline.BackfillMigrationID(o.Table, o.Sets, o.Where)
	eng.store.states[id] = ir.MigrationState{MigrationID: id, Phase: phase}
	if progress != nil {
		eng.store.progress[id] = map[string]ir.TableProgress{o.Table: *progress}
	}
	return id
}

// TestExpandContract_FreshCycleRestartsCompletedPriorState pins the
// live failure shape, fixed: run 2 on a reused database (run 1's
// contract dropped the column; run 2's expand just re-created it
// empty) finds run 1's COMPLETED marker for the identical spec. The
// expand leg deployed this run, so the marker is provably stale — the
// walk must run fresh instead of no-op'ing into the loud-but-wrong
// SLUICE-E-BACKFILL-INCOMPLETE verify failure.
func TestExpandContract_FreshCycleRestartsCompletedPriorState(t *testing.T) {
	ps := newFakePS(t)
	o, eng, _, out := newTestOrchestrator(t, ps)
	seedBackfillState(o, eng, ir.MigrationPhaseComplete, nil) // rows stay UNFILLED: the re-created column is empty

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if eng.ex.execCalls == 0 {
		t.Fatal("backfill no-op'd on the prior cycle's completed marker — the stale-state restart did not engage")
	}
	if result.Backfill == nil || result.Backfill.RowsUpdated != 10 || result.Backfill.AlreadyComplete {
		t.Errorf("backfill result = %+v; want a fresh 10-row walk, not AlreadyComplete", result.Backfill)
	}
	if !result.Verified || !result.ContractRun {
		t.Errorf("result = %+v; want verified + contract run", result)
	}
	if !strings.Contains(out.String(), "fresh walk") {
		t.Errorf("narration should explain the stale-state restart:\n%s", out.String())
	}
}

// TestExpandContract_ResumeFromMigrateHonorsCompletedState pins the
// in-cycle half: on --resume-from migrate the expand leg did NOT run
// this process, so a completed marker is trusted exactly as before —
// zero chunk UPDATEs, AlreadyComplete, verify still gating.
func TestExpandContract_ResumeFromMigrateHonorsCompletedState(t *testing.T) {
	ps := newFakePS(t)
	o, eng, _, _ := newTestOrchestrator(t, ps)
	o.ResumeFrom = LegMigrate
	seedBackfillState(o, eng, ir.MigrationPhaseComplete, nil)
	for i := range eng.ex.rows {
		eng.ex.rows[i].filled = true // the completed walk's outcome — verify must pass
	}

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if eng.ex.execCalls != 0 {
		t.Errorf("exec calls = %d; want 0 (completed marker honored on --resume-from migrate)", eng.ex.execCalls)
	}
	if result.Backfill == nil || !result.Backfill.AlreadyComplete {
		t.Errorf("backfill result = %+v; want AlreadyComplete", result.Backfill)
	}
	if !result.Verified || !result.ContractRun {
		t.Errorf("result = %+v; want verified + contract run", result)
	}
}

// TestExpandContract_ResumeFromMigrateResumesCursor pins the
// at-most-one-chunk contract across a mid-walk crash: --resume-from
// migrate with a persisted cursor continues AFTER it — the resumed
// result carries the prior rows (5 persisted + 5 new = 10) and the
// Resumed flag, which a stale-state restart (RowsUpdated reset to the
// 5 newly-filled rows) could not produce.
func TestExpandContract_ResumeFromMigrateResumesCursor(t *testing.T) {
	ps := newFakePS(t)
	o, eng, _, _ := newTestOrchestrator(t, ps)
	o.ResumeFrom = LegMigrate
	// The crashed walk had filled rows 1-5 and checkpointed pk=5.
	for i := range eng.ex.rows[:5] {
		eng.ex.rows[i].filled = true
	}
	// "backfill" is the pipeline's unexported in-flight phase value —
	// mirrored here the way the real control table would carry it.
	seedBackfillState(o, eng, ir.MigrationPhase("backfill"),
		&ir.TableProgress{LastPK: []any{int64(5)}, RowsCopied: 5})

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Backfill == nil || !result.Backfill.Resumed {
		t.Fatalf("backfill result = %+v; want Resumed (cursor honored)", result.Backfill)
	}
	if result.Backfill.RowsUpdated != 10 {
		t.Errorf("rows updated = %d; want 10 (5 persisted + 5 new — a restart would report 5)", result.Backfill.RowsUpdated)
	}
	if !result.Verified || !result.ContractRun {
		t.Errorf("result = %+v; want verified + contract run", result)
	}
}
