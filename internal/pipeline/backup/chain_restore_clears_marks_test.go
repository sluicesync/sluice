// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// markClearingEngine is a chain-restore target whose change applier declares
// [ir.ApplyMarksClearer] and records, in the engine's phase list, every clear
// and the first change each apply run receives.
type markClearingEngine struct {
	*chainRestoreRecorderEngine
}

func (e *markClearingEngine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	inner, err := e.chainRestoreRecorderEngine.OpenChangeApplier(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &markClearingApplier{chainRestoreRecordingApplier: inner.(*chainRestoreRecordingApplier), owner: e}, nil
}

type markClearingApplier struct {
	*chainRestoreRecordingApplier
	owner *markClearingEngine
}

func (a *markClearingApplier) ClearApplyMarks(_ context.Context, streamID string) error {
	a.owner.recordPhase("ClearApplyMarks:" + streamID)
	return nil
}

func (a *markClearingApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	a.owner.recordPhase("Apply:" + streamID)
	return a.chainRestoreRecordingApplier.Apply(ctx, streamID, changes)
}

var _ ir.ApplyMarksClearer = (*markClearingApplier)(nil)

// TestChainRestore_ClearsItsApplyMarksBeforeApplyingAnything is ADR-0191
// §3.4 (2) for chain restore (§13 R9): once the codec restores the reader's
// identities, chain restore's applier writes and trusts ADR-0190 apply marks
// under ChainRestoreStreamID. A restore starts over from the full every run,
// and the full can put back exactly the row a crashed earlier attempt's marked
// change had moved away — so a surviving mark would then skip that change and
// leave both rows, silently. The run must clear the stream's marks before it
// applies anything, the full included.
//
// Graded on the order the target saw: the clear, under ChainRestoreStreamID,
// before the first apply run and before any table is created.
func TestChainRestore_ClearsItsApplyMarksBeforeApplyingAnything(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, trailingTruncateRows)
	eng := &markClearingEngine{chainRestoreRecorderEngine: &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}}
	if err := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store, ApplyConcurrency: 1}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
	phases, _ := eng.snapshot()
	cleared := slices.Index(phases, "ClearApplyMarks:"+ChainRestoreStreamID)
	firstApply := slices.Index(phases, "Apply:"+ChainRestoreStreamID)
	firstCreate := slices.Index(phases, "CreateTablesWithoutConstraints")
	t.Logf("target saw: %v", phases)
	if firstApply < 0 || firstCreate < 0 {
		t.Fatalf("the restore applied nothing (apply %d, create %d): the cell grades nothing", firstApply, firstCreate)
	}
	if cleared < 0 {
		t.Fatalf("chain restore never cleared %s's apply marks; a mark a failed earlier attempt left could skip a change this run replays", ChainRestoreStreamID)
	}
	if cleared > firstApply || cleared > firstCreate {
		t.Errorf("chain restore cleared its marks at phase %d, after it began applying (create %d, apply %d)", cleared, firstCreate, firstApply)
	}
}
