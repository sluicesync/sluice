// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the audit-MED-A1 restore-mode threading of the ADR-0148
// deploy-request index-build fallback: an armed [Restore] hands the
// fallback to the target SchemaWriter (via the optional
// [ir.IndexBuildFallbackSetter]) BEFORE its Phase-4 CreateIndexes runs —
// the same walled deferred build migrate's index phase runs — and an
// unarmed restore never touches the setter (byte-identical to before the
// fallback existed). The chain dispatch carries the field to the
// segment-0 full (the segment that builds indexes). The engine half —
// walled-error classification, route-vs-surface, never-worse — is pinned
// against the real MySQL writer in
// internal/engines/mysql/schema_writer_index_fallback_test.go; these
// pins cover the orchestrator seam that was missing (the fallback was
// threaded on the migrate path ONLY).

package backup

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// restoreFakeIndexFallback is an inert ir.IndexBuildFallback carrying an
// identity for the threading assertions.
type restoreFakeIndexFallback struct{ id string }

func (restoreFakeIndexFallback) BuildIndexDDL(context.Context, string, []string, error) error {
	return nil
}

// phaseIndex returns the position of the first phase named want, or -1.
func phaseIndex(phases []string, want string) int {
	for i, p := range phases {
		if p == want {
			return i
		}
	}
	return -1
}

// TestRestore_ThreadsIndexBuildFallbackBeforeIndexPhase pins the armed
// path: Restore.Run sets the fallback on the schema writer before
// CreateIndexes runs, and the exact value survives the threading.
func TestRestore_ThreadsIndexBuildFallbackBeforeIndexPhase(t *testing.T) {
	store, _ := restoreChunkFixture(t, 1, 5, 100)
	tgt := newRestoreRecorderEngine("mysql")
	fb := restoreFakeIndexFallback{id: "armed"}

	if err := (&Restore{
		Target:             tgt,
		TargetDSN:          "tgt",
		Store:              store,
		IndexBuildFallback: fb,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	phases, _ := tgt.snapshot()
	setAt, idxAt := phaseIndex(phases, "SetIndexBuildFallback"), phaseIndex(phases, "CreateIndexes")
	if setAt < 0 || idxAt < 0 {
		t.Fatalf("phases = %v; want both SetIndexBuildFallback and CreateIndexes", phases)
	}
	if setAt > idxAt {
		t.Errorf("SetIndexBuildFallback (at %d) must precede CreateIndexes (at %d): %v", setAt, idxAt, phases)
	}
	if tgt.indexFallback != ir.IndexBuildFallback(fb) {
		t.Errorf("threaded fallback = %#v; want the armed value", tgt.indexFallback)
	}
}

// TestRestore_UnarmedNeverTouchesTheSetter pins the zero-value default:
// a nil fallback (every pre-existing caller) never calls the setter, so
// the restore is byte-identical to before ADR-0148 reached this mode.
func TestRestore_UnarmedNeverTouchesTheSetter(t *testing.T) {
	store, _ := restoreChunkFixture(t, 1, 5, 100)
	tgt := newRestoreRecorderEngine("mysql")

	if err := (&Restore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	if tgt.indexFallbackSets != 0 {
		t.Errorf("SetIndexBuildFallback called %d times on an unarmed restore; want 0", tgt.indexFallbackSets)
	}
}

// TestRestore_ChainDispatchCarriesIndexBuildFallback pins the chain leg's
// field plumbing: the dispatching Restore hands the fallback to the
// ChainRestore it constructs (whose applyFull copies it onto the
// segment-0 full's Restore — the one segment that builds indexes).
func TestRestore_ChainDispatchCarriesIndexBuildFallback(t *testing.T) {
	fb := restoreFakeIndexFallback{id: "chain"}
	cr := (&Restore{IndexBuildFallback: fb}).newChainRestore()
	if cr.IndexBuildFallback != ir.IndexBuildFallback(fb) {
		t.Errorf("newChainRestore fallback = %#v; want the dispatching Restore's value", cr.IndexBuildFallback)
	}
}
