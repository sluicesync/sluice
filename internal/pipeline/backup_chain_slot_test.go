// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the --chain-slot chain-provisioning shape (task #40):
//
//   - the orchestrator threads ChainSlot → opts.PersistChainSlot,
//   - the snapshot's CommitFn fires exactly once, AFTER the final
//     manifest commit, and only on success (a failed run must leave
//     CommitFn uncalled so the engine's Close drops the slot),
//   - --chain-slot REFUSES the v0.17.x fallback in both flavours
//     (snapshot open error / engine without an opener) instead of
//     silently degrading into a chain that cannot exist,
//   - the incremental orchestrator runs the engine's
//     [ir.ChainResumePreflighter] before opening CDC and surfaces its
//     refusal verbatim.
//
// The Postgres-side behaviour (slot actually kept/dropped, the
// confirmed_flush gap refusal against a real server) is pinned by the
// integration tests in backup_chain_slot_pg_integration_test.go.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func chainSlotTestEngine(t *testing.T) (*snapshotOpeningEngine, *ir.Schema) {
	t.Helper()
	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	src := &snapshotOpeningEngine{
		capturingBackupEngine: &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
				"users": {{"id": int64(1)}},
			}),
			cdc:    ir.CDCLogicalReplication,
			reader: &capturingSchemaReader{schema: schema},
		},
		snapshotPos: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/SNAP00"}`},
	}
	return src, schema
}

func TestBackup_ChainSlot_CommitsSnapshotOnSuccess(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	src, _ := chainSlotTestEngine(t)

	b := &Backup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		SlotName:  "sluice_slot",
		ChainSlot: true,
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if !src.gotPersistChainSlot {
		t.Error("opts.PersistChainSlot = false; want true (ChainSlot not threaded)")
	}
	if src.commitCalls != 1 {
		t.Errorf("snapshot CommitFn calls = %d; want exactly 1 on success", src.commitCalls)
	}
	if src.snapshotCloses != 1 {
		t.Errorf("snapshot Close calls = %d; want 1 (Close still required after Commit)", src.snapshotCloses)
	}
}

// erroringSnapshotRowReader fails the table sweep so the run errors
// AFTER the snapshot opened — the commit-only-on-success pin.
type erroringSnapshotRowReader struct{}

func (erroringSnapshotRowReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, errors.New("boom: injected sweep failure")
}
func (erroringSnapshotRowReader) Err() error { return nil }

func TestBackup_ChainSlot_NoCommitOnFailedRun(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	src, _ := chainSlotTestEngine(t)
	src.useSnapshotRows = true
	src.snapshotRowsHook = func() ir.RowReader { return erroringSnapshotRowReader{} }

	b := &Backup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		ChainSlot: true,
	}
	if err := b.Run(context.Background()); err == nil {
		t.Fatal("Backup.Run succeeded; want injected sweep failure")
	}
	if src.commitCalls != 0 {
		t.Errorf("snapshot CommitFn calls = %d on a FAILED run; want 0 (engine Close must drop the slot)", src.commitCalls)
	}
	if src.snapshotCloses != 1 {
		t.Errorf("snapshot Close calls = %d; want 1 (cleanup on failure)", src.snapshotCloses)
	}
}

func TestBackup_ChainSlot_RefusesSnapshotFallback(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	t.Run("snapshot open error", func(t *testing.T) {
		src, _ := chainSlotTestEngine(t)
		src.snapshotErr = errors.New("wal_level is replica")
		b := &Backup{Source: src, SourceDSN: "src", Store: store, ChainSlot: true}
		err := b.Run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "--chain-slot") {
			t.Errorf("err = %v; want loud --chain-slot refusal instead of the v0.17.x fallback", err)
		}
	})

	t.Run("engine without snapshot opener", func(t *testing.T) {
		schema := &ir.Schema{Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}}
		src := &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{}),
			cdc:                  ir.CDCLogicalReplication,
			reader:               &capturingSchemaReader{schema: schema},
		}
		b := &Backup{Source: src, SourceDSN: "src", Store: store, ChainSlot: true}
		err := b.Run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "--chain-slot") {
			t.Errorf("err = %v; want loud --chain-slot refusal (engine has no snapshot opener)", err)
		}
	})
}

// preflightingBackupEngine implements [ir.ChainResumePreflighter] on
// top of the standard recorder engine so the incremental orchestrator
// discovers and runs the preflight.
type preflightingBackupEngine struct {
	*capturingBackupEngine
	preflightErr   error
	preflightCalls int
	gotFrom        ir.Position
}

func (e *preflightingBackupEngine) PreflightChainResume(_ context.Context, _ string, from ir.Position) error {
	e.preflightCalls++
	e.gotFrom = from
	return e.preflightErr
}

func TestIncremental_ChainPreflightRefusalStopsBeforeCDC(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	src, _ := chainSlotTestEngine(t)

	// Seed the store with a completed full (the parent) the
	// incremental chains off.
	full := &Backup{Source: src, SourceDSN: "src", Store: store, SlotName: "sluice_slot"}
	if err := full.Run(context.Background()); err != nil {
		t.Fatalf("seed full backup: %v", err)
	}

	pf := &preflightingBackupEngine{
		capturingBackupEngine: src.capturingBackupEngine,
		preflightErr:          errors.New("slot sluice_slot confirmed_flush_lsn is AHEAD of the parent"),
	}
	incr := &IncrementalBackup{
		Source:    pf,
		SourceDSN: "src",
		Store:     store,
	}
	err = incr.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "chain preflight") || !strings.Contains(err.Error(), "AHEAD of the parent") {
		t.Fatalf("err = %v; want the preflight refusal surfaced verbatim", err)
	}
	if pf.preflightCalls != 1 {
		t.Errorf("PreflightChainResume calls = %d; want 1", pf.preflightCalls)
	}
	if pf.gotFrom.Token == "" {
		t.Error("preflight received an empty position; want the parent's EndPosition")
	}
}
