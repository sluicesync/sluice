// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// triggerCDCFake is a fakeCDCEngine that declares trigger CDC — the one
// source class whose fulls carry no position by construction and whose
// chain extension is therefore allowed to anchor "from now".
type triggerCDCFake struct{ *fakeCDCEngine }

func (triggerCDCFake) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCTriggers}
}

func positionlessFullParent(t *testing.T, store *blobcodec.LocalStore) *irbackup.Manifest {
	t.Helper()
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema: &ir.Schema{Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}},
		Kind:         irbackup.BackupKindFull,
		PartialState: irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	return parent
}

// TestResumeStartFromParent_PositionlessFullRoot pins the v0.153.1 door in
// both directions at the chokepoint both chain extenders share: a FULL
// parent with no EndPosition is refused (ErrPositionInvalid, under the
// POSITIONLESS-FULL-ROOT marker) on a source that records positions, and
// is let through — empty, "from now" — only for a trigger-CDC source,
// whose fulls never carry one. A full that DID record a position is
// returned unchanged either way.
func TestResumeStartFromParent_PositionlessFullRoot(t *testing.T) {
	t.Parallel()
	logical := &fakeCDCEngine{name: "postgres"}
	triggers := triggerCDCFake{&fakeCDCEngine{name: "pgtrigger"}}
	recorded := ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}

	for _, tc := range []struct {
		name       string
		src        ir.Engine
		parent     *irbackup.Manifest
		wantPos    ir.Position
		wantRefuse bool
	}{
		{
			"positionless full on a logical-replication source is refused", logical,
			&irbackup.Manifest{BackupID: "full0", Kind: irbackup.BackupKindFull},
			ir.Position{},
			true,
		},
		{
			"positionless full on a trigger-CDC source anchors from now", triggers,
			&irbackup.Manifest{BackupID: "full0", Kind: irbackup.BackupKindFull},
			ir.Position{},
			false,
		},
		{
			"a full with a recorded position resumes from it", logical,
			&irbackup.Manifest{BackupID: "full1", Kind: irbackup.BackupKindFull, EndPosition: recorded}, recorded, false,
		},
		{
			"a full with a recorded position resumes from it on a trigger source too", triggers,
			&irbackup.Manifest{BackupID: "full1", Kind: irbackup.BackupKindFull, EndPosition: recorded}, recorded, false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resumeStartFromParent(context.Background(), nil, tc.src, tc.parent, "manifests/"+tc.parent.BackupID)
			if tc.wantRefuse {
				if err == nil {
					t.Fatalf("resumeStartFromParent = (%+v, nil); want a refusal", got)
				}
				if !errors.Is(err, ir.ErrPositionInvalid) {
					t.Errorf("err = %v; want errors.Is ErrPositionInvalid", err)
				}
				if !strings.Contains(err.Error(), positionlessFullRootMarker) {
					t.Errorf("err = %v; want the %s marker", err, positionlessFullRootMarker)
				}
				return
			}
			if err != nil {
				t.Fatalf("resumeStartFromParent: %v; want no refusal", err)
			}
			if got != tc.wantPos {
				t.Errorf("start = %+v; want %+v", got, tc.wantPos)
			}
		})
	}
}

// TestIncremental_RefusesPositionlessFullRoot drives the refusal through
// `backup incremental`: the run must fail on OUR door — the marker proves
// it, since the fake reader would answer a different error to an empty
// position — before any CDC reader is opened, and the store must still
// hold only the parent. The trigger-CDC twin below is the other direction.
func TestIncremental_RefusesPositionlessFullRoot(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := positionlessFullParent(t, store)
	src := &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{parent.Schema},
		cdcExpectedFromOK: true,
	}
	now := time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}
	err = b.Run(context.Background())
	if err == nil {
		t.Fatal("IncrementalBackup.Run = nil; want the POSITIONLESS-FULL-ROOT refusal")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) || !strings.Contains(err.Error(), positionlessFullRootMarker) {
		t.Fatalf("err = %v; want ErrPositionInvalid under the %s marker (a different error means another guard fired first)", err, positionlessFullRootMarker)
	}
	if src.cdcSeenFrom != (ir.Position{}) {
		t.Errorf("the CDC reader was opened (from=%+v); the refusal must fire before it", src.cdcSeenFrom)
	}
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("manifests in store = %d; want 1 (the parent only — nothing may be written past the refusal)", len(records))
	}
}

// TestIncremental_TriggerCDCSourceStillAnchorsFromNow pins the stated
// exemption: a trigger-CDC source's positionless full still extends, with
// the reader handed an empty position (its own "from now" anchor). If this
// starts refusing, the trigger engines gained a position capturer and the
// exemption in resumeStartFromParent should be removed, not this test.
func TestIncremental_TriggerCDCSourceStillAnchorsFromNow(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := positionlessFullParent(t, store)
	inner := &fakeCDCEngine{
		name:           "pgtrigger",
		schemaSequence: []*ir.Schema{parent.Schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "pgtrigger", Token: `{"last_id":10}`}},
			ir.Insert{Position: ir.Position{Engine: "pgtrigger", Token: `{"last_id":11}`}, Table: "users", Row: ir.Row{"id": int64(1)}},
			ir.TxCommit{Position: ir.Position{Engine: "pgtrigger", Token: `{"last_id":12}`}},
		},
	}
	now := time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        triggerCDCFake{inner},
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run on a trigger-CDC source: %v; the exemption must let it anchor from now", err)
	}
	if inner.cdcSeenFrom != (ir.Position{}) {
		t.Errorf("CDC reader from = %+v; want the empty (from-now) position", inner.cdcSeenFrom)
	}
}

// TestBackupStream_RefusesPositionlessFullRoot is the `backup stream` twin
// of the incremental refusal — the second caller of the shared chokepoint,
// pinned separately so a door that moves out of resumeStartFromParent
// cannot leave one extender uncovered.
func TestBackupStream_RefusesPositionlessFullRoot(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := positionlessFullParent(t, store)
	src := &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{parent.Schema},
		cdcExpectedFromOK: true,
	}
	stream := &BackupStream{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		ParentRef: parent.BackupID,
		pidHostFn: func() (int, string) { return 1, "h" },
	}
	err = stream.Run(context.Background())
	if err == nil {
		t.Fatal("BackupStream.Run = nil; want the POSITIONLESS-FULL-ROOT refusal")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) || !strings.Contains(err.Error(), positionlessFullRootMarker) {
		t.Fatalf("err = %v; want ErrPositionInvalid under the %s marker", err, positionlessFullRootMarker)
	}
	if src.cdcSeenFrom != (ir.Position{}) {
		t.Errorf("the CDC reader was opened (from=%+v); the refusal must fire before it", src.cdcSeenFrom)
	}
}
