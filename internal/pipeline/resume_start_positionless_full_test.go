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

// triggerCDCFake is a fakeCDCEngine that declares trigger CDC — until
// v0.153.3 the one source class whose positionless fulls were allowed to
// anchor "from now"; since roadmap item 163 every trigger engine records
// the change log's anchor on a full, and the exemption is gone.
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

// TestResumeStartFromParent_PositionlessFullRoot pins the v0.153.1 door at
// the chokepoint both chain extenders share: a FULL parent with no
// EndPosition is refused (ErrPositionInvalid, under the
// POSITIONLESS-FULL-ROOT marker) on EVERY source — the trigger-CDC
// exemption v0.153.1 stated is gone since roadmap item 163 (v0.153.3),
// because those engines record an anchor on a full now, so a
// positionless trigger full is a pre-v0.153.3 one or one whose
// snapshot-anchored open refused. With the exemption gone the chokepoint
// no longer consults the source at all; the per-source-class pins are the
// extender twins below, which drive a trigger-CDC source through the real
// IncrementalBackup / BackupStream. A full that DID record a position —
// a pgoutput LSN or a trigger change-log anchor — is returned unchanged.
func TestResumeStartFromParent_PositionlessFullRoot(t *testing.T) {
	t.Parallel()
	recorded := ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}
	triggerRecorded := ir.Position{Engine: "postgres-trigger", Token: `{"last_id":42}`}

	for _, tc := range []struct {
		name       string
		parent     *irbackup.Manifest
		wantPos    ir.Position
		wantRefuse bool
	}{
		{
			"a positionless full is refused",
			&irbackup.Manifest{BackupID: "full0", Kind: irbackup.BackupKindFull},
			ir.Position{},
			true,
		},
		{
			"a full with a recorded pgoutput position resumes from it",
			&irbackup.Manifest{BackupID: "full1", Kind: irbackup.BackupKindFull, EndPosition: recorded}, recorded, false,
		},
		{
			"a trigger full with a recorded change-log anchor resumes from it",
			&irbackup.Manifest{BackupID: "full2", Kind: irbackup.BackupKindFull, EndPosition: triggerRecorded}, triggerRecorded, false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resumeStartFromParent(context.Background(), nil, tc.parent, "manifests/"+tc.parent.BackupID)
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
// hold only the parent. The trigger-CDC twin below is the same door on
// the other source class.
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
	assertIncrementalRefusesPositionlessRoot(t, store, parent, src, &src.cdcSeenFrom)
}

// TestIncremental_TriggerCDCPositionlessFullRefused flips the pin that
// held the v0.153.1 exemption (TestIncremental_TriggerCDCSourceStillAnchorsFromNow,
// whose own doc said to flip it once the trigger engines gained a position
// capturer — roadmap item 163). A trigger-CDC source's positionless full is
// now refused at the same door, before the reader opens: the "from now"
// anchor it used to take silently omitted every write between the full's
// sweep and the incremental's open (100 of 100 rows, v0.153.1 regression
// cycle), and a trigger full taken by this binary records the anchor
// instead. Restoring the exemption in resumeStartFromParent fails this.
func TestIncremental_TriggerCDCPositionlessFullRefused(t *testing.T) {
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
	assertIncrementalRefusesPositionlessRoot(t, store, parent, triggerCDCFake{inner}, &inner.cdcSeenFrom)
}

// assertIncrementalRefusesPositionlessRoot runs one incremental off a
// positionless full and asserts the POSITIONLESS-FULL-ROOT refusal fired
// before the reader opened and before anything was written.
func assertIncrementalRefusesPositionlessRoot(t *testing.T, store *blobcodec.LocalStore, parent *irbackup.Manifest, src ir.Engine, seenFrom *ir.Position) {
	t.Helper()
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
	err := b.Run(context.Background())
	if err == nil {
		t.Fatal("IncrementalBackup.Run = nil; want the POSITIONLESS-FULL-ROOT refusal")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) || !strings.Contains(err.Error(), positionlessFullRootMarker) {
		t.Fatalf("err = %v; want ErrPositionInvalid under the %s marker (a different error means another guard fired first)", err, positionlessFullRootMarker)
	}
	if *seenFrom != (ir.Position{}) {
		t.Errorf("the CDC reader was opened (from=%+v); the refusal must fire before it", *seenFrom)
	}
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("manifests in store = %d; want 1 (the parent only — nothing may be written past the refusal)", len(records))
	}
}

// TestBackupStream_RefusesPositionlessFullRoot is the `backup stream` twin
// of the incremental refusal — the second caller of the shared chokepoint,
// pinned separately so a door that moves out of resumeStartFromParent
// cannot leave one extender uncovered. Both source classes, for the same
// reason the incremental twin has both.
func TestBackupStream_RefusesPositionlessFullRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  func(parent *irbackup.Manifest) (ir.Engine, *ir.Position)
	}{
		{"logical-replication source", func(parent *irbackup.Manifest) (ir.Engine, *ir.Position) {
			src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{parent.Schema}, cdcExpectedFromOK: true}
			return src, &src.cdcSeenFrom
		}},
		{"trigger-CDC source", func(parent *irbackup.Manifest) (ir.Engine, *ir.Position) {
			inner := &fakeCDCEngine{name: "pgtrigger", schemaSequence: []*ir.Schema{parent.Schema}}
			return triggerCDCFake{inner}, &inner.cdcSeenFrom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			parent := positionlessFullParent(t, store)
			src, seenFrom := tc.src(parent)
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
			if *seenFrom != (ir.Position{}) {
				t.Errorf("the CDC reader was opened (from=%+v); the refusal must fire before it", *seenFrom)
			}
		})
	}
}
