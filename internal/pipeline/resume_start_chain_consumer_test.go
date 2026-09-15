// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// The prune-floor half of roadmap item 163: both chain extenders seat the
// chain in a trigger-CDC source's change-log consumer registry at the
// position they resume from, and move the seat to each committed link's
// EndPosition. These pins drive the REAL IncrementalBackup / BackupStream
// against a reader that records every registration, so the two calls per
// link — before the read, after the commit — and the token each carries
// are asserted, not inferred. The engine-side half (the prune cutting at
// the registry MIN) is item 115's, already pinned per engine.

// registration is one RegisterChangeLogConsumer call as the reader saw it.
type registration struct {
	consumerID string
	token      string
}

// registeringCDCReader is a fakeCDCReader that also implements
// [ir.ChangeLogConsumerRegistry], recording each registration. registerErr,
// when set, fails every registration — the failure-isolation pin.
type registeringCDCReader struct {
	*fakeCDCReader
	registerErr   error
	registrations []registration
}

func (r *registeringCDCReader) RegisterChangeLogConsumer(_ context.Context, consumerID, token string) error {
	r.registrations = append(r.registrations, registration{consumerID, token})
	return r.registerErr
}

func (r *registeringCDCReader) PruneConsumedChangeLogToRegisteredMin(context.Context, string, string, int64) (int64, error) {
	return 0, errors.New("not used")
}

// registeringCDCEngine is a trigger-CDC fake whose reader is a
// registeringCDCReader, so the pipeline's type assertion finds the
// registry surface exactly as it does on pgtrigger / sqlite-trigger.
type registeringCDCEngine struct {
	*fakeCDCEngine
	reader *registeringCDCReader
}

func (registeringCDCEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCTriggers}
}

func (e registeringCDCEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	e.reader.fakeCDCReader = &fakeCDCReader{engine: e.fakeCDCEngine}
	return e.reader, nil
}

// triggerChainParent writes a trigger full with a recorded change-log
// anchor and returns it.
func triggerChainParent(t *testing.T, store *blobcodec.LocalStore, anchor string) *irbackup.Manifest {
	t.Helper()
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres-trigger",
		Schema: &ir.Schema{Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}}},
		Kind:         irbackup.BackupKindFull,
		EndPosition:  ir.Position{Engine: "postgres-trigger", Token: anchor},
		PartialState: irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	return parent
}

func triggerTok(n int64) ir.Position {
	return ir.Position{Engine: "postgres-trigger", Token: `{"last_id":` + strconv.FormatInt(n, 10) + `}`}
}

// TestIncremental_RegistersChainAsChangeLogConsumer pins the incremental
// lane: seat at the resume position BEFORE the read, then at the window's
// EndPosition after the commit, both under the chain's root-derived id.
func TestIncremental_RegistersChainAsChangeLogConsumer(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := triggerChainParent(t, store, `{"last_id":10}`)
	inner := &fakeCDCEngine{
		name:           "postgres-trigger",
		schemaSequence: []*ir.Schema{parent.Schema},
		cdcChanges: []ir.Change{
			ir.Insert{Position: triggerTok(11), Table: "users", Row: ir.Row{"id": int64(1)}},
			ir.Insert{Position: triggerTok(12), Table: "users", Row: ir.Row{"id": int64(2)}},
		},
	}
	reader := &registeringCDCReader{}
	now := time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        registeringCDCEngine{inner, reader},
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
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	wantID := chainConsumerIDPrefix + parent.BackupID
	want := []registration{
		{wantID, `{"last_id":10}`}, // before the read: the resume position
		{wantID, `{"last_id":12}`}, // after the commit: the window's end
	}
	assertRegistrations(t, reader.registrations, want)
	// The seat's second value must be what the manifest actually recorded —
	// the independent expected value is the committed link, not the fake's
	// last event.
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var incr *irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incr = r.Manifest
		}
	}
	if incr == nil {
		t.Fatal("no incremental manifest was written")
	}
	if incr.EndPosition.Token != reader.registrations[1].token {
		t.Errorf("post-commit registration token = %q; want the committed EndPosition %q", reader.registrations[1].token, incr.EndPosition.Token)
	}
}

// TestIncremental_ChainRegistrationFailureDoesNotFailTheBackup pins the
// failure isolation: a registry write that fails WARNs and the backup
// still commits — a chain that cannot register is invisible to the
// pruner, which the WARN says, but a working backup must not be refused
// for a registry problem on the source.
func TestIncremental_ChainRegistrationFailureDoesNotFailTheBackup(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := triggerChainParent(t, store, `{"last_id":10}`)
	inner := &fakeCDCEngine{
		name:           "postgres-trigger",
		schemaSequence: []*ir.Schema{parent.Schema},
		cdcChanges: []ir.Change{
			ir.Insert{Position: triggerTok(11), Table: "users", Row: ir.Row{"id": int64(1)}},
		},
	}
	reader := &registeringCDCReader{registerErr: errors.New("no such table: sluice_change_log_consumers")}
	now := time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        registeringCDCEngine{inner, reader},
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
		t.Fatalf("IncrementalBackup.Run: %v; a failed registry write must WARN, never fail the backup", err)
	}
	if len(reader.registrations) != 2 {
		t.Errorf("registrations = %d; want 2 (both attempted despite failing)", len(reader.registrations))
	}
}

// TestBackupStream_RegistersChainAsChangeLogConsumer pins the stream lane:
// one seat at the resume position before the first read, then one per
// committed rollover at that rollover's EndPosition.
func TestBackupStream_RegistersChainAsChangeLogConsumer(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := triggerChainParent(t, store, `{"last_id":100}`)
	// Two rollovers of 3 inserts each under RolloverMaxChanges=3; the
	// channel closes after the sixth, ending the stream.
	var changes []ir.Change
	for n := int64(101); n <= 106; n++ {
		changes = append(changes, ir.Insert{Position: triggerTok(n), Table: "users", Row: ir.Row{"id": n}})
	}
	inner := &fakeCDCEngine{
		name:           "postgres-trigger",
		schemaSequence: []*ir.Schema{parent.Schema},
		cdcChanges:     changes,
	}
	reader := &registeringCDCReader{}
	now := time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             registeringCDCEngine{inner, reader},
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute,
		RolloverMaxChanges: 3,
		RolloverMaxBytes:   1 << 40,
		ChunkChanges:       100,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 1, "h" },
		streamStatePath:    DefaultStreamStateFilename,
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("BackupStream.Run: %v", err)
	}
	wantID := chainConsumerIDPrefix + parent.BackupID
	if len(reader.registrations) < 2 {
		t.Fatalf("registrations = %+v; want the resume seat plus one per committed rollover", reader.registrations)
	}
	if reader.registrations[0] != (registration{wantID, `{"last_id":100}`}) {
		t.Errorf("first registration = %+v; want the resume position under %q", reader.registrations[0], wantID)
	}
	// Every later registration is a committed rollover's EndPosition, in
	// commit order, and strictly advancing.
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var ends []string
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			ends = append(ends, r.Manifest.EndPosition.Token)
		}
	}
	got := reader.registrations[1:]
	if len(got) != len(ends) {
		t.Fatalf("post-commit registrations = %d; want one per committed rollover (%d): %+v", len(got), len(ends), got)
	}
	for i, reg := range got {
		if reg.consumerID != wantID {
			t.Errorf("registration %d consumer_id = %q; want %q", i+1, reg.consumerID, wantID)
		}
		found := false
		for _, e := range ends {
			if e == reg.token {
				found = true
			}
		}
		if !found {
			t.Errorf("registration %d token %q is not any committed rollover's EndPosition %v", i+1, reg.token, ends)
		}
	}
}

// TestRegisterChainConsumer_NoOpShapes pins the two silent no-ops: a
// reader without the registry surface (every non-trigger source), and an
// empty position (a link that recorded nothing keeps the previous seat).
func TestRegisterChainConsumer_NoOpShapes(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := triggerChainParent(t, store, `{"last_id":1}`)
	plain := &fakeCDCReader{engine: &fakeCDCEngine{name: "postgres"}}
	registerChainConsumer(context.Background(), plain, store, triggerTok(5), "test") // must not panic
	reader := &registeringCDCReader{fakeCDCReader: plain}
	registerChainConsumer(context.Background(), reader, store, ir.Position{}, "test")
	if len(reader.registrations) != 0 {
		t.Errorf("an empty position registered %+v; want no call", reader.registrations)
	}
	registerChainConsumer(context.Background(), reader, store, triggerTok(5), "test")
	assertRegistrations(t, reader.registrations, []registration{{chainConsumerIDPrefix + parent.BackupID, `{"last_id":5}`}})
}

func assertRegistrations(t *testing.T, got, want []registration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("registrations = %+v; want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("registration %d = %+v; want %+v", i, got[i], want[i])
		}
	}
}
