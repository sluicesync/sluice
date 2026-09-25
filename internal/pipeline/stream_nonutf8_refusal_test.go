// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// countingCDCEngine counts the CDC pump opens a [fakeCDCEngine] serves, so
// a test can tell "stopped" from "reopened and retried".
type countingCDCEngine struct {
	*fakeCDCEngine
	cdcOpens int
}

func (e *countingCDCEngine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	e.cdcOpens++
	return e.fakeCDCEngine.OpenCDCReader(ctx, dsn)
}

// TestBackupStream_NonUTF8StringStopsTheStream pins where the "backup
// stream stops" premise lives: the codec refusal (BACKUP-VALUE-NOT-UTF8)
// is not an ir.RetriableError, so Run returns it instead of reopening the
// pump from the parent and replaying the same value. It also pins that the
// refused rollover commits no manifest — the chain still ends at the last
// good rollover — and that it no longer re-reads the source schema for a
// window that is never committed (the short-circuit in runRollover), which
// would also have let a schema-read or fill failure replace the refusal's
// own message.
func TestBackupStream_NonUTF8StringStopsTheStream(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
		},
	}}}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// Rollover 1 (good) closes at its commit; rollover 2 carries the value.
	changes := []ir.Change{
		ir.TxBegin{Position: posTok(101)},
		ir.Insert{Position: posTok(102), Table: "users", Row: ir.Row{"id": int64(1), "name": "ok"}},
		ir.Insert{Position: posTok(103), Table: "users", Row: ir.Row{"id": int64(2), "name": "fine"}},
		ir.TxCommit{Position: posTok(104)},
		ir.TxBegin{Position: posTok(105)},
		ir.Insert{Position: posTok(106), Table: "users", Row: ir.Row{"id": int64(3), "name": "caf\xe9"}},
		ir.TxCommit{Position: posTok(107)},
	}
	src := &countingCDCEngine{fakeCDCEngine: &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{schema},
		cdcChanges:        changes,
		cdcExpectedFromOK: true,
	}}
	now := time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute,
		RolloverMaxChanges: 3,
		RolloverMaxBytes:   1 << 40,
		ChunkChanges:       100,
		RetryAttempts:      5, // a retriable error WOULD be retried
		RetryBackoffBase:   time.Millisecond,
		RetryBackoffCap:    time.Millisecond,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 12345, "test-host" },
		streamStatePath:    DefaultStreamStateFilename,
	}

	// Bounded: a stream that retried the refusal would replay the same value
	// from the parent for as long as it was allowed to.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = stream.Run(ctx)
	if src.cdcOpens != 1 {
		t.Fatalf("the CDC pump was opened %d times; want 1 — the refusal was retried from the parent, which replays the same value (Run: %v)", src.cdcOpens, err)
	}
	if err == nil {
		t.Fatal("backup stream completed through a non-UTF-8 string; its chunk would restore U+FFFD")
	}
	for _, want := range []string{"BACKUP-VALUE-NOT-UTF8", `"users"`, `"name"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %s: %v", want, err)
		}
	}

	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	var incrementals []*irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incrementals = append(incrementals, r.Manifest)
		}
	}
	if len(incrementals) != 1 {
		t.Fatalf("incremental manifests = %d; want 1 (the good rollover only — the refused window commits nothing)", len(incrementals))
	}
	var rows int64
	for _, c := range incrementals[0].ChangeChunks {
		rows += c.RowCount
	}
	if rows != 4 {
		t.Errorf("committed rollover carries %d changes; want the 4 of the good transaction", rows)
	}

	// Schema reads: the good rollover's refresh is the only one (the stream
	// does not read the source schema at setup). The refused rollover must not
	// read it again (runRollover's short-circuit); without it this is 2.
	if got := src.schemaReadCalls; got != 1 {
		t.Errorf("source schema read %d times; want 1 — the refused rollover still refreshed the schema (and would have captured an ADD COLUMN fill) for a window it never commits", got)
	}
}
