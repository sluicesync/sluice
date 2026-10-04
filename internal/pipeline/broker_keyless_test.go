// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the audit F-E1 interim door on `sync from-backup`: the
// keyless refusal (SLUICE-E-BROKER-KEYLESS-TABLE) and the mid-incremental
// cancel that is no longer a clean exit (BROKER-INCREMENTAL-PARTIAL). They
// drive SyncFromBackup.Run end to end over a real local-FS chain, against a
// fake target whose applier records what reached it. The real-database
// pins (the {error, cancel} × {serial, lanes} × {keyless, keyed} matrix on
// Postgres, the MySQL target cells) live in broker_fe1_integration_test.go.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// brokerReplayFixture builds a plaintext full + one incremental carrying two
// inserts into "users", which is keyed (PRIMARY KEY) or keyless as asked.
// Returns the store, the full's BackupID and the incremental's BackupID.
func brokerReplayFixture(t *testing.T, keyed bool) (store irbackup.Store, fullID, incrID string) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	table := &ir.Table{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	if keyed {
		table.PrimaryKey = &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}
	}
	schema := &ir.Schema{Tables: []*ir.Table{table}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: pos("0/110")},
			ir.Insert{Position: pos("0/120"), Table: "users", Row: ir.Row{"id": int64(3)}},
			ir.Insert{Position: pos("0/130"), Table: "users", Row: ir.Row{"id": int64(4)}},
			ir.TxCommit{Position: pos("0/140")},
		},
	}
	if err := (&IncrementalBackup{
		Source: cdc, SourceDSN: "src", Store: store, ParentRef: full.BackupID,
		Window: time.Minute, ChunkChanges: 1,
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	recs, err := lineage.ListAllManifestsViaWalk(ctx, store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	for _, r := range recs {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incrID = lineage.ManifestBackupID(r.Manifest)
		}
	}
	if incrID == "" {
		t.Fatal("fixture produced no incremental")
	}
	return store, full.BackupID, incrID
}

// replayApplier is a target applier for SyncFromBackup.Run: it can present
// a persisted broker position (warm resume) and runs a hook on every change
// and every position write, recording both.
type replayApplier struct {
	capturingApplier
	resume   *ir.Position
	onChange func(ctx context.Context) error
	onWrite  func(p ir.Position)
}

func (a *replayApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	if a.resume == nil {
		return ir.Position{}, false, nil
	}
	return *a.resume, true, nil
}

func (a *replayApplier) ApplyBatch(ctx context.Context, _ string, ch <-chan ir.Change, _ int) error {
	for c := range ch {
		a.rec(c.Pos())
		if a.onChange != nil {
			if err := a.onChange(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *replayApplier) WritePosition(ctx context.Context, s string, p ir.Position) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.capturingApplier.WritePosition(ctx, s, p); err != nil {
		return err
	}
	if a.onWrite != nil {
		a.onWrite(p)
	}
	return nil
}

// replayKeyWriter answers the target-catalog judgment: every table exists
// and is keyed as configured.
type replayKeyWriter struct{ keyed bool }

func (replayKeyWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (w replayKeyWriter) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	return true, w.keyed, nil
}

// replayTargetEngine is stubTargetEngine with a working applier and row
// writer.
type replayTargetEngine struct {
	stubTargetEngine
	applier ir.ChangeApplier
	rw      ir.RowWriter
}

func (e replayTargetEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return e.applier, nil
}

func (e replayTargetEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return e.rw, nil
}

func newReplayBroker(store irbackup.Store, app *replayApplier, targetKeyed bool) *SyncFromBackup {
	return &SyncFromBackup{
		Target:       replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: targetKeyed}},
		TargetDSN:    "tgt",
		Store:        store,
		ChainURL:     "test://fe1",
		StreamID:     "fe1",
		PollInterval: time.Hour,
		pidHostFn:    func() (int, string) { return 1, "test" },
	}
}

// TestSyncFromBackup_KeylessTable_RefusedBeforeAnyApply pins the F-E1 door
// on every entry the broker has — warm resume, --at-chain-id and
// --reset-target-data cold starts — and on both judgments: a table keyless
// in the chain's recorded schema, and a recorded-keyed table the TARGET
// holds without a key. Before the door, warm resume applied the incremental.
func TestSyncFromBackup_KeylessTable_RefusedBeforeAnyApply(t *testing.T) {
	cases := []struct {
		name         string
		recordedKey  bool
		targetKeyed  bool
		entry        string // "warm", "at-chain-id", "reset"
		wantInReason string
	}{
		{"recorded keyless, warm resume", false, true, "warm", "recorded schema"},
		{"recorded keyless, --at-chain-id", false, true, "at-chain-id", "recorded schema"},
		{"recorded keyless, --reset-target-data", false, true, "reset", "recorded schema"},
		{"target keyless, warm resume", true, false, "warm", "target table"},
		{"target keyless, --at-chain-id", true, false, "at-chain-id", "target table"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, fullID, _ := brokerReplayFixture(t, tc.recordedKey)
			app := &replayApplier{}
			b := newReplayBroker(store, app, tc.targetKeyed)
			switch tc.entry {
			case "warm":
				p := encodeBrokerPosition(b.ChainURL, fullID)
				app.resume = &p
			case "at-chain-id":
				b.AtChainID = fullID
			case "reset":
				b.ResetTargetData = true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := b.Run(ctx)
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBrokerKeylessTable {
				t.Fatalf("Run = %v; want %s", err, sluicecode.CodeBrokerKeylessTable)
			}
			if !strings.Contains(err.Error(), `"users"`) || !strings.Contains(err.Error(), tc.wantInReason) {
				t.Errorf("refusal does not name the table and the failing judgment (%q): %v", tc.wantInReason, err)
			}
			if len(app.received) != 0 || len(app.written) != 0 {
				t.Errorf("the door fired AFTER the broker touched the target: %d changes applied, %d positions written",
					len(app.received), len(app.written))
			}
		})
	}
}

// TestSyncFromBackup_CancelMidIncremental_ExitsWithPartialMarker pins F-E1
// (a): a cancel that lands while an incremental is being applied returns the
// BROKER-INCREMENTAL-PARTIAL error naming the incremental — not nil (exit 0),
// which is what the broker returned before — and the error must not unwrap
// to context.Canceled, or the live panel would launder it into "stopped.".
func TestSyncFromBackup_CancelMidIncremental_ExitsWithPartialMarker(t *testing.T) {
	store, fullID, incrID := brokerReplayFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	app := &replayApplier{}
	p := encodeBrokerPosition("test://fe1", fullID)
	app.resume = &p
	app.onChange = func(ctx context.Context) error {
		runCancel() // SIGINT lands after the first change of the incremental
		return ctx.Err()
	}
	err := newReplayBroker(store, app, true).Run(runCtx)
	if err == nil {
		t.Fatal("Run returned nil (exit 0) for a cancel that interrupted an incremental")
	}
	if !strings.Contains(err.Error(), BrokerIncrementalPartialMarker) || !strings.Contains(err.Error(), incrID) {
		t.Errorf("error lacks the %s marker or the incremental's id %s: %v", BrokerIncrementalPartialMarker, incrID, err)
	}
	if errors.Is(err, context.Canceled) {
		t.Errorf("the partial error unwraps to context.Canceled, so the live panel would report it as a clean stop: %v", err)
	}
	if len(app.received) == 0 {
		t.Fatal("no change reached the applier; the mid-incremental cancel was not exercised")
	}
	incrPos := encodeBrokerPosition("test://fe1", incrID)
	for _, w := range app.written {
		if w == incrPos {
			t.Error("the interrupted incremental's position was persisted")
		}
	}
}

// TestSyncFromBackup_CancelAfterIncremental_ExitsClean is the other half of
// the rule: a cancel observed once the incremental is fully applied and its
// position advanced is the clean stop it always was.
func TestSyncFromBackup_CancelAfterIncremental_ExitsClean(t *testing.T) {
	store, fullID, incrID := brokerReplayFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	app := &replayApplier{}
	p := encodeBrokerPosition("test://fe1", fullID)
	app.resume = &p
	incrPos := encodeBrokerPosition("test://fe1", incrID)
	app.onWrite = func(w ir.Position) {
		if w == incrPos {
			runCancel() // SIGINT lands right after the incremental committed
		}
	}
	if err := newReplayBroker(store, app, true).Run(runCtx); err != nil {
		t.Fatalf("Run = %v; want nil for a cancel between incrementals", err)
	}
	if len(app.received) != 4 {
		t.Errorf("applier received %d changes; want the incremental's 4 events (begin, two inserts, commit)", len(app.received))
	}
}

// TestSyncFromBackup_CancelBetweenIncrementals_ExitsClean pins the clean
// half of the rule INSIDE a tick, which the after-tick test above cannot
// reach (its cancel is observed by the between-ticks stop poll): a tick with
// two incrementals is cancelled right after the first one's position
// committed, so the cancel is observed at the between-incrementals check of
// replayNewIncrementals. Nothing of the second incremental was applied, so
// this is the clean stop it always was — nil, not the partial error.
func TestSyncFromBackup_CancelBetweenIncrementals_ExitsClean(t *testing.T) {
	store, fullID, incrID := brokerReplayFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	users := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	if err := (&IncrementalBackup{
		Source: &fakeCDCEngine{
			name:           "postgres",
			schemaSequence: []*ir.Schema{{Tables: []*ir.Table{users}}},
			cdcChanges: []ir.Change{
				ir.TxBegin{Position: pos("0/150")},
				ir.Insert{Position: pos("0/160"), Table: "users", Row: ir.Row{"id": int64(5)}},
				ir.TxCommit{Position: pos("0/170")},
			},
		},
		SourceDSN: "src", Store: store, ParentRef: incrID, Window: time.Minute, ChunkChanges: 1,
	}).Run(ctx); err != nil {
		t.Fatalf("second IncrementalBackup.Run: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	app := &replayApplier{}
	p := encodeBrokerPosition("test://fe1", fullID)
	app.resume = &p
	firstPos := encodeBrokerPosition("test://fe1", incrID)
	app.onWrite = func(w ir.Position) {
		if w == firstPos {
			runCancel() // SIGINT lands between the tick's two incrementals
		}
	}
	if err := newReplayBroker(store, app, true).Run(runCtx); err != nil {
		t.Fatalf("Run = %v; want nil for a cancel between incrementals", err)
	}
	if len(app.received) != 4 {
		t.Errorf("applier received %d changes; want only the first incremental's 4 (the cancel must stop the tick before the second)", len(app.received))
	}
}

// TestSyncFromBackup_KeylessTableAddedLater_RefusedAtTick pins the TICK-time
// half of the door, which the start-time door cannot stand in for: the
// broker starts cleanly on a keyed chain, applies its incremental, and only
// THEN does the producer append an incremental whose window created a
// keyless table. The next tick must refuse before applying any of it.
func TestSyncFromBackup_KeylessTableAddedLater_RefusedAtTick(t *testing.T) {
	store, fullID, incrID := brokerReplayFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	app := &replayApplier{}
	p := encodeBrokerPosition("test://fe1", fullID)
	app.resume = &p
	b := newReplayBroker(store, app, true)
	b.PollInterval = 200 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	incrPos := encodeBrokerPosition("test://fe1", incrID)
	for applied := false; !applied; {
		select {
		case err := <-done:
			t.Fatalf("broker exited before applying the keyed incremental: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		app.mu.Lock()
		for _, w := range app.written {
			applied = applied || w == incrPos
		}
		app.mu.Unlock()
	}
	app.mu.Lock()
	receivedBefore := len(app.received)
	app.mu.Unlock()

	users := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	events := &ir.Table{Name: "events", Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}}}
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	cdc := &fakeCDCEngine{
		name: "postgres",
		// The window-START schema is the parent's recorded one; the single
		// read here is the window-END read, which sees the new table — so
		// the incremental records an AddTable delta for "events".
		schemaSequence: []*ir.Schema{{Tables: []*ir.Table{users, events}}},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: pos("0/150")},
			ir.Insert{Position: pos("0/160"), Table: "events", Row: ir.Row{"v": int64(1)}},
			ir.TxCommit{Position: pos("0/170")},
		},
	}
	if err := (&IncrementalBackup{
		Source: cdc, SourceDSN: "src", Store: store, ParentRef: incrID,
		Window: time.Minute, ChunkChanges: 1,
	}).Run(ctx); err != nil {
		t.Fatalf("second IncrementalBackup.Run: %v", err)
	}

	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("broker did not refuse the keyless table the second incremental added")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBrokerKeylessTable || !strings.Contains(err.Error(), `"events"`) {
		t.Fatalf("Run = %v; want %s naming \"events\"", err, sluicecode.CodeBrokerKeylessTable)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.received) != receivedBefore {
		t.Errorf("the tick applied %d change(s) of the keyless incremental before refusing", len(app.received)-receivedBefore)
	}
}
