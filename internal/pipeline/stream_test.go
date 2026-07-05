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
)

// TestBackupStream_Validate covers the same validation surface as
// IncrementalBackup but for the long-running shape.
func TestBackupStream_Validate(t *testing.T) {
	cases := []struct {
		name string
		b    *BackupStream
		want string
	}{
		{"nil source", &BackupStream{SourceDSN: "x", Store: &blobcodec.LocalStore{}}, "Source engine is nil"},
		{"empty DSN", &BackupStream{Source: &fakeCDCEngine{name: "postgres"}, Store: &blobcodec.LocalStore{}}, "SourceDSN is empty"},
		{"nil store", &BackupStream{Source: &fakeCDCEngine{name: "postgres"}, SourceDSN: "x"}, "Store is nil"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.b.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contains %q", err, c.want)
			}
		})
	}
}

// TestBackupStream_Validate_NoCDC mirrors the IncrementalBackup CDC-
// capability gate.
func TestBackupStream_Validate_NoCDC(t *testing.T) {
	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}
	wrapped := &noCDCCapEngine{src: src}
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	b := &BackupStream{Source: wrapped, SourceDSN: "x", Store: store}
	err := b.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not declare CDC support") {
		t.Errorf("err = %v; want CDC capability refusal", err)
	}
}

// TestBackupStream_RolloverOnMaxChanges drives a stream against a fake
// CDC source that emits 25 inserts, with --rollover-max-changes=10.
// Expects 3 rollover manifests committed (10 + 10 + 5).
//
// The rollover loop terminates when the CDC channel closes (the fake
// emits all changes then closes); this stand-in for "stream stops on
// source-side end-of-stream" is the cleanest unit-test shape.
func TestBackupStream_RolloverOnMaxChanges(t *testing.T) {
	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	// 30 changes structured as 5 transactions each carrying 4 inserts
	// + 1 begin + 1 commit (= 6 events per tx, 30 total). With
	// RolloverMaxChanges=10 and the "close at next TxCommit boundary"
	// straddle policy, rollover 1 closes after tx 2 (12 events),
	// rollover 2 closes after tx 4 (12 events), rollover 3 closes when
	// the channel closes after tx 5 (6 events).
	var changes []ir.Change
	posN := 100
	for tx := 0; tx < 5; tx++ {
		changes = append(changes, ir.TxBegin{Position: posTok(posN)})
		posN++
		for i := 0; i < 4; i++ {
			changes = append(changes, ir.Insert{
				Position: posTok(posN),
				Table:    "users",
				Row:      ir.Row{"id": int64(tx*10 + i)},
			})
			posN++
		}
		changes = append(changes, ir.TxCommit{Position: posTok(posN)})
		posN++
	}

	src := &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{schema},
		cdcChanges:        changes,
		cdcExpectedFromOK: true,
	}

	now := time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute,
		RolloverMaxChanges: 10,
		RolloverMaxBytes:   1 << 40, // huge — never fires
		ChunkChanges:       100,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 12345, "test-host" },
		streamStatePath:    DefaultStreamStateFilename,
	}

	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("stream.Run: %v", err)
	}

	// Verify we got 3 rollovers. Total changes across them = 30 (==
	// source emitted). The third rollover closes because the CDC
	// channel closes (no more changes), not because max-changes fires.
	records, err := listAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("listAllManifestsViaWalk: %v", err)
	}
	var incrementals []*irbackup.Manifest
	for _, r := range records {
		if r.manifest.Kind == irbackup.BackupKindIncremental {
			incrementals = append(incrementals, r.manifest)
		}
	}
	if len(incrementals) != 3 {
		t.Fatalf("incremental rollovers = %d; want 3", len(incrementals))
	}

	totalChanges := int64(0)
	for _, m := range incrementals {
		for _, c := range m.ChangeChunks {
			totalChanges += c.RowCount
		}
	}
	if totalChanges != 30 {
		t.Errorf("total rollover changes = %d; want 30", totalChanges)
	}

	// Sort incrementals by CreatedAt so chain ordering is deterministic
	// even when two manifests land in the same UnixMilli (the test's
	// pinned clock does that). The path's BackupID prefix already
	// disambiguates files on disk.
	sortIncrementalsByChain(t, incrementals, parent.BackupID)
	parentID := parent.BackupID
	for i, m := range incrementals {
		if m.ParentBackupID != parentID {
			t.Errorf("rollover %d ParentBackupID = %q; want %q", i, m.ParentBackupID, parentID)
		}
		parentID = m.BackupID
	}

	// stream_state.json should exist with last_rollover_at set.
	state, err := readStreamState(context.Background(), store, DefaultStreamStateFilename)
	if err != nil {
		t.Fatalf("readStreamState: %v", err)
	}
	if state == nil {
		t.Fatal("stream_state.json missing")
	}
	if state.PID != 12345 || state.Host != "test-host" {
		t.Errorf("state pid/host = %d/%q; want 12345/test-host", state.PID, state.Host)
	}
}

// TestBackupStream_SkipEmptyRollover_OnChannelClose verifies that when
// the CDC channel closes immediately with no events, no manifest is
// committed (skip-empty-rollover default).
func TestBackupStream_SkipEmptyRollover_OnChannelClose(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{{}},
		cdcChanges:        nil, // empty
		cdcExpectedFromOK: true,
	}
	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     time.Minute,
		RolloverMaxChanges: 10,
		RolloverMaxBytes:   1 << 30,
		pidHostFn:          func() (int, string) { return 1, "h" },
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("stream.Run: %v", err)
	}
	records, _ := listAllManifestsViaWalk(context.Background(), store)
	for _, r := range records {
		if r.manifest.Kind == irbackup.BackupKindIncremental {
			t.Errorf("unexpected incremental manifest committed for empty rollover: %+v", r.manifest)
		}
	}
}

// TestBackupStream_IncludeEmptyRollover_WritesManifest verifies the
// opt-in: --rollover-include-empty commits a manifest even with zero
// changes.
func TestBackupStream_IncludeEmptyRollover_WritesManifest(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:              "postgres",
		schemaSequence:    []*ir.Schema{{}},
		cdcChanges:        nil,
		cdcExpectedFromOK: true,
	}
	stream := &BackupStream{
		Source:                src,
		SourceDSN:             "src",
		Store:                 store,
		ParentRef:             parent.BackupID,
		RolloverWindow:        time.Minute,
		IncludeEmptyRollovers: true,
		pidHostFn:             func() (int, string) { return 1, "h" },
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("stream.Run: %v", err)
	}
	records, _ := listAllManifestsViaWalk(context.Background(), store)
	var sawIncr bool
	for _, r := range records {
		if r.manifest.Kind == irbackup.BackupKindIncremental {
			sawIncr = true
			// Empty rollover's EndPosition should fall back to
			// StartPosition (= parent's EndPosition).
			if r.manifest.EndPosition != parent.EndPosition {
				t.Errorf("empty rollover EndPosition = %+v; want parent's = %+v",
					r.manifest.EndPosition, parent.EndPosition)
			}
		}
	}
	if !sawIncr {
		t.Errorf("expected an incremental rollover manifest with --include-empty, got none")
	}
}

// TestBackupStream_PositionInvalid_LoudFailure verifies the parent's-
// WAL-pruned surface fires the same loud-failure path as
// IncrementalBackup.
func TestBackupStream_PositionInvalid_LoudFailure(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{{}},
		cdcStartErr:    ir.ErrPositionInvalid,
	}
	stream := &BackupStream{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		ParentRef: parent.BackupID,
		pidHostFn: func() (int, string) { return 1, "h" },
	}
	err := stream.Run(context.Background())
	if err == nil {
		t.Fatal("err = nil; want loud failure on pruned WAL")
	}
	if !strings.Contains(err.Error(), "fresh full") {
		t.Errorf("err = %v; want 'fresh full' guidance", err)
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Errorf("err = %v; want errors.Is ErrPositionInvalid", err)
	}
}

// TestBackupStream_ContextCancel_DuringRollover_CleanExit verifies
// SIGTERM/SIGINT-shape: ctx.Done returns nil from Run (clean exit).
//
// The fake CDC reader's emit-then-close shape doesn't naturally model
// a blocking source. Use a blocking-fakeCDCEngine subclass that emits
// on a delay so the rollover loop is mid-window when ctx fires.
func TestBackupStream_ContextCancel_DuringRollover_CleanExit(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &blockingCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}}
	stream := &BackupStream{
		Source:             src,
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     time.Hour, // long enough that ctx-cancel wins
		RolloverMaxChanges: 1_000_000,
		RolloverMaxBytes:   1 << 30,
		pidHostFn:          func() (int, string) { return 1, "h" },
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := stream.Run(ctx)
	if err != nil {
		t.Errorf("stream.Run on ctx cancel = %v; want nil (clean exit)", err)
	}
}

// blockingCDCEngine is a fake source whose CDC reader emits no changes
// and the channel only closes when ctx is cancelled. Mimics a quiet
// production source for ctx-cancel + stop-signal tests.
type blockingCDCEngine struct {
	name           string
	schemaSequence []*ir.Schema
	readCalls      int
}

func (e *blockingCDCEngine) Name() string { return e.name }

func (e *blockingCDCEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCLogicalReplication}
}

func (e *blockingCDCEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	idx := e.readCalls
	if idx >= len(e.schemaSequence) {
		idx = len(e.schemaSequence) - 1
	}
	if idx < 0 {
		return nil, errors.New("blockingCDCEngine: no schema configured")
	}
	e.readCalls++
	return &recordingSchemaReader{schema: e.schemaSequence[idx]}, nil
}

func (*blockingCDCEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, errors.New("not used")
}

func (*blockingCDCEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("not used")
}

func (*blockingCDCEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, errors.New("not used")
}

func (*blockingCDCEngine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return &blockingCDCReader{}, nil
}

func (*blockingCDCEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used")
}

func (*blockingCDCEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not used")
}

type blockingCDCReader struct{}

func (r *blockingCDCReader) StreamChanges(ctx context.Context, _ ir.Position) (<-chan ir.Change, error) {
	out := make(chan ir.Change)
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

func (r *blockingCDCReader) Close() error { return nil }

// sortIncrementalsByChain sorts incrementals into chain order by
// walking ParentBackupID linkage starting from rootParent. Used by
// stream tests where two rollovers commit in the same UnixMilli (the
// test's pinned clock pins CreatedAt) so the lexically-sorted file
// ordering doesn't match chain order.
func sortIncrementalsByChain(t *testing.T, incrs []*irbackup.Manifest, rootParent string) {
	t.Helper()
	if len(incrs) == 0 {
		return
	}
	byID := make(map[string]*irbackup.Manifest, len(incrs))
	for _, m := range incrs {
		byID[m.BackupID] = m
	}
	ordered := make([]*irbackup.Manifest, 0, len(incrs))
	parentID := rootParent
	for {
		var next *irbackup.Manifest
		for _, m := range incrs {
			if m.ParentBackupID == parentID {
				next = m
				break
			}
		}
		if next == nil {
			break
		}
		ordered = append(ordered, next)
		parentID = next.BackupID
	}
	if len(ordered) != len(incrs) {
		t.Fatalf("chain walk found %d of %d incrementals; chain is broken", len(ordered), len(incrs))
	}
	copy(incrs, ordered)
}

// posTok is a small helper for building positions in tests.
func posTok(n int) ir.Position {
	return ir.Position{Engine: "postgres", Token: tokFor(n)}
}

func tokFor(n int) string {
	// JSON-shaped token mirrors the production format.
	return `{"slot":"sluice_slot","lsn":"0/` + intHex(n) + `"}`
}

func intHex(n int) string {
	const hexd = "0123456789ABCDEF"
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(hexd[n%16]) + out
		n /= 16
	}
	return out
}
