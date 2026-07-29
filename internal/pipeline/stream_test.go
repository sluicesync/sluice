// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/progress"
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
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ListAllManifestsViaWalk: %v", err)
	}
	var incrementals []*irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incrementals = append(incrementals, r.Manifest)
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
	records, _ := lineage.ListAllManifestsViaWalk(context.Background(), store)
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			t.Errorf("unexpected incremental manifest committed for empty rollover: %+v", r.Manifest)
		}
	}
}

// TestBackupStream_CleanExit_AllowsImmediateRestart is the end-to-end
// shape behind the handoff fix: a stream that exits cleanly must leave
// the destination restartable RIGHT AWAY, not after a staleness window.
// This is the supervisor / k8s restart loop (ADR-0087's rotation-born
// resume) — pre-fix the second Run below failed with "a stream is
// already running against this destination" and named a remedy the
// restarting operator had no way to use.
func TestBackupStream_CleanExit_AllowsImmediateRestart(t *testing.T) {
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

	newStream := func(pid int) *BackupStream {
		return &BackupStream{
			Source:             &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}, cdcExpectedFromOK: true},
			SourceDSN:          "src",
			Store:              store,
			ParentRef:          parent.BackupID,
			RolloverWindow:     time.Minute,
			RolloverMaxChanges: 10,
			RolloverMaxBytes:   1 << 30,
			pidHostFn:          func() (int, string) { return pid, "supervised.example.com" },
		}
	}

	if err := newStream(2328).Run(context.Background()); err != nil {
		t.Fatalf("first stream.Run: %v", err)
	}
	state, err := readStreamState(context.Background(), store, DefaultStreamStateFilename)
	if err != nil {
		t.Fatalf("readStreamState: %v", err)
	}
	if state == nil || state.StoppedAt == nil {
		t.Fatalf("stopped_at not stamped after a clean exit: %+v", state)
	}

	// Same host, new pid — exactly what a supervisor restart looks
	// like. last_rollover_at is seconds old, so only the handoff record
	// can admit this.
	if err := newStream(9001).Run(context.Background()); err != nil {
		t.Fatalf("restart after a clean exit: %v", err)
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
	records, _ := lineage.ListAllManifestsViaWalk(context.Background(), store)
	var sawIncr bool
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			sawIncr = true
			// Empty rollover's EndPosition should fall back to
			// StartPosition (= parent's EndPosition).
			if r.Manifest.EndPosition != parent.EndPosition {
				t.Errorf("empty rollover EndPosition = %+v; want parent's = %+v",
					r.Manifest.EndPosition, parent.EndPosition)
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
//
// The cancel is gated on the rollover loop actually starting, not on a
// wall-clock sleep. The sleep-only version raced Run's setup: on a slow
// host the cancel landed in a setup store read instead of in the window,
// and Run reported that step as a failure (roadmap item 89 — it failed
// roughly one run in a hundred, and only on the tag-only Windows job).
// Readout is the first thing each loop iteration touches, so it is the
// exact "the window is open" edge; the sleep after it is what keeps this
// a mid-window cancel rather than a boundary one, and it cannot race —
// the window is an hour long and the source never emits.
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
	windowOpen := make(chan struct{})
	var once sync.Once
	stream.Readout = func([]progress.Field) { once.Do(func() { close(windowOpen) }) }
	go func() {
		<-windowOpen
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := stream.Run(ctx)
	if err != nil {
		t.Errorf("stream.Run on ctx cancel = %v; want nil (clean exit)", err)
	}
}

// TestBackupStream_ContextCancel_DuringSetup_CleanExit is the deterministic
// half of the test above: it forces the cancellation to land INSIDE a setup
// store read rather than racing one.
//
// The wrapper cancels the run context at the moment a named setup read
// stats its object, so the store's ctx.Err() entry guard returns
// context.Canceled from within that step. Both cases are pinned rather than
// the reported one alone: the signed-chain probe is what the v0.104.0 tag's
// Windows job happened to name, but the whole setup phase reads the store
// through the same guard, so a classifier keyed on the probe's wording
// would leave every sibling step still reporting a cancel as a failure.
func TestBackupStream_ContextCancel_DuringSetup_CleanExit(t *testing.T) {
	cases := []struct {
		name string
		// cancelAtExistsPath is the object whose Exists call cancels the
		// run context, choosing which setup step gets interrupted.
		cancelAtExistsPath string
	}{
		// refuseSignedChain — the reported instance.
		{"signed-chain probe", lineage.LineageSigFileName},
		// preflightStreamState → readStreamState, the FIRST store read
		// newRolloverLoop makes. Different call site, different wrapping
		// prose ("stream: read existing stream_state: …"), same classifier.
		{"concurrent-writer preflight", DefaultStreamStateFilename},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			local, _ := blobcodec.NewLocalStore(dir)

			parent := &irbackup.Manifest{
				FormatVersion: irbackup.BackupFormatVersion,
				CreatedAt:     time.Now().UTC(),
				SourceEngine:  "postgres",
				Schema:        &ir.Schema{},
				Kind:          irbackup.BackupKindFull,
				EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/200"}`},
			}
			parent.BackupID = irbackup.ComputeBackupID(parent)
			writeParentFullManifest(t, local, parent)
			// The preflight only reads stream_state.json when it exists, so
			// seed one; a cold store would skip that read entirely.
			seedStreamState(t, local, DefaultStreamStateFilename)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &cancelOnExistsStore{Store: local, path: c.cancelAtExistsPath, cancel: cancel}

			stream := &BackupStream{
				Source:             &blockingCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}},
				SourceDSN:          "src",
				Store:              store,
				ParentRef:          parent.BackupID,
				RolloverWindow:     time.Hour,
				RolloverMaxChanges: 1_000_000,
				RolloverMaxBytes:   1 << 30,
				Force:              true, // the seeded state file is another "writer"
				pidHostFn:          func() (int, string) { return 1, "h" },
			}

			if err := stream.Run(ctx); err != nil {
				t.Errorf("stream.Run cancelled during setup = %v; want nil (clean exit)", err)
			}
			if !store.fired {
				t.Fatalf("the %s never read %q; the test proved nothing", c.name, c.cancelAtExistsPath)
			}
		})
	}
}

// cancelOnExistsStore cancels the run context the first time a setup step
// stats the named object, then lets the underlying store's ctx.Err() entry
// guard fail that same call. That puts the cancellation inside the step
// deterministically. Single-goroutine by construction — Run's setup phase
// is sequential and the fake CDC reader never touches the store.
type cancelOnExistsStore struct {
	irbackup.Store

	path   string
	cancel func()
	fired  bool
}

func (s *cancelOnExistsStore) Exists(ctx context.Context, path string) (bool, error) {
	if path == s.path && !s.fired {
		s.fired = true
		s.cancel()
	}
	return s.Store.Exists(ctx, path)
}

// seedStreamState writes a liveness file so the concurrent-writer preflight
// reaches its Get (it short-circuits on a store with no state file).
func seedStreamState(t *testing.T, store irbackup.Store, path string) {
	t.Helper()
	st := &streamState{PID: 999, Host: "other-host", StartedAt: time.Now().UTC(), LastRolloverAt: time.Now().UTC()}
	if err := writeStreamState(context.Background(), store, path, st); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}
}

// TestBackupStream_SetupError_StaysLoud is the other half of the pin above:
// the clean-exit arm must not swallow a real setup failure. Both of
// cleanExitOnCallerCancel's guards get their own case, because a classifier
// that turns a genuine failure into exit 0 is strictly worse than the wrong
// error message it was added to fix.
func TestBackupStream_SetupError_StaysLoud(t *testing.T) {
	cases := []struct {
		name string
		// probeErr is what the signed-chain probe's store read returns.
		probeErr error
		// cancelFirst cancels the run context before Run starts, so the
		// ctx.Err() guard is satisfied and only the errors.Is arm can hold
		// the error loud.
		cancelFirst bool
		want        string
	}{
		{
			name:     "unrelated failure, live context",
			probeErr: errors.New("store: 403 forbidden"),
			want:     "403 forbidden",
		},
		{
			name:        "unrelated failure racing a cancel",
			probeErr:    errors.New("store: 403 forbidden"),
			cancelFirst: true,
			want:        "403 forbidden",
		},
		{
			// The store reports a cancellation from a context that is not
			// ours (its own per-call deadline, a pooled connection's). Our
			// context is live, so this is a store fault and stays loud.
			name:     "foreign context.Canceled, live context",
			probeErr: fmt.Errorf("store: internal read: %w", context.Canceled),
			want:     "probe signed chain",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			local, _ := blobcodec.NewLocalStore(dir)

			parent := &irbackup.Manifest{
				FormatVersion: irbackup.BackupFormatVersion,
				CreatedAt:     time.Now().UTC(),
				SourceEngine:  "postgres",
				Schema:        &ir.Schema{},
				Kind:          irbackup.BackupKindFull,
				EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/300"}`},
			}
			parent.BackupID = irbackup.ComputeBackupID(parent)
			writeParentFullManifest(t, local, parent)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &failingSigProbeStore{Store: local, err: c.probeErr}
			if c.cancelFirst {
				// Cancel only once setup is past its own store reads, so the
				// probe is still reached: Readout fires at the top of the
				// rollover loop, which this test never gets to — so cancel
				// from the probe wrapper itself instead.
				store.alsoCancel = cancel
			}

			stream := &BackupStream{
				Source:             &blockingCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{{}}},
				SourceDSN:          "src",
				Store:              store,
				ParentRef:          parent.BackupID,
				RolloverWindow:     time.Hour,
				RolloverMaxChanges: 1_000_000,
				RolloverMaxBytes:   1 << 30,
				pidHostFn:          func() (int, string) { return 1, "h" },
			}

			err := stream.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("stream.Run = %v; want an error containing %q (a real setup failure must stay loud)", err, c.want)
			}
		})
	}
}

// failingSigProbeStore fails the signed-chain probe with a caller-supplied
// error, optionally cancelling the run context first so the errors.Is arm
// of cleanExitOnCallerCancel is the only thing keeping the error loud.
type failingSigProbeStore struct {
	irbackup.Store

	err        error
	alsoCancel func()
}

func (s *failingSigProbeStore) Exists(ctx context.Context, path string) (bool, error) {
	if path == lineage.LineageSigFileName {
		if s.alsoCancel != nil {
			s.alsoCancel()
		}
		return false, s.err
	}
	return s.Store.Exists(ctx, path)
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
