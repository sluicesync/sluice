// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// capturingSchemaReader is a [ir.SchemaReader] that also implements
// [ir.BackupPositionCapturer]. Used by the EndPosition tests to verify
// the full-backup orchestrator threads the captured position into the
// manifest. Returning a sentinel position lets the test pin the wire
// shape without standing up real engine machinery.
type capturingSchemaReader struct {
	schema       *ir.Schema
	captured     ir.Position
	captureErr   error
	gotSlotName  string
	captureCalls int
}

func (r *capturingSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return r.schema, nil
}

func (r *capturingSchemaReader) CaptureBackupPosition(_ context.Context, slotName string) (ir.Position, error) {
	r.captureCalls++
	r.gotSlotName = slotName
	if r.captureErr != nil {
		return ir.Position{}, r.captureErr
	}
	return r.captured, nil
}

// capturingBackupEngine wraps backupRecorderEngine to surface a
// CDC capability and a [capturingSchemaReader] so the orchestrator's
// EndPosition path runs.
type capturingBackupEngine struct {
	*backupRecorderEngine
	cdc       ir.CDCMethod
	reader    *capturingSchemaReader
	openErr   error
	openCalls int
}

func (e *capturingBackupEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: e.cdc}
}

func (e *capturingBackupEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	e.openCalls++
	if e.openErr != nil {
		return nil, e.openErr
	}
	return e.reader, nil
}

// TestBackup_RecordsEndPosition pins Phase 3.3.A: a full backup against
// a CDC-capable engine that implements BackupPositionCapturer threads
// the captured position through to manifest.EndPosition. This is the
// load-bearing test for acceptance criterion 1 — chains rooted in a
// v0.17.2+ full no longer fire the v0.17.0 "parent has no EndPosition"
// warning.
func TestBackup_RecordsEndPosition(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	captured := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_chain_slot","lsn":"0/AABBCC"}`,
	}
	reader := &capturingSchemaReader{schema: schema, captured: captured}
	src := &capturingBackupEngine{
		backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
			"users": {{"id": int64(1)}},
		}),
		cdc:    ir.CDCLogicalReplication,
		reader: reader,
	}

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	b := &Backup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		SluiceVersion: "v0.17.2-test",
		SlotName:      "sluice_chain_slot",
		Now:           func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition != captured {
		t.Errorf("EndPosition = %+v; want %+v", got.EndPosition, captured)
	}
	if reader.gotSlotName != "sluice_chain_slot" {
		t.Errorf("CaptureBackupPosition slotName = %q; want %q", reader.gotSlotName, "sluice_chain_slot")
	}
	if got.Kind != ir.BackupKindFull {
		t.Errorf("Kind = %q; want %q", got.Kind, ir.BackupKindFull)
	}
	if got.BackupID == "" {
		t.Error("BackupID is empty; want non-empty after EndPosition recording")
	}
	if reader.captureCalls != 1 {
		t.Errorf("CaptureBackupPosition calls = %d; want 1", reader.captureCalls)
	}
}

// TestBackup_NoCDCSkipsEndPosition pins the graceful-skip path: an
// engine without CDC capability MUST NOT have CaptureBackupPosition
// invoked, and the manifest's EndPosition stays empty (matches the
// v0.16.x shape so the chain-walker treats it as orphan).
func TestBackup_NoCDCSkipsEndPosition(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}},
	})
	b := &Backup{Source: src, SourceDSN: "src", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition.Engine != "" || got.EndPosition.Token != "" {
		t.Errorf("EndPosition = %+v; want zero (engine doesn't support CDC)", got.EndPosition)
	}
}

// snapshotOpeningEngine wraps capturingBackupEngine to also implement
// [ir.BackupSnapshotOpener] — exercising the v0.18.0 snapshot-anchored
// EndPosition path. The snapshot is fed a sentinel position; the test
// verifies the orchestrator records that position on the manifest
// rather than calling the post-sweep CaptureBackupPosition fallback.
type snapshotOpeningEngine struct {
	*capturingBackupEngine
	snapshotPos      ir.Position
	snapshotErr      error
	snapshotCalls    int
	snapshotCloses   int
	gotSnapshotSlot  string
	useSnapshotRows  bool
	snapshotRowsHook func() ir.RowReader
}

// OpenBackupSnapshot implements [ir.BackupSnapshotOpener].
func (e *snapshotOpeningEngine) OpenBackupSnapshot(_ context.Context, _, slotName string) (*ir.BackupSnapshot, error) {
	e.snapshotCalls++
	e.gotSnapshotSlot = slotName
	if e.snapshotErr != nil {
		return nil, e.snapshotErr
	}
	var rows ir.RowReader
	if e.useSnapshotRows && e.snapshotRowsHook != nil {
		rows = e.snapshotRowsHook()
	} else {
		rows = &fakeRowReader{rows: e.rows}
	}
	return &ir.BackupSnapshot{
		Position: e.snapshotPos,
		Rows:     rows,
		CloseFn: func() error {
			e.snapshotCloses++
			return nil
		},
	}, nil
}

// TestBackup_RecordsSnapshotAnchoredEndPosition pins the v0.18.0
// snapshot-anchored EndPosition path: when the source engine
// implements [ir.BackupSnapshotOpener], the orchestrator opens a
// backup snapshot, threads its Position onto manifest.EndPosition,
// and never calls the post-sweep CaptureBackupPosition fallback.
func TestBackup_RecordsSnapshotAnchoredEndPosition(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	captured := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_chain_slot","lsn":"0/AABBCC"}`,
	}
	snapshotPos := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_chain_slot","lsn":"0/SNAP00"}`,
	}
	reader := &capturingSchemaReader{schema: schema, captured: captured}
	src := &snapshotOpeningEngine{
		capturingBackupEngine: &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
				"users": {{"id": int64(1)}},
			}),
			cdc:    ir.CDCLogicalReplication,
			reader: reader,
		},
		snapshotPos: snapshotPos,
	}

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	b := &Backup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		SluiceVersion: "v0.18.0-test",
		SlotName:      "sluice_chain_slot",
		Now:           func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition != snapshotPos {
		t.Errorf("EndPosition = %+v; want snapshot-anchored %+v", got.EndPosition, snapshotPos)
	}
	if src.snapshotCalls != 1 {
		t.Errorf("OpenBackupSnapshot calls = %d; want 1", src.snapshotCalls)
	}
	if src.snapshotCloses != 1 {
		t.Errorf("snapshot Close calls = %d; want 1", src.snapshotCloses)
	}
	if src.gotSnapshotSlot != "sluice_chain_slot" {
		t.Errorf("OpenBackupSnapshot slotName = %q; want %q", src.gotSnapshotSlot, "sluice_chain_slot")
	}
	// The post-sweep capturer must NOT fire on the snapshot path —
	// that's the whole point of v0.18.0's gap-fix.
	if reader.captureCalls != 0 {
		t.Errorf("CaptureBackupPosition calls = %d; want 0 (snapshot path bypasses it)", reader.captureCalls)
	}
}

// TestBackup_SnapshotOpenerErrorFallsBackToCapturer pins the v0.18.0
// graceful-fallback shape: when the engine implements
// [ir.BackupSnapshotOpener] but the call returns an error (e.g. PG
// without `wal_level=logical` can't create the temporary anchor slot),
// the orchestrator MUST NOT fail the run. It falls through to the
// v0.17.x path — basic OpenRowReader + post-sweep BackupPositionCapturer
// — and emits a WARN line that names the error and the operational
// implication so chain operators know to enable wal_level=logical.
//
// This unblocks one-shot full backups on PG environments where logical
// replication isn't enabled (legitimate no-CDC scenarios). Chain
// correctness still requires the snapshot path; the WARN is the
// operator-actionable surface for that.
func TestBackup_SnapshotOpenerErrorFallsBackToCapturer(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	captured := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_chain_slot","lsn":"0/POSTSWEEP"}`,
	}
	reader := &capturingSchemaReader{schema: schema, captured: captured}
	src := &snapshotOpeningEngine{
		capturingBackupEngine: &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
				"users": {{"id": int64(1)}},
			}),
			cdc:    ir.CDCLogicalReplication,
			reader: reader,
		},
		snapshotErr: errors.New(`postgres: cdc: wal_level is "replica"; must be 'logical' for logical replication`),
	}

	b := &Backup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		SlotName:  "sluice_chain_slot",
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: unexpected error; expected fallback success: %v", err)
	}

	// Snapshot was attempted exactly once and Close was NOT called
	// (the snapshot open failed before a *BackupSnapshot was returned).
	if src.snapshotCalls != 1 {
		t.Errorf("OpenBackupSnapshot calls = %d; want 1", src.snapshotCalls)
	}
	if src.snapshotCloses != 0 {
		t.Errorf("snapshot Close calls = %d; want 0 (no snapshot to close on error)", src.snapshotCloses)
	}

	// EndPosition should be the post-sweep capturer's position, NOT
	// the (zero-valued) snapshot position.
	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition != captured {
		t.Errorf("EndPosition = %+v; want fallback-captured %+v", got.EndPosition, captured)
	}
	if reader.captureCalls != 1 {
		t.Errorf("CaptureBackupPosition calls = %d; want 1 (fallback path)", reader.captureCalls)
	}

	// The fallback WARN must surface both the underlying error AND
	// the operational implication so operators can act on it.
	logged := logBuf.String()
	if !strings.Contains(logged, "snapshot-anchored consistent view unavailable") {
		t.Errorf("WARN line missing fallback header; log=%q", logged)
	}
	if !strings.Contains(logged, "wal_level") {
		t.Errorf("WARN line missing underlying error context; log=%q", logged)
	}
	if !strings.Contains(logged, "implication") {
		t.Errorf("WARN line missing operational implication field; log=%q", logged)
	}
}

// TestBackup_FallbackWhenNoSnapshotOpener pins the v0.17.x-shape
// fallback path: when the engine doesn't implement
// [ir.BackupSnapshotOpener], the orchestrator routes through the
// post-sweep CaptureBackupPosition fallback. The during-backup
// write-window gap is documented; this test pins the dispatch.
func TestBackup_FallbackWhenNoSnapshotOpener(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
	}
	captured := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_chain_slot","lsn":"0/POSTSWEEP"}`,
	}
	reader := &capturingSchemaReader{schema: schema, captured: captured}
	// capturingBackupEngine does NOT implement BackupSnapshotOpener.
	src := &capturingBackupEngine{
		backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
			"users": {{"id": int64(1)}},
		}),
		cdc:    ir.CDCLogicalReplication,
		reader: reader,
	}
	b := &Backup{Source: src, SourceDSN: "src", Store: store, SlotName: "sluice_chain_slot"}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	got, _ := readManifest(context.Background(), store)
	if got.EndPosition != captured {
		t.Errorf("EndPosition = %+v; want fallback-captured %+v", got.EndPosition, captured)
	}
	if reader.captureCalls != 1 {
		t.Errorf("CaptureBackupPosition calls = %d; want 1 (fallback path)", reader.captureCalls)
	}
}

// TestBackup_CapturerErrorSurfacesAsRunFailure pins the loud-failure
// shape: a CaptureBackupPosition error becomes a Backup.Run error so
// operators don't end up with a manifest claiming "complete" without
// the position the chain machinery depends on.
func TestBackup_CapturerErrorSurfacesAsRunFailure(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
	}
	reader := &capturingSchemaReader{schema: schema, captureErr: errors.New("simulated source failure")}
	src := &capturingBackupEngine{
		backupRecorderEngine: newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
			"users": {{"id": int64(1)}},
		}),
		cdc:    ir.CDCLogicalReplication,
		reader: reader,
	}
	b := &Backup{Source: src, SourceDSN: "src", Store: store}
	err := b.Run(context.Background())
	if err == nil {
		t.Fatal("Backup.Run: nil; want error from capturer failure")
	}
	if !strings.Contains(err.Error(), "simulated source failure") {
		t.Errorf("err = %v; want containing simulated source failure", err)
	}
}
