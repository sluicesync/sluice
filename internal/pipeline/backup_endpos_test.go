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
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// capturingSchemaReader is a [ir.SchemaReader] that also implements
// [irbackup.BackupPositionCapturer]. Used by the EndPosition tests to verify
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
	if got.Kind != irbackup.BackupKindFull {
		t.Errorf("Kind = %q; want %q", got.Kind, irbackup.BackupKindFull)
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
// [irbackup.BackupSnapshotOpener] — exercising the v0.18.0 snapshot-anchored
// EndPosition path. The snapshot is fed a sentinel position; the test
// verifies the orchestrator records that position on the manifest
// rather than calling the post-sweep CaptureBackupPosition fallback.
type snapshotOpeningEngine struct {
	*capturingBackupEngine
	snapshotPos         ir.Position
	snapshotErr         error
	snapshotCalls       int
	snapshotCloses      int
	gotSnapshotSlot     string
	gotPersistChainSlot bool
	commitCalls         int
	useSnapshotRows     bool
	snapshotRowsHook    func() ir.RowReader
}

// OpenBackupSnapshot implements [irbackup.BackupSnapshotOpener]. Mirrors the
// Postgres engine's CommitFn contract: the commit hook is set only on
// the PersistChainSlot shape.
func (e *snapshotOpeningEngine) OpenBackupSnapshot(_ context.Context, _ string, opts irbackup.BackupSnapshotOptions) (*irbackup.BackupSnapshot, error) {
	e.snapshotCalls++
	e.gotSnapshotSlot = opts.SlotName
	e.gotPersistChainSlot = opts.PersistChainSlot
	if e.snapshotErr != nil {
		return nil, e.snapshotErr
	}
	var rows ir.RowReader
	if e.useSnapshotRows && e.snapshotRowsHook != nil {
		rows = e.snapshotRowsHook()
	} else {
		rows = &fakeRowReader{rows: e.rows}
	}
	snap := &irbackup.BackupSnapshot{
		Position: e.snapshotPos,
		Rows:     rows,
		CloseFn: func() error {
			e.snapshotCloses++
			return nil
		},
	}
	if opts.PersistChainSlot {
		snap.CommitFn = func(context.Context) error {
			e.commitCalls++
			return nil
		}
	}
	return snap, nil
}

// scopedSnapshotOpeningEngine implements BOTH [irbackup.BackupSnapshotOpener]
// and [irbackup.TableScopedBackupSnapshotOpener] — the shape a PlanetScale
// source presents (#2b). It records which surface the orchestrator chose
// and the table allowlist it threaded in, so the test can pin that a
// table-scoped backup prefers OpenBackupSnapshotForTables and never calls
// the whole-keyspace OpenBackupSnapshot.
type scopedSnapshotOpeningEngine struct {
	*capturingBackupEngine
	snapshotPos     ir.Position
	scopedCalls     int
	baseCalls       int
	gotScopedTables []string
	gotScopedSlot   string
	closes          int
}

func (e *scopedSnapshotOpeningEngine) makeSnapshot() *irbackup.BackupSnapshot {
	return &irbackup.BackupSnapshot{
		Position: e.snapshotPos,
		Rows:     &fakeRowReader{rows: e.rows},
		CloseFn: func() error {
			e.closes++
			return nil
		},
	}
}

// OpenBackupSnapshot implements [irbackup.BackupSnapshotOpener] (the base,
// whole-keyspace surface). The scoped test asserts this is NOT called.
func (e *scopedSnapshotOpeningEngine) OpenBackupSnapshot(_ context.Context, _ string, _ irbackup.BackupSnapshotOptions) (*irbackup.BackupSnapshot, error) {
	e.baseCalls++
	return e.makeSnapshot(), nil
}

// OpenBackupSnapshotForTables implements [irbackup.TableScopedBackupSnapshotOpener].
func (e *scopedSnapshotOpeningEngine) OpenBackupSnapshotForTables(_ context.Context, _ string, opts irbackup.BackupSnapshotOptions, tables []string) (*irbackup.BackupSnapshot, error) {
	e.scopedCalls++
	e.gotScopedSlot = opts.SlotName
	e.gotScopedTables = tables
	return e.makeSnapshot(), nil
}

// TestBackup_TableScopedSnapshotPrefersScopedOpener pins the #2b
// backup-path symmetry: when the source implements
// [irbackup.TableScopedBackupSnapshotOpener] AND the filtered schema has tables,
// the orchestrator opens the snapshot via OpenBackupSnapshotForTables
// (threading the filtered table names) and NEVER calls the whole-keyspace
// OpenBackupSnapshot. This is what stops a scoped PlanetScale backup from
// over-streaming the entire keyspace (ADR-0071 buffer overflow).
func TestBackup_TableScopedSnapshotPrefersScopedOpener(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "small_t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "other_t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	snapshotPos := ir.Position{Engine: "mysql", Token: `[{"keyspace":"ks","shard":"-","gtid":"x"}]`}
	reader := &capturingSchemaReader{schema: schema}
	src := &scopedSnapshotOpeningEngine{
		capturingBackupEngine: &capturingBackupEngine{
			backupRecorderEngine: newBackupRecorderEngine("mysql", schema, map[string][]ir.Row{
				"small_t": {{"id": int64(1)}},
				"other_t": {{"id": int64(2)}},
			}),
			cdc:    ir.CDCBinlog,
			reader: reader,
		},
		snapshotPos: snapshotPos,
	}

	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	b := &Backup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		SluiceVersion: "v0.99.13-test",
		SlotName:      "ignored_on_vstream",
		Now:           func() time.Time { return now },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	if src.scopedCalls != 1 {
		t.Errorf("OpenBackupSnapshotForTables calls = %d; want 1", src.scopedCalls)
	}
	if src.baseCalls != 0 {
		t.Errorf("OpenBackupSnapshot (base) calls = %d; want 0 (scoped opener must win)", src.baseCalls)
	}
	wantTables := []string{"small_t", "other_t"}
	if len(src.gotScopedTables) != len(wantTables) {
		t.Fatalf("scoped tables = %v; want %v", src.gotScopedTables, wantTables)
	}
	for i := range wantTables {
		if src.gotScopedTables[i] != wantTables[i] {
			t.Errorf("scoped tables = %v; want %v", src.gotScopedTables, wantTables)
			break
		}
	}
	if src.gotScopedSlot != "ignored_on_vstream" {
		t.Errorf("scoped slotName = %q; want %q", src.gotScopedSlot, "ignored_on_vstream")
	}

	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition != snapshotPos {
		t.Errorf("EndPosition = %+v; want scoped snapshot-anchored %+v", got.EndPosition, snapshotPos)
	}
	// The post-sweep capturer must NOT fire on the snapshot path.
	if reader.captureCalls != 0 {
		t.Errorf("CaptureBackupPosition calls = %d; want 0 (snapshot path bypasses it)", reader.captureCalls)
	}
}

// TestBackup_BaseOnlySnapshotOpenerStillRoutesToBase pins the no-regression
// case: a source that implements ONLY the base [irbackup.BackupSnapshotOpener]
// (NOT the table-scoped surface — PG, vanilla-via-base) still routes to
// OpenBackupSnapshot even when the filtered schema has tables. The #2b
// dispatch must be byte-identical for non-implementers of the new
// interface.
func TestBackup_BaseOnlySnapshotOpenerStillRoutesToBase(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	snapshotPos := ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/BASE00"}`}
	reader := &capturingSchemaReader{schema: schema}
	// snapshotOpeningEngine implements ONLY irbackup.BackupSnapshotOpener.
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
	// Compile-time guard: this stub must NOT satisfy the table-scoped
	// surface, or the test would be vacuous.
	if _, ok := interface{}(src).(irbackup.TableScopedBackupSnapshotOpener); ok {
		t.Fatal("snapshotOpeningEngine must NOT implement TableScopedBackupSnapshotOpener for this test")
	}

	b := &Backup{
		Source:    src,
		SourceDSN: "src",
		Store:     store,
		SlotName:  "sluice_chain_slot",
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	if src.snapshotCalls != 1 {
		t.Errorf("OpenBackupSnapshot calls = %d; want 1 (base path)", src.snapshotCalls)
	}
	if src.gotSnapshotSlot != "sluice_chain_slot" {
		t.Errorf("OpenBackupSnapshot slotName = %q; want %q", src.gotSnapshotSlot, "sluice_chain_slot")
	}
	got, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.EndPosition != snapshotPos {
		t.Errorf("EndPosition = %+v; want base snapshot-anchored %+v", got.EndPosition, snapshotPos)
	}
}

// TestBackup_RecordsSnapshotAnchoredEndPosition pins the v0.18.0
// snapshot-anchored EndPosition path: when the source engine
// implements [irbackup.BackupSnapshotOpener], the orchestrator opens a
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
// [irbackup.BackupSnapshotOpener] but the call returns an error (e.g. PG
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
// [irbackup.BackupSnapshotOpener], the orchestrator routes through the
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
