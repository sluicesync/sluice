// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the migrate shared exported snapshot (perf research
// delta 1, migrate_snapshot.go): the capability gate, the loud export
// fallback, the deps threading, and — the load-bearing pin — the
// release point at copy-phase end, strictly BEFORE the index phase.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeSnapshotImporter mints recordingRowReaders "pinned" to whatever
// snapshot name it is handed, recording the calls.
type fakeSnapshotImporter struct {
	mu       sync.Mutex
	lastName string
	imports  int
	closed   bool
}

func (f *fakeSnapshotImporter) ImportSnapshot(_ context.Context, snapshotName string, n int) ([]ir.RowReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.imports++
	f.lastName = snapshotName
	readers := make([]ir.RowReader, n)
	for i := range readers {
		readers[i] = &recordingRowReader{}
	}
	return readers, nil
}

func (f *fakeSnapshotImporter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// sharedSnapSourceEngine layers ir.SnapshotExporter +
// ir.SnapshotImporterOpener onto recordingEngine, recording lifecycle
// calls so tests can pin the release/close discipline.
type sharedSnapSourceEngine struct {
	*recordingEngine
	exportErr    error
	importer     *fakeSnapshotImporter
	releaseCalls int
	closeCalls   int
	readerOpens  int
}

func (e *sharedSnapSourceEngine) ExportSnapshot(context.Context, string) (*ir.ExportedSnapshot, error) {
	if e.exportErr != nil {
		return nil, e.exportErr
	}
	return &ir.ExportedSnapshot{
		Name:      "unit-test-snapshot",
		Rows:      &recordingRowReader{},
		ReleaseFn: func() error { e.releaseCalls++; return nil },
		CloseFn:   func() error { e.closeCalls++; return nil },
	}, nil
}

func (e *sharedSnapSourceEngine) OpenSnapshotImporter(context.Context, string) (ir.SnapshotImporter, error) {
	return e.importer, nil
}

func (e *sharedSnapSourceEngine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	e.readerOpens++
	return e.recordingEngine.OpenRowReader(ctx, dsn)
}

// TestMigrateSharedSnapshotReleasedAtCopyEnd is the load-bearing pin:
// with a source that exports a shareable snapshot, the run engages it
// (no independent OpenRowReader for the primary), and the release fires
// at COPY-PHASE END — after the rows are written, strictly before the
// index phase — never lingering through the DDL tail (the long-pin
// source-bloat lesson).
func TestMigrateSharedSnapshotReleasedAtCopyEnd(t *testing.T) {
	src := &sharedSnapSourceEngine{
		recordingEngine: newRecordingEngine("source"),
		importer:        &fakeSnapshotImporter{},
	}
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	var phasesAtRelease []string
	releasedFired := 0
	migrateSharedSnapshotReleasedObserver = func() {
		// Runs on the copy pool's completion path; with the non-IIB
		// recording writer that is the orchestrator goroutine, so reading
		// the target phase log here is race-free.
		releasedFired++
		phasesAtRelease = append([]string(nil), tgt.phaseLog...)
	}
	defer func() { migrateSharedSnapshotReleasedObserver = nil }()

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if src.readerOpens != 0 {
		t.Errorf("OpenRowReader called %d times; want 0 — the exporting snapshot's Rows must BE the primary reader", src.readerOpens)
	}
	if releasedFired != 1 {
		t.Fatalf("release observer fired %d times; want exactly 1 (Once-guarded)", releasedFired)
	}
	if src.releaseCalls != 1 {
		t.Errorf("ExportedSnapshot.Release called %d times; want 1", src.releaseCalls)
	}
	if indexOf(phasesAtRelease, "WriteRows:users") < 0 {
		t.Errorf("release fired before the bulk copy wrote rows; phases at release: %v", phasesAtRelease)
	}
	if indexOf(phasesAtRelease, "CreateIndexes") >= 0 {
		t.Errorf("release fired AFTER the index phase — the snapshot pinned source vacuum through the DDL tail; phases at release: %v", phasesAtRelease)
	}
	if !src.importer.closed {
		t.Error("importer not closed at release — its pool would linger for the run tail")
	}
	if src.closeCalls != 1 {
		t.Errorf("ExportedSnapshot.Close called %d times; want 1 (run teardown)", src.closeCalls)
	}
}

// TestMigrateSharedSnapshotExportFailureFallsBackLoudly pins the
// degrade path: an export failure (e.g. a hot-standby source) must WARN
// and fall back to the independent per-connection readers — never fail
// a previously-working migrate.
func TestMigrateSharedSnapshotExportFailureFallsBackLoudly(t *testing.T) {
	src := &sharedSnapSourceEngine{
		recordingEngine: newRecordingEngine("source"),
		importer:        &fakeSnapshotImporter{},
		exportErr:       errors.New("cannot export a snapshot during recovery"),
	}
	src.schema = sampleSchema()
	tgt := newRecordingEngine("target")

	logs := captureSlog(t)
	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.readerOpens != 1 {
		t.Errorf("OpenRowReader called %d times; want 1 (the fallback primary reader)", src.readerOpens)
	}
	if !strings.Contains(logs.String(), "shared source snapshot unavailable") {
		t.Errorf("expected the loud fallback WARN; got:\n%s", logs.String())
	}
	if indexOf(tgt.phaseLog, "WriteRows:users") < 0 {
		t.Errorf("fallback run did not copy rows: %v", tgt.phaseLog)
	}
}

// TestOpenSharedSourceSnapshotCapabilityGate pins the quiet gate: a
// source without the exporter/importer surfaces gets nil (no warning —
// the absence is by-design, e.g. MySQL's per-session snapshots).
func TestOpenSharedSourceSnapshotCapabilityGate(t *testing.T) {
	m := &Migrator{
		Source: newRecordingEngine("source"), Target: newRecordingEngine("target"),
		SourceDSN: "src", TargetDSN: "tgt",
	}
	if snap := m.openSharedSourceSnapshot(context.Background()); snap != nil {
		t.Errorf("openSharedSourceSnapshot = %v; want nil for a source without the snapshot surfaces", snap)
	}
}

// TestPhaseBuildCopyDepsThreadsSharedSnapshot pins the deps wiring:
// with a shared snapshot the chunk-reader factory mints readers pinned
// to ITS name and the release hook is set; without one, both stay nil
// (independent readers, byte-identical pre-existing behaviour).
func TestPhaseBuildCopyDepsThreadsSharedSnapshot(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	m := &Migrator{
		Source: src, Target: newRecordingEngine("target"),
		SourceDSN: "src", TargetDSN: "tgt",
	}
	ctx := context.Background()

	importer := &fakeSnapshotImporter{}
	shared := &sharedSourceSnapshot{
		snap:     &ir.ExportedSnapshot{Name: "deps-test-snap", Rows: &recordingRowReader{}},
		importer: importer,
	}
	deps := m.phaseBuildCopyDeps(ctx, src.schema, &recordingRowReader{}, &recordingRowWriter{phaseLog: new([]string), mu: &sync.Mutex{}}, false, 2, shared)
	if deps.chunkReaderFactory == nil {
		t.Fatal("chunkReaderFactory not set with a shared snapshot")
	}
	if deps.releaseSharedSnapshot == nil {
		t.Fatal("releaseSharedSnapshot not set with a shared snapshot")
	}
	if _, err := deps.chunkReaderFactory(ctx); err != nil {
		t.Fatalf("chunkReaderFactory: %v", err)
	}
	if importer.lastName != "deps-test-snap" {
		t.Errorf("factory imported snapshot %q; want %q", importer.lastName, "deps-test-snap")
	}

	depsNil := m.phaseBuildCopyDeps(ctx, src.schema, &recordingRowReader{}, &recordingRowWriter{phaseLog: new([]string), mu: &sync.Mutex{}}, false, 2, nil)
	if depsNil.chunkReaderFactory != nil || depsNil.releaseSharedSnapshot != nil {
		t.Error("nil shared snapshot must leave the factory and release hook nil (independent readers)")
	}
}
