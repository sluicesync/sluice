// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the Bug 137 resume-time orphan sweep dispatch: when
// the source engine implements the optional [ir.BackupAnchorSweeper]
// surface, the full-backup orchestrator invokes it exactly when a
// resume is detected (an in-progress prior manifest) — never on a
// fresh run — and a sweep failure is hygiene, not a run failure.
// The real Postgres sweep (live slots, real drops) is covered by the
// engine package's integration tests.

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// sweepingBackupEngine layers [ir.BackupAnchorSweeper] onto the
// package's stock backup test engine, recording each invocation.
type sweepingBackupEngine struct {
	*backupRecorderEngine

	sweepCalls int
	sweepDSNs  []string
	sweepErr   error
}

func (e *sweepingBackupEngine) SweepOrphanedBackupAnchors(_ context.Context, dsn string) error {
	e.sweepCalls++
	e.sweepDSNs = append(e.sweepDSNs, dsn)
	return e.sweepErr
}

var _ ir.BackupAnchorSweeper = (*sweepingBackupEngine)(nil)

// anchorSweepFixture returns a schema + rows pair small enough that
// the backup completes instantly but real enough to exercise the
// full run path.
func anchorSweepFixture() (schema *ir.Schema, rows map[string][]ir.Row) {
	schema = &ir.Schema{
		Tables: []*ir.Table{
			{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	rows = map[string][]ir.Row{"t": {{"id": int64(1)}}}
	return schema, rows
}

// writeInProgressManifest seeds the store with the minimal manifest
// shape that flips the orchestrator onto the resume path.
func writeInProgressManifest(t *testing.T, store ir.BackupStore, schema *ir.Schema) {
	t.Helper()
	partial := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  ir.BackupStateInProgress,
	}
	if err := writeManifest(context.Background(), store, partial); err != nil {
		t.Fatalf("writeManifest partial: %v", err)
	}
}

// TestBackup_FreshRunDoesNotSweepAnchors pins the negative: a fresh
// backup (no prior manifest) has nothing to clean up, so the sweep
// surface must not be touched.
func TestBackup_FreshRunDoesNotSweepAnchors(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema, rows := anchorSweepFixture()
	src := &sweepingBackupEngine{backupRecorderEngine: newBackupRecorderEngine("postgres", schema, rows)}

	b := &Backup{Source: src, SourceDSN: "src-dsn", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.sweepCalls != 0 {
		t.Errorf("sweepCalls = %d on a fresh run; want 0", src.sweepCalls)
	}
}

// TestBackup_ResumeSweepsAnchorsOnce pins the positive: resuming an
// in-progress backup invokes the sweep exactly once, against the
// source DSN.
func TestBackup_ResumeSweepsAnchorsOnce(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema, rows := anchorSweepFixture()
	writeInProgressManifest(t, store, schema)

	src := &sweepingBackupEngine{backupRecorderEngine: newBackupRecorderEngine("postgres", schema, rows)}
	b := &Backup{Source: src, SourceDSN: "src-dsn", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.sweepCalls != 1 {
		t.Errorf("sweepCalls = %d on resume; want 1", src.sweepCalls)
	}
	if len(src.sweepDSNs) != 1 || src.sweepDSNs[0] != "src-dsn" {
		t.Errorf("sweep DSNs = %v; want [src-dsn]", src.sweepDSNs)
	}
}

// TestBackup_ResumeSweepFailureDoesNotFailRun pins the best-effort
// contract: the sweep is hygiene — a failure is WARN-logged by the
// orchestrator but must never abort the resume the operator asked
// for.
func TestBackup_ResumeSweepFailureDoesNotFailRun(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema, rows := anchorSweepFixture()
	writeInProgressManifest(t, store, schema)

	src := &sweepingBackupEngine{
		backupRecorderEngine: newBackupRecorderEngine("postgres", schema, rows),
		sweepErr:             errors.New("synthetic sweep failure"),
	}
	b := &Backup{Source: src, SourceDSN: "src-dsn", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v (sweep failures must not fail the resume)", err)
	}
	if src.sweepCalls != 1 {
		t.Errorf("sweepCalls = %d; want 1", src.sweepCalls)
	}
	final, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if final.PartialState != ir.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", final.PartialState)
	}
}
