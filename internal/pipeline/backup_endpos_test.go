// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
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
