// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 3.3.B unit coverage: --position-from-manifest flag wires the
// chain terminal position into the streamer's resume path, replacing
// the per-target sluice_cdc_state lookup. These tests pin the
// behaviour without standing up real engines or testcontainers.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestStreamer_PositionFromManifest_BypassesAppliedReadPosition pins
// the load-bearing behaviour: when PositionFromManifestStore is set,
// the streamer ignores applier.ReadPosition (which would normally
// drive resume) and uses the chain's terminal manifest's EndPosition
// as the resume position instead.
func TestStreamer_PositionFromManifest_BypassesAppliedReadPosition(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	chainTerminal := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_slot","lsn":"1/300"}`,
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   chainTerminal,
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	cdcReader := &capturingCDCReader{captured: make(chan struct{})}
	source := &resumeDispatchEngine{
		name:      "postgres",
		cdcReader: cdcReader,
		caps:      ir.Capabilities{CDC: ir.CDCLogicalReplication},
	}
	target := &resumeDispatchEngine{name: "postgres"}

	// Applier reports a STALE position (token differs from chainTerminal).
	// The streamer must NOT use this — position-from-manifest takes over.
	applier := &resumeDispatchApplier{
		stored: ir.Position{Engine: "postgres", Token: `{"slot":"old","lsn":"0/100"}`},
		found:  true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s := &Streamer{
		Source:                    source,
		Target:                    target,
		SourceDSN:                 "src",
		TargetDSN:                 "tgt",
		StreamID:                  "test-stream",
		SchemaChanges:             "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:                   applier,
		PositionFromManifestStore: store,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	select {
	case <-cdcReader.captured:
	case <-time.After(time.Second):
		cancel()
		<-runErr
		t.Fatal("StreamChanges was not called within 1s")
	}
	cancel()
	err := <-runErr
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if cdcReader.position.Engine != "postgres" {
		t.Errorf("CDC saw Engine=%q; want %q", cdcReader.position.Engine, "postgres")
	}
	if cdcReader.position.Token != chainTerminal.Token {
		t.Errorf("CDC saw Token=%q; want chain-terminal Token=%q (the applier's stale token MUST be bypassed)",
			cdcReader.position.Token, chainTerminal.Token)
	}
}

// TestStreamer_PositionFromManifest_EmptyEndPosition refuses cleanly
// when the chain's terminal manifest has no recorded EndPosition.
// The operator's intent (chain handoff) can't be satisfied; a silent
// fall-through to cold-start would re-bulk and defeat the purpose.
func TestStreamer_PositionFromManifest_EmptyEndPosition(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		// No EndPosition.
		PartialState: irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	source := &resumeDispatchEngine{name: "postgres", caps: ir.Capabilities{CDC: ir.CDCLogicalReplication}}
	target := &resumeDispatchEngine{name: "postgres"}
	applier := &resumeDispatchApplier{}

	s := &Streamer{
		Source:                    source,
		Target:                    target,
		SourceDSN:                 "src",
		TargetDSN:                 "tgt",
		StreamID:                  "test-stream",
		SchemaChanges:             "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:                   applier,
		PositionFromManifestStore: store,
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; want error on empty EndPosition")
	}
	if !strings.Contains(err.Error(), "no EndPosition") {
		t.Errorf("err = %v; want 'no EndPosition' surface", err)
	}
}

// TestStreamer_PositionFromManifest_StrictPreflightWarningsRefuse pins
// the StrictPreflight behaviour: when the engine's preflight returns a
// warning AND --strict-preflight is set, the streamer refuses before
// CDC opens.
func TestStreamer_PositionFromManifest_StrictPreflightWarningsRefuse(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	chainTerminal := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_slot","lsn":"1/300"}`,
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   chainTerminal,
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	source := &preflightTestEngine{
		name: "postgres",
		caps: ir.Capabilities{CDC: ir.CDCLogicalReplication},
		report: PreflightReport{
			Warnings: []string{"wal_keep_size of 16MB may not cover the chain cadence"},
		},
	}
	target := &resumeDispatchEngine{name: "postgres"}
	applier := &resumeDispatchApplier{}

	s := &Streamer{
		Source:                    source,
		Target:                    target,
		SourceDSN:                 "src",
		TargetDSN:                 "tgt",
		StreamID:                  "test-stream",
		SchemaChanges:             "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:                   applier,
		PositionFromManifestStore: store,
		StrictPreflight:           true,
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; want refusal under StrictPreflight")
	}
	if !strings.Contains(err.Error(), "strict-preflight") {
		t.Errorf("err = %v; want 'strict-preflight' surface", err)
	}
}

// TestStreamer_PositionFromManifest_PreflightRefusalAlwaysRefuses pins
// the slot-existence-style refusal: a fatal preflight refusal aborts
// regardless of StrictPreflight.
func TestStreamer_PositionFromManifest_PreflightRefusalAlwaysRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	chainTerminal := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_slot","lsn":"1/300"}`,
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   chainTerminal,
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	source := &preflightTestEngine{
		name: "postgres",
		caps: ir.Capabilities{CDC: ir.CDCLogicalReplication},
		report: PreflightReport{
			Refusal: "replication slot \"sluice_slot\" wal_status=lost; recreate the slot",
		},
	}
	target := &resumeDispatchEngine{name: "postgres"}
	applier := &resumeDispatchApplier{}

	s := &Streamer{
		Source:                    source,
		Target:                    target,
		SourceDSN:                 "src",
		TargetDSN:                 "tgt",
		StreamID:                  "test-stream",
		SchemaChanges:             "refuse", // ADR-0091: dispatch test, not exercising DDL forwarding
		Applier:                   applier,
		PositionFromManifestStore: store,
		// StrictPreflight: false  -- refusal must trip regardless.
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; want refusal on slot-lost preflight")
	}
	if !strings.Contains(err.Error(), "wal_status=lost") {
		t.Errorf("err = %v; want refusal containing wal_status=lost", err)
	}
}

// preflightTestEngine extends resumeDispatchEngine with a SchemaReader
// that implements PositionFromManifestPreflight, returning the
// configured PreflightReport. Used to exercise 3.3.C surfaces from the
// streamer's perspective without standing up real engines.
type preflightTestEngine struct {
	name   string
	caps   ir.Capabilities
	report PreflightReport
	err    error
}

func (e *preflightTestEngine) Name() string                  { return e.name }
func (e *preflightTestEngine) Capabilities() ir.Capabilities { return e.caps }

func (e *preflightTestEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &preflightTestSchemaReader{report: e.report, err: e.err}, nil
}

func (e *preflightTestEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("not used")
}

func (e *preflightTestEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not used")
}

func (e *preflightTestEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not used")
}

func (e *preflightTestEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	// Returns an empty closed channel — apply loop has nothing to do.
	return &capturingCDCReader{captured: make(chan struct{})}, nil
}

func (e *preflightTestEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not used")
}

func (e *preflightTestEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not used")
}

type preflightTestSchemaReader struct {
	report PreflightReport
	err    error
}

func (r *preflightTestSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) {
	return &ir.Schema{}, nil
}

func (r *preflightTestSchemaReader) PreflightPositionFromManifest(
	_ context.Context,
	_ ir.Position,
	_ string,
) (PreflightReport, error) {
	if r.err != nil {
		return PreflightReport{}, r.err
	}
	return r.report, nil
}
