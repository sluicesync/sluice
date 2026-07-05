// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 5.4 unit tests: cross-engine broker EndPosition drop.
//
// When the chain's source engine differs from the broker's target
// engine, the chain's terminal EndPosition (engine-specific:
// `{slot,lsn}` JSON for Postgres, GTID set for MySQL) cannot be
// translated into a target-engine-shaped CDC position. The broker
// still writes its own `_engine="backup-broker"` envelope to
// `sluice_cdc_state` (so warm resume works), but the chain-source-
// engine-flavored EndPosition is intentionally omitted — operators
// continuing CDC from a cross-engine restored target run a fresh
// `sluice sync start` against the source's native engine, or pass
// --at-chain-id for a cross-engine resumption assertion.
//
// These tests assert:
//  1. detectChainSourceEngine reads the full's SourceEngine off the
//     chain's manifests (used to drive the cross-engine log line).
//  2. The persisted broker position is the broker's own envelope
//     shape, never an engine-specific raw token from the chain's
//     EndPosition. This is already the broker's invariant via
//     [encodeBrokerPosition], but Phase 5.4 documents it explicitly
//     so a future refactor doesn't accidentally start writing the
//     chain's terminal EndPosition.

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestSyncFromBackup_DetectChainSourceEngine_PG verifies the helper
// reads the full's SourceEngine from a PG-rooted chain.
func TestSyncFromBackup_DetectChainSourceEngine_PG(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.SourceEngine = "postgres"
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	b := &SyncFromBackup{
		Target: stubTargetEngine{}, TargetDSN: "x", Store: store, StreamID: "s",
	}
	if got := b.detectChainSourceEngine(context.Background()); got != "postgres" {
		t.Errorf("detectChainSourceEngine = %q; want postgres", got)
	}
}

// TestSyncFromBackup_DetectChainSourceEngine_EmptyOnNoChain verifies
// the best-effort behaviour: an empty chain returns "" so the broker
// loop's tick error surfaces normally.
func TestSyncFromBackup_DetectChainSourceEngine_EmptyOnNoChain(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	b := &SyncFromBackup{
		Target: stubTargetEngine{}, TargetDSN: "x", Store: store, StreamID: "s",
	}
	if got := b.detectChainSourceEngine(context.Background()); got != "" {
		t.Errorf("detectChainSourceEngine = %q; want empty on no chain", got)
	}
}

// TestEncodeBrokerPosition_CrossEngine_OmitsChainEndPosition pins the
// invariant: the broker's persisted position is the broker's own
// envelope shape, never the chain's source-engine-specific
// EndPosition. A cross-engine target sees only the broker envelope
// in the persisted position.
func TestEncodeBrokerPosition_CrossEngine_OmitsChainEndPosition(t *testing.T) {
	// Simulate the broker writing its position after applying an
	// incremental from a PG-rooted chain into a MySQL target.
	pos := encodeBrokerPosition("file:///tmp/chain", "incr-0001-abc")

	// Engine field carries the broker sentinel, not "postgres".
	if pos.Engine != BackupBrokerPositionEngine {
		t.Errorf("Engine = %q; want %q (cross-engine broker writes only its envelope)",
			pos.Engine, BackupBrokerPositionEngine)
	}
	// Token does NOT contain a PG-shaped {slot,lsn} structure or any
	// engine-specific raw data — only the broker's chain reference.
	if strings.Contains(pos.Token, `"slot":`) || strings.Contains(pos.Token, `"lsn":`) {
		t.Errorf("token contains PG-engine-specific fields; want only broker envelope: %s", pos.Token)
	}
	// Token MUST embed _engine sentinel for round-trip survivability.
	if !strings.Contains(pos.Token, `"_engine":"backup-broker"`) {
		t.Errorf("token does not embed _engine sentinel: %s", pos.Token)
	}
}

// brokerPositionWriterRecorder is a stub ChangeApplier that records
// every WritePosition call so a test can assert what the broker
// persisted to sluice_cdc_state. Mirrors the recorder shape used by
// chain_restore_cross_test.go.
type brokerPositionWriterRecorder struct {
	written []ir.Position
}

func (a *brokerPositionWriterRecorder) EnsureControlTable(_ context.Context) error { return nil }
func (a *brokerPositionWriterRecorder) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *brokerPositionWriterRecorder) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *brokerPositionWriterRecorder) Apply(_ context.Context, _ string, _ <-chan ir.Change) error {
	return nil
}

func (a *brokerPositionWriterRecorder) RequestStop(context.Context, string) error { return nil }

func (a *brokerPositionWriterRecorder) ClearStopRequested(context.Context, string) error { return nil }

func (a *brokerPositionWriterRecorder) Close() error { return nil }

func (a *brokerPositionWriterRecorder) WritePosition(_ context.Context, _ string, p ir.Position) error {
	a.written = append(a.written, p)
	return nil
}

// TestSyncFromBackup_WritePositionDirect_CrossEngine asserts that a
// cross-engine broker calling writePositionDirect persists ONLY the
// broker envelope — never an engine-specific raw token. This guards
// the Phase 5.4 invariant from a future refactor.
func TestSyncFromBackup_WritePositionDirect_CrossEngine(t *testing.T) {
	rec := &brokerPositionWriterRecorder{}
	b := &SyncFromBackup{
		Target:   stubTargetEngine{},
		ChainURL: "file:///tmp/chain",
		StreamID: "s",
	}
	if err := b.writePositionDirect(context.Background(), rec, "incr-0001-abc"); err != nil {
		t.Fatalf("writePositionDirect: %v", err)
	}
	if len(rec.written) != 1 {
		t.Fatalf("written = %d; want 1", len(rec.written))
	}
	got := rec.written[0]
	if got.Engine != BackupBrokerPositionEngine {
		t.Errorf("Engine = %q; want %q", got.Engine, BackupBrokerPositionEngine)
	}
	// No engine-specific raw token leaked.
	if strings.Contains(got.Token, `"slot":`) || strings.Contains(got.Token, `"lsn":`) {
		t.Errorf("token contains engine-specific fields: %s", got.Token)
	}
	if !strings.Contains(got.Token, "incr-0001-abc") {
		t.Errorf("token doesn't reference the BackupID: %s", got.Token)
	}
}
