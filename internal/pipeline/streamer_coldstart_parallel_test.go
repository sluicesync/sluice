// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// snapshotImporterEngine is a recordingEngine that ALSO implements
// [ir.SnapshotImporterOpener], so it qualifies for the ADR-0079 fast
// cold-start gate. Plain recordingEngine does not implement the opener, so
// it stands in for an engine (MySQL/VStream) that doesn't.
type snapshotImporterEngine struct {
	*recordingEngine
}

func (snapshotImporterEngine) OpenSnapshotImporter(context.Context, string) (ir.SnapshotImporter, error) {
	return nil, errors.New("not used in the pure gate test")
}

func newSnapshotImporterEngine(name string) snapshotImporterEngine {
	return snapshotImporterEngine{recordingEngine: newRecordingEngine(name)}
}

// TestColdStartFastEligible is the ADR-0079 capability-gate matrix: the
// fast parallel cold-start engages ONLY when every precondition holds —
// not resuming, not --schema-already-applied, a non-empty SHAREABLE
// snapshot name, AND a source that implements SnapshotImporterOpener. Every
// other combination falls back to the serial path with a named reason.
func TestColdStartFastEligible(t *testing.T) {
	importerSrc := newSnapshotImporterEngine("postgres") // implements opener
	plainSrc := newRecordingEngine("mysql")              // no opener

	tests := []struct {
		name         string
		resuming     bool
		schemaApplie bool
		snapshotName string
		source       ir.Engine
		wantOK       bool
		wantSub      string
	}{
		{
			name:         "all preconditions hold -> fast",
			snapshotName: "00000003-1-1",
			source:       importerSrc,
			wantOK:       true,
		},
		{
			name:         "resuming -> serial",
			resuming:     true,
			snapshotName: "00000003-1-1",
			source:       importerSrc,
			wantOK:       false,
			wantSub:      "resumable",
		},
		{
			name:         "schema-already-applied -> serial",
			schemaApplie: true,
			snapshotName: "00000003-1-1",
			source:       importerSrc,
			wantOK:       false,
			wantSub:      "schema-already-applied",
		},
		{
			name:         "empty snapshot name (MySQL/VStream) -> serial",
			snapshotName: "",
			source:       importerSrc,
			wantOK:       false,
			wantSub:      "not shareable",
		},
		{
			name:         "source lacks importer -> serial",
			snapshotName: "00000003-1-1",
			source:       plainSrc,
			wantOK:       false,
			wantSub:      "no snapshot importer",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := coldStartFastEligible(tc.resuming, tc.schemaApplie, tc.snapshotName, tc.source)
			if ok != tc.wantOK {
				t.Fatalf("coldStartFastEligible = %v (reason %q); want %v", ok, reason, tc.wantOK)
			}
			if !tc.wantOK {
				if reason == "" {
					t.Fatalf("negative gate must give a reason; got empty")
				}
				if tc.wantSub != "" && !strings.Contains(reason, tc.wantSub) {
					t.Errorf("reason %q does not contain %q", reason, tc.wantSub)
				}
			}
		})
	}
}

// TestOpenChunkReader_FactorySelection pins the [chunkReaderFactory]
// provenance seam (ADR-0079): migrate (nil factory) mints readers via
// source.OpenRowReader; sync (non-nil factory) mints them via the factory
// (the snapshot importer in production), NEVER via OpenRowReader. A miss
// here would mean a sync "fast" reader silently observing its own
// per-connection snapshot instead of the one exported snapshot — exactly
// the gap-free property the fast path is supposed to guarantee.
func TestOpenChunkReader_FactorySelection(t *testing.T) {
	ctx := context.Background()

	t.Run("migrate path uses OpenRowReader (nil factory)", func(t *testing.T) {
		src := &countingReaderEngine{recordingEngine: newRecordingEngine("postgres")}
		deps := &parallelBulkCopyDeps{source: src, sourceDSN: "dsn"}
		if _, err := openChunkReader(ctx, deps); err != nil {
			t.Fatalf("openChunkReader: %v", err)
		}
		if src.openRowReaderCalls != 1 {
			t.Errorf("OpenRowReader calls = %d; want 1", src.openRowReaderCalls)
		}
	})

	t.Run("sync path uses the factory (factory set)", func(t *testing.T) {
		src := &countingReaderEngine{recordingEngine: newRecordingEngine("postgres")}
		factoryCalls := 0
		deps := &parallelBulkCopyDeps{
			source:    src,
			sourceDSN: "dsn",
			chunkReaderFactory: func(context.Context) (ir.RowReader, error) {
				factoryCalls++
				return &recordingRowReader{}, nil
			},
		}
		if _, err := openChunkReader(ctx, deps); err != nil {
			t.Fatalf("openChunkReader: %v", err)
		}
		if factoryCalls != 1 {
			t.Errorf("factory calls = %d; want 1", factoryCalls)
		}
		if src.openRowReaderCalls != 0 {
			t.Errorf("OpenRowReader calls = %d; want 0 (factory must own reader provenance)", src.openRowReaderCalls)
		}
	})
}

// countingReaderEngine counts OpenRowReader invocations so the factory-
// selection test can assert which provenance the deps took.
type countingReaderEngine struct {
	*recordingEngine
	openRowReaderCalls int
}

func (e *countingReaderEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	e.openRowReaderCalls++
	return &recordingRowReader{}, nil
}

// TestResolveColdStartCopyBudget_CDCReservation pins the ADR-0079 budget
// composition: the sync cold-start reserves ONE connection for the CDC
// reader that goes live right after bulk-copy, so the load-bearing
// invariant tableP × withinP + indexBudget + 1(CDC) ≤ CopyBudget holds.
// A non-prober/degraded target (CopyBudget < 1) takes no reservation and
// resolves the axes to their requested values, exactly as migrate does.
func TestResolveColdStartCopyBudget_CDCReservation(t *testing.T) {
	ctx := context.Background()

	t.Run("measured budget reserves one CDC slot, product+index+1 fits", func(t *testing.T) {
		// budgetEngine reports CopyBudget=12. With within=2, the reservation
		// drops the divisible budget to 11; the index split reserves
		// clamp(0.25*11)=3 (within=2 floor), leaving 8 for the copy axes,
		// table=min(4, 8/2)=4. Invariant: 4*2 + 3 + 1 = 12 == CopyBudget.
		src := &budgetTargetEngine{recordingEngine: newRecordingEngine("postgres"), copyBudget: 12, effective: 2}
		s := &Streamer{Target: src, BulkParallelism: 2, TableParallelism: 4}
		tableP, withinP, indexBudget, err := resolveColdStartCopyBudget(ctx, s, true /*overlaps*/)
		if err != nil {
			t.Fatalf("resolveColdStartCopyBudget: %v", err)
		}
		const cdcReserve = 1
		if got := tableP*withinP + indexBudget + cdcReserve; got > 12 {
			t.Errorf("tableP(%d)*withinP(%d)+index(%d)+cdc(1) = %d; must be <= CopyBudget 12",
				tableP, withinP, indexBudget, got)
		}
		if withinP != 2 {
			t.Errorf("withinP = %d; want 2", withinP)
		}
	})

	t.Run("no measured ceiling (MySQL target) -> no reservation, axes unclamped", func(t *testing.T) {
		// recordingEngine doesn't implement the budget prober, so
		// migcore.ResolveTargetCopyParallelism returns the requested value with a
		// zero-value ConnectionBudget (CopyBudget 0 = no ceiling).
		src := newRecordingEngine("postgres")
		s := &Streamer{Target: src, BulkParallelism: 3, TableParallelism: 5}
		tableP, withinP, indexBudget, err := resolveColdStartCopyBudget(ctx, s, true)
		if err != nil {
			t.Fatalf("resolveColdStartCopyBudget: %v", err)
		}
		if withinP != 3 {
			t.Errorf("withinP = %d; want 3 (requested, unclamped)", withinP)
		}
		if tableP != 5 {
			t.Errorf("tableP = %d; want 5 (requested, unclamped)", tableP)
		}
		if indexBudget != 0 {
			t.Errorf("indexBudget = %d; want 0 (no measured ceiling => no overlap split)", indexBudget)
		}
	})

	t.Run("overlaps=false leaves index budget zero", func(t *testing.T) {
		src := &budgetTargetEngine{recordingEngine: newRecordingEngine("postgres"), copyBudget: 12, effective: 2}
		s := &Streamer{Target: src, BulkParallelism: 2, TableParallelism: 4}
		_, _, indexBudget, err := resolveColdStartCopyBudget(ctx, s, false /*no overlap*/)
		if err != nil {
			t.Fatalf("resolveColdStartCopyBudget: %v", err)
		}
		if indexBudget != 0 {
			t.Errorf("indexBudget = %d; want 0 when overlap disabled", indexBudget)
		}
	})
}

// budgetTargetEngine implements [ir.TargetConnectionBudgetProber] so the
// budget-resolution test exercises the real split math with a controllable
// CopyBudget, without a database.
type budgetTargetEngine struct {
	*recordingEngine
	copyBudget int
	effective  int
}

func (e *budgetTargetEngine) ProbeTargetConnectionBudget(_ context.Context, _ string, requested, _ int) (ir.ConnectionBudget, error) {
	eff := e.effective
	if eff == 0 {
		eff = requested
	}
	return ir.ConnectionBudget{
		EffectiveParallelism: eff,
		CopyBudget:           e.copyBudget,
		MaxConnections:       e.copyBudget,
		Available:            e.copyBudget,
	}, nil
}
