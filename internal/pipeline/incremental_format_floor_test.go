// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestIncrementalBackup_WarnsWhenItRaisesTheChainFormatFloor is the
// REACHABILITY half of the item-90 / Bug-212 fix, and it is the half that
// matters. The unit tests in internal/pipeline/lineage prove the warning
// helper behaves; they prove nothing about whether the extension paths
// actually call it, with the right two versions, on a real run. A fix that
// is present but unreachable from the path is the defect class this
// codebase has hit repeatedly, so the pin drives the real
// [IncrementalBackup.Run] end to end and reads the log it emits.
//
// The raise is produced WITHOUT encryption, via the VStream-shaped
// CDCPositionCommitsAfterRows capability that stamps
// [irbackup.FormatVersionCDCPositionBinding]. That is deliberate: it keeps
// the test free of key material, and it exercises the instance of this
// class that PREDATES ADR-0181 — any VStream chain rooted before v0.99.228
// has had its floor silently raised the same way. If the warning were
// wired only to the v9 encrypted stamp, this test would fail.
func TestIncrementalBackup_WarnsWhenItRaisesTheChainFormatFloor(t *testing.T) {
	warned := runIncrementalCapturingWarnings(t, irbackup.FormatVersionLegacy, true)

	var found map[string]any
	for _, rec := range warned {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "raises the chain's format version") {
			found = rec
			break
		}
	}
	if found == nil {
		t.Fatalf("no chain-format-floor warning was emitted by a real incremental run that raised the floor "+
			"from %d to %d; the helper is wired but unreachable from this path\ncaptured: %v",
			irbackup.FormatVersionLegacy, irbackup.FormatVersionCDCPositionBinding, warned)
	}

	// The versions must be the REAL ones, not placeholders — a call site
	// passing the wrong pair would still log something.
	if got, want := found["previous_format_version"], float64(irbackup.FormatVersionLegacy); got != want {
		t.Errorf("previous_format_version = %v; want %v (the parent full's recorded version)", got, want)
	}
	if got, want := found["new_format_version"], float64(irbackup.FormatVersionCDCPositionBinding); got != want {
		t.Errorf("new_format_version = %v; want %v (the version this segment actually stamped)", got, want)
	}
	if chain, _ := found["chain"].(string); chain == "" {
		t.Error("chain attr is empty; the warning cannot be tied to a backup")
	}
}

// TestIncrementalBackup_SilentWhenItDoesNotRaiseTheFloor is the other half.
// An ordinary incremental against a chain at the same version must say
// nothing — a warning on every routine run would be tuned out long before
// the one that matters, which would cost more than the silence did.
func TestIncrementalBackup_SilentWhenItDoesNotRaiseTheFloor(t *testing.T) {
	warned := runIncrementalCapturingWarnings(t, irbackup.FormatVersionCDCPositionBinding, true)
	for _, rec := range warned {
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "raises the chain's format version") {
			t.Fatalf("warned on an incremental that did not raise the floor (parent already at %d): %v",
				irbackup.FormatVersionCDCPositionBinding, rec)
		}
	}
}

// runIncrementalCapturingWarnings runs one real incremental against a
// parent full recorded at parentVersion and returns the WARN-level records
// it produced. Mirrors TestIncrementalBackup_RoundTrip's fixture.
func runIncrementalCapturingWarnings(t *testing.T, parentVersion int, vstreamShaped bool) []map[string]any {
	t.Helper()

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
		FormatVersion: parentVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:                        "postgres",
		schemaSequence:              []*ir.Schema{schema},
		cdcPositionCommitsAfterRows: vstreamShaped,
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`},
				Table:    "users",
				Row:      ir.Row{"id": int64(42)},
			},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
		cdcExpectedFromOK: true,
	}

	now := time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	runErr := b.Run(context.Background())
	slog.SetDefault(prev)

	if runErr != nil {
		t.Fatalf("incremental run: %v", runErr)
	}

	// Guard the premise: if the run did not actually stamp the version we
	// expect, the assertions above would be testing the wrong transition.
	final := readFinalIncrementalVersion(t, store)
	wantFinal := irbackup.FormatVersionFor(schema)
	if vstreamShaped {
		wantFinal = max(wantFinal, irbackup.FormatVersionCDCPositionBinding)
	}
	if final != wantFinal {
		t.Fatalf("the incremental stamped format version %d, not the %d this test assumes; "+
			"the fixture no longer exercises the intended transition", final, wantFinal)
	}

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode captured log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// readFinalIncrementalVersion returns the FormatVersion the run actually
// recorded on the incremental manifest, read back off the store rather than
// from the in-memory value — the recorded number is the one that decides
// whether an older binary can read the chain.
func readFinalIncrementalVersion(t *testing.T, store *blobcodec.LocalStore) int {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			return r.Manifest.FormatVersion
		}
	}
	t.Fatal("no incremental manifest written")
	return 0
}
