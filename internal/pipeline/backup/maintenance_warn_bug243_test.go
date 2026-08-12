// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Pins for the Bug 243 MAINTENANCE door (the v0.120.1 follow-up filing):
// `backup compact` and `backup prune` WARN — never refuse — when the
// chain's restorable window carries a recorded schema that cannot be
// emitted as valid SQL, instead of maintaining an un-restorable chain in
// silence. The incremental door's sibling, wired through
// [warnIfChainRecordedSchemaMalformed]; the wiring pins run the REAL
// CompactChain / PruneChain so removing either call site fails a test,
// not just the helper's own pin.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// captureMaintenanceSlog swaps the default slog logger for a
// buffer-backed one for the duration of the test (single-goroutine use).
func captureMaintenanceSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// mangleSegmentFullSchema rewrites the given segment's full manifest so
// its recorded schema carries the Bug 243 apostrophe mangle.
func mangleSegmentFullSchema(t *testing.T, store irbackup.Store, segIdx int) {
	t.Helper()
	ctx := context.Background()
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	seg := &cat.Segments[segIdx]
	ss := seg.Store(store)
	m, err := lineage.ReadManifestAt(ctx, ss, seg.FullManifestPath)
	if err != nil {
		t.Fatalf("read segment %d full: %v", segIdx, err)
	}
	m.Schema = &ir.Schema{Tables: []*ir.Table{{
		Name:             "ck",
		Columns:          []*ir.Column{{Name: "name", Type: ir.Text{}}},
		CheckConstraints: []*ir.CheckConstraint{{Name: "ck_name", Expr: bug243MangledExpr}},
	}}}
	if err := lineage.WriteManifestAt(ctx, ss, seg.FullManifestPath, m); err != nil {
		t.Fatalf("rewrite segment %d full: %v", segIdx, err)
	}
}

const maintenanceWarnMarker = "SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED"

func TestCompactChain_WarnsOnMalformedRecordedSchema_Bug243(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	opts := func() CompactOpts {
		return CompactOpts{
			MergeWindow:  2 * time.Hour,
			Now:          func() time.Time { return now.Add(10 * time.Hour) },
			newSegmentID: func() string { return "merged-A" },
		}
	}

	t.Run("mangled chain: compact succeeds AND warns with the code", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		mangleSegmentFullSchema(t, store, 0)
		buf := captureMaintenanceSlog(t)
		if _, err := CompactChain(context.Background(), store, opts()); err != nil {
			t.Fatalf("CompactChain: %v (the door is a WARN, never a refusal)", err)
		}
		out := buf.String()
		for _, want := range []string{maintenanceWarnMarker, "backup compact", "will NOT restore"} {
			if !strings.Contains(out, want) {
				t.Errorf("compact WARN output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("healthy chain: silent", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		buf := captureMaintenanceSlog(t)
		if _, err := CompactChain(context.Background(), store, opts()); err != nil {
			t.Fatalf("CompactChain: %v", err)
		}
		if strings.Contains(buf.String(), maintenanceWarnMarker) {
			t.Errorf("healthy chain produced the Bug 243 WARN:\n%s", buf.String())
		}
	})
}

func TestPruneChain_WarnsOnMalformedRecordedSchema_Bug243(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	t.Run("mangled chain: prune succeeds AND warns with the code", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		mangleSegmentFullSchema(t, store, 0)
		buf := captureMaintenanceSlog(t)
		if _, err := PruneChain(context.Background(), store, PruneOpts{
			KeepIncrementals: 1,
			Now:              func() time.Time { return now.Add(10 * time.Hour) },
		}); err != nil {
			t.Fatalf("PruneChain: %v (the door is a WARN, never a refusal)", err)
		}
		out := buf.String()
		for _, want := range []string{maintenanceWarnMarker, "prune", "will NOT restore"} {
			if !strings.Contains(out, want) {
				t.Errorf("prune WARN output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("healthy chain: silent", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		buf := captureMaintenanceSlog(t)
		if _, err := PruneChain(context.Background(), store, PruneOpts{
			KeepIncrementals: 1,
			Now:              func() time.Time { return now.Add(10 * time.Hour) },
		}); err != nil {
			t.Fatalf("PruneChain: %v", err)
		}
		if strings.Contains(buf.String(), maintenanceWarnMarker) {
			t.Errorf("healthy chain produced the Bug 243 WARN:\n%s", buf.String())
		}
	})
}

// TestWarnIfChainRecordedSchemaMalformed_Coverage pins the helper's own
// stated coverage boundaries: an incremental's SchemaDelta mangle is
// seen (the scan reaches incremental manifests, not only fulls), and a
// mangle in a RETIRED segment is silent (retired segments sit outside
// the restore path — the WARN speaks for the restorable window only).
func TestWarnIfChainRecordedSchemaMalformed_Coverage(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	t.Run("incremental SchemaDelta mangle is seen", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
		if err != nil || !ok {
			t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
		}
		seg := &cat.Segments[0]
		ss := seg.Store(store)
		ip := seg.Incrementals[0]
		im, err := lineage.ReadManifestAt(ctx, ss, ip)
		if err != nil {
			t.Fatalf("read incremental: %v", err)
		}
		im.SchemaDelta = append(im.SchemaDelta, &irbackup.SchemaDeltaEntry{
			Kind:  irbackup.SchemaDeltaAddTable,
			Table: "ck",
			After: &ir.Table{
				Name:             "ck",
				Columns:          []*ir.Column{{Name: "name", Type: ir.Text{}}},
				CheckConstraints: []*ir.CheckConstraint{{Name: "ck_name", Expr: bug243MangledExpr}},
			},
		})
		if err := lineage.WriteManifestAt(ctx, ss, ip, im); err != nil {
			t.Fatalf("rewrite incremental: %v", err)
		}
		buf := captureMaintenanceSlog(t)
		warnIfChainRecordedSchemaMalformed(ctx, "backup compact", store, cat)
		if !strings.Contains(buf.String(), maintenanceWarnMarker) {
			t.Errorf("incremental-delta mangle produced no WARN:\n%s", buf.String())
		}
	})

	t.Run("retired-segment mangle is outside the scan", func(t *testing.T) {
		store := newMemStore()
		seedTwoSegmentLineage(t, store, now, time.Hour)
		mangleSegmentFullSchema(t, store, 0)
		cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
		if err != nil || !ok {
			t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
		}
		cat.RestorableFromSegment = 1 // segment 0 retired
		buf := captureMaintenanceSlog(t)
		warnIfChainRecordedSchemaMalformed(ctx, "prune", store, cat)
		if strings.Contains(buf.String(), maintenanceWarnMarker) {
			t.Errorf("retired-segment mangle produced a WARN:\n%s", buf.String())
		}
	})
}
