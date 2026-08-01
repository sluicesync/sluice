// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestItem112_SyncColdStartArmsMetadataOnlyFK pins roadmap item 112: an
// UNFILTERED sync cold-start arms the item-109 metadata-only FK path on its
// target writer (via ir.ForeignKeyConsistencyDeclarer), exactly as migrate does
// — but a `--where`-filtered stream does NOT, because filtering a parent can
// legitimately orphan children, so it must keep full target-side FK validation.
func TestItem112_SyncColdStartArmsMetadataOnlyFK(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "orders"}, {Name: "customers"}}}

	armed := func(t *testing.T, rowFilters map[string]string) bool {
		t.Helper()
		tgt := newRecordingEngine("target")
		s := &Streamer{Target: tgt, RowFilters: rowFilters}
		stream := &ir.SnapshotStream{CloseFn: func() error { return nil }, AbandonFn: func() error { return nil }}
		if _, _, err := s.coldStartOpenTargetWriters(context.Background(), schema, stream); err != nil {
			t.Fatalf("coldStartOpenTargetWriters: %v", err)
		}
		return tgt.fkConsistent
	}

	t.Run("unfiltered cold-start arms the metadata-only FK path", func(t *testing.T) {
		if !armed(t, nil) {
			t.Error("unfiltered sync cold-start did NOT arm SetCopiedRowsForeignKeyConsistent(true) — item 112 not wired")
		}
	})

	t.Run("--where-filtered cold-start keeps full FK validation", func(t *testing.T) {
		if armed(t, map[string]string{"orders": "region='EU'"}) {
			t.Error("a --where-filtered sync cold-start armed the metadata-only FK path; a filtered run can orphan children by filtering a parent and MUST keep full FK validation as the loud net")
		}
	})
}

// fakeOrphanWriter is a SchemaWriter that also reports metadata-only-added FKs
// and can scan/drop them — the surfaces the item-109 orphan scan needs. It lets
// the sync-side verify path run without a real MySQL target.
type fakeOrphanWriter struct {
	recordingSchemaWriter
	unvalidated []ir.UnvalidatedForeignKey
	orphan      bool // ScanForeignKeyOrphan reports a violation
	scanCalls   int
	dropped     []string
}

func (w *fakeOrphanWriter) UnvalidatedForeignKeys() []ir.UnvalidatedForeignKey { return w.unvalidated }

func (w *fakeOrphanWriter) ScanForeignKeyOrphan(_ context.Context, _ *ir.Table, _ *ir.ForeignKey, _, _ []any) (violatingKey []any, found bool, err error) {
	w.scanCalls++
	if w.orphan {
		return []any{int64(100)}, true, nil
	}
	return nil, false, nil
}

func (w *fakeOrphanWriter) DropForeignKey(_ context.Context, child, fk string) error {
	w.dropped = append(w.dropped, child+"."+fk)
	return nil
}

// TestItem112_SyncColdStartOrphanScanFires is the CRITICAL half of item 112: the
// metadata-only FK add skips validation, so the sync cold-start MUST run the
// bounded orphan scan to recover the loud signal — otherwise a dirty source's
// orphans are silently accepted (a silent-loss regression). It pins that
// [Streamer.verifyUnvalidatedForeignKeys] actually scans an armed FK, refuses
// loudly with SLUICE-E-FK-SOURCE-ORPHAN (dropping the trusted-wrong FK first) on
// a violation, passes a clean source, and is a no-op when nothing was added
// metadata-only.
func TestItem112_SyncColdStartOrphanScanFires(t *testing.T) {
	fk := &ir.ForeignKey{Name: "orders_customer_fk", ReferencedTable: "customers"}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "orders", PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}}},
		{Name: "customers"},
	}}
	s := &Streamer{Target: newRecordingEngine("target")}
	oneFK := []ir.UnvalidatedForeignKey{{Table: "orders", FK: fk}}

	t.Run("dirty source: orphan scan drops the FK and refuses loudly", func(t *testing.T) {
		var pl []string
		w := &fakeOrphanWriter{recordingSchemaWriter: recordingSchemaWriter{phaseLog: &pl}, unvalidated: oneFK, orphan: true}
		err := s.verifyUnvalidatedForeignKeys(context.Background(), w, schema)
		if err == nil {
			t.Fatal("a dirty source (orphaned child) was SILENTLY ACCEPTED — the sync metadata-only FK add ran without its orphan-scan net (silent-loss)")
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeFKSourceOrphan {
			t.Fatalf("err = %v; want a SLUICE-E-FK-SOURCE-ORPHAN refusal", err)
		}
		if len(w.dropped) != 1 {
			t.Errorf("the trusted-wrong FK was not dropped before refusing (target would keep an unproven constraint): dropped=%v", w.dropped)
		}
	})

	t.Run("clean source: proven clean, scanned, not dropped", func(t *testing.T) {
		var pl []string
		w := &fakeOrphanWriter{recordingSchemaWriter: recordingSchemaWriter{phaseLog: &pl}, unvalidated: oneFK, orphan: false}
		if err := s.verifyUnvalidatedForeignKeys(context.Background(), w, schema); err != nil {
			t.Fatalf("a clean source should pass the orphan scan: %v", err)
		}
		if w.scanCalls == 0 {
			t.Error("the armed FK was never scanned — the net did not run")
		}
		if len(w.dropped) != 0 {
			t.Errorf("a clean FK was dropped: %v", w.dropped)
		}
	})

	t.Run("no metadata-only FKs: no-op", func(t *testing.T) {
		var pl []string
		w := &fakeOrphanWriter{recordingSchemaWriter: recordingSchemaWriter{phaseLog: &pl}}
		if err := s.verifyUnvalidatedForeignKeys(context.Background(), w, schema); err != nil {
			t.Fatalf("the no-metadata-only-FK case must be a clean no-op: %v", err)
		}
		if w.scanCalls != 0 {
			t.Errorf("scanned %d time(s) despite no metadata-only FKs", w.scanCalls)
		}
	})
}
