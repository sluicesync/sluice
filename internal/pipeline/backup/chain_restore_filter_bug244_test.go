// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Bug 244 pins: `restore --exclude-table` / `--include-table` on a chain
// carrying an incremental. The base full's restore honoured the filter
// (the excluded table was never created), but the chain's post-full walk
// did not — an excluded table's change events reached the applier, which
// died loudly against the missing table ("table X has no columns"), and
// an AlterTable delta on it died the same way — breaking `--exclude-table`
// as Bug 243's own documented salvage on any full+incremental chain.
//
// Door enumeration for the class (chain doors that consume manifest
// content after the filtered full), stated per the sibling-sweep rule:
//   - applyFull                      → already filtered (Restore.Filter)
//   - applyIncremental change stream → FIXED; pinned here
//   - applySchemaDeltas              → FIXED; pinned here (incl. the
//     Bug 243 delta door turning filter-aware by construction)
//   - reprimeStandaloneSequences     → FIXED; pinned here
//   - streamSchemaHistorySnapshots   → EXEMPT: writes only
//     sluice_cdc_schema_history (stream state), never the user table
//   - syncIdentitySequencesAtTail    → already filtered
//   - refuseUnrepresentableTargetShape (both restore paths) → filter-blind
//     for the emit preflights; FILED as a separate loud sibling in
//     docs/dev/audit-backlog.md, not fixed here.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func b244pos(n int) ir.Position {
	return ir.Position{Engine: "mysql", Token: fmt.Sprintf("binlog:%04d", n)}
}

func b244Filter(t *testing.T, exclude ...string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(nil, exclude)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	return f
}

// b244SeedIncremental writes one change chunk carrying events for tables
// "keep" and "dropme" into store and returns the incremental's link. The
// TAIL change — the one carrying the manifest's EndPosition — belongs to
// "dropme", deliberately: that is the F1 trap direction (the filter must
// drop the event without forgetting its position, or a legitimate
// filtered restore reads as a truncated chain).
func b244SeedIncremental(t *testing.T, store irbackup.Store) *lineage.SegmentRecord {
	t.Helper()
	var buf bytes.Buffer
	cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	changes := []ir.Change{
		ir.TxBegin{Position: b244pos(1)},
		ir.Insert{Position: b244pos(2), Table: "keep", Row: ir.Row{"id": int64(1)}},
		ir.Insert{Position: b244pos(3), Table: "dropme", Row: ir.Row{"id": int64(1)}},
		ir.Update{Position: b244pos(4), Table: "keep", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "n": "x"}},
		ir.Delete{Position: b244pos(5), Table: "dropme", Before: ir.Row{"id": int64(1)}},
		ir.Truncate{Position: b244pos(6), Table: "dropme"},
		ir.Insert{Position: b244pos(7), Table: "dropme", Row: ir.Row{"id": int64(2)}},
		// A BARE TxCommit (no source position — a legitimate shape per the
		// window writer's lastPos rule) so the dropme insert above is the
		// ONLY carrier of EndPosition: the F1 trap has no rescuer.
		ir.TxCommit{},
	}
	for _, c := range changes {
		if err := cw.WriteChange(c); err != nil {
			t.Fatalf("write change: %v", err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("close chunk writer: %v", err)
	}
	const chunkPath = "chunks/_changes/incr-0.jsonl.gz"
	if err := store.Put(context.Background(), chunkPath, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	return &lineage.SegmentRecord{
		Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{
			Path: "manifests/incr-0001.json",
			Manifest: &irbackup.Manifest{
				BackupID:      "incr-0001",
				SourceEngine:  "mysql",
				Kind:          irbackup.BackupKindIncremental,
				StartPosition: b244pos(1),
				EndPosition:   b244pos(7),
				ChangeChunks: []*irbackup.ChunkInfo{
					{File: chunkPath, RowCount: cw.ChangeCount(), SHA256: cw.Hash()},
				},
			},
		},
	}
}

// TestApplyIncremental_FilterSkipsExcludedTableChanges_Bug244 is the
// headline pin: an excluded table's change events never reach the
// applier, the kept table's events all do, and the F1 tail-truncation
// backstop still passes even though the EndPosition-bearing change was
// itself filtered out (nil error IS that assertion — F1 refuses with
// SLUICE-E-BACKUP-INCOMPLETE when the replay falls short).
func TestApplyIncremental_FilterSkipsExcludedTableChanges_Bug244(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, filter migcore.TableFilter) *chainRestoreRecorderEngine {
		t.Helper()
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		link := b244SeedIncremental(t, store)
		eng := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("mysql")}
		applier, err := eng.OpenChangeApplier(ctx, "tgt")
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		cr := &ChainRestore{Target: eng, TargetDSN: "tgt", Store: store, Filter: filter}
		if err := cr.applyIncremental(ctx, link, applier, DefaultChainRestoreBatchSize); err != nil {
			t.Fatalf("applyIncremental: %v", err)
		}
		return eng
	}

	t.Run("exclude dropme: its events never reach the applier, F1 still passes", func(t *testing.T) {
		eng := run(t, b244Filter(t, "dropme"))
		var keepEvents, txEvents int
		for _, c := range eng.applied {
			if name := c.QualifiedName(); strings.Contains(name, "dropme") {
				t.Fatalf("excluded table's change reached the applier: %T %s", c, name)
			}
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				txEvents++
			default:
				keepEvents++
			}
		}
		if keepEvents != 2 {
			t.Errorf("kept-table events applied = %d; want 2 (insert + update on keep)", keepEvents)
		}
		if txEvents != 2 {
			t.Errorf("transaction boundary events applied = %d; want 2 (TxBegin + TxCommit pass unfiltered)", txEvents)
		}
	})

	t.Run("empty filter control: every event applies", func(t *testing.T) {
		eng := run(t, migcore.TableFilter{})
		if len(eng.applied) != 8 {
			t.Errorf("applied = %d events; want all 8", len(eng.applied))
		}
	})
}

// TestApplySchemaDeltas_FilterSkipsExcludedTable_Bug244 pins the delta
// door: an AddTable delta on an excluded table is not resurrected and an
// AlterTable delta on it is not applied — while the kept table's deltas
// go through untouched.
func TestApplySchemaDeltas_FilterSkipsExcludedTable_Bug244(t *testing.T) {
	ctx := context.Background()
	mkLink := func() *lineage.SegmentRecord {
		return &lineage.SegmentRecord{
			Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
			ManifestRecord: lineage.ManifestRecord{
				Path: "manifests/incr-0001.json",
				Manifest: &irbackup.Manifest{
					BackupID:     "incr-0001",
					SourceEngine: "mysql",
					Kind:         irbackup.BackupKindIncremental,
					SchemaDelta: []*irbackup.SchemaDeltaEntry{
						{
							Kind:  irbackup.SchemaDeltaAddTable,
							Table: "dropme",
							After: &ir.Table{Name: "dropme", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
						},
						{
							Kind:   irbackup.SchemaDeltaAlterTable,
							Table:  "dropme",
							Before: &ir.Table{Name: "dropme", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
							After: &ir.Table{Name: "dropme", Columns: []*ir.Column{
								{Name: "id", Type: ir.Integer{Width: 64}},
								{Name: "extra", Type: ir.Boolean{}},
							}},
						},
						{
							Kind:  irbackup.SchemaDeltaAddTable,
							Table: "keep",
							After: &ir.Table{Name: "keep", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
						},
					},
				},
			},
		}
	}

	t.Run("exclude dropme: only keep's delta applies", func(t *testing.T) {
		tgt := newSchemaDeltaRecorderEngine("mysql")
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}, Filter: b244Filter(t, "dropme")}
		if err := cr.applySchemaDeltas(ctx, mkLink()); err != nil {
			t.Fatalf("applySchemaDeltas: %v", err)
		}
		if len(tgt.addedTables) != 1 || tgt.addedTables[0].Name != "keep" {
			t.Errorf("addedTables = %+v; want exactly [keep]", tgt.addedTables)
		}
		if tgt.alterTable != nil {
			t.Errorf("AlterAddColumn called for %q; excluded table's alter must be skipped", tgt.alterTable.Name)
		}
	})

	t.Run("exclude everything the deltas touch: no schema writer work at all", func(t *testing.T) {
		tgt := newSchemaDeltaRecorderEngine("mysql")
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}, Filter: b244Filter(t, "dropme", "keep")}
		if err := cr.applySchemaDeltas(ctx, mkLink()); err != nil {
			t.Fatalf("applySchemaDeltas: %v", err)
		}
		if len(tgt.addedTables) != 0 || tgt.alterTable != nil {
			t.Errorf("schema writer touched (added=%d alter=%v); want none", len(tgt.addedTables), tgt.alterTable)
		}
	})

	t.Run("empty filter control: all three deltas apply", func(t *testing.T) {
		tgt := newSchemaDeltaRecorderEngine("mysql")
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}}
		if err := cr.applySchemaDeltas(ctx, mkLink()); err != nil {
			t.Fatalf("applySchemaDeltas: %v", err)
		}
		if len(tgt.addedTables) != 2 {
			t.Errorf("addedTables = %d; want 2 (dropme + keep)", len(tgt.addedTables))
		}
		if tgt.alterTable == nil || tgt.alterTable.Name != "dropme" {
			t.Errorf("alterTable = %+v; want dropme's alter applied", tgt.alterTable)
		}
	})
}

// TestApplySchemaDeltas_Bug243DoorIsFilterAware pins the interaction
// with the Bug 243 malformed-recorded-schema door: excluding the table
// whose delta carries the mangled expression releases the gate (the
// documented `--exclude-table=<affected>` salvage now works through the
// delta door too), while the unfiltered chain still refuses with the
// coded error. This supersedes the v0.120.1 "unconditional because
// deltas apply unfiltered" premise — deltas are filtered now, so the
// door follows the filtered set.
func TestApplySchemaDeltas_Bug243DoorIsFilterAware(t *testing.T) {
	ctx := context.Background()
	mkLink := func() *lineage.SegmentRecord {
		return &lineage.SegmentRecord{
			Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
			ManifestRecord: lineage.ManifestRecord{
				Path: "manifests/incr-0001.json",
				Manifest: &irbackup.Manifest{
					BackupID:     "incr-0001",
					SourceEngine: "mysql",
					Kind:         irbackup.BackupKindIncremental,
					SchemaDelta: []*irbackup.SchemaDeltaEntry{
						{
							Kind:  irbackup.SchemaDeltaAddTable,
							Table: "dropme",
							After: &ir.Table{
								Name:             "dropme",
								Columns:          []*ir.Column{{Name: "c", Type: ir.Text{}}},
								CheckConstraints: []*ir.CheckConstraint{{Name: "ck", Expr: bug243MangledExpr}},
							},
						},
						{
							Kind:  irbackup.SchemaDeltaAddTable,
							Table: "keep",
							After: &ir.Table{Name: "keep", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
						},
					},
				},
			},
		}
	}

	t.Run("excluding the affected table releases the door", func(t *testing.T) {
		tgt := newSchemaDeltaRecorderEngine("mysql")
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}, Filter: b244Filter(t, "dropme")}
		if err := cr.applySchemaDeltas(ctx, mkLink()); err != nil {
			t.Fatalf("applySchemaDeltas with the mangled table excluded: %v", err)
		}
		if len(tgt.addedTables) != 1 || tgt.addedTables[0].Name != "keep" {
			t.Errorf("addedTables = %+v; want exactly [keep]", tgt.addedTables)
		}
	})

	t.Run("unfiltered chain still refuses with the coded error", func(t *testing.T) {
		tgt := newSchemaDeltaRecorderEngine("mysql")
		cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}}
		err := cr.applySchemaDeltas(ctx, mkLink())
		if err == nil {
			t.Fatal("applySchemaDeltas: nil error; want the Bug 243 refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
			t.Errorf("error = %v; want %s", err, sluicecode.CodeBackupRecordedSchemaMalformed)
		}
		if len(tgt.addedTables) != 0 {
			t.Errorf("addedTables = %d; the refusal must be pre-DDL", len(tgt.addedTables))
		}
	})
}

// TestReprimeStandaloneSequences_FilterPrunesExcludedOwners_Bug244 pins
// the sequence tail. The recorder engine's schema writer deliberately
// lacks the sequenceReprimer surface, which makes the pin cheap in both
// directions: with the excluded-owner sequence pruned there is nothing
// to re-prime and the tail returns nil before ever demanding the
// surface; unfiltered, the same chain reaches the surface check and
// errors — proof the prune, not a silent skip, is what changed.
func TestReprimeStandaloneSequences_FilterPrunesExcludedOwners_Bug244(t *testing.T) {
	ctx := context.Background()
	mkLinks := func() []lineage.SegmentRecord {
		return []lineage.SegmentRecord{{
			Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
			ManifestRecord: lineage.ManifestRecord{
				Path: "manifest.json",
				Manifest: &irbackup.Manifest{
					BackupID:     "full-0001",
					SourceEngine: "postgres",
					Kind:         irbackup.BackupKindFull,
					Schema: &ir.Schema{
						Tables: []*ir.Table{{Name: "dropme", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}},
						Sequences: []*ir.Sequence{
							{Name: "dropme_custom_seq", OwnedByTable: "dropme", OwnedByColumn: "id"},
						},
					},
				},
			},
		}}
	}

	t.Run("excluded owner: sequence pruned, tail is a no-op", func(t *testing.T) {
		eng := newRestoreRecorderEngine("postgres")
		cr := &ChainRestore{Target: eng, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}, Filter: b244Filter(t, "dropme")}
		if err := cr.reprimeStandaloneSequences(ctx, mkLinks()); err != nil {
			t.Fatalf("reprimeStandaloneSequences: %v (the excluded-owner sequence must be pruned, not re-primed)", err)
		}
	})

	t.Run("unfiltered control: the same chain demands the reprimer surface", func(t *testing.T) {
		eng := newRestoreRecorderEngine("postgres")
		cr := &ChainRestore{Target: eng, TargetDSN: "tgt", Store: &blobcodec.LocalStore{}}
		err := cr.reprimeStandaloneSequences(ctx, mkLinks())
		if err == nil || !strings.Contains(err.Error(), "cannot re-prime") {
			t.Fatalf("err = %v; want the no-sequence-surface refusal (proves the filtered run pruned rather than silently skipped)", err)
		}
	})
}

// TestFilterDropsChange_EveryChangeVariant rosters the sealed [ir.Change]
// set (change.go: Insert, Update, Delete, Truncate, TxBegin, TxCommit,
// SchemaSnapshot — new variants must be added in package ir) against the
// filter decision: the four table-scoped events filter on their bare
// table name; transaction boundaries and schema snapshots always pass
// (the snapshot exemption is deliberate — it writes stream state, not
// the user table; see filterDropsChange).
func TestFilterDropsChange_EveryChangeVariant(t *testing.T) {
	exclude := migcore.TableFilter{Exclude: []string{"t"}}
	include := migcore.TableFilter{Include: []string{"other"}}
	cases := []struct {
		name       string
		change     ir.Change
		filterable bool
	}{
		{"Insert", ir.Insert{Table: "t"}, true},
		{"Update", ir.Update{Table: "t"}, true},
		{"Delete", ir.Delete{Table: "t"}, true},
		{"Truncate", ir.Truncate{Table: "t"}, true},
		{"TxBegin", ir.TxBegin{}, false},
		{"TxCommit", ir.TxCommit{}, false},
		{"SchemaSnapshot", ir.SchemaSnapshot{Table: "t"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if drop, _ := filterDropsChange(migcore.TableFilter{}, tc.change); drop {
				t.Error("empty filter must drop nothing")
			}
			gotEx, _ := filterDropsChange(exclude, tc.change)
			if gotEx != tc.filterable {
				t.Errorf("exclude[t]: drop = %v; want %v", gotEx, tc.filterable)
			}
			gotIn, _ := filterDropsChange(include, tc.change)
			if gotIn != tc.filterable {
				t.Errorf("include[other]: drop = %v; want %v", gotIn, tc.filterable)
			}
		})
	}
}
