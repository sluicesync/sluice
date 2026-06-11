// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestCompactChain_SmartCompaction_CollapsesAcrossGroup pins the
// end-to-end CompactChain wiring with --smart-compaction on: a
// 2-segment in-window lineage whose incrementals carry INSERT +
// UPDATE event chains gets compacted with collapse applied to each
// merged group's chunks. The resulting merged segment's incremental
// chunks carry strictly fewer events than the source chain.
func TestCompactChain_SmartCompaction_CollapsesAcrossGroup(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineage(t, store, now)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:     2 * time.Hour,
		SmartCompaction: true,
		PKStrategy:      PKStrategyPK,
		Now:             func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID:    func() string { return "merged-smart" },
	})
	if err != nil {
		t.Fatalf("CompactChain smart: %v", err)
	}
	if res.GroupsMerged != 1 {
		t.Fatalf("GroupsMerged = %d; want 1", res.GroupsMerged)
	}
	if res.EventsBefore == 0 {
		t.Fatalf("EventsBefore = 0; expected the seed lineage to carry events")
	}
	if res.EventsAfter >= res.EventsBefore {
		t.Errorf("EventsAfter = %d; want < EventsBefore = %d (smart compact should reduce)",
			res.EventsAfter, res.EventsBefore)
	}
	if res.EventsCollapsed != res.EventsBefore-res.EventsAfter {
		t.Errorf("EventsCollapsed = %d; want %d (Before - After)",
			res.EventsCollapsed, res.EventsBefore-res.EventsAfter)
	}
	if res.RowsCollapsed == 0 {
		t.Errorf("RowsCollapsed = 0; expected >= 1 (the synthetic lineage has same-row chains)")
	}

	// Re-load the catalog and verify the merged segment exists.
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 1 {
		t.Fatalf("post-compact segments = %d; want 1", len(cat.Segments))
	}
	merged := cat.Segments[0]
	if merged.SegmentID != "merged-smart" {
		t.Errorf("SegmentID = %q; want merged-smart", merged.SegmentID)
	}

	t.Logf("smart compact chain pin: events %d → %d (%d collapsed, %d row chains), bytes %d → %d",
		res.EventsBefore, res.EventsAfter, res.EventsCollapsed, res.RowsCollapsed,
		res.BytesBefore, res.BytesAfter)
}

// TestCompactChain_SmartCompaction_TablesWithoutPK reports the
// no-PK fall-through table in the per-group plan + the top-level
// result.
func TestCompactChain_SmartCompaction_TablesWithoutPK(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageNoPK(t, store, now)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:     2 * time.Hour,
		SmartCompaction: true,
		Now:             func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID:    func() string { return "merged-nopk" },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if len(res.TablesWithoutPK) == 0 {
		t.Fatalf("TablesWithoutPK = empty; want at least one entry")
	}
	if res.TablesWithoutPK[0] != "public.audit_log" {
		t.Errorf("TablesWithoutPK[0] = %q; want public.audit_log", res.TablesWithoutPK[0])
	}
}

// TestCompactChain_SmartCompaction_RefusesEncryptedChain pins the
// ADR-0064 v1 limitation: encrypted chains refuse with an
// actionable recovery hint.
func TestCompactChain_SmartCompaction_RefusesEncryptedChain(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageEncrypted(t, store, now)

	_, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:     2 * time.Hour,
		SmartCompaction: true,
		Now:             func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID:    func() string { return "merged-enc" },
	})
	if err == nil {
		t.Fatal("CompactChain on encrypted chain with smart: nil error; want refusal")
	}
	if !strings.Contains(err.Error(), "--smart-compaction is not yet supported on encrypted chains") {
		t.Errorf("err = %q; want the documented encrypted-chain refusal", err.Error())
	}
	if !strings.Contains(err.Error(), "--smart-compaction-off") {
		t.Errorf("err = %q; want the --smart-compaction-off recovery hint", err.Error())
	}
}

// TestCompactChain_SmartCompactionOff_NaiveBehaviorUnchanged
// confirms the existing naive-compact behaviour is byte-identical
// when SmartCompaction is left off (the v1 default). The fact that
// the existing chain_compact_test.go pins still pass after this
// chunk demonstrates the SAME claim; this test is the explicit
// belt-and-suspenders pin against accidental opt-in.
func TestCompactChain_SmartCompactionOff_NaiveBehaviorUnchanged(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
		// SmartCompaction left explicitly false (the default).
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-naive" },
	})
	if err != nil {
		t.Fatalf("naive compact: %v", err)
	}
	if res.EventsBefore != 0 || res.EventsAfter != 0 || res.EventsCollapsed != 0 {
		t.Errorf("naive compact event tallies = %d/%d/%d; want 0/0/0 (naive doesn't decode events)",
			res.EventsBefore, res.EventsAfter, res.EventsCollapsed)
	}
	if res.RowsCollapsed != 0 || len(res.TablesWithoutPK) != 0 {
		t.Errorf("naive compact: RowsCollapsed=%d, TablesWithoutPK=%v; want zero",
			res.RowsCollapsed, res.TablesWithoutPK)
	}
	if res.BytesBefore != res.BytesAfter {
		t.Errorf("naive compact bytes %d → %d; want equal (bytes moved, not rewritten)",
			res.BytesBefore, res.BytesAfter)
	}
}

// ----- Seed helpers -----

// seedSmartCompactLineage builds a 2-segment lineage where each
// segment's two incrementals carry a synthetic INSERT+UPDATE chain
// on the same PK — designed to collapse heavily under smart
// compaction.
func seedSmartCompactLineage(t *testing.T, store irbackup.BackupStore, base time.Time) {
	t.Helper()
	seedSmartCompactLineageWithSchemaAndEnc(t, store, base, usersSchema(), nil, smartCompactRowsHappyPath)
}

// seedSmartCompactLineageNoPK seeds a lineage whose schema has a
// table WITHOUT a declared PK; smart compaction skips it.
func seedSmartCompactLineageNoPK(t *testing.T, store irbackup.BackupStore, base time.Time) {
	t.Helper()
	seedSmartCompactLineageWithSchemaAndEnc(t, store, base, noPKSchema(), nil, smartCompactRowsNoPKTable)
}

// seedSmartCompactLineageEncrypted seeds a lineage whose full
// manifests carry a ChainEncryption marker (so smart compact
// refuses).
func seedSmartCompactLineageEncrypted(t *testing.T, store irbackup.BackupStore, base time.Time) {
	t.Helper()
	enc := &irbackup.ChainEncryption{
		Algorithm: "AES-256-GCM",
		Mode:      "per-chain",
		KEKMode:   "passphrase-argon2id",
		KEKRef:    "",
		Argon2id:  &irbackup.Argon2idParams{Salt: []byte("salt-test")},
	}
	seedSmartCompactLineageWithSchemaAndEnc(t, store, base, usersSchema(), enc, smartCompactRowsHappyPath)
}

// smartCompactRowsFn synthesises change events for one incremental,
// given a starting LSN. Returns the events + the next LSN to use
// for the next incremental.
type smartCompactRowsFn func(startLSN uint64, incrIdx int) (events []ir.Change, nextLSN uint64)

// smartCompactRowsHappyPath synthesises 5 INSERT + 5 UPDATE events
// on rows 1..5, designed to collapse to 5 INSERTs (50% reduction).
func smartCompactRowsHappyPath(startLSN uint64, incrIdx int) (events []ir.Change, nextLSN uint64) {
	lsn := startLSN
	out := make([]ir.Change, 0, 12)
	out = append(out, ir.TxBegin{Position: pos(lsn)})
	lsn++
	for i := int64(1); i <= 5; i++ {
		out = append(out, ir.Insert{
			Position: pos(lsn),
			Schema:   "public",
			Table:    "users",
			Row:      ir.Row{"id": i, "name": fmt.Sprintf("init-%d-%d", incrIdx, i)},
		})
		lsn++
	}
	for i := int64(1); i <= 5; i++ {
		out = append(out, ir.Update{
			Position: pos(lsn),
			Schema:   "public",
			Table:    "users",
			Before:   ir.Row{"id": i},
			After:    ir.Row{"id": i, "name": fmt.Sprintf("updated-%d-%d", incrIdx, i)},
		})
		lsn++
	}
	out = append(out, ir.TxCommit{Position: pos(lsn)})
	lsn++
	return out, lsn
}

// smartCompactRowsNoPKTable synthesises 4 INSERTs on a no-PK
// table; smart compaction skips it (passes through verbatim).
func smartCompactRowsNoPKTable(startLSN uint64, incrIdx int) (events []ir.Change, nextLSN uint64) {
	lsn := startLSN
	out := []ir.Change{
		ir.TxBegin{Position: pos(lsn)},
	}
	lsn++
	for i := 0; i < 4; i++ {
		out = append(out, ir.Insert{
			Position: pos(lsn),
			Schema:   "public",
			Table:    "audit_log",
			Row:      ir.Row{"ts": "2026-05-26", "msg": fmt.Sprintf("entry-%d-%d", incrIdx, i)},
		})
		lsn++
	}
	out = append(out, ir.TxCommit{Position: pos(lsn)})
	lsn++
	return out, lsn
}

// seedSmartCompactLineageWithSchemaAndEnc is the workhorse: builds
// a 2-segment lineage where each segment has 2 incrementals
// carrying real (gzip-encoded) change-chunks with the given event
// shape. Positions stitch contiguously across segments + chunks
// so the §14d position-gap pre-flight stays green.
func seedSmartCompactLineageWithSchemaAndEnc(
	t *testing.T,
	store irbackup.BackupStore,
	base time.Time,
	schema *ir.Schema,
	enc *irbackup.ChainEncryption,
	rowsFn smartCompactRowsFn,
) {
	t.Helper()
	cat := &LineageCatalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		CreatedAt:     base,
		UpdatedAt:     base,
	}
	const segmentCount = 2
	var prevEndLSN uint64 = 100
	curLSN := prevEndLSN
	cumulative := time.Duration(0)
	for i := 0; i < segmentCount; i++ {
		segCreatedAt := base.Add(cumulative)
		dir := ""
		if i > 0 {
			dir = fmt.Sprintf("seg-%d", i)
		}
		segStore := newPrefixedStore(store, dir)

		// Full at startLSN = prior segment's end.
		startLSN := curLSN
		full := &irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion,
			SourceEngine:  "postgres",
			CreatedAt:     segCreatedAt,
			Kind:          irbackup.BackupKindFull,
			EndPosition:   pos(startLSN),
			PartialState:  irbackup.BackupStateComplete,
			Schema:        schema,
		}
		if enc != nil {
			full.ChainEncryption = enc
		}
		full.BackupID = irbackup.ComputeBackupID(full)
		if err := writeManifestAt(context.Background(), segStore, ManifestFileName, full); err != nil {
			t.Fatalf("seed full %d: %v", i, err)
		}

		// Two incrementals; each one's events span contiguous LSNs.
		incrPaths := make([]string, 0, 2)
		for j := 1; j <= 2; j++ {
			incrCreatedAt := segCreatedAt.Add(time.Duration(j) * 10 * time.Minute)
			events, nextLSN := rowsFn(curLSN, j)

			// Encode events into a real change-chunk so smart-
			// compact has bytes to decode.
			chunkPath := fmt.Sprintf("chunks/_changes/seg%d-incr%d.jsonl.gz", i, j)
			buf := &bytes.Buffer{}
			cw, err := newChangeChunkWriter(buf, nil, CodecGzip)
			if err != nil {
				t.Fatalf("chunk writer: %v", err)
			}
			for _, e := range events {
				if err := cw.WriteChange(e); err != nil {
					t.Fatalf("write change: %v", err)
				}
			}
			if err := cw.Close(); err != nil {
				t.Fatalf("close chunk: %v", err)
			}
			if err := segStore.Put(context.Background(), chunkPath, bytes.NewReader(buf.Bytes())); err != nil {
				t.Fatalf("put chunk: %v", err)
			}

			im := &irbackup.Manifest{
				FormatVersion: irbackup.BackupFormatVersion,
				SourceEngine:  "postgres",
				CreatedAt:     incrCreatedAt,
				Kind:          irbackup.BackupKindIncremental,
				StartPosition: pos(curLSN),
				EndPosition:   pos(nextLSN),
				PartialState:  irbackup.BackupStateComplete,
				Schema:        schema,
				ChangeChunks: []*irbackup.ChunkInfo{
					{File: chunkPath, RowCount: cw.ChangeCount(), SHA256: cw.Hash()},
				},
			}
			im.BackupID = irbackup.ComputeBackupID(im)
			ip := fmt.Sprintf("manifests/incr-%05d-seg%d-%d.json", j, i, j)
			if err := writeManifestAt(context.Background(), segStore, ip, im); err != nil {
				t.Fatalf("seed incremental: %v", err)
			}
			incrPaths = append(incrPaths, ip)
			curLSN = nextLSN
		}

		segEntry := LineageSegment{
			SegmentID:        full.BackupID,
			Dir:              dir,
			FullManifestPath: ManifestFileName,
			Incrementals:     incrPaths,
			StartPosition:    pos(startLSN),
			EndPosition:      pos(curLSN),
			Codec:            CodecGzip,
		}
		if i < segmentCount-1 {
			cappedAt := segCreatedAt.Add(30 * time.Minute)
			segEntry.CappedAt = &cappedAt
			segEntry.CapReason = rotationReasonAge
		}
		cat.Segments = append(cat.Segments, segEntry)

		if i < segmentCount-1 {
			cumulative += time.Hour // gap < merge window
		}
	}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
}
