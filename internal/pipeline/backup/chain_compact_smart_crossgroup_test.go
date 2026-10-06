// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Two more pins from the sixth review of the F-E1-SEVERED-TAIL-REPLAY
// compaction work.
//
// (1) Collapse can also RAISE the second incremental's first-row position:
// a re-delivered INSERT of a keyed row followed by a later DELETE of it
// collapses to nothing, so the second incremental's first row is whatever
// comes after — and shape (B) stops firing. The harm is a LOST DELETE: the
// uncompacted chain re-applies the INSERT (an idempotent upsert) and then
// the DELETE, and is correct; the compacted chain keeps the first
// incremental's INSERT and drops the pair, leaving a PHANTOM row the source
// deleted. The pair here crosses a merge-group boundary — the first
// incremental is in a size-1 group the run does not rewrite, the second in a
// merged group — which is exactly the arm of `involves` nothing tested.
//
// (2) Smart compaction resolves the source engine's position order itself,
// and says so whenever it cannot judge shape (B).

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

type segmentRowsFn func(seg, j int, cur uint64) (out []ir.Change, end uint64)

// seedSegmentsAt writes one Postgres-sourced segment per offset (a full and
// two incrementals each), so the merge window decides which segments group.
func seedSegmentsAt(t *testing.T, store irbackup.Store, base time.Time, offsets []time.Duration, rows segmentRowsFn) {
	t.Helper()
	ctx := context.Background()
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", CreatedAt: base, UpdatedAt: base}
	cur := uint64(100)
	for i := range offsets {
		created := base.Add(offsets[i])
		dir := ""
		if i > 0 {
			dir = fmt.Sprintf("seg-%d", i)
		}
		ss := lineage.NewPrefixedStore(store, dir)
		start := cur
		full := &irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres", CreatedAt: created,
			Kind: irbackup.BackupKindFull, EndPosition: pos(start), PartialState: irbackup.BackupStateComplete, Schema: severedCompactSchema(),
		}
		full.BackupID = irbackup.ComputeBackupID(full)
		if err := lineage.WriteManifestAt(ctx, ss, lineage.ManifestFileName, full); err != nil {
			t.Fatal(err)
		}
		var paths []string
		for j := 1; j <= 2; j++ {
			ev, next := rows(i, j, cur)
			cp := fmt.Sprintf("chunks/_changes/seg%d-incr%d.jsonl.gz", i, j)
			var buf bytes.Buffer
			cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range ev {
				if err := cw.WriteChange(e); err != nil {
					t.Fatal(err)
				}
			}
			if err := cw.Close(); err != nil {
				t.Fatal(err)
			}
			if err := ss.Put(ctx, cp, bytes.NewReader(buf.Bytes())); err != nil {
				t.Fatal(err)
			}
			im := &irbackup.Manifest{
				FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
				CreatedAt: created.Add(time.Duration(j) * 10 * time.Minute), Kind: irbackup.BackupKindIncremental,
				StartPosition: pos(cur), EndPosition: pos(next), PartialState: irbackup.BackupStateComplete, Schema: severedCompactSchema(),
				ChangeChunks: []*irbackup.ChunkInfo{{File: cp, RowCount: cw.ChangeCount(), SHA256: cw.Hash()}},
			}
			im.BackupID = irbackup.ComputeBackupID(im)
			ip := fmt.Sprintf("manifests/incr-%05d-seg%d-%d.json", j, i, j)
			if err := lineage.WriteManifestAt(ctx, ss, ip, im); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, ip)
			cur = next
		}
		seg := lineage.Segment{
			SegmentID: full.BackupID, Dir: dir, FullManifestPath: lineage.ManifestFileName, Incrementals: paths,
			StartPosition: pos(start), EndPosition: pos(cur), Codec: blobcodec.CodecGzip,
		}
		if i < len(offsets)-1 {
			capped := created.Add(30 * time.Minute)
			seg.CappedAt, seg.CapReason = &capped, rotationReasonAge
		}
		cat.Segments = append(cat.Segments, seg)
	}
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		t.Fatal(err)
	}
}

// insertThenDeleteRows: segment 0's second incremental commits T (INSERT
// users 7) at LSN t; segment 1's first incremental re-delivers T (a
// pre-v0.138.0 resume), then DELETEs users 7 at t+1 and inserts users 8 at t+2.
func insertThenDeleteRows(seg, j int, cur uint64) (out []ir.Change, end uint64) {
	switch {
	case seg == 0 && j == 2:
		t := cur + 1
		return []ir.Change{
			ir.TxBegin{Position: pos(t)},
			ir.Insert{Position: pos(t), Schema: "public", Table: "users", Row: ir.Row{"id": int64(7), "name": "a"}},
			ir.TxCommit{Position: pos(t)},
		}, t
	case seg == 1 && j == 1:
		t := cur
		return []ir.Change{
			ir.TxBegin{Position: pos(t)},
			ir.Insert{Position: pos(t), Schema: "public", Table: "users", Row: ir.Row{"id": int64(7), "name": "a"}},
			ir.TxCommit{Position: pos(t)},
			ir.TxBegin{Position: pos(t + 1)},
			ir.Delete{Position: pos(t + 1), Schema: "public", Table: "users", Before: ir.Row{"id": int64(7)}},
			ir.TxCommit{Position: pos(t + 1)},
			ir.TxBegin{Position: pos(t + 2)},
			ir.Insert{Position: pos(t + 2), Schema: "public", Table: "users", Row: ir.Row{"id": int64(8), "name": "z"}},
			ir.TxCommit{Position: pos(t + 2)},
		}, t + 2
	}
	t := cur + 1
	return []ir.Change{
		ir.TxBegin{Position: pos(t)},
		ir.Insert{Position: pos(t), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1000 + seg*10 + j), "name": "q"}},
		ir.TxCommit{Position: pos(t)},
	}, t
}

// TestSmartCompaction_CrossGroupPairShapeB: segment 0 sits alone in its merge
// group (10 h before the others), segments 1 and 2 merge. The shape-(B) pair
// is (segment 0's last incremental, segment 1's first) — one link the run
// does not rewrite, one it does. Smart compaction must refuse before copying,
// and the chain must be left refused, not compacted into a phantom row.
func TestSmartCompaction_CrossGroupPairShapeB(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	base := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSegmentsAt(t, store, base, []time.Duration{0, 10 * time.Hour, 11 * time.Hour}, insertThenDeleteRows)
	refusedAsB := func(when string) {
		t.Helper()
		chain, err := lineage.BuildLineageChain(ctx, store, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = NewSeveredTransactionDoor(store, smartLSNComparator{}, nil).Check(ctx, chain)
		if codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction || !strings.Contains(err.Error(), "at or before the last rows") {
			t.Fatalf("%s: the door does not refuse the cross-group pair as shape (B): %v", when, err)
		}
	}
	refusedAsB("before compaction (control)")
	files := len(store.data)

	_, err := CompactChain(ctx, store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now:        func() time.Time { return base.Add(20 * time.Hour) },
		Comparator: smartLSNComparator{},
	})
	if codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction || !strings.Contains(err.Error(), "nothing was copied") {
		t.Fatalf("smart compaction did not refuse, before copying, a shape-(B) pair that crosses into a merged group: %v", err)
	}
	refusedAsB("after the refused compaction")
	if len(store.data) != files {
		t.Fatalf("the refused run changed the store: %d files before, %d after", files, len(store.data))
	}
}

// redeliveredInsideSegmentZero: segment 0's second incremental re-delivers
// the transaction its first ended on (a (B) pair entirely inside segment 0);
// every other incremental is a healthy, increasing transaction.
func redeliveredInsideSegmentZero(seg, j int, cur uint64) (out []ir.Change, end uint64) {
	if seg == 0 && j == 2 {
		return []ir.Change{
			ir.TxBegin{Position: pos(cur)},
			ir.Insert{Position: pos(cur), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup"}},
			ir.TxCommit{Position: pos(cur)},
			ir.TxBegin{Position: pos(cur + 1)},
			ir.Insert{Position: pos(cur + 1), Schema: "public", Table: "users", Row: ir.Row{"id": int64(20), "name": "n"}},
			ir.TxCommit{Position: pos(cur + 1)},
		}, cur + 1
	}
	t := cur + 1
	out = []ir.Change{
		ir.TxBegin{Position: pos(t)},
		ir.Insert{Position: pos(t), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1000 + seg*10 + j), "name": "q"}},
	}
	if seg == 0 && j == 1 {
		out = append(out, ir.Insert{Position: pos(t), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup"}})
	}
	return append(out, ir.TxCommit{Position: pos(t)}), t
}

// TestSmartCompaction_UnrelatedFindingDoesNotBlockAMergedGroup is the other
// direction of the cross-group pin: a (B) pair that lies entirely in a
// segment the run does NOT rewrite is not smart compaction's to refuse — the
// rewrite cannot move that evidence — so the merged group compacts, and the
// untouched pair is still refused by restore afterwards.
func TestSmartCompaction_UnrelatedFindingDoesNotBlockAMergedGroup(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	base := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSegmentsAt(t, store, base, []time.Duration{0, 10 * time.Hour, 11 * time.Hour}, redeliveredInsideSegmentZero)
	refusedAsB := func(when string) {
		t.Helper()
		chain, err := lineage.BuildLineageChain(ctx, store, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewSeveredTransactionDoor(store, smartLSNComparator{}, nil).Check(ctx, chain); codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
			t.Fatalf("%s: the door does not refuse segment 0's (B) pair: %v", when, err)
		}
	}
	refusedAsB("before compaction (control)")
	res, err := CompactChain(ctx, store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now:        func() time.Time { return base.Add(20 * time.Hour) },
		Comparator: smartLSNComparator{},
	})
	if err != nil {
		t.Fatalf("a finding in a segment the run does not rewrite blocked the merged group: %v", err)
	}
	if res.GroupsMerged != 1 {
		t.Fatalf("anti-vacuity: GroupsMerged = %d; want 1", res.GroupsMerged)
	}
	refusedAsB("after compaction")
}

// TestSmartCompaction_ResolvesThePositionOrder: CompactChain resolves the
// order from the catalog's source engine through the injected resolver, says
// at INFO and in the result whenever (B) is not judged, and refuses an engine
// the resolver does not know.
func TestSmartCompaction_ResolvesThePositionOrder(t *testing.T) {
	ordered := func(string) (ir.PositionMonotonicChecker, bool) { return smartLSNComparator{}, true }
	unordered := func(string) (ir.PositionMonotonicChecker, bool) { return nil, true }
	unknown := func(string) (ir.PositionMonotonicChecker, bool) { return nil, false }
	for _, tc := range []struct {
		name       string
		resolver   PositionOrderResolver
		wantJudged bool
		wantRefuse bool
	}{
		{"resolver gives the order", ordered, true, false},
		{"engine has no order", unordered, false, false},
		{"no resolver supplied", nil, false, false},
		{"engine unknown to the resolver", unknown, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, framedRows)
			files := len(store.data)
			res, err := CompactChain(context.Background(), store, CompactOpts{
				MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
				Now:           func() time.Time { return now.Add(10 * time.Hour) },
				PositionOrder: tc.resolver,
			})
			if tc.wantRefuse {
				if err == nil || !strings.Contains(err.Error(), "SMART-COMPACTION-SOURCE-ENGINE-UNKNOWN") {
					t.Fatalf("want the unknown-engine refusal, got %v", err)
				}
				if len(store.data) != files {
					t.Fatalf("the refusal changed the store: %d files before, %d after", files, len(store.data))
				}
				return
			}
			if err != nil {
				t.Fatalf("compact: %v", err)
			}
			if res.ShapeBJudged != tc.wantJudged {
				t.Fatalf("ShapeBJudged = %v; want %v", res.ShapeBJudged, tc.wantJudged)
			}
		})
	}
}
