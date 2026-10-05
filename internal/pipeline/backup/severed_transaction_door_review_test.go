// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Pins from the second review of F-E1-SEVERED-TAIL-REPLAY's read side:
//
//   - `backup verify` passed a shape-(C) chain at both depths, because it
//     propagated only the severed-transaction code and shape C is coded
//     SLUICE-E-BACKUP-INCOMPLETE (now: every verdict propagates; only a
//     decode failure, typed chunkDecodeError, is verify's WARN);
//   - an APPLIED link whose chunks the door could not read hid shapes A and B
//     on the next, UNAPPLIED link — the WARN-and-skip covered the evidence
//     for a finding the broker was about to apply;
//   - smart compaction of a marker-less (trigger-CDC) chain wrote an
//     incremental whose collapsed tail ended below its EndPosition, which the
//     door, restore and the broker all refuse.

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVerifyBackup_RefusesShapeC_AtHashAndReadDepth drives the public verify
// entry point over a chain whose incremental claims an EndPosition one past
// its last stored change — the evidence an old cancel drain leaves. The
// independent expected value is restore's verdict on the same chain:
// TestSeveredTransactionDoor_ShapeC_AgreementTable shows restore refuses it,
// so verify must too, at every depth.
func TestVerifyBackup_RefusesShapeC_AtHashAndReadDepth(t *testing.T) {
	ctx := context.Background()
	store, _ := seedReadDepthChain(t, false, shapeHealthy, shapeHealthy)
	if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{}); err != nil {
		t.Fatalf("control: healthy chain refused: %v", err)
	}
	m, err := lineage.ReadManifestAt(ctx, store, "manifests/incr-00001.json")
	if err != nil {
		t.Fatal(err)
	}
	m.EndPosition = pos(12) // the chunk ends at pos(11)
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifestAt(ctx, store, "manifests/incr-00001.json", m); err != nil {
		t.Fatal(err)
	}
	catalog, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	catalog.Segments[0].EndPosition = pos(12)
	if err := lineage.WriteLineageCatalog(ctx, store, catalog); err != nil {
		t.Fatal(err)
	}
	for _, depth := range []VerifyDepth{VerifyDepthHash, VerifyDepthRead} {
		_, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Depth: depth})
		if err == nil {
			t.Fatalf("depth %q: verify reported a shape-C chain healthy; restore refuses it", depth)
		}
		if codeOf(err) != sluicecode.CodeBackupIncomplete || !strings.Contains(err.Error(), "shape C") {
			t.Fatalf("depth %q: verify refused, but not with the door's shape-C verdict: %v", depth, err)
		}
	}
}

// TestSeveredTransactionDoor_CheckFrom_UnreadableAppliedLinkDoesNotHideTheNextLink:
// inc1 is applied and its only chunk is unreadable; inc2 carries a shape-A
// (inc1 ends open) or shape-B (inc2 re-delivers inc1's last transaction)
// finding against it. The finding lands on inc2, so:
//
//   - inc2 NOT applied (from=2): refuse — the evidence for a finding the
//     broker is about to apply is unreadable, and applying it may duplicate a
//     transaction;
//   - inc2 applied too (from=3): WARN and pass — nothing left to prevent;
//   - restore (from=0): refuse.
func TestSeveredTransactionDoor_CheckFrom_UnreadableAppliedLinkDoesNotHideTheNextLink(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []string{"A", "B"} {
		t.Run("shape "+shape, func(t *testing.T) {
			store := sevStore(t)
			var inc1, inc2 lineage.SegmentRecord
			if shape == "A" {
				inc1 = sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")[:2]))
				inc2 = sevWriteIncremental(t, store, "inc2", 100, sevTx(200, "a"))
			} else {
				inc1 = sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")))
				inc2 = sevWriteIncremental(t, store, "inc2", 100, cat(sevTx(200, "a"), sevTx(300, "y")))
			}
			chain := []lineage.SegmentRecord{sevFull(), inc1, inc2}
			// Control: readable, inc1 applied — the finding on inc2 refuses.
			requireSevered(t, NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).CheckFrom(ctx, chain, 2), "")

			if err := store.Put(ctx, inc1.Manifest.ChangeChunks[0].File, bytes.NewReader([]byte("garbage"))); err != nil {
				t.Fatal(err)
			}
			if err := NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).CheckFrom(ctx, chain, 2); err == nil {
				t.Fatal("an unreadable APPLIED link hid the finding on the next, UNAPPLIED link: the broker would apply it")
			} else if !isChunkDecodeError(err) {
				t.Fatalf("refused, but not on the unreadable evidence: %v", err)
			}
			if err := NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).CheckFrom(ctx, chain, 3); err != nil {
				t.Fatalf("both links applied: the unreadable link must WARN, not halt the broker: %v", err)
			}
			if err := NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).CheckFrom(ctx, chain, 0); err == nil {
				t.Fatal("restore form passed a chain with an unreadable incremental")
			}
		})
	}
	// The chain's LAST link unreadable and applied: its only finding (C) lands
	// on itself, so it WARNs.
	store := sevStore(t)
	inc1 := sevWriteIncremental(t, store, "inc1", 100, sevTx(100, "x"))
	if err := store.Put(ctx, inc1.Manifest.ChangeChunks[0].File, bytes.NewReader([]byte("garbage"))); err != nil {
		t.Fatal(err)
	}
	if err := NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, []lineage.SegmentRecord{sevFull(), inc1}, 2); err != nil {
		t.Fatalf("an applied, unreadable LAST link halted the broker: %v", err)
	}
}

// markerlessRows is a trigger-CDC-shaped window: three INSERTs then an UPDATE
// of each, no transaction markers, every change its own position. EndPosition
// is the last change's position — the writer's rule.
func markerlessRows(start uint64, _ int) (out []ir.Change, end uint64) {
	lsn := start + 1
	for i := int64(1); i <= 3; i++ {
		out = append(out, ir.Insert{Position: pos(lsn), Schema: "public", Table: "users", Row: ir.Row{"id": i, "name": "a"}})
		lsn++
	}
	for i := int64(1); i <= 3; i++ {
		out = append(out, ir.Update{Position: pos(lsn), Schema: "public", Table: "users", Before: ir.Row{"id": i}, After: ir.Row{"id": i, "name": "b"}})
		lsn++
	}
	return out, lsn - 1
}

// framedRows is the same work in two source transactions, each change carrying
// its transaction's position (the Postgres shape).
func framedRows(start uint64, _ int) (out []ir.Change, end uint64) {
	t1, t2 := start+1, start+2
	out = []ir.Change{ir.TxBegin{Position: pos(t1)}}
	for i := int64(1); i <= 3; i++ {
		out = append(out, ir.Insert{Position: pos(t1), Schema: "public", Table: "users", Row: ir.Row{"id": i, "name": "a"}})
	}
	out = append(out, ir.TxCommit{Position: pos(t1)}, ir.TxBegin{Position: pos(t2)})
	for i := int64(1); i <= 3; i++ {
		out = append(out, ir.Update{Position: pos(t2), Schema: "public", Table: "users", Before: ir.Row{"id": i}, After: ir.Row{"id": i, "name": "b"}})
	}
	return append(out, ir.TxCommit{Position: pos(t2)}), t2
}

// TestSmartCompaction_DoesNotHealAShortInput is the other direction: the
// boundary pair re-asserts the position the original stream RECORDED, so an
// incremental that was already short of its EndPosition (an old cancel
// drain's drop) must still be refused after compaction. A rewrite that
// stamped the manifest's EndPosition instead would launder the loss.
func TestSmartCompaction_DoesNotHealAShortInput(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, func(start uint64, j int) ([]ir.Change, uint64) {
		out, end := markerlessRows(start, j)
		return out, end + 5 // EndPosition past the last recorded change
	})
	pre, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireIncomplete(t, NewSeveredTransactionDoor(store, nil, nil).Check(ctx, pre))
	if _, err := CompactChain(ctx, store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-smart" },
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	post, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireIncomplete(t, NewSeveredTransactionDoor(store, nil, nil).Check(ctx, post))
}

// TestSmartCompaction_RewrittenIncrementalReachesItsEndPosition runs a real
// `backup compact --smart-compaction` over a two-segment lineage and hands
// its output to every reader of the tail-reach rule. The independent expected
// value is the INPUT chain: it passes the same readers before compaction (the
// control), and compaction must not change their verdict. Both stream
// families — marker-less (trigger-CDC) and framed — because the closing
// position is kept by a different mechanism in each (the appended empty
// boundary pair vs the held closing commit).
func TestSmartCompaction_RewrittenIncrementalReachesItsEndPosition(t *testing.T) {
	for _, fam := range []struct {
		name string
		rows smartCompactRowsFn
	}{
		{"marker-less (trigger-CDC)", markerlessRows},
		{"framed (Postgres-shaped)", framedRows},
	} {
		t.Run(fam.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, fam.rows)

			pre, err := lineage.BuildLineageChain(ctx, store, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, pre); err != nil {
				t.Fatalf("control: the INPUT chain is refused: %v", err)
			}

			res, err := CompactChain(ctx, store, CompactOpts{
				MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
				Now:          func() time.Time { return now.Add(10 * time.Hour) },
				newSegmentID: func() string { return "merged-smart" },
			})
			if err != nil {
				t.Fatalf("compact: %v", err)
			}
			if res.EventsAfter >= res.EventsBefore {
				t.Fatalf("anti-vacuity: nothing collapsed (%d -> %d)", res.EventsBefore, res.EventsAfter)
			}

			post, err := lineage.BuildLineageChain(ctx, store, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, post); err != nil {
				t.Fatalf("door refused the compacted chain: %v", err)
			}
			if err := verifySeveredTransactions(ctx, store, post, true, false, false, &chunkAuthProber{}); err != nil {
				t.Fatalf("`backup verify`'s door refused the compacted chain: %v", err)
			}
			incrementals := 0
			for i := range post {
				if !isIncrementalLink(&post[i]) {
					continue
				}
				incrementals++
				link := post[i]
				eng := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
				applier, err := eng.OpenChangeApplier(ctx, "tgt")
				if err != nil {
					t.Fatal(err)
				}
				link.Manifest.SchemaDelta = nil
				if err := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store}).applyIncremental(ctx, &link, applier, DefaultChainRestoreBatchSize); err != nil {
					t.Fatalf("restore refused compacted incremental %d: %v", i, err)
				}
			}
			if incrementals == 0 {
				t.Fatal("anti-vacuity: the compacted chain has no incremental")
			}
		})
	}
}
