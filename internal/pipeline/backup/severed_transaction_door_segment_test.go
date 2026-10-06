// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Two door defects from the third review of F-E1-SEVERED-TAIL-REPLAY:
//
//   - pairing stopped at a segment full. Both consumers apply the NEXT
//     INCREMENTAL after an incremental — the broker skips a full outright,
//     chain restore applies the full's snapshot and then that incremental,
//     which resumes from the previous segment's EndPosition
//     (`rotationBoundaryResumeStart`). So shapes (A) and (B), and the
//     unreadable-link landing rule, pair across a full;
//   - `backup verify` stopped at the first undecodable link, leaving every
//     later link unjudged.

import (
	"bytes"
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func sevSegmentFull(id string) lineage.SegmentRecord {
	f := sevFull()
	f.Manifest.BackupID = id
	return f
}

// TestSeveredTransactionDoor_PairsAcrossASegmentFull: [full, inc1, full2,
// inc2], where inc2 is the incremental both consumers apply after inc1.
func TestSeveredTransactionDoor_PairsAcrossASegmentFull(t *testing.T) {
	ctx := context.Background()

	t.Run("shape A lands on the next segment's incremental", func(t *testing.T) {
		store := sevStore(t)
		inc1 := sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")[:2]))
		inc2 := sevWriteIncremental(t, store, "inc2", 100, sevTx(200, "a"))
		chain := []lineage.SegmentRecord{sevFull(), inc1, sevSegmentFull("full2"), inc2}
		requireSevered(t, NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain), "ends inside an open source transaction")
		requireSevered(t, NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, chain, 2), "ends inside an open source transaction")
		requireSevered(t, NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, chain, 3), "ends inside an open source transaction")
		if err := NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, chain, 4); err != nil {
			t.Fatalf("inc2 applied too: the finding is history and must WARN: %v", err)
		}
	})

	t.Run("shape B is judged across a rotation", func(t *testing.T) {
		store := sevStore(t)
		inc1 := sevWriteIncremental(t, store, "inc1", 100, sevTx(200, "a"))
		inc2 := sevWriteIncremental(t, store, "inc2", 100, cat(sevTx(200, "a"), sevTx(300, "b")))
		chain := []lineage.SegmentRecord{sevFull(), inc1, sevSegmentFull("full2"), inc2}
		requireSevered(t, NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).Check(ctx, chain), "at or before the last rows")
		if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); err != nil {
			t.Fatalf("no comparator: (B) is not judged, and nothing else is wrong: %v", err)
		}
	})

	t.Run("unreadable applied link with the next segment's incremental unapplied", func(t *testing.T) {
		store := sevStore(t)
		inc1 := sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")[:2]))
		inc2 := sevWriteIncremental(t, store, "inc2", 100, sevTx(200, "a"))
		chain := []lineage.SegmentRecord{sevFull(), inc1, sevSegmentFull("full2"), inc2}
		if err := store.Put(ctx, inc1.Manifest.ChangeChunks[0].File, bytes.NewReader([]byte("garbage"))); err != nil {
			t.Fatal(err)
		}
		err := NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, chain, 2)
		if err == nil {
			t.Fatal("an unreadable APPLIED link hid the finding on the next segment's UNAPPLIED incremental")
		}
		if !isChunkDecodeError(err) {
			t.Fatalf("refused, but not on the unreadable evidence: %v", err)
		}
		if err := NewSeveredTransactionDoor(store, nil, nil).CheckFrom(ctx, chain, 4); err != nil {
			t.Fatalf("both incrementals applied: the unreadable link must WARN, not halt the broker: %v", err)
		}
	})

	t.Run("an open tail with no later incremental is the chain's last", func(t *testing.T) {
		store := sevStore(t)
		inc1 := sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")[:2]))
		chain := []lineage.SegmentRecord{sevFull(), inc1, sevSegmentFull("full2")}
		if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); err != nil {
			t.Fatalf("no incremental follows to re-deliver the transaction, so (A) does not refuse: %v", err)
		}
	})
}

// TestVerifySeveredTransactions_JudgesPastAnUndecodableLink: an undecodable
// incremental is skipped (its chunk is the chunk checks' to report), and a
// shape-(C) loss in a LATER incremental is still refused. Restore keeps
// refusing on the undecodable link itself.
func TestVerifySeveredTransactions_JudgesPastAnUndecodableLink(t *testing.T) {
	ctx := context.Background()
	store := sevStore(t)
	bad := sevWithEnd(sevWriteIncremental(t, store, "bad", 100, sevTx(10, "a")), sevLSN(1), sevLSN(11))
	lost := sevWithEnd(sevWriteIncremental(t, store, "lost", 100, sevTx(20, "b")), sevLSN(11), sevLSN(40))
	if err := store.Put(ctx, bad.Manifest.ChangeChunks[0].File, bytes.NewReader([]byte("garbage"))); err != nil {
		t.Fatal(err)
	}
	chain := []lineage.SegmentRecord{sevFull(), bad, lost}

	err := verifySeveredTransactions(ctx, store, chain, true, false, false, &chunkAuthProber{})
	if codeOf(err) != sluicecode.CodeBackupIncomplete {
		t.Fatalf("verify stopped at the undecodable link and did not judge the later shape-(C) loss: %v", err)
	}
	// Control: the undecodable link alone is verify's WARN, not a refusal.
	if err := verifySeveredTransactions(ctx, store, chain[:2], true, false, false, &chunkAuthProber{}); err != nil {
		t.Fatalf("an undecodable link alone must WARN at verify (the chunk checks own it): %v", err)
	}
	// Restore keeps its refusal on the undecodable link.
	if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); !isChunkDecodeError(err) {
		t.Fatalf("restore's form must still refuse on the undecodable link: %v", err)
	}
}
