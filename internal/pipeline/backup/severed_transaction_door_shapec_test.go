// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Shape (C) pins — an incremental whose EndPosition is not the position of
// the last change its chunks record — plus the bare-TxBegin exemption to
// shape (A) and the broker's applied-prefix handling (CheckFrom).
//
// The fixtures are hand-built in the shape OLD writers produced (the
// reviewer's reproduction of the v0.19.1–v0.156.11 cancel-drain drop on the
// pre-fix tree: stored chunks ending at a TxCommit, a whole committed
// transaction in the dropped in-flight chunk, EndPosition advanced to the
// next transaction's rows).

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func sevWithEnd(l lineage.SegmentRecord, start, end ir.Position) lineage.SegmentRecord {
	l.Manifest.StartPosition, l.Manifest.EndPosition = start, end
	return l
}

// TestSeveredTransactionDoor_ShapeC_OldDrainDrop is the reviewer's case: inc1
// stored tx@10 and committed EndPosition at tx@30's rows; tx@20 sat in the
// dropped chunk. inc2 resumes at tx@30 and re-delivers it whole. Neither A
// (inc1 ends on a TxCommit) nor B (inc2 starts after inc1's rows) sees it;
// C does, with and without a comparator, and on the chain's FINAL link too.
func TestSeveredTransactionDoor_ShapeC_OldDrainDrop(t *testing.T) {
	store := sevStore(t)
	inc1 := sevWithEnd(sevWriteIncremental(t, store, "inc1", 100, sevTx(10, "a")), sevLSN(1), sevLSN(30))
	inc2 := sevWithEnd(sevWriteIncremental(t, store, "inc2", 100, cat(sevTx(30, "c1", "c2"), sevTx(40, "d"))), sevLSN(30), sevLSN(41))
	for _, cmp := range []ir.PositionMonotonicChecker{nil, sevLSNComparator{}} {
		err := NewSeveredTransactionDoor(store, cmp, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), inc1, inc2})
		requireIncomplete(t, err)
	}
	// The final link carries the same defect: refused too (restore's own
	// tail backstop refuses it as well — see the agreement test below).
	err := NewSeveredTransactionDoor(store, nil, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), inc1})
	requireIncomplete(t, err)
}

// shapeCCase is one link for the agreement table.
type shapeCCase struct {
	name       string
	changes    []ir.Change
	start, end ir.Position
	fill       bool
	refuse     bool
}

func shapeCCases() []shapeCCase {
	return []shapeCCase{
		// Healthy: what every current writer stamps.
		{name: "ends on TxCommit, EndPosition = its position", changes: cat(sevTx(10, "a"), sevTx(20, "b")), start: sevLSN(1), end: sevLSN(21)},
		{name: "keepalive boundary last (row-less TxBegin/TxCommit at the walsender position)", changes: cat(sevTx(10, "a"), []ir.Change{ir.TxBegin{Position: sevLSN(50)}, ir.TxCommit{Position: sevLSN(55)}}), start: sevLSN(1), end: sevLSN(55)},
		{name: "marker-less trigger-CDC stream ends on its last row's id", changes: []ir.Change{ir.Insert{Position: sevLSN(7), Table: "t", Row: ir.Row{"v": 1}}, ir.Insert{Position: sevLSN(8), Table: "t", Row: ir.Row{"v": 2}}}, start: sevLSN(1), end: sevLSN(8)},
		{name: "ADD COLUMN fill appended at EndPosition", changes: cat(sevTx(10, "a"), sevFillTx(11)), start: sevLSN(1), end: sevLSN(11), fill: true},
		{name: "empty EndPosition (one-shot window that recorded nothing)", changes: []ir.Change{ir.TxBegin{}, ir.TxCommit{}}, start: sevLSN(1)},
		{name: "EndPosition = StartPosition (empty rollover written anyway / replay-only)", changes: sevTx(10, "a"), start: sevLSN(5), end: sevLSN(5)},
		{name: "trailing position-less TxCommit: the last POSITIONED change counts", changes: cat(sevTx(10, "a")[:2], []ir.Change{ir.TxCommit{}}), start: sevLSN(1), end: sevLSN(10)},
		// Refused.
		{name: "old cancel-drain drop: EndPosition past every stored change", changes: sevTx(10, "a"), start: sevLSN(1), end: sevLSN(30), refuse: true},
		{name: "pre-v0.117.0 schema snapshot moved EndPosition (no chunk record there)", changes: sevTx(10, "a"), start: sevLSN(1), end: sevLSN(15), refuse: true},
		{name: "fill-bearing link whose EndPosition is past the fill", changes: cat(sevTx(10, "a"), sevFillTx(11)), start: sevLSN(1), end: sevLSN(40), fill: true, refuse: true},
		{name: "advanced EndPosition with no positioned change at all", changes: []ir.Change{ir.TxBegin{}, ir.TxCommit{}}, start: sevLSN(1), end: sevLSN(9), refuse: true},
	}
}

// TestSeveredTransactionDoor_ShapeC_AgreesWithRestoreTailBackstop is the
// no-false-positive argument made executable. Shape C compares EndPosition
// with exactly what chain restore's own tail backstop compares it with
// (`reachedEnd`, with the same exemptions), so the door must refuse a link
// IF AND ONLY IF applying that link would refuse — it only moves the
// refusal before anything is applied and names the cause. Each case runs
// through both: the door, and ChainRestore.applyIncremental against a
// recording applier.
func TestSeveredTransactionDoor_ShapeC_AgreesWithRestoreTailBackstop(t *testing.T) {
	for _, tc := range shapeCCases() {
		t.Run(tc.name, func(t *testing.T) {
			store := sevStore(t)
			link := sevWithEnd(sevWriteIncremental(t, store, "x", 2, tc.changes), tc.start, tc.end)
			if tc.fill {
				link = sevWithFill(link)
			}
			doorErr := NewSeveredTransactionDoor(store, sevLSNComparator{}, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), link})

			eng := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
			applier, err := eng.OpenChangeApplier(context.Background(), "tgt")
			if err != nil {
				t.Fatal(err)
			}
			link.Manifest.SchemaDelta = nil // the delta apply is not under test
			restoreErr := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store}).applyIncremental(context.Background(), &link, applier, DefaultChainRestoreBatchSize)
			restoreRefused := restoreErr != nil && codeOf(restoreErr) == sluicecode.CodeBackupIncomplete

			if tc.refuse {
				requireIncomplete(t, doorErr)
			} else if doorErr != nil {
				t.Fatalf("door refused a healthy link: %v", doorErr)
			}
			if restoreRefused != tc.refuse {
				t.Fatalf("door and restore's tail backstop DISAGREE: door refuse=%v, restore refuse=%v (%v)", tc.refuse, restoreRefused, restoreErr)
			}
		})
	}
}

func codeOf(err error) sluicecode.Code {
	if ce, ok := sluicecode.FromError(err); ok {
		return ce.Code
	}
	return ""
}

// TestSeveredTransactionDoor_BareTxBeginIsNotSevered: a link ending in a
// TxBegin with NO row of that transaction after it severs nothing (the next
// link re-delivers the transaction whole, this one carries none of it) and
// passes; the same tail with one row after the TxBegin is shape A. Both on
// the tail walk and across a chunk boundary.
func TestSeveredTransactionDoor_BareTxBeginIsNotSevered(t *testing.T) {
	store := sevStore(t)
	next := sevWriteIncremental(t, store, "next", 100, sevTx(200, "a", "b"))
	for _, chunkSize := range []int{100, 2} {
		bare := sevWriteIncremental(t, store, "bare"+string(rune('0'+chunkSize%10)), chunkSize, cat(sevTx(100, "x"), []ir.Change{ir.TxBegin{Position: sevLSN(200)}}))
		if err := NewSeveredTransactionDoor(store, nil, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), bare, next}); err != nil {
			t.Fatalf("chunk=%d: a link ending in a bare TxBegin was refused: %v", chunkSize, err)
		}
		oneRow := sevWriteIncremental(t, store, "row"+string(rune('0'+chunkSize%10)), chunkSize, cat(sevTx(100, "x"), sevTx(200, "a")[:2]))
		requireSevered(t, NewSeveredTransactionDoor(store, nil, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), oneRow, next}),
			"ends inside an open source transaction")
	}
	// The fill path: a bare TxBegin followed by the fill's own transaction.
	fillBare := sevWithFill(sevWriteIncremental(t, store, "fillbare", 2, cat(sevTx(100, "x"), []ir.Change{ir.TxBegin{Position: sevLSN(200)}}, sevFillTx(200))))
	if err := NewSeveredTransactionDoor(store, nil, nil).Check(context.Background(), []lineage.SegmentRecord{sevFull(), fillBare, next}); err != nil {
		t.Fatalf("fill path: a bare TxBegin before the fill was refused: %v", err)
	}
}

// TestSeveredTransactionDoor_CheckFrom_AppliedPrefixWarns is the broker's
// rule: a finding landing on a link the broker already applied WARNs (once)
// and passes; the same finding on a link not yet applied refuses.
func TestSeveredTransactionDoor_CheckFrom_AppliedPrefixWarns(t *testing.T) {
	store := sevStore(t)
	// Shape B pair (inc1, inc2) — lands on inc2 (index 2).
	inc1 := sevWriteIncremental(t, store, "inc1", 100, cat(sevTx(100, "x"), sevTx(200, "a")))
	inc2 := sevWriteIncremental(t, store, "inc2", 100, cat(sevTx(200, "a"), sevTx(300, "y")))
	inc3 := sevWriteIncremental(t, store, "inc3", 100, sevTx(400, "z"))
	chain := []lineage.SegmentRecord{sevFull(), inc1, inc2, inc3}

	door := NewSeveredTransactionDoor(store, sevLSNComparator{}, nil)
	requireSevered(t, door.CheckFrom(context.Background(), chain, 2), "at or before the last rows")
	if err := door.CheckFrom(context.Background(), chain, 3); err != nil {
		t.Fatalf("a finding on an already-applied link refused the broker: %v", err)
	}
	if len(door.warned) != 1 {
		t.Fatalf("applied finding warned %d times; want exactly once", len(door.warned))
	}
	if err := door.CheckFrom(context.Background(), chain, 3); err != nil || len(door.warned) != 1 {
		t.Fatalf("second tick: err=%v, warned=%d; want nil and still 1", err, len(door.warned))
	}

	// Shape C lands on the link itself.
	drop := sevWithEnd(sevWriteIncremental(t, store, "drop", 100, sevTx(10, "a")), sevLSN(1), sevLSN(30))
	after := sevWriteIncremental(t, store, "after", 100, sevTx(30, "b"))
	chainC := []lineage.SegmentRecord{sevFull(), drop, after}
	requireIncomplete(t, NewSeveredTransactionDoor(store, nil, nil).CheckFrom(context.Background(), chainC, 1))
	if err := NewSeveredTransactionDoor(store, nil, nil).CheckFrom(context.Background(), chainC, 2); err != nil {
		t.Fatalf("shape C on an applied link refused: %v", err)
	}
}

// requireIncomplete: shape C refuses under the code restore's own tail
// backstop uses for the same evidence, naming the shape.
func requireIncomplete(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("door passed an incremental whose EndPosition is past its last stored change")
	}
	if codeOf(err) != sluicecode.CodeBackupIncomplete {
		t.Fatalf("shape C refusal is not coded %s: %v", sluicecode.CodeBackupIncomplete, err)
	}
	if !strings.Contains(err.Error(), "shape C") {
		t.Fatalf("shape C refusal does not name the shape: %v", err)
	}
}
