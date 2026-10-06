// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Smart compaction must not change the severed-transaction door's verdict.
//
// b953b09d appended an empty TxBegin/TxCommit pair whenever the collapsed
// output ended below the input's last position. A FRAMED incremental that an
// old binary severed (it ends inside an open transaction) has no commit for
// the compactor to hold back, so its collapsed tail ended below the last input
// position and got the pair — which CLOSED the open transaction. The door's
// shape (A) went quiet, shape (C) stayed quiet (lastIn == EndPosition), and the
// keyless head of the re-delivered transaction restored 4 times against the
// source's 2, at exit 0 (the review's repro, ported below).

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func severedCompactSchema() *ir.Schema {
	s := usersSchema()
	s.Tables = append(s.Tables, noPKSchema().Tables...)
	return s
}

// severedPGRows is the Postgres shape: every change of a transaction carries
// that transaction's position. Incremental 1 commits t1 (INSERT users 1) then
// stores the HEAD of t2 — a keyless audit_log INSERT and an UPDATE of users 1
// — with no commit (an old binary's stop mid-transaction). Incremental 2
// re-delivers t2 whole.
func severedPGRows(start uint64, j int) (out []ir.Change, end uint64) {
	if j == 1 {
		t1, t2 := start+1, start+2
		return []ir.Change{
			ir.TxBegin{Position: pos(t1)},
			ir.Insert{Position: pos(t1), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "a"}},
			ir.TxCommit{Position: pos(t1)},
			ir.TxBegin{Position: pos(t2)},
			ir.Insert{Position: pos(t2), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup-me"}},
			ir.Update{Position: pos(t2), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "b"}},
		}, t2
	}
	return []ir.Change{
		ir.TxBegin{Position: pos(start)},
		ir.Insert{Position: pos(start), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup-me"}},
		ir.Update{Position: pos(start), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "b"}},
		ir.TxCommit{Position: pos(start + 1)},
	}, start + 1
}

// severedFilePosRows is the MySQL file/pos shape: every event carries its own
// position, and the next incremental resumes PART-WAY through the open
// transaction.
func severedFilePosRows(start uint64, j int) (out []ir.Change, end uint64) {
	p := start
	next := func() ir.Position { p++; return pos(p) }
	if j == 1 {
		out = []ir.Change{
			ir.TxBegin{Position: next()},
			ir.Insert{Position: next(), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "a"}},
			ir.TxCommit{Position: next()},
			ir.TxBegin{Position: next()},
			ir.Insert{Position: next(), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "x", "msg": "dup-me"}},
			ir.Update{Position: next(), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "b"}},
		}
		return out, p
	}
	out = []ir.Change{
		ir.Insert{Position: next(), Schema: "public", Table: "audit_log", Row: ir.Row{"ts": "y", "msg": "rest"}},
		ir.TxCommit{Position: next()},
	}
	return out, p
}

// severedFillRows is severedPGRows with a v0.156.1–v0.156.11 ADD COLUMN fill
// appended after the severed window: the incremental ENDS on the fill's
// TxCommit while the window's transaction is still open. The fill rows are
// stamped at EndPosition.
func severedFillRows(start uint64, j int) (out []ir.Change, end uint64) {
	out, end = severedPGRows(start, j)
	if j != 1 {
		return out, end
	}
	return append(
		out,
		ir.TxBegin{Position: pos(end)},
		ir.Update{Position: pos(end), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "b", "extra": int64(1)}},
		ir.TxCommit{Position: pos(end)},
	), end
}

// markFillOnFirstIncrementals records the ADD COLUMN fill on every j==1
// incremental the seed wrote, the way the capture lanes record it.
func markFillOnFirstIncrementals(t *testing.T, store irbackup.Store) {
	t.Helper()
	ctx := context.Background()
	for _, dir := range []string{"", "seg-1"} {
		seg := lineage.NewPrefixedStore(store, dir)
		for _, p := range []string{"manifests/incr-00001-seg0-1.json", "manifests/incr-00001-seg1-1.json"} {
			m, err := lineage.ReadManifestAt(ctx, seg, p)
			if err != nil {
				continue
			}
			m.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
				Kind: irbackup.SchemaDeltaAlterTable, Schema: "public", Table: "users",
				AddColumnFill: &irbackup.AddColumnFill{Columns: []string{"extra"}, Rows: 1},
			}}
			if err := lineage.WriteManifestAt(ctx, seg, p, m); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func smartCompact(t *testing.T, store irbackup.Store, now time.Time) error {
	t.Helper()
	_, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-smart" },
	})
	return err
}

// requireSeveredVerdict: the chain is refused as shape (A) by the door, by
// `backup verify`'s door and by chain restore's own pre-target preflight.
func requireSeveredVerdict(t *testing.T, store irbackup.Store, when string) {
	t.Helper()
	ctx := context.Background()
	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
		t.Fatalf("%s: the door does not refuse the severed chain as shape (A): %v", when, err)
	}
	if err := verifySeveredTransactions(ctx, store, chain, true, false, false, &chunkAuthProber{}); codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
		t.Fatalf("%s: `backup verify`'s door does not refuse the severed chain: %v", when, err)
	}
	eng := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	if err := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store}).PreflightBeforeTarget(ctx); codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
		t.Fatalf("%s: chain restore's preflight does not refuse the severed chain: %v", when, err)
	}
}

// severedCollapseAwayRows: the open transaction's only row is a DELETE that
// cancels an INSERT from an earlier, committed transaction, so a collapse
// would leave the open transaction with no row — a bare TxBegin, which the
// door passes.
func severedCollapseAwayRows(start uint64, j int) (out []ir.Change, end uint64) {
	if j == 1 {
		t1, t2 := start+1, start+2
		return []ir.Change{
			ir.TxBegin{Position: pos(t1)},
			ir.Insert{Position: pos(t1), Schema: "public", Table: "users", Row: ir.Row{"id": int64(5), "name": "a"}},
			ir.TxCommit{Position: pos(t1)},
			ir.TxBegin{Position: pos(t2)},
			ir.Delete{Position: pos(t2), Schema: "public", Table: "users", Before: ir.Row{"id": int64(5), "name": "a"}},
		}, t2
	}
	return []ir.Change{
		ir.TxBegin{Position: pos(start)},
		ir.Delete{Position: pos(start), Schema: "public", Table: "users", Before: ir.Row{"id": int64(5), "name": "a"}},
		ir.TxCommit{Position: pos(start + 1)},
	}, start + 1
}

// TestSmartCompaction_RefusesASeveredFramedIncremental: for every framed shape
// an old binary could sever, `backup compact --smart-compaction` refuses
// (coded -CHAIN-SEVERED-TRANSACTION) and leaves the chain exactly as it was,
// so the door, `backup verify` and chain restore's preflight still refuse it
// as shape (A). The independent expected value is the INPUT chain's verdict,
// read before compaction by the same three readers.
func TestSmartCompaction_RefusesASeveredFramedIncremental(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows smartCompactRowsFn
		fill bool
	}{
		{"Postgres-shaped (every row at its transaction's position)", severedPGRows, false},
		{"MySQL file/pos-shaped (every event its own position)", severedFilePosRows, false},
		{"ADD COLUMN fill appended after the severed window", severedFillRows, true},
		{"open transaction whose only row collapses away", severedCollapseAwayRows, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store, now, severedCompactSchema(), nil, tc.rows)
			if tc.fill {
				markFillOnFirstIncrementals(t, store)
			}
			requireSeveredVerdict(t, store, "before compaction (control)")
			if err := smartCompact(t, store, now); codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
				t.Fatalf("smart compaction did not refuse a severed incremental: %v", err)
			}
			requireSeveredVerdict(t, store, "after the refused compaction")
		})
	}
}

// TestSmart_ClosingPairNeverClosesAnOpenTransaction isolates the closing-pair
// guard from the refusals (which would otherwise mask it). The pair is owed by
// every input that does NOT end inside an open transaction and whose collapsed
// output ends below its last position — marker-less, and framed with a trailing
// unframed event (MySQL's DDL TRUNCATE) — and must never be given to one that
// does: a framed severed window (the pair would close it: b953b09d) or a bare
// trailing TxBegin (whose held begin already ends the output at the last
// position).
func TestSmart_ClosingPairNeverClosesAnOpenTransaction(t *testing.T) {
	framed, _ := severedPGRows(100, 1)
	markerless, _ := markerlessRows(100, 1)
	truncate, _ := trailingTruncateRows(100, 1)
	bare, _ := bareBeginRows(100, 1)
	for _, tc := range []struct {
		name     string
		events   []ir.Change
		wantPair bool
	}{
		{"framed, ends inside an open transaction", framed, false},
		{"framed, ends on a bare TxBegin", bare, false},
		{"framed, ends on an unframed TRUNCATE", truncate, true},
		{"marker-less, collapsed tail below the last input position", markerless, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSmartCompactor(PKStrategyPK, severedCompactSchema())
			for _, e := range tc.events {
				if err := c.process(e); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.finalize(); err != nil {
				t.Fatal(err)
			}
			before := len(c.sink.(*sliceSink).out)
			if err := c.closeAtLastInputPosition(); err != nil {
				t.Fatal(err)
			}
			out := c.sink.(*sliceSink).out
			if got := len(out) - before; (got == 2) != tc.wantPair {
				t.Fatalf("closing pair appended %d events; want pair=%v", got, tc.wantPair)
			}
			if !tc.wantPair && tc.name == "framed, ends on a bare TxBegin" {
				if _, ok := out[len(out)-1].(ir.TxBegin); !ok || lastPosOf(out) != lastPosOf(tc.events) {
					t.Fatalf("a bare trailing TxBegin must be emitted LAST, at the last input position (the held begin): tail %T at %+v", out[len(out)-1], lastPosOf(out))
				}
			}
		})
	}
}

// TestSmartCompaction_MarkerlessCloseRestoresTheSameRows: the closing pair on
// a marker-less input carries no row, so the rows a restore applies are the
// compacted rows and nothing else (the pair adds two markers, never a row).
func TestSmartCompaction_MarkerlessCloseRestoresTheSameRows(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, markerlessRows)
	if err := smartCompact(t, store, now); err != nil {
		t.Fatalf("compact: %v", err)
	}
	post, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range post {
		if !isIncrementalLink(&post[i]) {
			continue
		}
		var rows, markers int
		var last ir.Change
		d := NewSeveredTransactionDoor(store, nil, nil)
		for c := range post[i].Manifest.ChangeChunks {
			if err := d.decodeChunk(ctx, &post[i], c, func(ch ir.Change) {
				switch ch.(type) {
				case ir.TxBegin, ir.TxCommit:
					markers++
				default:
					rows++
				}
				last = ch
			}); err != nil {
				t.Fatal(err)
			}
		}
		if markers != 2 {
			t.Fatalf("link %d: %d transaction markers; a marker-less input gets exactly the closing pair", i, markers)
		}
		if _, ok := last.(ir.TxCommit); !ok {
			t.Fatalf("link %d: the closing pair is not last (%T)", i, last)
		}
		if rows == 0 {
			t.Fatalf("link %d: anti-vacuity: no rows", i)
		}
	}
}
