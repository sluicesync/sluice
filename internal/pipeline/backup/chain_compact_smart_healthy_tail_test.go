// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// The fourth review of the F-E1-SEVERED-TAIL-REPLAY compaction work, ported
// as pins. Round 4 (e9cbfc42) scoped the closing pair to marker-less inputs,
// which compacted a HEALTHY framed chain into one restore refuses (a trailing
// unframed TRUNCATE), refused another healthy chain with an uncoded message (a
// bare trailing TxBegin), and left a full merged copy behind on every refusal.

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// trailingTruncateRows is a healthy MySQL-shaped window: a committed
// transaction touching one row twice, then a TRUNCATE outside any transaction
// as the window's LAST event (the binlog reader emits DDL TRUNCATE unframed,
// and captureWindow ends a window whenever no transaction is open). Every
// event carries its own position; EndPosition is the TRUNCATE's.
func trailingTruncateRows(start uint64, j int) (out []ir.Change, end uint64) {
	p := start
	next := func() ir.Position { p++; return pos(p) }
	out = []ir.Change{
		ir.TxBegin{Position: next()},
		ir.Insert{Position: next(), Schema: "public", Table: "users", Row: ir.Row{"id": int64(10 + j), "name": "a"}},
		ir.Update{Position: next(), Schema: "public", Table: "users", Before: ir.Row{"id": int64(10 + j)}, After: ir.Row{"id": int64(10 + j), "name": "b"}},
		ir.TxCommit{Position: next()},
		ir.Truncate{Position: next(), Schema: "public", Table: "audit_log"},
	}
	return out, p
}

// bareBeginRows: incremental 1 ends on a BARE TxBegin (an older binary's stop
// just after a BEGIN: no row of that transaction, which the door passes);
// incremental 2 re-delivers that transaction whole.
func bareBeginRows(start uint64, j int) (out []ir.Change, end uint64) {
	if j != 1 {
		t := start + 1
		return []ir.Change{
			ir.TxBegin{Position: pos(t)},
			ir.Insert{Position: pos(t), Schema: "public", Table: "users", Row: ir.Row{"id": int64(50), "name": "z"}},
			ir.TxCommit{Position: pos(t + 1)},
		}, t + 1
	}
	t1, t2 := start+1, start+2
	return []ir.Change{
		ir.TxBegin{Position: pos(t1)},
		ir.Insert{Position: pos(t1), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "a"}},
		ir.Update{Position: pos(t1), Schema: "public", Table: "users", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "name": "b"}},
		ir.TxCommit{Position: pos(t1)},
		ir.TxBegin{Position: pos(t2)},
	}, t2
}

// requireRestorable: the door, `backup verify`'s door and chain restore's
// preflight all pass the chain on the store.
func requireRestorable(t *testing.T, store irbackup.Store, when string) {
	t.Helper()
	ctx := context.Background()
	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatalf("%s: build chain: %v", when, err)
	}
	if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); err != nil {
		t.Fatalf("%s: the door refuses the chain: %v", when, err)
	}
	if err := verifySeveredTransactions(ctx, store, chain, true, false, false, &chunkAuthProber{}); err != nil {
		t.Fatalf("%s: `backup verify`'s door refuses the chain: %v", when, err)
	}
	eng := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	if err := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store}).PreflightBeforeTarget(ctx); err != nil {
		t.Fatalf("%s: chain restore's preflight refuses the chain: %v", when, err)
	}
	for i := range chain {
		if !isIncrementalLink(&chain[i]) {
			continue
		}
		link := chain[i]
		link.Manifest.SchemaDelta = nil
		applier, err := eng.OpenChangeApplier(ctx, "tgt")
		if err != nil {
			t.Fatal(err)
		}
		if err := (&ChainRestore{Target: eng, TargetDSN: "tgt", Store: store}).applyIncremental(ctx, &link, applier, DefaultChainRestoreBatchSize); err != nil {
			t.Fatalf("%s: chain restore refuses incremental %d on apply: %v", when, i, err)
		}
	}
}

// TestSmartCompaction_HealthyFramedTailsStayRestorable: a chain every reader
// passes before `backup compact --smart-compaction` must compact and still
// pass. The independent expected value is the input chain's own verdict.
func TestSmartCompaction_HealthyFramedTailsStayRestorable(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows smartCompactRowsFn
	}{
		{"framed window ending on an unframed TRUNCATE (MySQL/MariaDB)", trailingTruncateRows},
		{"window ending on a bare TxBegin (older binary)", bareBeginRows},
		{"framed window ending on its commit (control)", framedRows},
		{"marker-less window (trigger-CDC)", markerlessRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store, now, severedCompactSchema(), nil, tc.rows)
			requireRestorable(t, store, "before compaction (control)")
			if err := smartCompact(t, store, now); err != nil {
				t.Fatalf("compaction refused a chain that restores: %v", err)
			}
			requireRestorable(t, store, "after smart compaction")
		})
	}
}

// TestSmartCompaction_RefusedRunLeavesNoMergedCopy: a refused compaction
// leaves the store exactly as it found it — three refused runs used to leave
// three full `seg-merged-*` copies of the group.
func TestSmartCompaction_RefusedRunLeavesNoMergedCopy(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, severedCompactSchema(), nil, severedPGRows)
	before := len(store.data)
	for i := 0; i < 3; i++ {
		_, err := CompactChain(context.Background(), store, CompactOpts{
			MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
			Now: func() time.Time { return now.Add(10 * time.Hour) },
		})
		if codeOf(err) != sluicecode.CodeBackupChainSeveredTransaction {
			t.Fatalf("run %d: want the severed-transaction refusal, got %v", i, err)
		}
	}
	for f := range store.data {
		if strings.HasPrefix(f, mergedSegmentDirPrefix) {
			t.Errorf("refused run left %s behind", f)
		}
	}
	if after := len(store.data); after != before {
		t.Fatalf("refused runs changed the store: %d files before, %d after", before, after)
	}
}

// TestSmart_KeepsTheSeveredVerdict_SecondLayer grades the in-stream layer on
// its own: the pre-copy judge in CompactChain refuses a severed input first,
// so end to end this layer is masked. Over the whole decoded stream it refuses
// a severed input (coded) and passes a bare trailing TxBegin.
func TestSmart_KeepsTheSeveredVerdict_SecondLayer(t *testing.T) {
	severed, _ := severedPGRows(100, 1)
	bare, _ := bareBeginRows(100, 1)
	for _, tc := range []struct {
		name   string
		events []ir.Change
		want   sluicecode.Code
	}{
		{"severed framed input", severed, sluicecode.CodeBackupChainSeveredTransaction},
		{"bare trailing TxBegin", bare, ""},
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
			if err := c.closeAtLastInputPosition(); err != nil {
				t.Fatal(err)
			}
			err := c.keepsTheSeveredVerdict()
			if codeOf(err) != tc.want || (tc.want == "") != (err == nil) {
				t.Fatalf("keepsTheSeveredVerdict = %v; want code %q", err, tc.want)
			}
		})
	}
}

// TestCompactChain_VerdictRegressionGate is the belt over the whole class: the
// gate refuses a post-compaction catalog whose chain the door would refuse
// when the current one passes, and accepts one that passes. The prospective
// chain here is the current chain with its last incremental's manifest
// re-pointed at a copy whose EndPosition is past its chunks (what both review
// rounds' defects produced).
func TestCompactChain_VerdictRegressionGate(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersSchema(), nil, framedRows)
	current, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := refuseVerdictRegression(ctx, store, current, current); err != nil {
		t.Fatalf("an unchanged chain must pass: %v", err)
	}

	last := len(current.Segments) - 1
	seg := lineage.NewPrefixedStore(store, current.Segments[last].Dir)
	incrs := current.Segments[last].Incrementals
	m, err := lineage.ReadManifestAt(ctx, seg, incrs[len(incrs)-1])
	if err != nil {
		t.Fatal(err)
	}
	m.EndPosition = pos(999)
	m.BackupID = irbackup.ComputeBackupID(m)
	const shortPath = "manifests/incr-short.json"
	if err := lineage.WriteManifestAt(ctx, seg, shortPath, m); err != nil {
		t.Fatal(err)
	}
	prospective := *current
	prospective.Segments = append([]lineage.Segment(nil), current.Segments...)
	prospective.Segments[last].Incrementals = append(append([]string(nil), incrs[:len(incrs)-1]...), shortPath)
	prospective.Segments[last].EndPosition = pos(999)

	err = refuseVerdictRegression(ctx, store, current, &prospective)
	if codeOf(err) != sluicecode.CodeBackupChainUnreadable || !strings.Contains(err.Error(), "would be refused by restore") {
		t.Fatalf("the gate passed a compaction that turns a restorable chain into a refused one: %v", err)
	}
	// Other direction: an already-refused chain is not this gate's to refuse.
	if err := refuseVerdictRegression(ctx, store, &prospective, &prospective); err != nil {
		t.Fatalf("a chain refused before compaction is not a regression: %v", err)
	}
}
