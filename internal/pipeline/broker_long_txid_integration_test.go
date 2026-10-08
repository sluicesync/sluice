//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// longTxChainVStreamTxID is a VStream-shaped transaction identity past the
// MySQL mark table's tx_id VARCHAR(255): six server UUIDs in the shard's
// executed GTID set, the transaction ordinal in the last.
func longTxChainVStreamTxID(n int) string {
	parts := make([]string, 0, 6)
	for i := range 6 {
		parts = append(parts, fmt.Sprintf("%08x-aaaa-bbbb-cccc-%012x:1-%d", i, i, 5000+n))
	}
	return "vstream:commerce/-80:MySQL56/" + strings.Join(parts, ",")
}

// longTxChain is a full (kl keyless, su with a secondary UNIQUE) and one
// identity-bearing incremental, one change per chunk, whose two source
// transactions carry over-long TxIDs: T1 inserts two identical keyless rows
// and moves a unique value off one su row and onto another (the
// collision-on-replay shape the marks exist for); T2 pads.
func longTxChain(t *testing.T) (store *blobcodec.LocalStore, fullID string, incr *lineage.SegmentRecord) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	intCol := func(n string) *ir.Column { return &ir.Column{Name: n, Type: ir.Integer{Width: 64}} }
	kl := &ir.Table{Name: "kl", Columns: []*ir.Column{intCol("v")}}
	su := &ir.Table{
		Name:       "su",
		Columns:    []*ir.Column{intCol("id"), intCol("u")},
		PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes:    []*ir.Index{{Name: "su_u", Unique: true, Columns: []ir.IndexColumn{{Column: "u"}}}},
	}
	schema := &ir.Schema{Tables: []*ir.Table{kl, su}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"kl": {{"v": int64(0)}},
		"su": {{"id": int64(1), "u": int64(10)}, {"id": int64(2), "u": int64(20)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	t1, t2 := longTxChainVStreamTxID(1), longTxChainVStreamTxID(2)
	id := func(tx string, seq uint64) ir.ApplyID { return ir.ApplyID{TxID: tx, Seq: seq} }
	changes := []ir.Change{
		ir.TxBegin{Position: pos("0/110")},
		ir.Insert{Position: pos("0/120"), Table: "kl", Row: ir.Row{"v": int64(1)}, ApplyID: id(t1, 1)},
		ir.Insert{Position: pos("0/120"), Table: "kl", Row: ir.Row{"v": int64(1)}, ApplyID: id(t1, 2)},
		ir.Update{Position: pos("0/120"), Table: "su", Before: ir.Row{"id": int64(1), "u": int64(10)}, After: ir.Row{"id": int64(1), "u": int64(99)}, ApplyID: id(t1, 1)},
		ir.Update{Position: pos("0/120"), Table: "su", Before: ir.Row{"id": int64(2), "u": int64(20)}, After: ir.Row{"id": int64(2), "u": int64(10)}, ApplyID: id(t1, 2)},
		ir.TxCommit{Position: pos("0/130")},
		ir.TxBegin{Position: pos("0/140")},
		ir.Insert{Position: pos("0/150"), Table: "kl", Row: ir.Row{"v": int64(2)}, ApplyID: id(t2, 1)},
		ir.TxCommit{Position: pos("0/160")},
	}
	cdc := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{schema, schema}, cdcChanges: changes, stampsIdentity: true}
	if err := (&IncrementalBackup{Source: cdc, SourceDSN: "src", Store: store, ParentRef: full.BackupID, Window: time.Minute, ChunkChanges: 1}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	chain, err := (&SyncFromBackup{Store: store, ChainURL: "x", StreamID: "x"}).brokerChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	link := chain[len(chain)-1]
	if lineage.CanonicalKind(link.Manifest.Kind) != irbackup.BackupKindIncremental || !link.Manifest.ApplyIdentity {
		t.Fatal("the fixture's tail is not an identity-bearing incremental")
	}
	return store, full.BackupID, &link
}

// longTxState is the two tables' contents on a MySQL target; the expected
// value is the source's end state, derived from the fixture by hand: kl
// {0, 1, 1, 2}, su {1:99, 2:10}.
func longTxState(t *testing.T, dsn string) string {
	t.Helper()
	return sevQuery(t, "mysql", dsn, "SELECT CONCAT((SELECT COALESCE(GROUP_CONCAT(v ORDER BY v),'') FROM kl), ' | ', "+
		"(SELECT COALESCE(GROUP_CONCAT(CONCAT(id,':',u) ORDER BY id),'') FROM su))")
}

const longTxWant = "0,1,1,2 | 1:99,2:10"

// TestLongTxID_ChainRestoreAndBrokerOntoMySQL is the v0.157.0 review's HIGH
// end to end on a real MySQL target, strict sql_mode (sluice's default): a
// chain whose transactions carry VStream-length TxIDs.
//
//   - chain restore lands it (before the fix: 1406 at the first marked-class
//     change, after earlier links had been written);
//   - a broker killed after T1's first keyless row, then resumed, does not
//     duplicate that row — the lifted keyless table is exactly-once because
//     the mark it left is found again.
//
// The independent expected value is the source's end state (longTxWant),
// read back from MySQL with SQL.
func TestLongTxID_ChainRestoreAndBrokerOntoMySQL(t *testing.T) {
	_, dst, cleanup := startMySQLGTID(t)
	t.Cleanup(cleanup)
	store, fullID, incr := longTxChain(t)
	eng, _ := engines.Get("mysql")
	e := &sevEnv{t: t, engine: "mysql", driver: "mysql", src: dst, dst: dst}
	ctx := context.Background()

	t.Run("chain restore", func(t *testing.T) {
		dsn := e.database("lt_restore")
		if err := (&backup.Restore{Target: eng, TargetDSN: dsn, Store: store}).Run(ctx); err != nil {
			t.Fatalf("chain restore of a long-TxID chain: %v", err)
		}
		if got := longTxState(t, dsn); got != longTxWant {
			t.Errorf("restored %q; want %q", got, longTxWant)
		}
	})

	t.Run("broker killed after a keyless row, resumed", func(t *testing.T) {
		dsn := e.database("lt_broker")
		if err := (&backup.Restore{Target: eng, TargetDSN: dsn, Store: store, SkipChainDispatch: true}).Run(ctx); err != nil {
			t.Fatalf("restore the full: %v", err)
		}
		bc := &brokerCrashChain{e: e, store: store, fullID: fullID, incr: *incr}
		// Chunk 2 holds T1's second keyless row: the kill lands after the first.
		killCtx, cancel := context.WithTimeout(ctx, time.Minute)
		err := bc.killBefore(bc.broker(dsn, "lt", 1, fullID), 2).Run(killCtx)
		cancel()
		if !errors.Is(err, errBrokerCrashKill) {
			t.Fatalf("the broker did not die at the kill: %v", err)
		}
		if n := sevQuery(t, "mysql", dsn, "SELECT COUNT(*) FROM kl WHERE v = 1"); n != "1" {
			t.Fatalf("at the kill kl holds %s row(s) of v=1; the cell needs exactly the first committed", n)
		}
		var marks string
		marks = sevQuery(t, "mysql", dsn, "SELECT COALESCE(GROUP_CONCAT(tx_id), '') FROM sluice_cdc_apply_marks WHERE stream_id = 'lt'")
		if marks != applymarks.MarkTxKey(longTxChainVStreamTxID(1)) {
			t.Errorf("at the kill the stored mark names %q; want T1's bounded key", marks)
		}
		bc.runToTailWith(t, dsn, bc.broker(dsn, "lt", 1, ""))
		if got := longTxState(t, dsn); got != longTxWant {
			t.Errorf("after the resume %q; want %q (the keyless row duplicated?)", got, longTxWant)
		}
	})
}
