//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
)

// longVStreamTxIDForTest is a VStream transaction identity on a shard that has
// seen six primaries — "vstream:<ks>/<shard>:" and a six-UUID executed GTID
// set, about 300 characters, past the mark table's tx_id VARCHAR(255).
func longVStreamTxIDForTest() string {
	parts := make([]string, 0, 6)
	for i := range 6 {
		parts = append(parts, fmt.Sprintf("%08x-1111-2222-3333-%012x:1-%d", i, i, 4242))
	}
	return "vstream:commerce/-80:MySQL56/" + strings.Join(parts, ",")
}

// TestApplyMarks_LongTxIDRoundTrip is the v0.157.0 pre-release review's HIGH
// on a real MySQL target, under the strict sql_mode sluice sets AND a relaxed
// one (an empty --mysql-sql-mode, here the DSN's sql_mode): a transaction whose
// TxID is longer than the mark table's tx_id VARCHAR(255) is applied in part
// (the channel closes after its first change, which commits it with its mark
// and no position — a crash's committed prefix), then re-delivered whole. The
// first change must be SKIPPED by the mark it left, so the keyless table ends
// with the transaction's two rows — not three. Before the fix the strict mode
// failed the first apply with 1406, and the relaxed mode truncated the stored
// tx_id, so the re-delivered change found "another transaction's" mark and
// re-applied: a duplicate at exit 0.
//
// The independent expected value is the target's own row count, read with
// SQL, plus the stored tx_id itself.
func TestApplyMarks_LongTxIDRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	tx := longVStreamTxIDForTest()
	if len(tx) <= applymarks.MarkTxIDMaxLen {
		t.Fatalf("fixture: the TxID is %d characters; the case needs more than %d", len(tx), applymarks.MarkTxIDMaxLen)
	}
	for _, mode := range []struct{ name, param string }{
		{"strict (sluice default)", ""},
		{"relaxed (--mysql-sql-mode='')", "sql_mode=" + url.QueryEscape("''")},
	} {
		t.Run(mode.name, func(t *testing.T) {
			table := "kl_" + strings.Fields(mode.name)[0]
			applyMySQLApplier(t, dsn, `CREATE TABLE `+table+` (v INT NOT NULL) ENGINE=InnoDB;`)
			full := dsn
			if mode.param != "" {
				sep := "?"
				if strings.Contains(full, "?") {
					sep = "&"
				}
				full += sep + mode.param
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, full)
			if err != nil {
				t.Fatalf("OpenChangeApplier: %v", err)
			}
			defer func() { _ = applier.(interface{ Close() error }).Close() }()
			if err := applier.EnsureControlTable(ctx); err != nil {
				t.Fatal(err)
			}
			stream := "long-" + table
			pos := ir.Position{Token: "long-tx"}
			row := func(seq uint64, v int64) ir.Change {
				return ir.Insert{Position: pos, Schema: "target_db", Table: table, Row: ir.Row{"v": v}, ApplyID: ir.ApplyID{TxID: tx, Seq: seq}}
			}
			run := func(changes ...ir.Change) error {
				ch := make(chan ir.Change, len(changes))
				for _, c := range changes {
					ch <- c
				}
				close(ch)
				return applier.(ir.BatchedChangeApplier).ApplyBatch(ctx, stream, ch, 1)
			}
			// The crash's committed prefix: the first change, no commit.
			if err := run(ir.TxBegin{Position: pos}, row(1, 1)); err != nil {
				t.Fatalf("the first apply (one change of the long-TxID transaction) failed: %v", err)
			}
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			var stored string
			if err := db.QueryRowContext(ctx, `SELECT tx_id FROM sluice_cdc_apply_marks WHERE stream_id = ?`, stream).Scan(&stored); err != nil {
				t.Fatalf("read the stored mark: %v", err)
			}
			if stored != applymarks.MarkTxKey(tx) {
				t.Errorf("stored tx_id %q (%d chars); want the bounded key %q", stored, len(stored), applymarks.MarkTxKey(tx))
			}
			// The re-delivery: the whole transaction.
			if err := run(ir.TxBegin{Position: pos}, row(1, 1), row(2, 2), ir.TxCommit{Position: pos}); err != nil {
				t.Fatalf("the re-delivery failed: %v", err)
			}
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 2 {
				t.Errorf("%s holds %d rows after the re-delivery; want 2 (the first change re-applied instead of being skipped by its mark)", table, n)
			}
		})
	}
}
