//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// TestVStream_ControlKeyspaceTornCommit_PositionStaysBehindData is GC-41
// (c)'s real-tear pin, and the check on the premise the DATA BEFORE CONTROL
// rule rests on (see writePositionTx): vtgate in transaction_mode=MULTI —
// vttestserver's default, asserted below — commits a cross-keyspace
// transaction shard by shard in first-touch order and stops at the first
// failure.
//
// The shape is the triage's: a target keyspace "data" and a --control-keyspace
// sidecar "ctl", each its own shard. The test holds FOR UPDATE on the
// stream's sluice_cdc_state row, so the batch applier's transaction — its
// rows, its apply marks and its position — blocks at the position write
// with both shards touched. It then kills the data shard's backend
// connection underneath vtgate and releases the lock. At COMMIT the data
// shard fails. With data sent first, nothing of the control shard commits:
// the position stays at the previous transaction and the re-delivered
// transaction applies. v0.156.5's batch hooks sent the marks first, so the
// control shard committed first — position past rows that were gone for good.
//
// It runs the batch WritePosition hook over a transaction larger than one
// batch — the shape whose position write carries a control statement (the
// DELETE of the transaction's marks) that the v0.156.5 order sent ahead of
// the last batch's rows; mutation-run against that order, it reports the
// position past the transaction with 3 of its 5 rows on the target. The
// Commit hook's tear (marks committed without their rows, so the replay
// SKIPS rows that never landed) needs a lock between the rows and the marks
// that only the fixed order admits, so its order is pinned by the
// statement-order roster (TestWriteCoreStatementOrder) rather than here.
func TestVStream_ControlKeyspaceTornCommit_PositionStaysBehindData(t *testing.T) {
	vt := bootVTTestServer(t, "data,ctl", "1,1")
	defer vt.terminate()
	dsn := vt.dsn("data")
	applyVTTestSQL(t, dsn, `CREATE TABLE tear_items (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL, UNIQUE KEY (code)) ENGINE=InnoDB`)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var mode string
	if err := db.QueryRowContext(ctx, `SELECT @@transaction_mode`).Scan(&mode); err != nil || mode != "MULTI" {
		t.Fatalf("premise: vtgate @@transaction_mode = %q (err %v); this test measures MULTI's commit order", mode, err)
	}

	eng, err := Engine{Flavor: FlavorVitess}.WithControlKeyspace("ctl")
	if err != nil {
		t.Fatalf("WithControlKeyspace: %v", err)
	}
	open := func() *ChangeApplier {
		t.Helper()
		a, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		return a.(*ChangeApplier)
	}
	a := open()
	defer func() { _ = a.Close() }()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// txn is one source transaction of n marked-class rows (a secondary
	// unique index, so each writes an apply mark with its row).
	txn := func(txID string, first, n int64, tok string) <-chan ir.Change {
		p := ir.Position{Engine: engineNameMySQL, Token: tok}
		ch := make(chan ir.Change, n+2)
		ch <- ir.TxBegin{Position: p}
		for i := range n {
			id := first + i
			ch <- ir.Insert{
				Schema: "data", Table: "tear_items", Row: ir.Row{"id": id, "code": fmt.Sprintf("c%d", id)},
				Position: p, ApplyID: ir.ApplyID{TxID: txID, Seq: uint64(i + 1)},
			}
		}
		ch <- ir.TxCommit{Position: p}
		close(ch)
		return ch
	}
	const (
		tok0 = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10"}`
		tokA = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-11"}`
	)
	if err := a.ApplyBatch(ctx, testStreamID, txn("tx-0", 100, 1, tok0), 50); err != nil {
		t.Fatalf("baseline transaction: %v", err)
	}
	rowsA := func() int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tear_items WHERE id BETWEEN 1 AND 5`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Hold the position row.
	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	var held string
	if err := lockTx.QueryRowContext(ctx, "SELECT stream_id FROM "+controlTableRef("ctl", controlTableName)+" WHERE stream_id = ? FOR UPDATE", testStreamID).Scan(&held); err != nil {
		_ = lockTx.Rollback()
		t.Fatalf("lock the position row: %v", err)
	}

	// Five rows at batch size 3: the first three commit mid-transaction with
	// their marks (no position), so at the TxCommit the transaction's marks
	// are on the target and the position write's plan DELETES them — a
	// control statement the v0.156.5 order sent before rows 4 and 5.
	applyErr := make(chan error, 1)
	go func() { applyErr <- a.ApplyBatch(ctx, testStreamID, txn("tx-A", 1, 5, tokA), 3) }()

	// Wait for the applier's position write to block on the lock with the
	// data shard's transaction open, then kill that data connection.
	thread := waitForTornCommitSetup(ctx, t, vt, applyErr)
	mysqldExec(ctx, t, vt, "KILL "+strconv.FormatInt(thread, 10))
	if err := lockTx.Rollback(); err != nil {
		t.Fatalf("release the position row: %v", err)
	}
	if err := <-applyErr; err == nil {
		t.Fatal("the apply committed although its data-shard connection was killed before COMMIT — the tear was not induced, so this run proves nothing")
	}

	got, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after the torn commit: ok=%v err=%v", ok, err)
	}
	if got.Token == tokA {
		t.Fatalf("SILENT LOSS (GC-41 (c)): the torn commit persisted the position %s past its transaction while only %d of its 5 rows reached the target — the control shard committed before the data shard, so nothing will ever re-deliver the rest",
			tokA, rowsA())
	}
	if got.Token != tok0 {
		t.Fatalf("position after the torn commit = %q; want the previous transaction's %q", got.Token, tok0)
	}
	if n := rowsA(); n == 5 {
		t.Fatal("all 5 of the torn transaction's rows committed; the final batch's data-shard commit was meant to fail")
	}

	// Redelivery: the position is behind the data, so the whole transaction
	// comes again — the rows of the batch that committed are skipped on
	// their marks or re-applied idempotently, and the rest apply.
	a2 := open()
	defer func() { _ = a2.Close() }()
	if err := a2.ApplyBatch(ctx, testStreamID, txn("tx-A", 1, 5, tokA), 3); err != nil {
		t.Fatalf("re-delivered transaction: %v", err)
	}
	if n := rowsA(); n != 5 {
		t.Fatalf("after re-delivery the target holds %d of the transaction's 5 rows", n)
	}
	if got, _, _ := a2.ReadPosition(ctx, testStreamID); got.Token != tokA {
		t.Fatalf("after re-delivery the position = %q; want %q", got.Token, tokA)
	}
}

// TestVStream_ControlKeyspaceTornCommit_BarrierFoldStaysBehindData is the
// lane-barrier twin of the test above (ADR-0190 amendment E, §E.6 / P10).
// Under the fold a barrier that writes NO control row of its own — a keyless
// change with no ADR-0190 identity, so no apply mark — still becomes a
// cross-keyspace transaction, because it now carries the folded checkpoint:
// its row on the data shard, then the closed transaction's mark deletion
// and the position on the control shard. The two-order argument says a tear
// is harmless either way; this measures the order that matters on a real
// vtgate MULTI commit.
//
// T−1's apply mark is planted and loaded, and the folded checkpoint closes
// T−1, so the barrier's transaction deletes it. The stream's position row is
// held FOR UPDATE, so the barrier's transaction blocks at its position
// write with both shards touched; the data shard's backend connection is
// killed and the lock released. At COMMIT the data shard fails first, so
// nothing of the control shard commits: the position stays at T−1's start,
// T−1's mark survives, and the barrier's row is not on the target. The
// re-delivered barrier then applies once.
func TestVStream_ControlKeyspaceTornCommit_BarrierFoldStaysBehindData(t *testing.T) {
	vt := bootVTTestServer(t, "data,ctl", "1,1")
	defer vt.terminate()
	dsn := vt.dsn("data")
	applyVTTestSQL(t, dsn, `CREATE TABLE tear_keyless (v VARCHAR(32) NOT NULL) ENGINE=InnoDB`)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var mode string
	if err := db.QueryRowContext(ctx, `SELECT @@transaction_mode`).Scan(&mode); err != nil || mode != "MULTI" {
		t.Fatalf("premise: vtgate @@transaction_mode = %q (err %v); this test measures MULTI's commit order", mode, err)
	}
	eng, err := Engine{Flavor: FlavorVitess}.WithControlKeyspace("ctl")
	if err != nil {
		t.Fatalf("WithControlKeyspace: %v", err)
	}
	open := func() *ChangeApplier {
		t.Helper()
		a, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		return a.(*ChangeApplier)
	}
	a := open()
	defer func() { _ = a.Close() }()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	const (
		tokPrev = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20"}` // T−1's start: the position persisted before
		tokT    = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-21"}` // T's start: the checkpoint the barrier folds
	)
	if err := a.WritePosition(ctx, testStreamID, ir.Position{Engine: engineNameMySQL, Token: tokPrev}); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	marksRef := controlTableRef("ctl", "sluice_cdc_apply_marks")
	if _, err := db.ExecContext(ctx, "INSERT INTO "+marksRef+` (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES (?, 'data.tear_items', 'k-prev', 'tx-prev', 1, 'd', '')`, testStreamID); err != nil {
		t.Fatalf("plant T−1's mark: %v", err)
	}
	if err := a.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	prevMarks := func() int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+marksRef+" WHERE tx_id = 'tx-prev'").Scan(&n); err != nil {
			t.Fatalf("count marks: %v", err)
		}
		return n
	}
	keylessRows := func() int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tear_keyless`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	var held string
	if err := lockTx.QueryRowContext(ctx, "SELECT stream_id FROM "+controlTableRef("ctl", controlTableName)+" WHERE stream_id = ? FOR UPDATE", testStreamID).Scan(&held); err != nil {
		_ = lockTx.Rollback()
		t.Fatalf("lock the position row: %v", err)
	}
	la := &laneApplierAdapter{a: a, streamID: testStreamID}
	barrier := ir.Insert{Schema: "data", Table: "tear_keyless", Row: ir.Row{"v": "b"}, Position: ir.Position{Engine: engineNameMySQL, Token: tokT}}
	at := &laneapply.BarrierCheckpoint{Pos: ir.Position{Engine: engineNameMySQL, Token: tokT}, RowsApplied: 1, ClosedTxs: []string{"tx-prev"}}
	applyErr := make(chan error, 1)
	go func() { applyErr <- la.ApplyBarrierChange(ctx, barrier, at) }()

	// The barrier's ONE transaction blocked at its position write with its
	// row open on the data shard — a separate checkpoint commit could not
	// build this state (it would wait with no data transaction open).
	thread := waitForTornCommitSetup(ctx, t, vt, applyErr)
	mysqldExec(ctx, t, vt, "KILL "+strconv.FormatInt(thread, 10))
	if err := lockTx.Rollback(); err != nil {
		t.Fatalf("release the position row: %v", err)
	}
	if err := <-applyErr; err == nil {
		t.Fatal("the barrier committed although its data-shard connection was killed before COMMIT — the tear was not induced, so this run proves nothing")
	}

	got, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after the torn commit: ok=%v err=%v", ok, err)
	}
	if got.Token == tokT {
		t.Fatalf("the torn barrier commit persisted its folded position %s while its row is on the target %d times — the "+
			"control shard committed before the data shard", tokT, keylessRows())
	}
	if got.Token != tokPrev {
		t.Fatalf("position after the torn commit = %q; want T−1's start %q", got.Token, tokPrev)
	}
	if n := prevMarks(); n != 1 {
		t.Fatalf("T−1's apply mark is on the target %d times after the torn commit; want 1 — its deletion committed without "+
			"the position and data it travels with", n)
	}
	if n := keylessRows(); n != 0 {
		t.Fatalf("the barrier's row is on the target %d times; the data shard's commit was meant to fail", n)
	}

	// Redelivery from T−1's start: the barrier applies once, with its fold.
	a2 := open()
	defer func() { _ = a2.Close() }()
	if err := a2.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	if err := (&laneApplierAdapter{a: a2, streamID: testStreamID}).ApplyBarrierChange(ctx, barrier, at); err != nil {
		t.Fatalf("re-delivered barrier: %v", err)
	}
	if n := keylessRows(); n != 1 {
		t.Fatalf("after re-delivery the target holds %d copies of the barrier's row; want 1", n)
	}
	if got, _, _ := a2.ReadPosition(ctx, testStreamID); got.Token != tokT {
		t.Fatalf("after re-delivery the position = %q; want %q", got.Token, tokT)
	}
	if n := prevMarks(); n != 0 {
		t.Fatalf("after re-delivery T−1's mark is still on the target (%d); the fold closes it", n)
	}
}

// waitForTornCommitSetup polls the mysqld underneath vtgate until a
// transaction waits on the position row's lock and the data shard
// (vt_data_0) holds an open transaction, and returns that data
// transaction's connection id.
func waitForTornCommitSetup(ctx context.Context, t *testing.T, vt vtTestServer, applyErr <-chan error) int64 {
	t.Helper()
	const q = `SELECT CONCAT(` +
		`(SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'), ':', ` +
		`IFNULL((SELECT MAX(t.trx_mysql_thread_id) FROM information_schema.innodb_trx t ` +
		`JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id WHERE p.db = 'vt_data_0' AND t.trx_rows_modified > 0), 0))`
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case err := <-applyErr:
			t.Fatalf("the apply finished before its position write blocked on the held row: %v", err)
		default:
		}
		out := strings.TrimSpace(mysqldExec(ctx, t, vt, q))
		waiting, thread, _ := strings.Cut(out, ":")
		if waiting != "0" && thread != "0" && thread != "" {
			id, err := strconv.ParseInt(thread, 10, 64)
			if err != nil {
				t.Fatalf("parse thread id %q: %v", thread, err)
			}
			return id
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw the applier blocked on the position row with a data transaction open (last %q)", out)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// mysqldExec runs one statement on vttestserver's embedded mysqld over its
// unix socket — beneath vtgate, where a tablet's backend connection can be
// seen and killed — and returns its tab-separated output.
func mysqldExec(ctx context.Context, t *testing.T, vt vtTestServer, stmt string) string {
	t.Helper()
	script := `mysql -S "$(find /vt/vtdataroot -name mysql.sock | head -1)" -u root -N -B -e "$1"`
	code, r, err := vt.container.Exec(ctx, []string{"sh", "-c", script, "sh", stmt}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("mysqld exec %q: %v", stmt, err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("mysqld exec %q: exit %d: %s", stmt, code, out)
	}
	return string(out)
}
