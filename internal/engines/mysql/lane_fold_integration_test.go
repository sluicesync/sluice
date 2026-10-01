//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// errFoldHook is the injected pre-commit failure of the atomicity pin.
var errFoldHook = errors.New("test: fail the fold batch after the position is written")

// foldFixture is the MySQL lane adapter under --exactly-once-lanes on a fresh
// target with table fold_items (a secondary-unique table, so its changes
// write marks), on a DEDICATED lane pool, the stream's position at p0 with
// rows_applied 0, and one durable mark of a transaction "closed-tx" the
// fold's ticket closes.
type foldFixture struct {
	a   *ChangeApplier
	la  *laneApplierAdapter
	db  *sql.DB
	pos func(n int) ir.Position
}

func newFoldFixture(ctx context.Context, t *testing.T, dsn string) foldFixture {
	t.Helper()
	applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS fold_items;
		CREATE TABLE fold_items (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL, UNIQUE KEY fold_code (code)) ENGINE=InnoDB`)
	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	a := opened.(*ChangeApplier)
	t.Cleanup(func() { _ = a.Close() })
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	pos := func(n int) ir.Position {
		return ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"file":"binlog.000001","pos":%d}`, 1000+n)}
	}
	if err := a.WritePosition(ctx, testStreamID, pos(0)); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, q := range []string{`DELETE FROM sluice_cdc_apply_marks`, `UPDATE sluice_cdc_state SET rows_applied = 0`} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES (?, 'target_db.fold_items', 'k-closed', 'closed-tx', 1, 'd', '')`, testStreamID); err != nil {
		t.Fatalf("plant the closed transaction's mark: %v", err)
	}
	if err := a.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	a.exactlyOnceLanes = true
	laneDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open the lane pool: %v", err)
	}
	t.Cleanup(func() { _ = laneDB.Close() })
	return foldFixture{a: a, la: &laneApplierAdapter{a: a, streamID: testStreamID, laneDB: laneDB}, db: db, pos: pos}
}

// state is the target's view of everything a fold writes.
func (f foldFixture) state(ctx context.Context, t *testing.T) (pos string, rows int64, marks map[string]int, data int) {
	t.Helper()
	if err := f.db.QueryRowContext(ctx, `SELECT source_position, rows_applied FROM sluice_cdc_state WHERE stream_id = ?`, testStreamID).
		Scan(&pos, &rows); err != nil {
		t.Fatalf("read the control row: %v", err)
	}
	marks = map[string]int{}
	rs, err := f.db.QueryContext(ctx, `SELECT tx_id, COUNT(*) FROM sluice_cdc_apply_marks WHERE stream_id = ? GROUP BY tx_id`, testStreamID)
	if err != nil {
		t.Fatalf("read marks: %v", err)
	}
	defer func() { _ = rs.Close() }()
	for rs.Next() {
		var tx string
		var n int
		if err := rs.Scan(&tx, &n); err != nil {
			t.Fatalf("scan marks: %v", err)
		}
		marks[tx] = n
	}
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fold_items`).Scan(&data); err != nil {
		t.Fatalf("count data: %v", err)
	}
	return pos, rows, marks, data
}

func foldItem(id int64, code, tx string, seq uint64) ir.Change {
	return ir.Insert{
		Schema: "target_db", Table: "fold_items", Row: ir.Row{"id": id, "code": code},
		ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
	}
}

// TestLaneApplyBatch_FoldIsOneTransaction pins ADR-0190 amendment D's
// atomicity on the MySQL lane write core (the coalescing mysqlBatchTx): a
// fold batch's data, its marks (and the closed transaction's mark deletion)
// and the anchor position are ONE target transaction. Failed after the
// position statement executed, nothing of the three is on the target;
// committed, all three are, and rows_applied moved by exactly the ticket's
// increment.
func TestLaneApplyBatch_FoldIsOneTransaction(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := newFoldFixture(ctx, t, dsn)
	batch := []ir.Change{foldItem(1, "a", "fold-tx", 1), foldItem(2, "b", "fold-tx", 2)}
	ticket := &laneapply.FoldTicket{Tx: "fold-tx", Pos: f.pos(1), RowsApplied: 7, ClosedTxs: []string{"closed-tx"}}
	f.la.fence.Open("fold-tx", false)

	f.la.laneCommitHook = func([]laneChange) error { return errFoldHook }
	if _, err := f.la.ApplyLaneBatch(ctx, 0, batch, ticket); !errors.Is(err, errFoldHook) {
		t.Fatalf("the injected failure did not surface: %v", err)
	}
	pos, rows, marks, data := f.state(ctx, t)
	if pos != f.pos(0).Token || rows != 0 || data != 0 || marks["fold-tx"] != 0 || marks["closed-tx"] != 1 {
		t.Fatalf("a fold batch that failed before COMMIT left something durable: position %s (want %s), rows_applied %d, "+
			"marks %v, %d data rows — the data, marks and position must be one transaction", pos, f.pos(0).Token, rows, marks, data)
	}
	if f.la.fence.Admits([]applymarks.Mark{{TxID: "fold-tx"}}, "") {
		t.Fatal("a fold that never committed anchored its transaction")
	}

	f.la.laneCommitHook = nil
	if _, err := f.la.ApplyLaneBatch(ctx, 0, batch, ticket); err != nil {
		t.Fatalf("the fold batch did not commit: %v", err)
	}
	pos, rows, marks, data = f.state(ctx, t)
	if pos != f.pos(1).Token || rows != 7 || data != 2 || marks["fold-tx"] != 2 || marks["closed-tx"] != 0 {
		t.Fatalf("the committed fold batch left position %s (want %s), rows_applied %d (want 7), marks %v (want fold-tx:2, "+
			"closed-tx gone), %d data rows (want 2)", pos, f.pos(1).Token, rows, marks, data)
	}
	if !f.la.fence.Admits([]applymarks.Mark{{TxID: "fold-tx"}}, "") {
		t.Fatal("the committed fold did not anchor its transaction: later batches could not write its marks")
	}
}

// TestLaneApplyBatch_FoldCommitStepErrorIsOutcomeUnknown pins the engine half
// of amendment D's retry-semantics change on the MySQL lane write core: an
// error raised by the COMMIT of a fold batch reaches the orchestrator marked
// laneapply.CommitOutcomeUnknown, and the same failure of a batch with no
// ticket is not. The COMMIT-step error is real: the commit hook (which runs
// after every statement, the position included) kills the lane's connection
// from another session, so the COMMIT itself is what fails.
//
// Reach, stated: this pins the CLASSIFICATION only. A connection killed
// before COMMIT is a known rollback, not the committed-but-unacknowledged
// outcome the rule exists for; that shape is pinned on Postgres alone
// (TestLaneApplyBatch_FoldCommitStepErrorIsOutcomeUnknown there, a
// synchronous-standby wait abandoned by the watchdog). Building it on MySQL
// needs semi-synchronous replication with a replica that never acknowledges,
// which this suite does not run.
func TestLaneApplyBatch_FoldCommitStepErrorIsOutcomeUnknown(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	for _, withFold := range []bool{true, false} {
		t.Run(fmt.Sprintf("fold=%v", withFold), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			f := newFoldFixture(ctx, t, dsn)
			var ticket *laneapply.FoldTicket
			if withFold {
				ticket = &laneapply.FoldTicket{Tx: "fold-tx", Pos: f.pos(1), RowsApplied: 2}
			}
			f.la.fence.Open("fold-tx", !withFold)
			killed := 0
			f.la.laneCommitHook = func([]laneChange) error {
				// The lane's open transaction is the only session holding a
				// row lock on fold_items; kill every connection of ours that
				// is not this one, the lane's among them.
				rows, err := f.db.QueryContext(ctx, `SELECT ID FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID() AND USER = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)`)
				if err != nil {
					return err
				}
				var ids []int64
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						_ = rows.Close()
						return err
					}
					ids = append(ids, id)
				}
				_ = rows.Close()
				for _, id := range ids {
					if _, err := f.db.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", id)); err == nil {
						killed++
					}
				}
				return nil
			}
			_, err := f.la.ApplyLaneBatch(ctx, 0, []ir.Change{foldItem(1, "a", "fold-tx", 1), foldItem(2, "b", "fold-tx", 2)}, ticket)
			if killed == 0 {
				t.Fatal("the hook killed no connection: the COMMIT-step failure was never built")
			}
			if err == nil {
				t.Fatal("a COMMIT on a killed connection returned no error")
			}
			if got := laneapply.IsCommitOutcomeUnknown(err); got != withFold {
				t.Fatalf("IsCommitOutcomeUnknown = %v for a COMMIT-step error (fold %v); want %v: %v", got, withFold, withFold, err)
			}
		})
	}
}
