//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// errBarrierFoldHook is the injected pre-COMMIT failure of the barrier-fold
// atomicity pin.
var errBarrierFoldHook = errors.New("test: fail the barrier's transaction after its folded position is written")

// barrierFoldFixture is the lane adapter on a fresh target holding bf_keyless
// (no key: the keyless barrier) and bf_items (a primary key: the PK-change
// barrier), with the stream's position at pos(0), rows_applied 0, and one
// durable, loaded mark of transaction "closed-tx" — the T−1 the folded
// checkpoint closes.
type barrierFoldFixture struct {
	a   *ChangeApplier
	la  *laneApplierAdapter
	db  *sql.DB
	pos func(n int) ir.Position
}

func newBarrierFoldFixture(ctx context.Context, t *testing.T, dsn, setup string) barrierFoldFixture {
	t.Helper()
	applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS bf_keyless; DROP TABLE IF EXISTS bf_items; DROP TABLE IF EXISTS bf_absent;
		CREATE TABLE bf_keyless (v VARCHAR(32) NOT NULL) ENGINE=InnoDB;
		CREATE TABLE bf_items (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL) ENGINE=InnoDB;`+setup)
	opened, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	a := opened.(*ChangeApplier)
	t.Cleanup(func() { _ = a.Close() })
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	pos := func(n int) ir.Position {
		return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"file":"binlog.000001","pos":%d}`, 2000+n)}
	}
	if err := a.WritePosition(ctx, testStreamID, pos(0)); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, q := range []string{
		`DELETE FROM sluice_cdc_apply_marks`, `DELETE FROM sluice_cdc_schema_history`, `DELETE FROM sluice_cdc_skipped_tables`,
		`UPDATE sluice_cdc_state SET rows_applied = 0`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil && !strings.Contains(err.Error(), "doesn't exist") {
			t.Fatalf("reset %q: %v", q, err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES (?, 'target_db.bf_items', 'k-closed', 'closed-tx', 1, 'd', '')`, testStreamID); err != nil {
		t.Fatalf("plant the closed transaction's mark: %v", err)
	}
	if err := a.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	return barrierFoldFixture{a: a, la: &laneApplierAdapter{a: a, streamID: testStreamID}, db: db, pos: pos}
}

// state is the target's own view of what a barrier fold writes: the
// position, rows_applied, and the marks per transaction.
func (f barrierFoldFixture) state(ctx context.Context, t *testing.T) (pos string, rows int64, marks map[string]int) {
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
	if err := rs.Err(); err != nil {
		t.Fatalf("read marks: %v", err)
	}
	return pos, rows, marks
}

// probe renders the data a barrier kind changes, read from the target.
func (f barrierFoldFixture) probe(ctx context.Context, t *testing.T, query string) string {
	t.Helper()
	var out sql.NullString
	if err := f.db.QueryRowContext(ctx, query).Scan(&out); err != nil {
		t.Fatalf("probe %q: %v", query, err)
	}
	return out.String
}

// mysqlBarrierFoldKinds is every barrier kind of ADR-0190 amendment E §E.3
// that MySQL folds — all but Truncate (TestLaneBarrier_MySQLTruncateDeclines).
var mysqlBarrierFoldKinds = []struct {
	name      string
	setup     string
	change    ir.Change
	probe     string
	before    string
	after     string
	wantMarks int
}{{
	name:   "keyless insert",
	change: ir.Insert{Schema: "target_db", Table: "bf_keyless", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT CAST(COUNT(*) AS CHAR) FROM bf_keyless`, before: "0", after: "1", wantMarks: 1,
}, {
	name:   "keyless delete",
	setup:  `INSERT INTO bf_keyless VALUES ('y'), ('z');`,
	change: ir.Delete{Schema: "target_db", Table: "bf_keyless", Before: ir.Row{"v": "y"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT GROUP_CONCAT(v ORDER BY v) FROM bf_keyless`, before: "y,z", after: "z", wantMarks: 1,
}, {
	name:  "primary-key change",
	setup: `INSERT INTO bf_items VALUES (1, 'a');`,
	change: ir.Update{
		Schema: "target_db", Table: "bf_items", Before: ir.Row{"id": int64(1), "code": "a"}, After: ir.Row{"id": int64(2), "code": "a"},
		ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1},
	},
	probe: `SELECT GROUP_CONCAT(id) FROM bf_items`, before: "1", after: "2", wantMarks: 2,
}, {
	// No data and no mark; the probe is the skip ledger, flushed inside the
	// call on its own autocommit (H-4) — so the failed arm has already
	// counted the skip once, and the replay that commits counts it again:
	// the over-count §E.8 accepts, exactly as H-4 does on the serial path.
	name:   "absent-table skip",
	change: ir.Insert{Schema: "target_db", Table: "bf_absent", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT CAST(COALESCE(SUM(skip_count), 0) AS CHAR) FROM sluice_cdc_skipped_tables`, before: "1", after: "2", wantMarks: 0,
}, {
	name: "SchemaSnapshot",
	change: ir.SchemaSnapshot{Schema: "target_db", Table: "bf_items", IR: &ir.Table{
		Name:       "bf_items",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "code", Type: ir.Varchar{Length: 32}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}},
	probe:  `SELECT CAST(COUNT(*) AS CHAR) FROM sluice_cdc_schema_history WHERE table_name = 'bf_items'`,
	before: "0", after: "1", wantMarks: 0,
}}

// TestLaneBarrier_FoldIsOneTransaction is the Postgres pin's MySQL twin
// (ADR-0190 amendment E): for every kind MySQL folds, the barrier's data, its
// marks (and T−1's mark deletion) and the folded position are ONE target
// transaction — failed after the position is written, none of them is
// durable; committed, all are, with rows_applied moved by exactly the folded
// increment, T−1's marks gone and the barrier's own present. Each row also
// requires FoldsBarrierCheckpoint to answer true. The independent evidence
// is the target's own tables, read on a separate connection.
func TestLaneBarrier_FoldIsOneTransaction(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	for _, k := range mysqlBarrierFoldKinds {
		t.Run(k.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			f := newBarrierFoldFixture(ctx, t, dsn, k.setup)
			if !f.la.FoldsBarrierCheckpoint(k.change) {
				t.Fatalf("FoldsBarrierCheckpoint(%T) = false; MySQL runs it as one transaction (ADR-0190 amendment E §E.3)", k.change)
			}
			at := &laneapply.BarrierCheckpoint{Pos: f.pos(1), RowsApplied: 5, ClosedTxs: []string{"closed-tx"}}

			f.a.barrierFoldCommitHookForTest = func() error { return errBarrierFoldHook }
			if err := f.la.ApplyBarrierChange(ctx, k.change, at); !errors.Is(err, errBarrierFoldHook) {
				t.Fatalf("the injected failure did not surface: %v", err)
			}
			pos, rows, marks := f.state(ctx, t)
			if pos != f.pos(0).Token || rows != 0 || marks["bf-tx"] != 0 || marks["closed-tx"] != 1 {
				t.Fatalf("a barrier fold that failed before COMMIT left something durable: position %s (want %s), rows_applied %d, "+
					"marks %v — the data, marks and position must be one transaction", pos, f.pos(0).Token, rows, marks)
			}
			if got := f.probe(ctx, t, k.probe); got != k.before {
				t.Fatalf("after the failed barrier the target's data reads %q, want %q", got, k.before)
			}

			f.a.barrierFoldCommitHookForTest = nil
			if err := f.la.ApplyBarrierChange(ctx, k.change, at); err != nil {
				t.Fatalf("the barrier fold did not commit: %v", err)
			}
			pos, rows, marks = f.state(ctx, t)
			if pos != f.pos(1).Token || rows != 5 || marks["closed-tx"] != 0 || marks["bf-tx"] != k.wantMarks {
				t.Fatalf("the committed barrier fold left position %s (want %s), rows_applied %d (want exactly the folded 5), "+
					"marks %v (want bf-tx:%d, closed-tx gone)", pos, f.pos(1).Token, rows, marks, k.wantMarks)
			}
			if got := f.probe(ctx, t, k.probe); got != k.after {
				t.Fatalf("the committed barrier's data: %q, want %q", got, k.after)
			}
		})
	}
}

// TestLaneBarrier_MySQLTruncateDeclines binds the environmental premise the
// MySQL Truncate exclusion rests on to the engine's answer, in one test so
// the two cannot drift apart: a TRUNCATE inside a transaction survives that
// transaction's ROLLBACK (MySQL commits implicitly around DDL — the premise
// TestApplyOne_TruncateSurvivesTheRollback also proves), so
// FoldsBarrierCheckpoint(Truncate) must answer false, and a Truncate handed a
// checkpoint anyway refuses with BARRIER-FOLD-NOT-TRANSACTIONAL before it
// writes anything. The reverse direction is TestLaneBarrier_FoldIsOneTransaction's
// SchemaSnapshot row, which MySQL folds.
func TestLaneBarrier_MySQLTruncateDeclines(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := newBarrierFoldFixture(ctx, t, dsn, `INSERT INTO bf_keyless VALUES ('y'), ('z');`)

	tx, err := f.a.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "TRUNCATE TABLE bf_keyless"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_ = tx.Rollback()
	if got := f.probe(ctx, t, `SELECT CAST(COUNT(*) AS CHAR) FROM bf_keyless`); got != "0" {
		t.Fatalf("bf_keyless holds %s rows after a rolled-back TRUNCATE — MySQL did not commit the DDL implicitly. The premise "+
			"FoldsBarrierCheckpoint(Truncate)=false rests on is gone; re-examine ADR-0190 amendment E §E.3", got)
	}

	truncate := ir.Truncate{Schema: "target_db", Table: "bf_keyless"}
	if f.la.FoldsBarrierCheckpoint(truncate) {
		t.Fatal("FoldsBarrierCheckpoint(Truncate) = true on MySQL: the statements after an implicitly-committed TRUNCATE would " +
			"each autocommit, and a crash between the closed-mark DELETE and the position could duplicate a keyless row silently")
	}
	applyMySQLApplier(t, dsn, `INSERT INTO bf_keyless VALUES ('y'), ('z');`)
	err = f.la.ApplyBarrierChange(ctx, truncate, &laneapply.BarrierCheckpoint{Pos: f.pos(1), RowsApplied: 5, ClosedTxs: []string{"closed-tx"}})
	if err == nil || !strings.Contains(err.Error(), laneapply.BarrierFoldNotTransactionalMarker) {
		t.Fatalf("a Truncate handed a checkpoint returned %v; want the %s refusal", err, laneapply.BarrierFoldNotTransactionalMarker)
	}
	pos, rows, marks := f.state(ctx, t)
	if got := f.probe(ctx, t, `SELECT CAST(COUNT(*) AS CHAR) FROM bf_keyless`); got != "2" || pos != f.pos(0).Token || rows != 0 || marks["closed-tx"] != 1 {
		t.Fatalf("the refused Truncate wrote something: %s rows (want 2), position %s (want %s), rows_applied %d, marks %v",
			got, pos, f.pos(0).Token, rows, marks)
	}
}

// TestLaneBarrier_SkippedBarrierStillPersistsItsCheckpoint is the Postgres
// pin's MySQL twin: a barrier the apply marks prove already applied writes
// nothing of its own, but persists a checkpoint it was handed; with none, it
// writes nothing at all. See the twin for why the shape carries no
// ClosedTxs.
func TestLaneBarrier_SkippedBarrierStillPersistsItsCheckpoint(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := newBarrierFoldFixture(ctx, t, dsn, "")
	if _, err := f.db.ExecContext(ctx, `DELETE FROM sluice_cdc_apply_marks`); err != nil {
		t.Fatalf("clear marks: %v", err)
	}
	barrier := ir.Insert{Schema: "target_db", Table: "bf_keyless", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}}
	if err := f.a.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	if err := f.la.ApplyBarrierChange(ctx, barrier, nil); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	for _, arm := range []struct {
		at       *laneapply.BarrierCheckpoint
		wantPos  string
		wantRows int64
	}{
		{nil, f.pos(0).Token, 0},
		{&laneapply.BarrierCheckpoint{Pos: f.pos(1), RowsApplied: 2}, f.pos(1).Token, 2},
	} {
		if err := f.a.startApplyMarks(ctx, testStreamID); err != nil {
			t.Fatalf("startApplyMarks: %v", err)
		}
		if err := f.la.ApplyBarrierChange(ctx, barrier, arm.at); err != nil {
			t.Fatalf("at=%v: re-delivered barrier: %v", arm.at != nil, err)
		}
		if got := f.probe(ctx, t, `SELECT CAST(COUNT(*) AS CHAR) FROM bf_keyless`); got != "1" {
			t.Fatalf("at=%v: bf_keyless holds %s rows; the marks must have skipped the re-delivered barrier (want 1) — "+
				"the test is not reaching the skip path", arm.at != nil, got)
		}
		pos, rows, marks := f.state(ctx, t)
		if pos != arm.wantPos || rows != arm.wantRows || marks["bf-tx"] != 1 {
			t.Fatalf("at=%v: the skipped barrier left position %s (want %s), rows_applied %d (want %d), marks %v (want bf-tx:1) — "+
				"a skipped barrier must still persist the checkpoint it was handed, and only that", arm.at != nil, pos, arm.wantPos,
				rows, arm.wantRows, marks)
		}
	}
}
