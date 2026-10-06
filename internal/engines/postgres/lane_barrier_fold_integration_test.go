//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	applyPGApplier(t, dsn, `DROP TABLE IF EXISTS bf_keyless; DROP TABLE IF EXISTS bf_items; DROP TABLE IF EXISTS bf_absent;
		CREATE TABLE bf_keyless (v TEXT NOT NULL); CREATE TABLE bf_items (id BIGINT PRIMARY KEY, code TEXT NOT NULL);`+setup)
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
		return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x6000000+n*0x100)}
	}
	if err := a.WritePosition(ctx, testStreamID, pos(0)); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `DELETE FROM sluice_cdc_apply_marks; DELETE FROM sluice_cdc_schema_history;
		UPDATE sluice_cdc_state SET rows_applied = 0`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES ($1, 'public.bf_items', 'k-closed', 'closed-tx', 1, 'd', '')`, testStreamID); err != nil {
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
	if err := f.db.QueryRowContext(ctx, `SELECT source_position, rows_applied FROM sluice_cdc_state WHERE stream_id = $1`, testStreamID).
		Scan(&pos, &rows); err != nil {
		t.Fatalf("read the control row: %v", err)
	}
	marks = map[string]int{}
	rs, err := f.db.QueryContext(ctx, `SELECT tx_id, COUNT(*) FROM sluice_cdc_apply_marks WHERE stream_id = $1 GROUP BY tx_id`, testStreamID)
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
	var out string
	if err := f.db.QueryRowContext(ctx, query).Scan(&out); err != nil {
		t.Fatalf("probe %q: %v", query, err)
	}
	return out
}

// pgBarrierFoldKinds is every barrier kind of ADR-0190 amendment E §E.3 —
// all IN on Postgres — with the data it touches and the marks it writes.
var pgBarrierFoldKinds = []struct {
	name      string
	setup     string // extra target DDL/DML before the barrier
	change    ir.Change
	probe     string // a query rendering the data the change touches
	before    string // probe before the change applied
	after     string // probe once it committed
	wantMarks int    // the barrier's own apply marks once committed
}{{
	name:   "keyless insert",
	change: ir.Insert{Schema: "public", Table: "bf_keyless", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT COUNT(*)::text FROM bf_keyless`, before: "0", after: "1", wantMarks: 1,
}, {
	name:   "keyless delete",
	setup:  `INSERT INTO bf_keyless VALUES ('y'), ('z');`,
	change: ir.Delete{Schema: "public", Table: "bf_keyless", Before: ir.Row{"v": "y"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT string_agg(v, ',' ORDER BY v) FROM bf_keyless`, before: "y,z", after: "z", wantMarks: 1,
}, {
	name:  "primary-key change",
	setup: `INSERT INTO bf_items VALUES (1, 'a');`,
	change: ir.Update{
		Schema: "public", Table: "bf_items", Before: ir.Row{"id": int64(1), "code": "a"}, After: ir.Row{"id": int64(2), "code": "a"},
		ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1},
	},
	probe: `SELECT string_agg(id::text, ',') FROM bf_items`, before: "1", after: "2", wantMarks: 2,
}, {
	// No data and no mark; the probe is the skip ledger, flushed inside the
	// call on its own autocommit (H-4) — so the failed arm has already
	// counted the skip once ("before" is read after it), and the replay that
	// commits counts it again: the over-count §E.8 accepts, exactly as H-4
	// accepts it on the serial path (a counter, re-counted on a replay).
	name:   "absent-table skip",
	change: ir.Insert{Schema: "public", Table: "bf_absent", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}},
	probe:  `SELECT COALESCE(SUM(skip_count), 0)::text FROM sluice_cdc_skipped_tables`, before: "1", after: "2", wantMarks: 0,
}, {
	name: "SchemaSnapshot",
	change: ir.SchemaSnapshot{Schema: "public", Table: "bf_items", IR: &ir.Table{
		Name:       "bf_items",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "code", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}},
	probe:  `SELECT COUNT(*)::text FROM sluice_cdc_schema_history WHERE table_name = 'bf_items'`,
	before: "0", after: "1", wantMarks: 0,
}, {
	// The transactional-TRUNCATE premise this amendment adds (E-Q2), bound
	// in one test to FoldsBarrierCheckpoint(Truncate)=true below.
	name:   "Truncate",
	setup:  `INSERT INTO bf_keyless VALUES ('y'), ('z');`,
	change: ir.Truncate{Schema: "public", Table: "bf_keyless"},
	probe:  `SELECT COUNT(*)::text FROM bf_keyless`, before: "2", after: "0", wantMarks: 0,
}}

// TestLaneBarrier_FoldIsOneTransaction pins ADR-0190 amendment E's atomicity
// on Postgres, for every barrier kind: the barrier's data, its marks (and
// T−1's mark deletion), the skip ledger's flush and the folded position are
// ONE target transaction. Failed after the position is written, the data,
// the marks (T−1's still present), the position and rows_applied are all
// unchanged; committed, all land, rows_applied moved by exactly the folded
// increment, T−1's marks are gone and the barrier's own are present — the
// mark a CloseOpen in place of CloseTxs would drop. Each kind's row also
// requires FoldsBarrierCheckpoint to answer true for it, so the Truncate
// row binds the transactional-TRUNCATE premise to the engine's answer: the
// two cannot drift apart. The independent evidence is the target's own
// tables, read on a separate connection.
//
// The absent-table skip writes no data and no mark; its probe is the skip
// ledger, which is NOT part of the transaction (see its row).
func TestLaneBarrier_FoldIsOneTransaction(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	for _, k := range pgBarrierFoldKinds {
		t.Run(k.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			f := newBarrierFoldFixture(ctx, t, dsn, k.setup)
			if !f.la.FoldsBarrierCheckpoint(k.change) {
				t.Fatalf("FoldsBarrierCheckpoint(%T) = false; every kind is transactional on Postgres (ADR-0190 amendment E §E.3)", k.change)
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

// TestLaneBarrier_SkippedBarrierStillPersistsItsCheckpoint pins the skip
// path's half of the contract: a barrier the apply marks prove already
// applied writes nothing of its own, but a checkpoint it was handed is still
// persisted — the coordinator records it written when the call returns. The
// reverse direction is the same call with no checkpoint, which writes
// nothing at all.
//
// The shape is the one production reaches: a restart whose first
// re-delivered transaction's barrier is skipped on its own mark, folding the
// only anchor that can precede it — a GC-41 (j) keepalive boundary, which
// closes no transaction (so its ClosedTxs is empty; a non-empty one would
// mean an earlier transaction of the run, and then the barrier's marks could
// not be the run's trusted first). rows_applied is non-zero here to grade
// the contract ("a non-nil at is persisted"), not because a keepalive
// carries rows.
func TestLaneBarrier_SkippedBarrierStillPersistsItsCheckpoint(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f := newBarrierFoldFixture(ctx, t, dsn, "")
	if _, err := f.db.ExecContext(ctx, `DELETE FROM sluice_cdc_apply_marks`); err != nil {
		t.Fatalf("clear marks: %v", err)
	}
	barrier := ir.Insert{Schema: "public", Table: "bf_keyless", Row: ir.Row{"v": "x"}, ApplyID: ir.ApplyID{TxID: "bf-tx", Seq: 1}}
	// The previous run: the barrier applied and its mark is durable; the
	// position stayed at the transaction's start.
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
		// The restart: the marks reload, and the barrier is re-delivered first.
		if err := f.a.startApplyMarks(ctx, testStreamID); err != nil {
			t.Fatalf("startApplyMarks: %v", err)
		}
		if err := f.la.ApplyBarrierChange(ctx, barrier, arm.at); err != nil {
			t.Fatalf("at=%v: re-delivered barrier: %v", arm.at != nil, err)
		}
		if got := f.probe(ctx, t, `SELECT COUNT(*)::text FROM bf_keyless`); got != "1" {
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
