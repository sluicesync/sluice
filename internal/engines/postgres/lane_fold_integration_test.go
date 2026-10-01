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

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// errFoldHook is the injected pre-commit failure of the atomicity pin.
var errFoldHook = errors.New("test: fail the fold batch after the position is queued")

// laneFoldCores are the two Postgres lane write cores a fold batch runs on:
// the pipelined path (ApplyLaneBatch) and the serial fall-back
// (applyLaneBatchSerial, which errPipelineUnavailable routes to; driven
// directly here).
var laneFoldCores = []struct {
	name  string
	apply func(ctx context.Context, la *laneApplierAdapter, batch []ir.Change, fold *laneapply.FoldTicket) error
}{
	{"pipelined", func(ctx context.Context, la *laneApplierAdapter, batch []ir.Change, fold *laneapply.FoldTicket) error {
		_, err := la.ApplyLaneBatch(ctx, 0, batch, fold)
		return err
	}},
	{"serial-fallback", func(ctx context.Context, la *laneApplierAdapter, batch []ir.Change, fold *laneapply.FoldTicket) error {
		_, err := la.applyLaneBatchSerial(ctx, batch, fold)
		return err
	}},
}

// foldFixture is one lane adapter under --exactly-once-lanes on a fresh
// target with table fold_items (a secondary-unique table, so its changes
// write marks), the stream's position at p0 with rows_applied 0, and one
// durable mark of a transaction "closed-tx" the fold's ticket closes.
type foldFixture struct {
	a   *ChangeApplier
	la  *laneApplierAdapter
	db  *sql.DB
	pos func(n int) ir.Position
}

func newFoldFixture(ctx context.Context, t *testing.T, dsn, ddl string) foldFixture {
	t.Helper()
	applyPGApplier(t, dsn, `DROP TABLE IF EXISTS fold_items; `+ddl)
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
		return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x5000000+n*0x100)}
	}
	if err := a.WritePosition(ctx, testStreamID, pos(0)); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `DELETE FROM sluice_cdc_apply_marks; UPDATE sluice_cdc_state SET rows_applied = 0`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES ($1, 'public.fold_items', 'k-closed', 'closed-tx', 1, 'd', '')`, testStreamID); err != nil {
		t.Fatalf("plant the closed transaction's mark: %v", err)
	}
	if err := a.startApplyMarks(ctx, testStreamID); err != nil {
		t.Fatalf("startApplyMarks: %v", err)
	}
	a.exactlyOnceLanes = true
	laneDB, err := a.pipelinePool()
	if err != nil {
		t.Fatalf("pipelinePool: %v", err)
	}
	return foldFixture{a: a, la: &laneApplierAdapter{a: a, streamID: testStreamID, laneDB: laneDB}, db: db, pos: pos}
}

// state is the target's view of everything a fold writes: the position, the
// rows_applied counter, the marks per transaction, and the data rows.
func (f foldFixture) state(ctx context.Context, t *testing.T) (pos string, rows int64, marks map[string]int, data int) {
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
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fold_items`).Scan(&data); err != nil {
		t.Fatalf("count data: %v", err)
	}
	return pos, rows, marks, data
}

func foldItem(id int64, code, tx string, seq uint64) ir.Change {
	return ir.Insert{
		Schema: "public", Table: "fold_items", Row: ir.Row{"id": id, "code": code},
		ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
	}
}

// TestLaneApplyBatch_FoldIsOneTransaction pins ADR-0190 amendment D's
// atomicity on each Postgres lane write core: a fold batch's data, its marks
// (and the closed transaction's mark deletion) and the anchor position are
// ONE target transaction. Failed after the position is queued, nothing of
// the three is on the target; committed, all three are, and rows_applied
// moved by exactly the ticket's increment. The independent evidence is the
// target's own tables, read on a separate connection.
func TestLaneApplyBatch_FoldIsOneTransaction(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	for _, core := range laneFoldCores {
		t.Run(core.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			f := newFoldFixture(ctx, t, dsn, `CREATE TABLE fold_items (id BIGINT PRIMARY KEY, code TEXT NOT NULL UNIQUE)`)
			batch := []ir.Change{foldItem(1, "a", "fold-tx", 1), foldItem(2, "b", "fold-tx", 2)}
			ticket := &laneapply.FoldTicket{Tx: "fold-tx", Pos: f.pos(1), RowsApplied: 7, ClosedTxs: []string{"closed-tx"}}
			f.la.fence.Open("fold-tx", false)

			f.la.laneCommitHook = func([]laneChange) error { return errFoldHook }
			if err := core.apply(ctx, f.la, batch, ticket); !errors.Is(err, errFoldHook) {
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
			if err := core.apply(ctx, f.la, batch, ticket); err != nil {
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
		})
	}
}

// TestLaneApplyBatch_FoldCommitStepErrorIsOutcomeUnknown pins the engine half
// of amendment D's retry-semantics change on each Postgres lane write core:
// an error raised by the COMMIT of a fold batch reaches the orchestrator
// marked laneapply.CommitOutcomeUnknown (which then fails the run rather than
// retrying in place — TestLaneApplyBatch_FoldCommitOutcomeUnknownIsFatal),
// while the same failure of a batch with no ticket is not.
//
// The COMMIT-step error is the real shape the rule exists for, a commit
// whose outcome the client cannot know: synchronous_standby_names names a
// standby that does not exist, so every COMMIT is flushed locally and then
// waits for an acknowledgement that never comes, until the applier's
// per-exec watchdog (Bug 56) gives up. The transaction HAS committed — the
// test reads the fold's position and its one rows_applied increment back —
// which is exactly why an in-place retry would count those rows twice.
func TestLaneApplyBatch_FoldCommitStepErrorIsOutcomeUnknown(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = admin.Close() }()
	setStandby := func(names string) {
		t.Helper()
		if _, err := admin.Exec(`ALTER SYSTEM SET synchronous_standby_names = '` + names + `'`); err != nil {
			t.Fatalf("set synchronous_standby_names: %v", err)
		}
		if _, err := admin.Exec(`SELECT pg_reload_conf()`); err != nil {
			t.Fatalf("reload: %v", err)
		}
		time.Sleep(500 * time.Millisecond) // the reload is asynchronous
	}
	for _, core := range laneFoldCores {
		for _, withFold := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/fold=%v", core.name, withFold), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				f := newFoldFixture(ctx, t, dsn, `CREATE TABLE fold_items (id BIGINT PRIMARY KEY, code TEXT NOT NULL UNIQUE)`)
				f.a.execTimeout = 2 * time.Second
				var ticket *laneapply.FoldTicket
				if withFold {
					ticket = &laneapply.FoldTicket{Tx: "fold-tx", Pos: f.pos(1), RowsApplied: 2}
				}
				f.la.fence.Open("fold-tx", !withFold)
				setStandby("sluice_no_such_standby")
				defer setStandby("") // releases the orphaned COMMIT waits too
				err := core.apply(ctx, f.la, []ir.Change{foldItem(1, "a", "fold-tx", 1), foldItem(2, "b", "fold-tx", 2)}, ticket)
				if err == nil {
					t.Fatal("a COMMIT waiting on a standby that never acknowledges returned no error")
				}
				if got := laneapply.IsCommitOutcomeUnknown(err); got != withFold {
					t.Fatalf("IsCommitOutcomeUnknown = %v for a COMMIT-step error (fold %v); want %v: %v", got, withFold, withFold, err)
				}
				setStandby("")
				if !withFold {
					return
				}
				// The outcome the lane could not know: the commit landed.
				deadline := time.Now().Add(30 * time.Second)
				for {
					pos, rows, _, data := f.state(ctx, t)
					if pos == f.pos(1).Token && rows == 2 && data == 2 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("the fold's COMMIT never became visible (position %s, rows_applied %d, %d rows): the test did not "+
							"build the committed-but-unacknowledged shape it is about", pos, rows, data)
					}
					time.Sleep(200 * time.Millisecond)
				}
			})
		}
	}
}
