//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/applyorder"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// TestWriteCoreStatementOrder is the MySQL GC-41 (c) pin's Postgres twin:
// every write core — the pipelined AND serial-fallback batch handles and
// lane paths included — runs through connections whose pgx tracer records
// each transaction's statements in the order they reached the server (a
// pipelined batch's statements in queue order), and no committed
// transaction may send a row statement after its first control-table
// statement. On one Postgres server the commit is atomic and the order is
// moot; the pin exists so the order stays right for a target where it is
// not (the Neki UNVERIFIED PREMISE on writePositionTx). The set of cores
// comes from the AST roster (writeCoreClass), and each run must produce the
// mixed transactions it is built to — a floor PER RUN, not per core (the
// lanes run drives four cores and proves its floor in aggregate).
func TestWriteCoreStatementOrder(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE wco_items (id BIGINT PRIMARY KEY, code TEXT NOT NULL UNIQUE);
		CREATE TABLE wco_log (v TEXT NOT NULL);`)
	defer applyPGApplier(t, dsn, `DROP TABLE IF EXISTS wco_items; DROP TABLE IF EXISTS wco_log;`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	a := opened.(*ChangeApplier)
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	rec := &applyorder.Recorder{}
	recDB := openTracedDB(t, dsn, rec, false)
	recPipe := openTracedDB(t, dsn, rec, true)
	plain, plainPipe, plainCfg := a.db, a.pipelineDB, a.pipelineCfg
	defer func() {
		_ = recDB.Close()
		_ = recPipe.Close()
		_ = plain.Close()
		if plainPipe != nil {
			_ = plainPipe.Close()
		}
	}()
	a.db, a.pipelineDB = recDB, recPipe

	id := int64(0)
	pos := func() ir.Position {
		id++
		return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x3000000+id*0x100)}
	}
	// item is a marked-class change (a secondary unique constraint), so every
	// applied one writes an apply mark with its row.
	item := func(tx string, seq uint64) ir.Change {
		id++
		return ir.Insert{
			Schema: "public", Table: "wco_items", Row: ir.Row{"id": id, "code": fmt.Sprintf("c%d", id)},
			Position: pos(), ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	logRow := func(tx string, seq uint64) ir.Change {
		id++
		return ir.Insert{
			Schema: "public", Table: "wco_log", Row: ir.Row{"v": fmt.Sprintf("v%d", id)},
			Position: pos(), ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	snapshot := func() ir.Change {
		return ir.SchemaSnapshot{Position: pos(), Schema: "public", Table: "wco_items", IR: &ir.Table{
			Name: "wco_items",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "code", Type: ir.Text{}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}}
	}
	feed := func(cs ...ir.Change) <-chan ir.Change {
		ch := make(chan ir.Change, len(cs))
		for _, c := range cs {
			ch <- c
		}
		close(ch)
		return ch
	}
	// batch drives the batch loop at size 3 over a four-row source
	// transaction (the row cap trips mid-transaction — the Commit hook — and
	// the TxCommit arrives with rows queued — the WritePosition hook), then
	// a keyless row, then a marker-less row whose SchemaSnapshot rides the
	// same batch (the history row after the data, then the position).
	batch := func(tag string) error {
		p1, p2 := pos(), pos()
		return a.ApplyBatch(ctx, testStreamID, feed(
			ir.TxBegin{Position: p1}, item(tag+"-1", 1), item(tag+"-1", 2), item(tag+"-1", 3), item(tag+"-1", 4), ir.TxCommit{Position: p1},
			ir.TxBegin{Position: p2}, logRow(tag+"-2", 1), ir.TxCommit{Position: p2},
			item(tag+"-3", 1), snapshot(),
		), 3)
	}
	lane := &laneApplierAdapter{a: a, streamID: testStreamID, laneDB: recPipe}

	type run struct {
		name      string
		cores     []string
		wantMixed int // mixed transactions the run must produce; 0 = control-only
		do        func() error
		// foldLast: every committed mixed transaction must end with the
		// position — the ADR-0190 amendment D fold writes it LAST, so the
		// stream's control row is locked only across COMMIT.
		foldLast bool
	}
	runs := []run{{
		name:      "serial",
		cores:     []string{"serial", "serial-tx-commit-position"},
		wantMixed: 3,
		do: func() error {
			p := pos()
			return a.Apply(ctx, testStreamID, feed(
				item("s-1", 1),
				ir.TxBegin{Position: p}, item("s-2", 1), logRow("s-2", 2), ir.TxCommit{Position: p},
				snapshot(),
			))
		},
	}, {
		name:  "bare-write-position",
		cores: []string{"bare-write-position"},
		do:    func() error { return a.WritePosition(ctx, testStreamID, pos()) },
	}, {
		name:      "batch (pipelined)",
		cores:     []string{"batch"},
		wantMixed: 4,
		do:        func() error { return batch("bp") },
	}, {
		// No pipelined pool: the batch loop's BeginTx falls back to the
		// serial *sql.Tx handle, whose WritePosition / Commit arms are a
		// separate branch of the same closures.
		name:      "batch (serial fallback)",
		cores:     []string{"batch"},
		wantMixed: 4,
		do: func() error {
			a.pipelineDB, a.pipelineCfg = nil, nil
			defer func() { a.pipelineDB, a.pipelineCfg = recPipe, plainCfg }()
			return batch("bs")
		},
	}, {
		// Lane transactions under --exactly-once-lanes with the mark fence
		// open (the only shape in which a lane writes marks): pipelined and
		// serial, then the checkpoint that closes them (control only), then
		// a barrier change (keyless: applyOneImpl with no position).
		name:      "lanes",
		cores:     []string{"lane-batch", "lane-batch-serial", "lane-checkpoint", "lane-barrier"},
		wantMixed: 3,
		do: func() error {
			if err := a.startApplyMarks(ctx, testStreamID); err != nil {
				return err
			}
			a.exactlyOnceLanes = true
			defer func() { a.exactlyOnceLanes = false }()
			lane.fence.Open("l-1", true)
			if _, err := lane.ApplyLaneBatch(ctx, 0, []ir.Change{item("l-1", 1), item("l-1", 2)}, nil); err != nil {
				return err
			}
			if _, err := lane.applyLaneBatchSerial(ctx, []ir.Change{item("l-1", 3)}, nil); err != nil {
				return err
			}
			if err := lane.WriteCheckpoint(ctx, pos(), 3, []string{"l-1"}); err != nil {
				return err
			}
			return lane.ApplyBarrierChange(ctx, logRow("l-2", 1))
		},
	}, {
		// Fold batches (ADR-0190 amendment D) on both lane write cores: the
		// fenced transaction's rows, its marks with the closed transaction's
		// deleted, then the anchor position — data first, position last.
		name:      "lane folds",
		cores:     []string{"lane-batch", "lane-batch-serial"},
		wantMixed: 2,
		foldLast:  true,
		do: func() error {
			if err := a.startApplyMarks(ctx, testStreamID); err != nil {
				return err
			}
			a.exactlyOnceLanes = true
			defer func() { a.exactlyOnceLanes = false }()
			lane.fence.Open("l-3", false)
			if _, err := lane.ApplyLaneBatch(ctx, 0, []ir.Change{item("l-3", 1), item("l-3", 2)},
				&laneapply.FoldTicket{Tx: "l-3", Pos: pos(), RowsApplied: 1, ClosedTxs: []string{"l-2"}}); err != nil {
				return err
			}
			lane.fence.Open("l-4", false)
			_, err := lane.applyLaneBatchSerial(ctx, []ir.Change{item("l-4", 1)},
				&laneapply.FoldTicket{Tx: "l-4", Pos: pos(), RowsApplied: 2, ClosedTxs: []string{"l-3"}})
			return err
		},
	}}

	driven := map[string]bool{}
	for _, r := range runs {
		rec.Take()
		if err := r.do(); err != nil {
			t.Fatalf("%s: %v", r.name, err)
		}
		mixed, control := 0, 0
		for _, tx := range rec.Take() {
			if !tx.Committed {
				continue
			}
			if v := tx.Violation(); v != "" {
				t.Errorf("%s: a committed transaction sent a row after a control-table statement (GC-41 (c)): %s\n  statements: %q", r.name, v, tx.Stmts)
			}
			if tx.Mixed() {
				mixed++
			}
			if tx.Has(applyorder.Control) {
				control++
			}
			if r.foldLast && tx.Mixed() {
				if last := tx.Stmts[len(tx.Stmts)-1]; !strings.Contains(last, "sluice_cdc_state") {
					t.Errorf("%s: a fold transaction did not write the position last (it ended with %q)\n  statements: %q", r.name, last, tx.Stmts)
				}
			}
		}
		if mixed < r.wantMixed {
			t.Errorf("%s: %d mixed data+control transactions; the run is built to produce %d — it is not exercising the order under test", r.name, mixed, r.wantMixed)
		}
		if control == 0 {
			t.Errorf("%s: no transaction wrote a control table — the run is not reaching its core", r.name)
		}
		for _, c := range r.cores {
			driven[c] = true
		}
	}
	cores := applyorder.Cores(writeCoreClass)
	for c := range cores {
		if !driven[c] {
			t.Errorf("write core %q (writeCoreClass) is driven by no run here — add one", c)
		}
	}
	for c := range driven {
		if !cores[c] {
			t.Errorf("run claims core %q, which writeCoreClass does not name — stale", c)
		}
	}
}

// openTracedDB opens dsn with a pgx tracer reporting every transaction's
// statements to rec — simple queries and each queued statement of a
// pipelined batch alike. describeExec builds the pipelined-pool shape
// (openPgxDBDescribeExec's exec mode). The connection stays a *stdlib.Conn,
// so the pipelined raw-conn escape still works through it.
func openTracedDB(t *testing.T, dsn string, rec *applyorder.Recorder, describeExec bool) *sql.DB {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.Tracer = orderTracer{rec: rec}
	if describeExec {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	}
	return stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(afterConnectSessionPins))
}

// orderTracer maps pgx's trace hooks onto an applyorder.Recorder, keyed by
// connection: BEGIN opens a transaction, COMMIT / ROLLBACK close it, and
// every other statement — including each statement of a batch — is recorded
// in the order pgx reports it.
type orderTracer struct{ rec *applyorder.Recorder }

func (tr orderTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	s := strings.ToLower(strings.TrimSpace(data.SQL))
	switch {
	case strings.HasPrefix(s, "begin"):
		tr.rec.Begin(conn)
	case strings.HasPrefix(s, "commit"):
		tr.rec.End(conn, true)
	case strings.HasPrefix(s, "rollback"):
		tr.rec.End(conn, false)
	default:
		tr.rec.Statement(conn, data.SQL)
	}
	return ctx
}

func (orderTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (orderTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (tr orderTracer) TraceBatchQuery(_ context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	tr.rec.Statement(conn, data.SQL)
}

func (orderTracer) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}
