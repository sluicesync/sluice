//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/applyorder"
	"sluicesync.dev/sluice/internal/ir"
)

// TestWriteCoreStatementOrder is GC-41 (c)'s per-core pin on a real MySQL:
// every write core runs through a connection that records each
// transaction's statements in the order the driver sent them, and no
// committed transaction may send a row statement after its first
// control-table statement (the DATA BEFORE CONTROL rule on writePositionTx).
// v0.156.5's batch WritePosition and Commit hooks sent the apply marks
// before flushing the coalesced rows, and under a vtgate MULTI target with a
// --control-keyspace sidecar that order lost the rows of a torn commit.
//
// The cores are not a hand list: the set this test must drive is
// Cores(writeCoreClass), which TestWriteCoreRoster_EveryControlWriterCallerIsClassified
// derives from the package's AST, and a core the roster names but no run
// below claims fails here. Each mixing core must also produce at least the
// mixed (data + control) transactions its run is built to produce — the
// shape the rule can be broken in — so a run that silently stopped writing
// marks cannot pass vacuously; each control-only core must write control
// and no data.
//
// Classification is applyorder.Classify's (by text; its reach is in the
// package doc). The Postgres twin lives in that package.
func TestWriteCoreStatementOrder(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `CREATE TABLE wco_items (id BIGINT NOT NULL PRIMARY KEY, code VARCHAR(32) NOT NULL, UNIQUE KEY (code)) ENGINE=InnoDB;
		CREATE TABLE wco_log (v VARCHAR(32) NOT NULL) ENGINE=InnoDB;`)
	defer applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS wco_items; DROP TABLE IF EXISTS wco_log;`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	opened, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	a := opened.(*ChangeApplier)
	defer func() { _ = a.Close() }()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	rec := &applyorder.Recorder{}
	recDB := openRecordingDB(t, dsn, rec)
	defer func() { _ = recDB.Close() }()
	plain := a.db
	defer func() { _ = plain.Close() }()
	a.db = recDB

	id := int64(0)
	pos := func() ir.Position {
		id++
		return ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf(`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-%d"}`, 1000+id)}
	}
	// item is a marked-class change (a secondary unique index), so every
	// applied one writes an apply mark with its row.
	item := func(tx string, seq uint64) ir.Change {
		id++
		return ir.Insert{
			Schema: "target_db", Table: "wco_items", Row: ir.Row{"id": id, "code": fmt.Sprintf("c%d", id)},
			Position: pos(), ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	logRow := func(tx string, seq uint64) ir.Change {
		id++
		return ir.Insert{
			Schema: "target_db", Table: "wco_log", Row: ir.Row{"v": fmt.Sprintf("v%d", id)},
			Position: pos(), ApplyID: ir.ApplyID{TxID: tx, Seq: seq},
		}
	}
	feed := func(cs ...ir.Change) <-chan ir.Change {
		ch := make(chan ir.Change, len(cs))
		for _, c := range cs {
			ch <- c
		}
		close(ch)
		return ch
	}
	lane := &laneApplierAdapter{a: a, streamID: testStreamID, laneDB: recDB}

	type run struct {
		cores     []string
		wantMixed int // mixed transactions the run must produce; 0 = control-only
		do        func() error
	}
	runs := []run{{
		// Apply: a change outside a source transaction (data + marks +
		// position), a transaction's changes (data + marks each, position
		// deferred), its TxCommit (persistSourceTxCommit: marks GC +
		// position, control only) and a SchemaSnapshot (history + position).
		cores:     []string{"serial", "serial-tx-commit-position"},
		wantMixed: 3,
		do: func() error {
			p := pos()
			return a.Apply(ctx, testStreamID, feed(
				item("s-1", 1),
				ir.TxBegin{Position: p}, item("s-2", 1), logRow("s-2", 2), ir.TxCommit{Position: p},
				ir.SchemaSnapshot{Position: pos(), Schema: "target_db", Table: "wco_items", IR: &ir.Table{
					Name: "wco_items",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
						{Name: "code", Type: ir.Varchar{Length: 32}},
					},
					PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
				}},
			))
		},
	}, {
		cores: []string{"bare-write-position"},
		do:    func() error { return a.WritePosition(ctx, testStreamID, pos()) },
	}, {
		// The serial batch loop at batch size 3 over a four-row source
		// transaction: the row cap trips mid-transaction (the Commit hook —
		// a flush with no position, marks written with the coalesced rows),
		// and the TxCommit then arrives with coalesced rows still pending
		// (the WritePosition hook — the v0.156.5 order put the marks ahead
		// of exactly those rows). The keyless row of the second transaction
		// takes the batch-of-1 path.
		cores:     []string{"batch"},
		wantMixed: 3,
		do: func() error {
			p1, p2 := pos(), pos()
			return a.ApplyBatch(ctx, testStreamID, feed(
				ir.TxBegin{Position: p1}, item("b-1", 1), item("b-1", 2), item("b-1", 3), item("b-1", 4), ir.TxCommit{Position: p1},
				ir.TxBegin{Position: p2}, logRow("b-2", 1), ir.TxCommit{Position: p2},
			), 3)
		},
	}, {
		// One lane transaction under --exactly-once-lanes with its mark
		// fence open (the only shape in which a lane writes marks), then the
		// frontier checkpoint that closes it (control only), then a barrier
		// change (keyless: applyOneImpl with no position).
		cores:     []string{"lane-batch", "lane-checkpoint", "lane-barrier"},
		wantMixed: 2,
		do: func() error {
			if err := a.startApplyMarks(ctx, testStreamID); err != nil {
				return err
			}
			a.exactlyOnceLanes = true
			defer func() { a.exactlyOnceLanes = false }()
			lane.fence.Open("l-1")
			if _, err := lane.ApplyLaneBatch(ctx, 0, []ir.Change{item("l-1", 1), item("l-1", 2)}); err != nil {
				return err
			}
			if err := lane.WriteCheckpoint(ctx, pos(), 2, []string{"l-1"}); err != nil {
				return err
			}
			return lane.ApplyBarrierChange(ctx, logRow("l-2", 1))
		},
	}}

	driven := map[string]bool{}
	for _, r := range runs {
		name := strings.Join(r.cores, "+")
		rec.Take()
		if err := r.do(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		mixed, control := 0, 0
		for _, tx := range rec.Take() {
			if !tx.Committed {
				continue
			}
			if v := tx.Violation(); v != "" {
				t.Errorf("%s: a committed transaction sent a row after a control-table statement (GC-41 (c)): %s\n  statements: %q", name, v, tx.Stmts)
			}
			if tx.Mixed() {
				mixed++
			}
			if tx.Has(applyorder.Control) {
				control++
			}
		}
		if mixed < r.wantMixed {
			t.Errorf("%s: %d mixed data+control transactions; the run is built to produce %d — it is not exercising the order under test", name, mixed, r.wantMixed)
		}
		if control == 0 {
			t.Errorf("%s: no transaction wrote a control table — the run is not reaching its core", name)
		}
		for _, c := range r.cores {
			driven[c] = true
		}
	}
	for c := range applyorder.Cores(writeCoreClass) {
		if !driven[c] {
			t.Errorf("write core %q (writeCoreClass) is driven by no run here — add one", c)
		}
	}
	for c := range driven {
		if !applyorder.Cores(writeCoreClass)[c] {
			t.Errorf("run claims core %q, which writeCoreClass does not name — stale", c)
		}
	}
}

// openRecordingDB opens dsn through a connector that reports every
// transaction's statements to rec.
func openRecordingDB(t *testing.T, dsn string, rec *applyorder.Recorder) *sql.DB {
	t.Helper()
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	inner, err := gomysql.NewConnector(cfg)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	return sql.OpenDB(recordingConnector{inner: inner, rec: rec})
}

type recordingConnector struct {
	inner driver.Connector
	rec   *applyorder.Recorder
}

func (c recordingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &recordingConn{Conn: conn, rec: c.rec}, nil
}

func (c recordingConnector) Driver() driver.Driver { return c.inner.Driver() }

// recordingConn forwards every optional interface database/sql probes for,
// recording a statement when it is sent: at Exec/Query, or at a prepared
// statement's execution when the driver declines the direct form
// (driver.ErrSkip, the no-interpolateParams case).
type recordingConn struct {
	driver.Conn
	rec *applyorder.Recorder
}

func (c *recordingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	c.rec.Begin(c)
	return recordingTx{Tx: tx, conn: c}, nil
}

func (c *recordingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err != driver.ErrSkip { //nolint:errorlint // database/sql's own sentinel protocol compares by identity
		c.rec.Statement(c, query)
	}
	return res, err
}

func (c *recordingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != driver.ErrSkip { //nolint:errorlint // database/sql's own sentinel protocol compares by identity
		c.rec.Statement(c, query)
	}
	return rows, err
}

func (c *recordingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	st, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return recordingStmt{Stmt: st, query: query, conn: c}, nil
}

func (c *recordingConn) Ping(ctx context.Context) error { return c.Conn.(driver.Pinger).Ping(ctx) }

func (c *recordingConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *recordingConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *recordingConn) CheckNamedValue(nv *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(nv)
}

type recordingStmt struct {
	driver.Stmt
	query string
	conn  *recordingConn
}

func (s recordingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.conn.rec.Statement(s.conn, s.query)
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

func (s recordingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.conn.rec.Statement(s.conn, s.query)
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

type recordingTx struct {
	driver.Tx
	conn *recordingConn
}

func (tx recordingTx) Commit() error {
	err := tx.Tx.Commit()
	tx.conn.rec.End(tx.conn, err == nil)
	return err
}

func (tx recordingTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.conn.rec.End(tx.conn, false)
	return err
}
