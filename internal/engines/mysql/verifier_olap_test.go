// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0147 pins: on vtgate flavors ExactRowCount counts via OLAP
// (SET workload='olap' + SELECT COUNT(*)) on a dedicated connection, and
// falls back to the tested chunked/single-shot path on OLAP failure. Vanilla
// MySQL never issues the workload SET. All exercised with a scriptable fake
// database/sql driver — no testcontainers.

// countStep records one statement executed against the fake count driver:
// which underlying connection ran it, whether that connection was in olap
// mode at the time, and the SQL text. It lets a test assert "SET workload
// THEN SELECT COUNT(*) on ONE connection".
type countStep struct {
	connID int64
	olap   bool
	query  string
}

type countRecorder struct {
	mu    sync.Mutex
	steps []countStep
}

func (rec *countRecorder) add(s countStep) {
	rec.mu.Lock()
	rec.steps = append(rec.steps, s)
	rec.mu.Unlock()
}

func (rec *countRecorder) snapshot() []countStep {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([]countStep, len(rec.steps))
	copy(out, rec.steps)
	return out
}

// countConfig scripts the fake driver's answers and holds the shared recorder.
type countConfig struct {
	rec      *countRecorder
	count    int64 // value returned by COUNT(*)
	failOLAP bool  // when true, COUNT(*) on an olap conn returns an error
	nextConn atomic.Int64
}

// countDriver answers COUNT(*) (single-shot and olap) and records every
// statement with its connection. Each connection tracks whether `SET
// workload='olap'` has run on it; a driver session reset (modeling
// COM_RESET_CONNECTION) clears it — which both models the real leak-avoidance
// and lets the post-failure fallback count on the reused pooled conn succeed.
type countDriver struct{ cfg *countConfig }

func (d countDriver) Open(string) (driver.Conn, error) {
	return &countConn{cfg: d.cfg, id: d.cfg.nextConn.Add(1)}, nil
}

type countConn struct {
	cfg  *countConfig
	id   int64
	olap bool
}

func (*countConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (*countConn) Close() error                        { return nil }
func (*countConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

// ResetSession models the go-sql-driver COM_RESET_CONNECTION the pool issues
// before reusing a connection: the leaked `workload=olap` session var is
// cleared, so a pooled conn returned by a prior olap count is clean.
func (c *countConn) ResetSession(context.Context) error {
	c.olap = false
	return nil
}

func (c *countConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "workload") {
		c.olap = true
	}
	c.cfg.rec.add(countStep{connID: c.id, olap: c.olap, query: query})
	return driver.RowsAffected(0), nil
}

func (c *countConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.cfg.rec.add(countStep{connID: c.id, olap: c.olap, query: query})
	if strings.Contains(query, "COUNT(*)") {
		if c.cfg.failOLAP && c.olap {
			return nil, errors.New("injected olap count failure (errno 3024 simulated)")
		}
		return &countRows{val: c.cfg.count}, nil
	}
	return nil, fmt.Errorf("countConn: unexpected query: %s", query)
}

// countRows is a single-row, single-column result carrying the count.
type countRows struct {
	val  int64
	done bool
}

func (*countRows) Columns() []string { return []string{"count"} }
func (*countRows) Close() error      { return nil }

func (r *countRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.val
	return nil
}

func newCountDB(t *testing.T, cfg *countConfig) *sql.DB {
	t.Helper()
	if cfg.rec == nil {
		cfg.rec = &countRecorder{}
	}
	// sql.Register is global and panics on a duplicate name; t.Name() is
	// unique per (sub)test within a process.
	name := "sluice-count-test-" + t.Name()
	sql.Register(name, countDriver{cfg: cfg})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open count db: %v", err)
	}
	// One connection so the fallback deterministically reuses (and resets)
	// the same pooled conn the olap attempt left behind.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// noPKTable is a table with no usable single-integer PK, so the non-olap
// fall-through path routes to singleShotCount (plain COUNT(*)).
func noPKTable() *ir.Table {
	return &ir.Table{Name: "widgets"}
}

// TestOlapCount_SetThenSelectOnOneConn pins that olapCount issues `SET
// workload='olap'` and THEN `SELECT COUNT(*)` on a SINGLE connection, quotes
// the table, and returns the scanned count.
func TestOlapCount_SetThenSelectOnOneConn(t *testing.T) {
	t.Parallel()
	cfg := &countConfig{count: 123}
	db := newCountDB(t, cfg)

	n, err := olapCount(context.Background(), db, "widgets")
	if err != nil {
		t.Fatalf("olapCount: unexpected error: %v", err)
	}
	if n != 123 {
		t.Fatalf("olapCount = %d, want 123", n)
	}

	steps := cfg.rec.snapshot()
	var setStep, countStepFound *countStep
	for i := range steps {
		s := &steps[i]
		switch {
		case strings.Contains(s.query, "workload"):
			setStep = s
		case strings.Contains(s.query, "COUNT(*)"):
			countStepFound = s
		}
	}
	if setStep == nil {
		t.Fatalf("no `SET workload` statement issued; steps=%+v", steps)
	}
	if !strings.Contains(setStep.query, "workload = 'olap'") {
		t.Fatalf("SET statement not workload=olap: %q", setStep.query)
	}
	if countStepFound == nil {
		t.Fatalf("no COUNT(*) statement issued; steps=%+v", steps)
	}
	if !strings.Contains(countStepFound.query, "`widgets`") {
		t.Fatalf("COUNT query did not quote the table: %q", countStepFound.query)
	}
	// SET then SELECT, on the SAME connection.
	if steps[0].query != setStep.query {
		t.Fatalf("SET workload was not the first statement; steps=%+v", steps)
	}
	if setStep.connID != countStepFound.connID {
		t.Fatalf("SET (conn %d) and COUNT (conn %d) ran on different connections",
			setStep.connID, countStepFound.connID)
	}
}

// TestExactRowCount_FlavorDispatch pins that vtgate flavors route through the
// OLAP path (workload SET issued) while vanilla MySQL does NOT.
func TestExactRowCount_FlavorDispatch(t *testing.T) {
	t.Parallel()
	hasSet := func(steps []countStep) bool {
		for _, s := range steps {
			if strings.Contains(s.query, "workload") {
				return true
			}
		}
		return false
	}
	t.Run("planetscale_uses_olap", func(t *testing.T) {
		t.Parallel()
		cfg := &countConfig{count: 7}
		r := &SchemaReader{db: newCountDB(t, cfg), flavor: FlavorPlanetScale}
		n, err := r.ExactRowCount(context.Background(), noPKTable())
		if err != nil || n != 7 {
			t.Fatalf("ExactRowCount = (%d, %v), want (7, nil)", n, err)
		}
		if !hasSet(cfg.rec.snapshot()) {
			t.Fatal("PlanetScale flavor did not issue SET workload='olap'")
		}
	})
	t.Run("vitess_uses_olap", func(t *testing.T) {
		t.Parallel()
		cfg := &countConfig{count: 9}
		r := &SchemaReader{db: newCountDB(t, cfg), flavor: FlavorVitess}
		n, err := r.ExactRowCount(context.Background(), noPKTable())
		if err != nil || n != 9 {
			t.Fatalf("ExactRowCount = (%d, %v), want (9, nil)", n, err)
		}
		if !hasSet(cfg.rec.snapshot()) {
			t.Fatal("Vitess flavor did not issue SET workload='olap'")
		}
	})
	t.Run("vanilla_no_olap", func(t *testing.T) {
		t.Parallel()
		cfg := &countConfig{count: 5}
		r := &SchemaReader{db: newCountDB(t, cfg), flavor: FlavorVanilla}
		n, err := r.ExactRowCount(context.Background(), noPKTable())
		if err != nil || n != 5 {
			t.Fatalf("ExactRowCount = (%d, %v), want (5, nil)", n, err)
		}
		if hasSet(cfg.rec.snapshot()) {
			t.Fatal("vanilla MySQL must NOT issue SET workload='olap'")
		}
	})
}

// TestExactRowCount_FallbackOnOlapError pins that when the OLAP count errors
// on a vtgate flavor, ExactRowCount falls back to the tested single-shot path
// and returns the correct count.
func TestExactRowCount_FallbackOnOlapError(t *testing.T) {
	t.Parallel()
	cfg := &countConfig{count: 42, failOLAP: true}
	r := &SchemaReader{db: newCountDB(t, cfg), flavor: FlavorPlanetScale}

	n, err := r.ExactRowCount(context.Background(), noPKTable())
	if err != nil {
		t.Fatalf("ExactRowCount fallback: unexpected error: %v", err)
	}
	if n != 42 {
		t.Fatalf("ExactRowCount fallback = %d, want 42", n)
	}

	steps := cfg.rec.snapshot()
	var sawSet, sawFallbackCount bool
	for _, s := range steps {
		if strings.Contains(s.query, "workload") {
			sawSet = true
		}
		// The fallback single-shot COUNT runs on a NON-olap conn (the pooled
		// conn the failed olap attempt released, cleared by ResetSession).
		if strings.Contains(s.query, "COUNT(*)") && !s.olap {
			sawFallbackCount = true
		}
	}
	if !sawSet {
		t.Fatalf("expected an OLAP attempt (SET workload) before fallback; steps=%+v", steps)
	}
	if !sawFallbackCount {
		t.Fatalf("expected a non-olap fallback COUNT(*); steps=%+v", steps)
	}
}
