// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// --- Fake driver: SELECT @@GLOBAL.binlog_row_image answers per-DSN so
// every preflight branch is pinned deterministically without a
// container. Same pattern as diagDriver (cdc_reader_verify_timeout_test.go).

type rowImageDriver struct{}

type rowImageConn struct{ mode string }

func (rowImageDriver) Open(dsn string) (driver.Conn, error) { return rowImageConn{mode: dsn}, nil }

func (rowImageConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (rowImageConn) Close() error                        { return nil }
func (rowImageConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

type oneStringRow struct {
	val  string
	done bool
}

func (*oneStringRow) Columns() []string { return []string{"@@GLOBAL.binlog_row_image"} }
func (*oneStringRow) Close() error      { return nil }
func (r *oneStringRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.val
	return nil
}

func (c rowImageConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT @@GLOBAL.binlog_row_image" {
		return nil, errors.New("unexpected query: " + query)
	}
	if c.mode == "query_error" {
		return nil, errors.New("Unknown system variable 'binlog_row_image'")
	}
	return &oneStringRow{val: c.mode}, nil
}

var registerRowImageOnce sync.Once

func newRowImageDB(t *testing.T, mode string) *sql.DB {
	t.Helper()
	registerRowImageOnce.Do(func() { sql.Register("sluice-rowimage-test", rowImageDriver{}) })
	db, err := sql.Open("sluice-rowimage-test", mode) // DSN = the value to return
	if err != nil {
		t.Fatalf("open row-image db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPreflightBinlogRowImage pins the Bug 193 layer-1 refusal across
// the full binlog_row_image family — FULL passes, MINIMAL and NOBLOB
// (and any unknown non-FULL value) refuse with the coded error naming
// the value, the silent-UPDATE-loss consequence, and both remedies
// (SET GLOBAL + the Azure recipe). Case-insensitivity is pinned because
// the server reports the value as configured (full/FULL both occur).
func TestPreflightBinlogRowImage(t *testing.T) {
	t.Parallel()
	pass := []string{"FULL", "full", "Full"}
	for _, v := range pass {
		if err := preflightBinlogRowImage(context.Background(), newRowImageDB(t, v)); err != nil {
			t.Errorf("preflight(%q) = %v; want nil", v, err)
		}
	}

	refuse := []string{"MINIMAL", "minimal", "NOBLOB", "noblob", "SOME_FUTURE_MODE"}
	for _, v := range refuse {
		err := preflightBinlogRowImage(context.Background(), newRowImageDB(t, v))
		if err == nil {
			t.Errorf("preflight(%q) = nil; want the coded refusal", v)
			continue
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
			t.Errorf("preflight(%q): want %s; got %T: %v", v, sluicecode.CodeCDCRowImagePartial, err, err)
			continue
		}
		for _, phrase := range []string{
			"@@GLOBAL.binlog_row_image=" + v,         // names the value
			"silently lose every UPDATE",             // names the consequence
			"SET GLOBAL binlog_row_image=FULL",       // the generic remedy
			"az mysql flexible-server parameter set", // the Azure recipe (the platform whose DEFAULT trips this)
		} {
			if !strings.Contains(err.Error(), phrase) {
				t.Errorf("preflight(%q) message missing %q; got: %v", v, phrase, err)
			}
		}
		if ce.Hint == "" || !strings.Contains(ce.Hint, "binlog_row_image=FULL") {
			t.Errorf("preflight(%q) hint = %q; want the remedy hint", v, ce.Hint)
		}
	}
}

// TestPreflightBinlogRowImage_ReadFailureIsPlainError pins the failure
// shape when the variable cannot be read: a loud plain (uncoded) error
// — sluice cannot prove the full-image invariant, so it does not
// stream — that is NOT the refusal code (a broken read is not evidence
// of MINIMAL, and the refusal's remedy would be wrong advice).
func TestPreflightBinlogRowImage_ReadFailureIsPlainError(t *testing.T) {
	t.Parallel()
	err := preflightBinlogRowImage(context.Background(), newRowImageDB(t, "query_error"))
	if err == nil {
		t.Fatal("preflight with a failing read = nil; want a loud error")
	}
	if _, ok := sluicecode.FromError(err); ok {
		t.Fatalf("a failed @@GLOBAL.binlog_row_image read must not carry the refusal code: %v", err)
	}
	if !strings.Contains(err.Error(), "@@GLOBAL.binlog_row_image") {
		t.Errorf("error should name the variable it failed to read; got: %v", err)
	}
}

// TestRefusePartialRowImage pins the Bug 193 layer-2 belt: a rows-event
// image that skipped a non-generated column is refused loudly with the
// coded error naming the table, column, image, and binlog_row_image;
// empty skip lists (FULL images) and generated-column-only skips pass.
func TestRefusePartialRowImage(t *testing.T) {
	t.Parallel()
	tbl := &tableSchema{
		Schema: "source_db",
		Name:   "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "status", Type: ir.Varchar{Length: 32}},
			{Name: "total_cents", Type: ir.Integer{Width: 64}},
			{Name: "total_dollars", Type: ir.Integer{Width: 64}, GeneratedExpr: "total_cents DIV 100"},
		},
		PrimaryKey: []string{"id"},
	}

	if err := refusePartialRowImage(tbl, nil, "update", "before"); err != nil {
		t.Errorf("nil skip list (FULL image) = %v; want nil", err)
	}
	if err := refusePartialRowImage(tbl, []int{}, "update", "after"); err != nil {
		t.Errorf("empty skip list (FULL image) = %v; want nil", err)
	}
	// A skipped GENERATED column loses nothing (the decoder drops it
	// anyway) — no refusal.
	if err := refusePartialRowImage(tbl, []int{3}, "update", "after"); err != nil {
		t.Errorf("generated-column-only skip = %v; want nil", err)
	}

	// A skipped real column is the partial-image proof — refuse, naming
	// the pieces the operator needs.
	err := refusePartialRowImage(tbl, []int{1, 2}, "update", "before")
	if err == nil {
		t.Fatal("skipped non-generated column = nil; want the coded refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
	}
	for _, phrase := range []string{"source_db.orders", `"status"`, "before-image", "binlog_row_image", "update"} {
		if !strings.Contains(err.Error(), phrase) {
			t.Errorf("belt message missing %q; got: %v", phrase, err)
		}
	}

	// Defensive: an out-of-range index (schema drift between the binlog
	// image and the cached column list) still refuses — loudly, never a
	// panic — naming the ordinal.
	if err := refusePartialRowImage(tbl, []int{17}, "insert", "write"); err == nil {
		t.Error("out-of-range skipped index = nil; want a refusal")
	} else if !strings.Contains(err.Error(), "#17") {
		t.Errorf("out-of-range refusal should name the ordinal; got: %v", err)
	}
}

// TestSkippedColumnsFor pins the belt's event accessor: parallel
// SkippedColumns entries come back per image, and a hand-built event
// with a short (or absent) SkippedColumns yields nil rather than a
// panic — unit fixtures and hypothetical older decoders stay safe.
func TestSkippedColumnsFor(t *testing.T) {
	t.Parallel()
	ev := &replication.RowsEvent{
		Rows:           [][]any{{int64(1), "a"}, {int64(1), "b"}},
		SkippedColumns: [][]int{{1}},
	}
	if got := skippedColumnsFor(ev, 0); len(got) != 1 || got[0] != 1 {
		t.Errorf("skippedColumnsFor(ev, 0) = %v; want [1]", got)
	}
	if got := skippedColumnsFor(ev, 1); got != nil {
		t.Errorf("skippedColumnsFor(ev, 1) = %v; want nil (short parallel slice)", got)
	}
}
