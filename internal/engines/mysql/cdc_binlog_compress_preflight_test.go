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
	gosqlmysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// binlogCompressDriver is a minimal fake driver whose DSN encodes what
// SELECT @@GLOBAL.log_bin_compress returns: a value, "unknown_variable"
// (MySQL's error 1193 — the variable does not exist), or "query_error"
// (a plain broken read). Mirrors the binlogFormatDriver fixture one
// file over; kept separate so each preflight's fixture stays
// single-purpose.
type binlogCompressDriver struct{}

type binlogCompressConn struct{ mode string }

func (binlogCompressDriver) Open(dsn string) (driver.Conn, error) {
	return &binlogCompressConn{mode: dsn}, nil
}

func (c *binlogCompressConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *binlogCompressConn) Close() error              { return nil }
func (c *binlogCompressConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }

func (c *binlogCompressConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT @@GLOBAL.log_bin_compress" {
		return nil, errors.New("unexpected query: " + query)
	}
	switch c.mode {
	case "unknown_variable":
		return nil, &gosqlmysql.MySQLError{Number: 1193, Message: "Unknown system variable 'log_bin_compress'"}
	case "query_error":
		return nil, errors.New("connection reset")
	}
	return &oneCompressRow{val: c.mode}, nil
}

type oneCompressRow struct {
	val  string
	done bool
}

func (r *oneCompressRow) Columns() []string { return []string{"@@GLOBAL.log_bin_compress"} }
func (r *oneCompressRow) Close() error      { return nil }
func (r *oneCompressRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.val
	return nil
}

var registerBinlogCompressOnce sync.Once

func newBinlogCompressDB(t *testing.T, mode string) *sql.DB {
	t.Helper()
	registerBinlogCompressOnce.Do(func() { sql.Register("sluice-binlogcompress-test", binlogCompressDriver{}) })
	db, err := sql.Open("sluice-binlogcompress-test", mode)
	if err != nil {
		t.Fatalf("open binlog-compress db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPreflightBinlogCompress pins the G8 preflight's value handling:
// OFF spellings pass, ON (and any unrecognized spelling — the
// conservative direction) refuses with the coded error naming the
// mechanism and the remedy, and MySQL's absent-variable error 1193 is a
// PASS (a server without the variable cannot write compressed events).
func TestPreflightBinlogCompress(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"0", "OFF", "off"} {
		if err := preflightBinlogCompress(context.Background(), newBinlogCompressDB(t, v)); err != nil {
			t.Errorf("preflight(%q) = %v; want nil", v, err)
		}
	}
	if err := preflightBinlogCompress(context.Background(), newBinlogCompressDB(t, "unknown_variable")); err != nil {
		t.Errorf("preflight on MySQL (error 1193, variable absent) = %v; want nil (PASS)", err)
	}

	for _, v := range []string{"1", "ON", "on", "SOME_FUTURE_MODE"} {
		err := preflightBinlogCompress(context.Background(), newBinlogCompressDB(t, v))
		if err == nil {
			t.Errorf("preflight(%q) = nil; want the coded refusal", v)
			continue
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCBinlogCompressed {
			t.Errorf("preflight(%q): want %s; got %T: %v", v, sluicecode.CodeCDCBinlogCompressed, err, err)
			continue
		}
		for _, phrase := range []string{
			"log_bin_compress=ON",             // names the variable
			"silently dropped",                // names the consequence
			"log_bin_compress_min_len",        // names the size condition
			"SET GLOBAL log_bin_compress=OFF", // the remedy
			"FLUSH BINARY LOGS",               // the old-segments half of the remedy
		} {
			if !strings.Contains(err.Error(), phrase) {
				t.Errorf("preflight(%q) message missing %q; got: %v", v, phrase, err)
			}
		}
		if ce.Hint == "" || !strings.Contains(ce.Hint, "log_bin_compress=OFF") {
			t.Errorf("preflight(%q) hint = %q; want the remedy hint", v, ce.Hint)
		}
	}
}

// TestPreflightBinlogCompress_ReadFailureIsPlainError: only the
// absent-variable error 1193 passes; any other failed read is a loud
// plain (uncoded) error — a broken read is not evidence either way, and
// the refusal's remedy would be wrong advice (the format preflight's
// posture).
func TestPreflightBinlogCompress_ReadFailureIsPlainError(t *testing.T) {
	t.Parallel()
	err := preflightBinlogCompress(context.Background(), newBinlogCompressDB(t, "query_error"))
	if err == nil {
		t.Fatal("preflight with a failing read = nil; want a loud error")
	}
	if _, ok := sluicecode.FromError(err); ok {
		t.Fatalf("a failed @@GLOBAL.log_bin_compress read must not carry the refusal code: %v", err)
	}
}

// compressedRowsEvent builds a compressed-type rows event for the
// staging reader's table (TableID 7 = app.users). go-mysql fully
// decodes compressed events into the same *replication.RowsEvent shape
// as their uncompressed twins, so the fixture carries decoded rows —
// exactly what dispatch sees on the wire.
func compressedRowsEvent(et replication.EventType, rows [][]any) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: hdr(et),
		Event:  &replication.RowsEvent{TableID: 7, Rows: rows},
	}
}

// TestDispatchRows_MariaDBCompressedRowEvents_BeltRefuses pins the G8
// dispatch belt: each of the three compressed row-event types stops the
// stream with the coded refusal — never the old silent default-arm
// drop. All three verbs are pinned because all three occur on the wire
// (a big row's DELETE compresses via its before-image; ground-truthed
// mariadb:11.4.12).
func TestDispatchRows_MariaDBCompressedRowEvents_BeltRefuses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		et   replication.EventType
		rows [][]any
	}{
		{"write", replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, [][]any{{int64(1)}}},
		{"update", replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1, [][]any{{int64(1)}, {int64(2)}}},
		{"delete", replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1, [][]any{{int64(1)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newStagingReader(t, FlavorMariaDB, "0-1-1")
			out := make(chan ir.Change, 8)
			err := r.dispatch(context.Background(), compressedRowsEvent(tc.et, tc.rows), out)
			if err == nil {
				t.Fatalf("dispatch(%s) = nil — the compressed row event was silently dropped (the G8 "+
					"silent-loss shape); want the coded refusal", tc.et)
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeCDCBinlogCompressed {
				t.Fatalf("dispatch(%s): want %s; got: %v", tc.et, sluicecode.CodeCDCBinlogCompressed, err)
			}
			if !strings.Contains(err.Error(), "app.users") {
				t.Errorf("belt refusal does not name the table: %v", err)
			}
			if len(out) != 0 {
				t.Errorf("belt refusal emitted %d changes; want 0", len(out))
			}
		})
	}
}

// TestDispatchRows_MariaDBCompressedRowEvents_OutOfScopeDropped pins the
// belt's scope gating (the Bug 246 discipline, by construction): a
// compressed row event for a table OUTSIDE the stream's scope is
// dropped by the qn=="" check ahead of the belt, exactly like every
// other out-of-scope row event — a compressing writer on an unrelated
// database must not kill the sync.
func TestDispatchRows_MariaDBCompressedRowEvents_OutOfScopeDropped(t *testing.T) {
	t.Parallel()
	r := newStagingReader(t, FlavorMariaDB, "0-1-1")
	out := make(chan ir.Change, 8)
	tm := &replication.BinlogEvent{
		Header: hdr(replication.TABLE_MAP_EVENT),
		Event:  &replication.TableMapEvent{TableID: 9, Schema: []byte("other"), Table: []byte("t")},
	}
	if err := r.dispatch(context.Background(), tm, out); err != nil {
		t.Fatalf("dispatch table map: %v", err)
	}
	ev := &replication.BinlogEvent{
		Header: hdr(replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1),
		Event:  &replication.RowsEvent{TableID: 9, Rows: [][]any{{int64(1)}}},
	}
	if err := r.dispatch(context.Background(), ev, out); err != nil {
		t.Fatalf("dispatch out-of-scope compressed event = %v; want nil (dropped like every other "+
			"out-of-scope row event)", err)
	}
	if len(out) != 0 {
		t.Errorf("out-of-scope compressed event emitted %d changes; want 0", len(out))
	}
}
