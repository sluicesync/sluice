// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// fakeBinlog is an in-memory SHOW BINLOG EVENTS: rows per file, in order.
type fakeBinlog struct {
	files map[string][]binlogEventRow
	order []string
	calls int
	// tornAt, per file, is where an event the server is still writing
	// starts: a read that reaches it returns the rows before it and MySQL's
	// 1220 truncation, as a real server does (cdc_ddl_lookahead.go).
	tornAt map[string]uint32
	// failWith, when set, is returned for every read.
	failWith error
}

func (f *fakeBinlog) events(_ context.Context, file string, from uint32, limit int) ([]binlogEventRow, error) {
	f.calls++
	if f.failWith != nil {
		return nil, f.failWith
	}
	torn, hasTorn := f.tornAt[file]
	var out []binlogEventRow
	for _, ev := range f.files[file] {
		if hasTorn && ev.pos >= torn {
			break
		}
		if ev.pos >= from && len(out) < limit {
			out = append(out, ev)
		}
	}
	if hasTorn && from <= torn && len(out) < limit {
		return out, tornEventError(torn)
	}
	return out, nil
}

// tornEventError is the error mysql:8.4 returned, measured, for a SHOW
// BINLOG EVENTS that reached an event still being written at pos.
func tornEventError(pos uint32) error {
	return &sqldriver.MySQLError{Number: 1220, Message: fmt.Sprintf(
		"Error when executing command SHOW BINLOG EVENTS: Error reading Log_event at position %d: "+
			"Failed decoding event: binlog truncated in the middle of event; consider out of disk", pos,
	)}
}

func (f *fakeBinlog) list(context.Context) ([]string, error) { return f.order, nil }

func (f *fakeBinlog) lookahead() ddlLookahead {
	return ddlLookahead{listEvents: f.events, listFiles: f.list}
}

func queryEv(pos, end uint32, info string) binlogEventRow {
	return binlogEventRow{pos: pos, end: end, typ: "Query", info: info}
}

func rowsEv(pos, end uint32) binlogEventRow {
	return binlogEventRow{pos: pos, end: end, typ: "Write_rows", info: "table_id: 1 flags: STMT_END_F"}
}

// TestDDLLookahead_ProvesAndRefuses is the D2 decision on a two-file binlog:
// a reshaping DDL inside [loadedAt, catalogAt) refuses; one before loadedAt
// (crossed already) or at/after catalogAt (the catalog read cannot have
// seen it) does not; a neutral DDL never does; an in-place one is accepted
// with its columns marked for the per-event recheck.
func TestDDLLookahead_ProvesAndRefuses(t *testing.T) {
	bl := &fakeBinlog{
		order: []string{"bin.000001", "bin.000002"},
		files: map[string][]binlogEventRow{
			"bin.000001": {
				queryEv(100, 200, "use `app`; ALTER TABLE kd DROP COLUMN x /* xid=1 */"), // crossed before loadedAt
				rowsEv(200, 300),
				queryEv(300, 400, "BEGIN"),
				queryEv(400, 500, "use `app`; ALTER TABLE kd ADD INDEX ib (b)"),
				queryEv(500, 600, "use `other`; ALTER TABLE t MODIFY c INT AFTER id"),
				queryEv(600, 650, "SAVEPOINT s1"),
				rowsEv(650, 700),
			},
			"bin.000002": {
				queryEv(4, 120, "use `mysql`; ALTER TABLE app.kd MODIFY c VARCHAR(40) AFTER id"), // reshapes kd
				rowsEv(120, 200),
				queryEv(200, 300, "use `app`; ALTER TABLE kd ADD COLUMN late INT"), // at catalogAt: not seen
			},
		},
	}
	r := &CDCReader{ddlAhead: bl.lookahead()}
	tbl := &tableSchema{Schema: "app", Name: "kd", loadedAt: binlogCoord{"bin.000001", 200}, catalogAt: binlogCoord{"bin.000002", 200}}
	err := r.proveCatalogShape(context.Background(), tbl)
	var ce *sluicecode.CodedError
	if !errors.As(err, &ce) || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("a reorder of kd between the reader and the catalog read must refuse; got %v", err)
	}
	if tbl.proven {
		t.Error("a refused shape must not be marked proven")
	}

	// The same binlog, with the catalog read before the reorder: proven.
	r = &CDCReader{ddlAhead: bl.lookahead()}
	tbl = &tableSchema{Schema: "app", Name: "kd", loadedAt: binlogCoord{"bin.000001", 200}, catalogAt: binlogCoord{"bin.000002", 4}}
	if err := r.proveCatalogShape(context.Background(), tbl); err != nil {
		t.Fatalf("no reshaping DDL of kd in range: %v", err)
	}
	if !tbl.proven || tbl.retypedAhead.first != nil {
		t.Errorf("want proven with nothing to recheck; got %+v", tbl)
	}

	// An in-place retype ahead: proven, with the column marked.
	bl.files["bin.000002"][0] = queryEv(4, 120, "use `app`; ALTER TABLE kd MODIFY b VARCHAR(80)")
	r = &CDCReader{ddlAhead: bl.lookahead()}
	tbl = &tableSchema{Schema: "app", Name: "kd", loadedAt: binlogCoord{"bin.000001", 200}, catalogAt: binlogCoord{"bin.000002", 200}}
	if err := r.proveCatalogShape(context.Background(), tbl); err != nil {
		t.Fatalf("an in-place retype must be accepted for the per-event recheck: %v", err)
	}
	if !tbl.retypedAhead.covers("B") || tbl.retypedAhead.covers("c") {
		t.Errorf("recheck set = %+v; want b only", tbl.retypedAhead)
	}
}

// TestDDLLookahead_ScansOncePerRange pins the scanned-through marker: a
// second proof over a range already classified reads nothing new, and one
// that extends it reads only the extension.
func TestDDLLookahead_ScansOncePerRange(t *testing.T) {
	var rows []binlogEventRow
	for p := uint32(4); p < 4+20*10; p += 10 {
		rows = append(rows, rowsEv(p, p+10))
	}
	bl := &fakeBinlog{order: []string{"bin.000001"}, files: map[string][]binlogEventRow{"bin.000001": rows}}
	l := bl.lookahead()
	ctx := context.Background()
	if err := l.scan(ctx, nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 104}); err != nil {
		t.Fatal(err)
	}
	first := bl.calls
	if err := l.scan(ctx, nil, binlogCoord{"bin.000001", 54}, binlogCoord{"bin.000001", 104}); err != nil {
		t.Fatal(err)
	}
	if bl.calls != first {
		t.Errorf("re-proving a scanned range read the binlog again (%d → %d calls)", first, bl.calls)
	}
	if err := l.scan(ctx, nil, binlogCoord{"bin.000001", 54}, binlogCoord{"bin.000001", 204}); err != nil {
		t.Fatal(err)
	}
	if l.through != (binlogCoord{"bin.000001", 204}) {
		t.Errorf("marker = %v; want bin.000001:204", l.through)
	}
}

// TestDDLLookahead_PagesAndZeroEndOffsets pins paging across a page that
// ends inside a MariaDB transaction (End_log_pos 0 on its inner events) and
// a DDL that sits on the far side of that page boundary.
func TestDDLLookahead_PagesAndZeroEndOffsets(t *testing.T) {
	var rows []binlogEventRow
	p := uint32(4)
	for i := 0; i < lookaheadPage+3; i++ {
		rows = append(rows, binlogEventRow{pos: p, end: 0, typ: "Write_rows"})
		p += 10
	}
	rows = append(rows, queryEv(p, p+50, "use `app`; ALTER TABLE kd DROP COLUMN c"))
	bl := &fakeBinlog{order: []string{"bin.000001"}, files: map[string][]binlogEventRow{"bin.000001": rows}}
	l := bl.lookahead()
	if err := l.scan(context.Background(), nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", p + 50}); err != nil {
		t.Fatal(err)
	}
	if got := l.reaching(binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", p + 50}, "app", "kd"); len(got) != 1 {
		t.Errorf("found %d reshaping DDLs past a page of zero end offsets; want 1", len(got))
	}
}

// TestClassifyQueryInfo pins the SHOW BINLOG EVENTS Info parsing: the
// `use` prefix supplies the default database, transaction control is never
// DDL, and an unreadable `use` prefix reaches every table.
func TestClassifyQueryInfo(t *testing.T) {
	if _, ok := classifyQueryInfo("BEGIN"); ok {
		t.Error("BEGIN classified as DDL")
	}
	if _, ok := classifyQueryInfo("XA START 'x'"); ok {
		t.Error("XA START classified as DDL")
	}
	if _, ok := classifyQueryInfo("use `app`; ALTER TABLE kd ADD INDEX i (b)"); ok {
		t.Error("an index DDL classified as reshaping")
	}
	d, ok := classifyQueryInfo("use `a``pp`; ALTER TABLE kd ADD COLUMN x INT /* xid=7 */")
	if !ok || !d.reach.reaches("a`pp", "kd") {
		t.Errorf("use-prefixed ALTER: %+v %v", d, ok)
	}
	if d, ok := classifyQueryInfo("use `broken; ALTER TABLE kd ADD COLUMN x INT"); !ok || !d.reach.all {
		t.Errorf("an unreadable use prefix must reach every table; got %+v %v", d, ok)
	}
}

// TestClassifyBinlogEvent pins the fail-closed event-type allowlist: a
// MariaDB Query_compressed is classified by its statement like a Query (a
// first cut that read only "Query" let a compressed reorder through), a
// known non-DDL type reaches nothing, and a type the list does not know
// reaches every table.
func TestClassifyBinlogEvent(t *testing.T) {
	if d, ok := classifyBinlogEvent(binlogEventRow{typ: "Query_compressed", info: "use `app`; ALTER TABLE kd MODIFY c VARCHAR(40) AFTER id"}); !ok || !d.reach.reaches("app", "kd") {
		t.Errorf("a compressed reorder must reach its table; got %+v %v", d, ok)
	}
	for _, typ := range []string{"Write_rows", "Write_rows_compressed_v1", "Table_map", "Xid", "Gtid", "Annotate_rows", "Rotate", "View_change", "Transaction_context"} {
		if _, ok := classifyBinlogEvent(binlogEventRow{typ: typ}); ok {
			t.Errorf("%s classified as reshaping", typ)
		}
	}
	for _, typ := range []string{"Incident", "Execute_load_query", "Ignorable"} {
		if d, ok := classifyBinlogEvent(binlogEventRow{typ: typ}); !ok || !d.reach.all {
			t.Errorf("%s must reach every table (fail closed); got %+v %v", typ, d, ok)
		}
	}
	if d, ok := classifyBinlogEvent(binlogEventRow{typ: "Incident"}); !ok || !d.reach.all {
		t.Errorf("an unknown event type must reach every table; got %+v %v", d, ok)
	}
}

// TestDDLLookahead_ShortReadNeverMarksUnread pins the scanned-through marker
// against a binlog that serves less than the position the catalog read was
// stamped with: the scan refuses, and the marker never covers the offsets it
// did not read, so no later proof can skip a DDL that lands there.
func TestDDLLookahead_ShortReadNeverMarksUnread(t *testing.T) {
	bl := &fakeBinlog{order: []string{"bin.000001"}, files: map[string][]binlogEventRow{
		"bin.000001": {rowsEv(4, 100), rowsEv(100, 200)},
	}}
	l := bl.lookahead()
	l.shortReadWait = time.Millisecond
	err := l.scan(context.Background(), nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 500})
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCBinlogLookaheadShort {
		t.Fatalf("a scan that could not read up to the catalog's position must refuse with %s; got %v", sluicecode.CodeCDCBinlogLookaheadShort, err)
	}
	if l.through.after(binlogCoord{"bin.000001", 200}) {
		t.Errorf("marker %v covers offsets the scan never read (it read to 200)", l.through)
	}
	// The binlog catches up: a DDL now sits in the range the first scan could
	// not read, and the next proof must find it.
	bl.files["bin.000001"] = append(bl.files["bin.000001"], queryEv(200, 500, "use `app`; ALTER TABLE kd DROP COLUMN c"))
	if err := l.scan(context.Background(), nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 500}); err != nil {
		t.Fatal(err)
	}
	if got := l.reaching(binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 500}, "app", "kd"); len(got) != 1 {
		t.Errorf("found %d DDLs in the range the first scan could not read; want 1", len(got))
	}
}

// TestDDLLookahead_TornTailPastTheEndIsEnough pins the CI failure of
// 2026-10-13 (`look-ahead SHOW BINLOG EVENTS … Error 1220 … binlog truncated
// in the middle of event`): SHOW BINLOG EVENTS reads past the scan's end
// coordinate into the active file's live tail, and a transaction being
// flushed there tore the whole read. The rows before the torn event cover
// the scan's range, so the proof completes — and still finds the DDL inside
// it.
func TestDDLLookahead_TornTailPastTheEndIsEnough(t *testing.T) {
	for _, torn := range []uint32{300, 400} { // at the end coordinate, and past it
		t.Run(fmt.Sprint(torn), func(t *testing.T) {
			bl := &fakeBinlog{
				order: []string{"bin.000001"},
				files: map[string][]binlogEventRow{"bin.000001": {
					rowsEv(4, 100), queryEv(100, 200, "use `app`; ALTER TABLE kd DROP COLUMN c"), rowsEv(200, 300), rowsEv(300, 400),
				}},
				tornAt: map[string]uint32{"bin.000001": torn},
			}
			l := bl.lookahead()
			from, to := binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 300}
			if err := l.scan(context.Background(), nil, from, to); err != nil {
				t.Fatalf("a read torn at or past the scan's end must complete the scan: %v", err)
			}
			if l.through != to {
				t.Errorf("marker = %v; want %v", l.through, to)
			}
			if got := l.reaching(from, to, "app", "kd"); len(got) != 1 {
				t.Errorf("found %d reshaping DDLs in the range a torn read covered; want 1", len(got))
			}
		})
	}
}

// TestDDLLookahead_TornTailRefusals pins what a torn read must NOT be taken
// for. Short of the end coordinate it is a short page: re-read, the marker
// never past the last whole event, SLUICE-E-CDC-BINLOG-LOOKAHEAD-SHORT if it
// never heals, and a DDL in the range found once it does. In a file the
// server has rotated away from it is no live tail, and stays an error; and
// any other error on the active file stays one too.
func TestDDLLookahead_TornTailRefusals(t *testing.T) {
	ctx := context.Background()
	t.Run("short of the end", func(t *testing.T) {
		bl := &fakeBinlog{
			order:  []string{"bin.000001"},
			files:  map[string][]binlogEventRow{"bin.000001": {rowsEv(4, 100), rowsEv(100, 200)}},
			tornAt: map[string]uint32{"bin.000001": 200},
		}
		l := bl.lookahead()
		l.shortReadWait = time.Millisecond
		from, to := binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 500}
		err := l.scan(ctx, nil, from, to)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCBinlogLookaheadShort {
			t.Fatalf("a read torn short of the end that never heals must refuse with %s; got %v", sluicecode.CodeCDCBinlogLookaheadShort, err)
		}
		if l.through.after(binlogCoord{"bin.000001", 200}) {
			t.Errorf("marker %v covers offsets the scan never read (it read to 200)", l.through)
		}
		// The server finishes writing: a DDL sits where the read was torn.
		delete(bl.tornAt, "bin.000001")
		bl.files["bin.000001"] = append(bl.files["bin.000001"], queryEv(200, 500, "use `app`; ALTER TABLE kd DROP COLUMN c"))
		if err := l.scan(ctx, nil, from, to); err != nil {
			t.Fatal(err)
		}
		if got := l.reaching(from, to, "app", "kd"); len(got) != 1 {
			t.Errorf("found %d DDLs where the torn read stopped; want 1", len(got))
		}
	})
	t.Run("rotated file", func(t *testing.T) {
		bl := &fakeBinlog{
			order: []string{"bin.000001", "bin.000002"},
			files: map[string][]binlogEventRow{
				"bin.000001": {rowsEv(4, 100), rowsEv(100, 200)},
				"bin.000002": {rowsEv(4, 100)},
			},
			tornAt: map[string]uint32{"bin.000001": 100},
		}
		l := bl.lookahead()
		err := l.scan(ctx, nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000002", 100})
		if !isActiveTailTruncation(err) {
			t.Fatalf("a torn read in a rotated file must surface the truncation; got %v", err)
		}
	})
	t.Run("other error", func(t *testing.T) {
		denied := &sqldriver.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s)"}
		bl := &fakeBinlog{order: []string{"bin.000001"}, files: map[string][]binlogEventRow{}, failWith: denied}
		l := bl.lookahead()
		err := l.scan(ctx, nil, binlogCoord{"bin.000001", 4}, binlogCoord{"bin.000001", 100})
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCReplicationPermission {
			t.Fatalf("a refused read on the active file must stay a refusal; got %v", err)
		}
	})
}

// TestIsActiveTailTruncation pins the classifier to the server's own text:
// 1220 is the generic "error when executing command", and only its
// truncation form is a torn tail.
func TestIsActiveTailTruncation(t *testing.T) {
	if !isActiveTailTruncation(fmt.Errorf("wrapped: %w", tornEventError(114653))) {
		t.Error("the measured mysql:8.4 truncation (wrapped) was not recognised")
	}
	for _, err := range []error{
		nil,
		errors.New("binlog truncated in the middle of event"),
		&sqldriver.MySQLError{Number: 1220, Message: "Error when executing command SHOW BINLOG EVENTS: Could not find target log"},
		&sqldriver.MySQLError{Number: 1236, Message: "binlog truncated in the middle of event"},
	} {
		if isActiveTailTruncation(err) {
			t.Errorf("%v classified as a torn tail", err)
		}
	}
}

// TestCDCReader_RowMetadataFullStatement_PerFlavor pins the statement the
// backup lanes' re-hint takes from the reader (Bug 302): MySQL persists the
// setting with SET PERSIST; MariaDB has no SET PERSIST, so its statement is
// SET GLOBAL plus the option-file line without which a restart reverts to
// NO_LOG.
func TestCDCReader_RowMetadataFullStatement_PerFlavor(t *testing.T) {
	if got := (&CDCReader{flavor: FlavorVanilla}).RowMetadataFullStatement(); got != "SET PERSIST binlog_row_metadata = 'FULL'" {
		t.Errorf("MySQL: %q", got)
	}
	got := (&CDCReader{flavor: FlavorMariaDB}).RowMetadataFullStatement()
	for _, want := range []string{"SET GLOBAL binlog_row_metadata = 'FULL'", "binlog_row_metadata = FULL under [mariadbd]", "option file"} {
		if !strings.Contains(got, want) {
			t.Errorf("MariaDB: %q; want it to carry %q", got, want)
		}
	}
	if strings.Contains(got, "SET PERSIST binlog") {
		t.Errorf("MariaDB: %q offers SET PERSIST, a syntax error there", got)
	}
}
