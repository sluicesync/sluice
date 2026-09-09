//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 RC-1 — the binlog-filter door's database-name
// equality, re-derived from real servers rather than from the table in
// the preflight's file comment.
//
// Every cell here has the same shape: boot a server with a filter entry
// whose CASE differs from the synced database's, ask the door for its
// verdict, then INSERT a row and read `SHOW BINLOG EVENTS` to learn
// whether the server actually logged it. The two must agree — a refusal
// when the server logs the write is the Bug 246 over-refusal, a pass
// when it does not is RC-1's silent loss — and the server's answer is
// the independent expected value, so a server that changes its rule
// fails the cell instead of the cell repeating the door's belief.
//
// Cost, stated: eight container boots, two of them cold inits on the
// upstream mysql:8.0 image (the pre-baked image is initialised at
// lower_case_table_names=0 and MySQL 8 refuses to boot it under 1). The
// door is a silent-loss refusal on the MySQL lane's common
// configuration; that buys the boots.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// binlogFilterCaseCell is one measured row: the server's boot flags and
// the two verdicts that must agree.
type binlogFilterCaseCell struct {
	name string
	// args are the filter flag (and, for a folding server, the
	// lower_case_table_names flag) appended to the boot command.
	args []string
	// entry is the filter entry as typed on the command line; the
	// evidence-plumbing assertion checks the server surfaces it verbatim
	// in the status row's filter column, so a server that silently
	// ignored the flag cannot green a cell.
	entry string
	// doArm says which status column carries entry.
	doArm bool
	// wantRefusal is the door's expected verdict for a stream scoped to
	// the boot database; wantLogged is the server's expected behaviour
	// for a write to that database. They are recorded separately so the
	// test can say which of the two a failure disagrees with.
	wantRefusal, wantLogged bool
}

func (c binlogFilterCaseCell) run(t *testing.T, dsn, database string, flavor Flavor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if c.wantRefusal == c.wantLogged {
		t.Fatalf("cell %s is self-contradictory: wantRefusal=%v wantLogged=%v", c.name, c.wantRefusal, c.wantLogged)
	}

	applyMySQL(t, dsn, `CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;`)

	// Evidence plumbing: the running server carries the entry AS TYPED.
	db := openSQL(t, dsn)
	doList, ignoreList, ok, err := readBinlogDBFilters(ctx, db)
	if err != nil || !ok {
		t.Fatalf("read filter columns: ok=%v err=%v", ok, err)
	}
	got := ignoreList
	if c.doArm {
		got = doList
	}
	if len(got) != 1 || got[0] != c.entry {
		t.Fatalf("server's filter column = do:%v ignore:%v; want the entry %q as typed (doArm=%v) — the flag did not take, so nothing below would measure anything", doList, ignoreList, c.entry, c.doArm)
	}

	// The door's verdict.
	rdr, err := (Engine{Flavor: flavor}).OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(*CDCReader).Close() }()
	_, err = rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
	if c.wantRefusal {
		wantCodedRefusal(t, err, sluicecode.CodeCDCBinlogDBFiltered, "StreamChanges")
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", database)) {
			t.Errorf("refusal must name the synced database as sluice spells it (%q); got: %v", database, err)
		}
	} else if err != nil {
		t.Fatalf("StreamChanges = %v; want nil — the server logs this database under its own name rule, so a refusal here is the over-refusal the fold used to produce", err)
	}

	// The server's verdict, measured.
	logged := serverLogsWriteTo(t, ctx, db, database)
	if logged != c.wantLogged {
		t.Fatalf("server logged a write to %q = %v; the cell expected %v — the measured rule in the preflight's file comment is stale for this server, and the door's verdict above was graded against a wrong premise", database, logged, c.wantLogged)
	}
}

func openSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// serverLogsWriteTo inserts one row into <database>.t and reports whether
// the binlog gained a Table_map for it. Counting Table_map lines across
// every binlog file before and after is deliberately dumber than reading
// the status position: a filtered write advances nothing, but so would a
// write that failed, and the row-count assertion closes that hole.
func serverLogsWriteTo(t *testing.T, ctx context.Context, db *sql.DB, database string) bool {
	t.Helper()
	before := countTableMaps(t, ctx, db, database)
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&rows); err != nil {
		t.Fatalf("count before: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", rows+1); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var after int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&after); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if after != rows+1 {
		t.Fatalf("the probe INSERT did not land (rows %d → %d); a binlog verdict on a write that did not happen measures nothing", rows, after)
	}
	return countTableMaps(t, ctx, db, database) > before
}

// countTableMaps counts Table_map events naming <database>.t across every
// binlog file the server holds.
func countTableMaps(t *testing.T, ctx context.Context, db *sql.DB, database string) int {
	t.Helper()
	files, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	defer func() { _ = files.Close() }()
	var names []string
	for files.Next() {
		cols, err := files.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		dest := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range dest {
			holders[i] = &dest[i]
		}
		if err := files.Scan(holders...); err != nil {
			t.Fatalf("scan binary logs: %v", err)
		}
		name, _ := scanString(dest[0])
		names = append(names, name)
	}
	if err := files.Err(); err != nil {
		t.Fatalf("binary logs: %v", err)
	}

	want := "(" + database + ".t)"
	n := 0
	for _, f := range names {
		n += countTableMapsIn(t, ctx, db, f, want)
	}
	return n
}

func countTableMapsIn(t *testing.T, ctx context.Context, db *sql.DB, file, want string) int {
	t.Helper()
	events, err := db.QueryContext(ctx, "SHOW BINLOG EVENTS IN '"+file+"'")
	if err != nil {
		t.Fatalf("SHOW BINLOG EVENTS IN %s: %v", file, err)
	}
	defer func() { _ = events.Close() }()
	n := 0
	for events.Next() {
		cols, err := events.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		dest := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range dest {
			holders[i] = &dest[i]
		}
		if err := events.Scan(holders...); err != nil {
			t.Fatalf("scan events: %v", err)
		}
		var typ, info string
		for i, name := range cols {
			v, _ := scanString(dest[i])
			switch strings.ToLower(name) {
			case "event_type":
				typ = v
			case "info":
				info = v
			}
		}
		if typ == "Table_map" && strings.Contains(info, want) {
			n++
		}
	}
	if err := events.Err(); err != nil {
		t.Fatalf("events: %v", err)
	}
	return n
}

// TestCDCReader_BinlogDBFilterPreflight_CaseRule_MySQL: MySQL follows
// lower_case_table_names for its filter compare — byte-exact at 0 (the
// Linux default, where RC-1 was observed), case-folded otherwise.
func TestCDCReader_BinlogDBFilterPreflight_CaseRule_MySQL(t *testing.T) {
	const database = "source_db"
	cells := []struct {
		binlogFilterCaseCell
		image string
	}{
		{binlogFilterCaseCell{name: "exact_server_do_mismatch_refuses", args: []string{"--binlog-do-db=SOURCE_DB"}, entry: "SOURCE_DB", doArm: true, wantRefusal: true, wantLogged: false}, sharedMySQLImage},
		{binlogFilterCaseCell{name: "exact_server_ignore_mismatch_passes", args: []string{"--binlog-ignore-db=SOURCE_DB"}, entry: "SOURCE_DB", wantRefusal: false, wantLogged: true}, sharedMySQLImage},
		{binlogFilterCaseCell{name: "folding_server_do_mismatch_passes", args: []string{"--lower-case-table-names=1", "--binlog-do-db=SOURCE_DB"}, entry: "SOURCE_DB", doArm: true, wantRefusal: false, wantLogged: true}, "mysql:8.0"},
		{binlogFilterCaseCell{name: "folding_server_ignore_mismatch_refuses", args: []string{"--lower-case-table-names=1", "--binlog-ignore-db=SOURCE_DB"}, entry: "SOURCE_DB", wantRefusal: true, wantLogged: false}, "mysql:8.0"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			dsn, cleanup := startMySQLM2PreflightImage(t, c.image, c.args...)
			defer cleanup()
			c.run(t, dsn, database, FlavorVanilla)
		})
	}
}

// TestMariaDB_CDCReader_BinlogDBFilterPreflight_CaseRule: MariaDB does
// NOT follow lower_case_table_names for its filter compare — the entry
// as typed is compared byte-exactly against the name as STORED, which
// at lct=1 is the lowercase fold. A mixed-case entry therefore matches
// nothing at any setting; a lowercase entry matches on a folding server
// whatever the operator's spelling.
func TestMariaDB_CDCReader_BinlogDBFilterPreflight_CaseRule(t *testing.T) {
	const database = "cdc_src"
	cells := []binlogFilterCaseCell{
		{name: "exact_server_do_mismatch_refuses", args: []string{"--binlog-do-db=CDC_SRC"}, entry: "CDC_SRC", doArm: true, wantRefusal: true, wantLogged: false},
		{name: "folding_server_do_mismatch_refuses", args: []string{"--lower-case-table-names=1", "--binlog-do-db=CDC_SRC"}, entry: "CDC_SRC", doArm: true, wantRefusal: true, wantLogged: false},
		{name: "folding_server_do_lowercase_passes", args: []string{"--lower-case-table-names=1", "--binlog-do-db=cdc_src"}, entry: "cdc_src", doArm: true, wantRefusal: false, wantLogged: true},
		{name: "folding_server_ignore_mismatch_passes", args: []string{"--lower-case-table-names=1", "--binlog-ignore-db=CDC_SRC"}, entry: "CDC_SRC", wantRefusal: false, wantLogged: true},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, c.args...)
			defer cleanup()
			c.run(t, dsn, database, FlavorMariaDB)
		})
	}
}
