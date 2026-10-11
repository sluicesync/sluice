//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// RENAME-SWAP-FORWARD — the pin (column_order_boundary.go).
//
// A rename swap (`RENAME COLUMN b TO c, RENAME COLUMN c TO b`) on a source
// `sync start` follows was applied to the target as nothing: every row the
// target held kept its pre-swap values under its pre-swap names, at exit 0.
// The class is every boundary a by-name comparison cannot see, so the
// matrix is every shape of it × every way a stream meets the boundary, on
// every source family that emits schema boundaries:
//
//	shapes:  a same-type swap, a three-way rotation, a swap across types,
//	         a swap with a retype, a MySQL-family reorder (the stated cost),
//	         and the sibling the same classifier read as no change — two
//	         columns widened in one ALTER;
//	entries: live with the cache primed by an earlier row (CDC→CDC), live
//	         against the cold-start seed, a warm resume across the change,
//	         a restart after the refusal, the operator's reconcile, and
//	         --schema-changes=refuse;
//	servers: MySQL 8.0, MySQL 8.4, MariaDB 11.4 — each under its default
//	         binlog_row_metadata and again under FULL, where the reader
//	         decodes by the event's own column names (Bug 301) — and
//	         Postgres 16.
//
// The grade is the target against the source — declared column names in
// order, and every row's values by name — never the stream's own logs. A
// refusal must leave the target exactly as it was and the post-change row
// unapplied; the reconcile must converge the target onto the source.

package pipeline

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// rsfServer is one database server; every cell gets its own source and
// target database on it.
type rsfServer struct {
	label   string
	engine  string
	dialect string // "mysql" or "postgres"
	newDB   func(t *testing.T, name string) twfbDB
	// full: a MySQL-family server running binlog_row_metadata=FULL.
	full bool
}

// rsfFull marks srv as running binlog_row_metadata=FULL.
func rsfFull(srv rsfServer) rsfServer {
	srv.full = true
	return srv
}

func rsfMySQLFamily(t *testing.T, label, engine, rootDSN string, buildDSN func(orig, db string) (string, error)) rsfServer {
	t.Helper()
	root := twfbDB{engine: engine, dsn: rootDSN}
	return rsfServer{
		label: label, engine: engine, dialect: "mysql",
		newDB: func(t *testing.T, name string) twfbDB {
			t.Helper()
			root.exec(t, "CREATE DATABASE "+name)
			dsn, err := buildDSN(rootDSN, name)
			if err != nil {
				t.Fatalf("dsn for %s: %v", name, err)
			}
			return twfbDB{engine: engine, dsn: dsn}
		},
	}
}

// rsfBootMySQL boots a MySQL image with the binlog the stream needs.
func rsfBootMySQL(t *testing.T, image string) (dsn string, cleanup func()) {
	t.Helper()
	container := runMySQLImageWithRetry(
		t, image,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{
				"mysqld", "--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL",
			}},
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return conn, func() { _ = container.Terminate(context.Background()) }
}

// rsfShape is one source change: the table before it (ids 1..3), the change
// itself (applied verbatim to the target by the reconcile), and a row
// written after it, by name.
type rsfShape struct {
	name   string
	create string
	change string
	insert string
	// markers are the texts a refusal of this shape may carry (any one).
	markers []string
	// liveOnly limits the shape to the CDC→CDC live entry: its evidence
	// (a Postgres stable column id) exists only within one reader session.
	liveOnly bool
}

var (
	rsfOrderMarkers = []string{"COLUMN-ORDER-CHANGED"}
	rsfMultiMarkers = []string{"columns changed type or nullability in one schema boundary", "RESUME-SCHEMA-DIVERGENCE"}
)

func rsfShapes(dialect string) []rsfShape {
	if dialect == "postgres" {
		return []rsfShape{
			{
				name:    "swap",
				create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
				change:  "BEGIN; ALTER TABLE kd RENAME COLUMN b TO tmp; ALTER TABLE kd RENAME COLUMN c TO b; ALTER TABLE kd RENAME COLUMN tmp TO c; COMMIT;",
				insert:  "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)",
				markers: rsfOrderMarkers,
			},
			{
				name:    "rotation",
				create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT, d INT); INSERT INTO kd VALUES (1,10,20,30),(2,11,21,31),(3,12,22,32);",
				change:  "BEGIN; ALTER TABLE kd RENAME COLUMN b TO tmp; ALTER TABLE kd RENAME COLUMN d TO b; ALTER TABLE kd RENAME COLUMN c TO d; ALTER TABLE kd RENAME COLUMN tmp TO c; COMMIT;",
				insert:  "INSERT INTO kd (id,b,c,d) VALUES (100,1000,2000,3000)",
				markers: rsfOrderMarkers,
			},
			{
				name:    "swap-across-types",
				create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c TEXT); INSERT INTO kd VALUES (1,10,'x1'),(2,11,'x2'),(3,12,'x3');",
				change:  "BEGIN; ALTER TABLE kd RENAME COLUMN b TO tmp; ALTER TABLE kd RENAME COLUMN c TO b; ALTER TABLE kd RENAME COLUMN tmp TO c; COMMIT;",
				insert:  "INSERT INTO kd (id,b,c) VALUES (100,'nb',300)",
				markers: rsfOrderMarkers,
			},
			{
				name:    "swap-and-retype",
				create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
				change:  "BEGIN; ALTER TABLE kd RENAME COLUMN b TO tmp; ALTER TABLE kd RENAME COLUMN c TO b; ALTER TABLE kd RENAME COLUMN tmp TO c; ALTER TABLE kd ALTER COLUMN c TYPE BIGINT; COMMIT;",
				insert:  "INSERT INTO kd (id,b,c) VALUES (100,1000,5000000000)",
				markers: rsfOrderMarkers,
			},
			{
				name:    "two-columns-widened",
				create:  "CREATE TABLE kd (id INT PRIMARY KEY, b NUMERIC(10,2), c NUMERIC(10,2)); INSERT INTO kd VALUES (1,1.25,2.25),(2,1.5,2.5),(3,1.75,2.75);",
				change:  "ALTER TABLE kd ALTER COLUMN b TYPE NUMERIC(12,4), ALTER COLUMN c TYPE NUMERIC(12,4);",
				insert:  "INSERT INTO kd (id,b,c) VALUES (100,1.2345,2.3456)",
				markers: rsfMultiMarkers,
			},
			{
				// Review F2: the name b survives with c's column behind it —
				// a DROP of c to a name-only classifier. Visible only through
				// the stable column id, so live CDC→CDC only (a warm resume
				// or a seed-sourced first boundary has none: RENAME-SWAP-INVISIBLE).
				name:     "drop-then-rename-onto",
				create:   "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
				change:   "BEGIN; ALTER TABLE kd DROP COLUMN b; ALTER TABLE kd RENAME COLUMN c TO b; COMMIT;",
				insert:   "INSERT INTO kd (id,b) VALUES (100,1000)",
				markers:  []string{"now name a different column"},
				liveOnly: true,
			},
		}
	}
	return []rsfShape{
		{
			name:    "swap",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
			change:  "ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b",
			insert:  "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)",
			markers: rsfOrderMarkers,
		},
		{
			name:    "rotation",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT, d INT); INSERT INTO kd VALUES (1,10,20,30),(2,11,21,31),(3,12,22,32);",
			change:  "ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO d, RENAME COLUMN d TO b",
			insert:  "INSERT INTO kd (id,b,c,d) VALUES (100,1000,2000,3000)",
			markers: rsfOrderMarkers,
		},
		{
			name:    "swap-across-types",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c VARCHAR(20)); INSERT INTO kd VALUES (1,10,'x1'),(2,11,'x2'),(3,12,'x3');",
			change:  "ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b",
			insert:  "INSERT INTO kd (id,b,c) VALUES (100,'nb',300)",
			markers: rsfOrderMarkers,
		},
		{
			name:    "swap-and-retype",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
			change:  "ALTER TABLE kd CHANGE COLUMN b c BIGINT, CHANGE COLUMN c b INT",
			insert:  "INSERT INTO kd (id,b,c) VALUES (100,1000,5000000000)",
			markers: rsfOrderMarkers,
		},
		{
			name:    "two-columns-widened",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b DECIMAL(10,2), c DECIMAL(10,2)); INSERT INTO kd VALUES (1,1.25,2.25),(2,1.5,2.5),(3,1.75,2.75);",
			change:  "ALTER TABLE kd MODIFY COLUMN b DECIMAL(12,4), MODIFY COLUMN c DECIMAL(12,4)",
			insert:  "INSERT INTO kd (id,b,c) VALUES (100,1.2345,2.3456)",
			markers: rsfMultiMarkers,
		},
		{
			// The stated cost: a plain reorder is the same shape as a swap.
			name:    "reorder",
			create:  "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);",
			change:  "ALTER TABLE kd MODIFY COLUMN c INT AFTER id",
			insert:  "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)",
			markers: rsfOrderMarkers,
		},
	}
}

// rsfTable is a table's declared columns and its rows' values by name,
// ordered by id — what the grade compares.
type rsfTable struct {
	columns []string
	rows    string
}

func (r rsfTable) digest() string {
	sum := md5.Sum([]byte(r.rows))
	return hex.EncodeToString(sum[:])
}

// rsfColumns returns the one-column result of q as strings.
func rsfColumns(t *testing.T, conn *sql.DB, q string) []string {
	t.Helper()
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("read columns: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read columns: %v", err)
	}
	return out
}

func rsfRead(t *testing.T, db twfbDB) rsfTable {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	q := `SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kd' ORDER BY ORDINAL_POSITION`
	if db.engine == "postgres" {
		q = `SELECT attname FROM pg_attribute WHERE attrelid = 'public.kd'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`
	}
	var out rsfTable
	out.columns = rsfColumns(t, conn, q)
	// Values by NAME, in a fixed name order, so two tables declaring the
	// same columns in different orders still compare value for value.
	byName := append([]string(nil), out.columns...)
	sort.Strings(byName)
	data, err := conn.Query("SELECT " + strings.Join(byName, ", ") + " FROM kd ORDER BY id")
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	defer func() { _ = data.Close() }()
	var sb strings.Builder
	for data.Next() {
		vals := make([]sql.NullString, len(byName))
		ptrs := make([]any, len(byName))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := data.Scan(ptrs...); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		for i, v := range vals {
			fmt.Fprintf(&sb, "%s=%s;", byName[i], v.String)
		}
		sb.WriteString("\n")
	}
	if err := data.Err(); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	out.rows = sb.String()
	return out
}

// rsfAwaitRefusal waits for the run to stop on its own and checks the
// refusal names the shape.
func rsfAwaitRefusal(t *testing.T, run *twfbRun, markers []string, what string) error {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for !run.exited() {
		if time.Now().After(deadline) {
			_ = run.stop(t)
			t.Fatalf("%s: the stream did not refuse — it is still running past the change", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if run.err == nil || errors.Is(run.err, context.Canceled) {
		t.Fatalf("%s: the stream exited without refusing (err=%v)", what, run.err)
	}
	for _, m := range markers {
		if strings.Contains(run.err.Error(), m) {
			return run.err
		}
	}
	t.Fatalf("%s: the stream stopped, but not on this refusal (want one of %q): %v", what, markers, run.err)
	return nil
}

// rsfAssertUntouched grades a refusal: the target holds exactly what it
// held before the change, and the row written after it is not there.
func rsfAssertUntouched(t *testing.T, tgt twfbDB, before rsfTable, what string) {
	t.Helper()
	got := rsfRead(t, tgt)
	if strings.Join(got.columns, ",") != strings.Join(before.columns, ",") || got.digest() != before.digest() {
		t.Errorf("%s: the refusal changed the target\nbefore: %v\n%s\nafter:  %v\n%s", what, before.columns, before.rows, got.columns, got.rows)
	}
	if tgt.hasRow(t, "kd", 100) {
		t.Errorf("%s: the row written after the change was applied — the refusal came after a post-change row", what)
	}
}

// rsfAssertConverged grades the reconcile: the target declares the
// source's columns in the source's order and holds every row's values by
// name, md5 for md5.
func rsfAssertConverged(t *testing.T, src, tgt twfbDB, what string) {
	t.Helper()
	s, g := rsfRead(t, src), rsfRead(t, tgt)
	if strings.Join(s.columns, ",") != strings.Join(g.columns, ",") {
		t.Errorf("%s: target columns %v, source %v", what, g.columns, s.columns)
	}
	if s.digest() != g.digest() {
		t.Errorf("%s: target rows differ from the source\nsource:\n%s\ntarget:\n%s", what, s.rows, g.rows)
	}
}

// rsfEntry is one way a stream meets the change.
type rsfEntry string

const (
	rsfLiveCDC  rsfEntry = "live-cdc"  // running; an earlier row primed the cache
	rsfLiveSeed rsfEntry = "live-seed" // running; the change is the table's first boundary
	rsfWarm     rsfEntry = "warm"      // stopped across the change
	rsfRefuse   rsfEntry = "refuse"    // --schema-changes=refuse, live
)

func rsfRunCell(t *testing.T, srv rsfServer, shape rsfShape, entry rsfEntry, idx int) {
	t.Helper()
	tag := fmt.Sprintf("c%d", idx)
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
	if entry == rsfRefuse {
		cell.schemaChanges = "refuse"
	}
	src.exec(t, shape.create)
	what := fmt.Sprintf("%s/%s/%s", srv.label, shape.name, entry)

	var run *twfbRun
	if entry == rsfWarm {
		cell.coldStartAndStop(t, "kd", 3)
	} else {
		run = startTWFBRun(cell.streamer())
		if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
			t.Fatalf("%s: the cold start never delivered the seed rows (stream: %v)", what, run.stop(t))
		}
		cell.waitStreaming(t, run)
		if entry != rsfLiveSeed {
			src.exec(t, "INSERT INTO kd (id) VALUES (50)")
			if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
				t.Fatalf("%s: the priming row never landed (stream: %v)", what, run.stop(t))
			}
		}
	}
	before := rsfRead(t, tgt)
	src.exec(t, shape.change)
	src.exec(t, shape.insert)

	if entry == rsfWarm {
		run = startTWFBRun(cell.streamer())
	}
	err := rsfAwaitRefusal(t, run, shape.markers, what)
	t.Logf("%s refused: %v", what, err)
	rsfAssertUntouched(t, tgt, before, what)

	// A restart that changed nothing refuses again: the refused boundary is
	// never recorded, so the stream cannot forget it. Not for a liveOnly
	// shape: its evidence does not survive the restart, which accepts it —
	// the RENAME-SWAP-INVISIBLE residual (e), stated rather than pinned.
	if !shape.liveOnly {
		again := startTWFBRun(cell.streamer())
		_ = rsfAwaitRefusal(t, again, shape.markers, what+" (restart, target not reconciled)")
		rsfAssertUntouched(t, tgt, before, what+" (restart)")
	}

	// The operator applies the source's change on the target; the stream
	// then converges onto the source.
	tgt.exec(t, shape.change)
	final := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 100, final, 90*time.Second) {
		t.Fatalf("%s: after the reconcile the post-change row never landed (stream: %v)", what, final.stop(t))
	}
	rsfAssertConverged(t, src, tgt, what+" (after the reconcile)")
	if err := final.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("%s: the reconciled stream stopped with %v", what, err)
	}
	rsfAssertRecord(t, src, tgt, cell.streamID, what)
}

// rsfAssertRecord checks the stream's column-order record for kd on the
// target: the accepted change must have recorded the source's and the
// target's orders as they now stand — the record the NEXT order change is
// judged against.
func rsfAssertRecord(t *testing.T, src, tgt twfbDB, streamID, what string) {
	t.Helper()
	conn, err := sql.Open(tgt.driver(), tgt.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	q := "SELECT source_columns, target_columns FROM sluice_cdc_column_order WHERE stream_id = ? AND table_name = 'kd'"
	if tgt.engine == "postgres" {
		q = strings.Replace(q, "?", "$1", 1)
	}
	rows, err := conn.Query(q, streamID)
	if err != nil {
		t.Fatalf("%s: read the column-order record: %v", what, err)
	}
	defer func() { _ = rows.Close() }()
	want := [2]string{rsfEncodedOrder(t, rsfRead(t, src).columns), rsfEncodedOrder(t, rsfRead(t, tgt).columns)}
	var seen []string
	for rows.Next() {
		var s, g string
		if err := rows.Scan(&s, &g); err != nil {
			t.Fatal(err)
		}
		if [2]string{s, g} == want {
			return
		}
		seen = append(seen, s+" / "+g)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Errorf("%s: no column-order record for kd reads %s / %s; the target holds %v", what, want[0], want[1], seen)
}

func rsfEncodedOrder(t *testing.T, cols []string) string {
	t.Helper()
	s, err := ir.EncodeColumnNames(cols)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// rsfDivergence is how a cell's target comes to declare kd's columns in
// another order than the source before the swap — the review's F1 class,
// where the target already holding the swap's new order is NOT proof the
// operator applied it.
type rsfDivergence string

const (
	// rsfAddAfter: the source runs `ADD COLUMN x INT AFTER id` while the
	// stream forwards it; the target appends x at the END. MySQL family.
	rsfAddAfter rsfDivergence = "forwarded-add-after"
	// rsfAdopted: the target table was created ahead of the cold start in
	// another column order and adopted (ADR-0166).
	rsfAdopted rsfDivergence = "adopted-target"
)

// rsfRunDivergedCell drives a swap whose new order the target already
// holds: it must refuse as unprovable (the re-copy remedy), live or across a
// restart, leave the target untouched, refuse again on a plain restart, and
// converge after the remedy it names — `--reset-target-data`.
func rsfRunDivergedCell(t *testing.T, srv rsfServer, how rsfDivergence, warm bool, idx int) {
	t.Helper()
	tag := fmt.Sprintf("d%d", idx)
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
	what := fmt.Sprintf("%s/diverged/%s/warm=%t", srv.label, how, warm)
	var swap, insert string
	switch how {
	case rsfAddAfter:
		src.exec(t, "CREATE TABLE kd (id INT PRIMARY KEY, b INT); INSERT INTO kd VALUES (1,10),(2,11),(3,12);")
		swap = "ALTER TABLE kd RENAME COLUMN x TO b, RENAME COLUMN b TO x"
		insert = "INSERT INTO kd (id,b,x) VALUES (100,1000,2000)"
	case rsfAdopted:
		src.exec(t, "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);")
		tgt.exec(t, "CREATE TABLE kd (id INT PRIMARY KEY, c INT, b INT);")
		swap = rsfShapes(srv.dialect)[0].change
		insert = "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)"
	}
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("%s: the cold start never delivered the seed rows (stream: %v)", what, run.stop(t))
	}
	cell.waitStreaming(t, run)
	if how == rsfAddAfter {
		src.exec(t, "ALTER TABLE kd ADD COLUMN x INT AFTER id")
		src.exec(t, "INSERT INTO kd (id,x,b) VALUES (60,6,7)")
		if !tgt.waitRow(t, "kd", 60, run, 60*time.Second) {
			t.Fatalf("%s: the forwarded ADD COLUMN's row never landed (stream: %v)", what, run.stop(t))
		}
		if got := rsfRead(t, tgt).columns; strings.Join(got, ",") != "id,b,x" {
			t.Fatalf("%s: the target declares %v; the case needs the forwarded column at the end", what, got)
		}
	} else {
		src.exec(t, "INSERT INTO kd (id) VALUES (50)")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("%s: the priming row never landed (stream: %v)", what, run.stop(t))
		}
	}
	if warm {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: the clean stop returned %v", what, err)
		}
	}
	before := rsfRead(t, tgt)
	src.exec(t, swap)
	src.exec(t, insert)
	if warm {
		run = startTWFBRun(cell.streamer())
	}
	markers := []string{"--reset-target-data"}
	err := rsfAwaitRefusal(t, run, markers, what)
	if !strings.Contains(err.Error(), "COLUMN-ORDER-CHANGED") {
		t.Errorf("%s: the refusal does not carry COLUMN-ORDER-CHANGED: %v", what, err)
	}
	t.Logf("%s refused: %v", what, err)
	rsfAssertUntouched(t, tgt, before, what)
	again := startTWFBRun(cell.streamer())
	_ = rsfAwaitRefusal(t, again, markers, what+" (restart)")
	rsfAssertUntouched(t, tgt, before, what+" (restart)")

	// The remedy the refusal names: re-copy the stream — on a Postgres
	// source after dropping its slot, as the remedy says (here in SQL, what
	// `sluice slot drop` runs).
	if srv.dialect == "postgres" {
		src.exec(t, "SELECT pg_drop_replication_slot('sluice_rsf_"+tag+"')")
	}
	s := cell.streamer()
	s.ResetTargetData = true
	final := startTWFBRun(s)
	if !tgt.waitRow(t, "kd", 100, final, 180*time.Second) {
		t.Fatalf("%s: after --reset-target-data the post-change row never landed (stream: %v)", what, final.stop(t))
	}
	rsfAssertConverged(t, src, tgt, what+" (after --reset-target-data)")
	if err := final.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("%s: the re-copied stream stopped with %v", what, err)
	}
}

// rsfRunPreUpgradeCell is a stream whose target has no column-order
// records — what a stream started before this release presents to it,
// reproduced exactly by dropping the record table between a cold start and
// the resume (the resume's EnsureControlTable creates it empty again).
// swapWhileStopped: the swap happens before the stream can record
// anything, and must refuse as unprovable (the re-copy remedy). Otherwise
// the stream bootstraps a record at the table's first boundary, and a
// later live swap is then judged like any other: refused until the
// operator applies it, then converged, with the record graded.
func rsfRunPreUpgradeCell(t *testing.T, srv rsfServer, swapWhileStopped bool, idx int) {
	t.Helper()
	tag := fmt.Sprintf("u%d", idx)
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
	shape := rsfShapes(srv.dialect)[0]
	what := fmt.Sprintf("%s/pre-upgrade/swap-while-stopped=%t", srv.label, swapWhileStopped)
	src.exec(t, shape.create)
	cell.coldStartAndStop(t, "kd", 3)
	if srv.engine == "mariadb" {
		// The same records-free state, without the resume re-creating the
		// table: on MariaDB the applier's CREATE is logged with its session
		// prefix, which the look-ahead cannot bound to one table, and source
		// and target share this server's binlog — so the source stream would
		// refuse its own target's DDL (BUG301-MARIADB-SELF-DDL, filed).
		tgt.exec(t, "DELETE FROM sluice_cdc_column_order")
	} else {
		tgt.exec(t, "DROP TABLE sluice_cdc_column_order")
	}

	if swapWhileStopped {
		before := rsfRead(t, tgt)
		src.exec(t, shape.change)
		src.exec(t, shape.insert)
		run := startTWFBRun(cell.streamer())
		err := rsfAwaitRefusal(t, run, []string{"--reset-target-data"}, what)
		t.Logf("%s refused: %v", what, err)
		rsfAssertUntouched(t, tgt, before, what)
		return
	}
	run := startTWFBRun(cell.streamer())
	src.exec(t, "INSERT INTO kd (id) VALUES (50)")
	if !tgt.waitRow(t, "kd", 50, run, 90*time.Second) {
		t.Fatalf("%s: the first row after the upgrade never landed (stream: %v)", what, run.stop(t))
	}
	rsfAssertRecord(t, src, tgt, cell.streamID, what+" (bootstrapped at the first boundary)")
	before := rsfRead(t, tgt)
	src.exec(t, shape.change)
	src.exec(t, shape.insert)
	_ = rsfAwaitRefusal(t, run, []string{"RENAME COLUMN statements"}, what)
	rsfAssertUntouched(t, tgt, before, what)
	tgt.exec(t, shape.change)
	final := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 100, final, 90*time.Second) {
		t.Fatalf("%s: after the reconcile the post-change row never landed (stream: %v)", what, final.stop(t))
	}
	rsfAssertConverged(t, src, tgt, what+" (after the reconcile)")
	if err := final.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("%s: the reconciled stream stopped with %v", what, err)
	}
	rsfAssertRecord(t, src, tgt, cell.streamID, what)
}

// rsfSwapThenLater is a MySQL-family swap that a row was written under and
// that a later change then hid from the table's final shape: after the
// swap `c` holds what `b` held, and the later change drops or retypes the
// other swapped column before the stream reads the catalog. A stream that
// meets it behind the head (warm) sees a recorded shape for the swapped
// row that the catalog no longer describes; under binlog_row_metadata=FULL
// that shape decodes by its own names (Bug 301) and is never a schema
// boundary itself, so the swap reaches the doors only folded into the
// later boundary — where its names no longer show it.
var rsfSwapThenLater = []struct{ name, later string }{
	{"swap-then-drop", "ALTER TABLE kd DROP COLUMN b"},
	{"swap-then-retype", "ALTER TABLE kd MODIFY COLUMN b BIGINT"},
}

// rsfRunSwapThenLaterCell grades rsfSwapThenLater: refused before any
// row written after the swap reaches the target, the target untouched.
// No reconcile: the operator's remedy here is the re-copy, which the
// diverged cells already pin.
func rsfRunSwapThenLaterCell(t *testing.T, srv rsfServer, later string, entry rsfEntry, idx int) {
	t.Helper()
	tag := fmt.Sprintf("l%d", idx)
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
	src.exec(t, rsfShapes(srv.dialect)[0].create)
	what := fmt.Sprintf("%s/swap-then %q/%s", srv.label, later, entry)
	var run *twfbRun
	if entry == rsfWarm {
		cell.coldStartAndStop(t, "kd", 3)
	} else {
		run = startTWFBRun(cell.streamer())
		if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
			t.Fatalf("%s: the cold start never delivered the seed rows (stream: %v)", what, run.stop(t))
		}
		cell.waitStreaming(t, run)
		src.exec(t, "INSERT INTO kd (id) VALUES (50)")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("%s: the priming row never landed (stream: %v)", what, run.stop(t))
		}
	}
	before := rsfRead(t, tgt)
	src.exec(t, rsfShapes(srv.dialect)[0].change)
	src.exec(t, "INSERT INTO kd (id,b,c) VALUES (99,990,991)")
	src.exec(t, later)
	src.exec(t, "INSERT INTO kd (id,c) VALUES (100,2000)")
	if entry == rsfWarm {
		run = startTWFBRun(cell.streamer())
	}
	deadline := time.Now().Add(60 * time.Second)
	for !run.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !run.exited() {
		// Graded, not just timed out: what a stream that walked past the
		// swap left on the target, against the source.
		_ = run.stop(t)
		s, g := rsfRead(t, src), rsfRead(t, tgt)
		t.Fatalf("%s: the stream never refused — it ran past the swap\nsource %v:\n%s\ntarget %v:\n%s", what, s.columns, s.rows, g.columns, g.rows)
	}
	err := run.err
	// Behind the head without column names the reader's own look-ahead
	// refuses first (Bug 301, SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH); with
	// them, the boundary doors refuse the order change — or, on a warm
	// resume of a table with no retained typed shape (this cell stops
	// straight after the copy), the reader refuses the deferred shape for
	// having nothing to be held to (DEFERRED-SCHEMA-CHANGE). Every one
	// leaves the target untouched. The order check against a typed basis is
	// pinned by TestShapeAtPosition_MySQLPaths's swap-then-drop cells,
	// whose streams applied a row before stopping.
	ce, coded := sluicecode.FromError(err)
	if !errors.Is(err, ir.ErrColumnOrderChanged) && !strings.Contains(fmt.Sprint(err), "COLUMN-ORDER-CHANGED") &&
		!errors.Is(err, ir.ErrDeferredSchemaChange) &&
		(!coded || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch) {
		t.Fatalf("%s: the stream stopped, but not on a refusal of the swap: %v", what, err)
	}
	t.Logf("%s refused: %v", what, err)
	rsfAssertUntouched(t, tgt, before, what)
	if tgt.hasRow(t, "kd", 99) {
		t.Errorf("%s: the row written under the swapped names was applied", what)
	}
}

// rsfDeferredRetype is a MySQL-family type change a row was written under
// (BUG301-DEFERRED-RETYPE): `retype` changes b and writes row 99 under the
// new type, `then` drops c and writes row 100. Folded (a `then` that
// drops c): behind the head under FULL, row 99's recorded shape holds a
// column no read definition types, so it is never a boundary, and the row
// would reach the target under the target's OLD type before anything
// changed it — measured on DATETIME(6): the fractional seconds rounded
// away at exit 0. The unfolded widen is the control: its shape IS a
// boundary, so it forwards and converges.
var rsfDeferredRetype = []struct {
	name, create, retype, then string
	folded                     bool
}{
	{
		"datetime-precision-then-drop",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DATETIME, c INT); INSERT INTO kd VALUES (1,'2020-01-01 00:00:01',20),(2,'2020-01-01 00:00:02',21),(3,'2020-01-01 00:00:03',22);",
		"ALTER TABLE kd MODIFY COLUMN b DATETIME(6); INSERT INTO kd (id,b,c) VALUES (99,'2020-01-01 00:00:00.723456',991)",
		"ALTER TABLE kd DROP COLUMN c; INSERT INTO kd (id,b) VALUES (100,'2020-01-01 00:00:00.5')",
		true,
	},
	{
		"decimal-scale-then-drop",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DECIMAL(10,2), c INT); INSERT INTO kd VALUES (1,1.25,20),(2,1.5,21),(3,1.75,22);",
		"ALTER TABLE kd MODIFY COLUMN b DECIMAL(12,4); INSERT INTO kd (id,b,c) VALUES (99,1.2345,991)",
		"ALTER TABLE kd DROP COLUMN c; INSERT INTO kd (id,b) VALUES (100,2.3456)",
		true,
	},
	{
		"varchar-shrink-then-drop",
		"CREATE TABLE kd (id INT PRIMARY KEY, b VARCHAR(20), c INT); INSERT INTO kd VALUES (1,'a',20),(2,'bb',21),(3,'ccc',22);",
		"ALTER TABLE kd MODIFY COLUMN b VARCHAR(5); INSERT INTO kd (id,b,c) VALUES (99,'abc',991)",
		"ALTER TABLE kd DROP COLUMN c; INSERT INTO kd (id,b) VALUES (100,'x')",
		true,
	},
	{
		"varchar-widen-in-place",
		"CREATE TABLE kd (id INT PRIMARY KEY, b VARCHAR(20), c INT); INSERT INTO kd VALUES (1,'a',20),(2,'bb',21),(3,'ccc',22);",
		"ALTER TABLE kd MODIFY COLUMN b VARCHAR(40); INSERT INTO kd (id,b,c) VALUES (99,'abcdefghijklmnopqrstuvwxyz0123',991)",
		"INSERT INTO kd (id,b,c) VALUES (100,'x',1000)",
		false,
	},
}

// rsfDoubleRetype is a column retyped twice (review H1): the second change
// leaves row 99's recorded b with no read definition, so b is typed from
// the TABLE_MAP alone — the case a check that skipped such columns let
// through, landing row 99 rounded in the target's original type. Held on
// what the TABLE_MAP records (tableMapRecorded): DATETIME(6) differs from
// DATETIME on precision, DECIMAL(12,4) from DECIMAL(10,2) on scale. Warm
// entries only: live, each retype is its own boundary, judged by the
// forward path.
var rsfDoubleRetype = []struct{ name, create, retype, then string }{
	{
		"datetime6-then-varchar",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DATETIME, c INT); INSERT INTO kd VALUES (1,'2020-01-01 00:00:01',20),(2,'2020-01-01 00:00:02',21),(3,'2020-01-01 00:00:03',22);",
		"ALTER TABLE kd MODIFY COLUMN b DATETIME(6); INSERT INTO kd (id,b,c) VALUES (99,'2020-01-01 00:00:00.723456',991)",
		"ALTER TABLE kd MODIFY COLUMN b VARCHAR(40); INSERT INTO kd (id,b,c) VALUES (100,'x',1000)",
	},
	{
		"datetime6-then-text",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DATETIME, c INT); INSERT INTO kd VALUES (1,'2020-01-01 00:00:01',20),(2,'2020-01-01 00:00:02',21),(3,'2020-01-01 00:00:03',22);",
		"ALTER TABLE kd MODIFY COLUMN b DATETIME(6); INSERT INTO kd (id,b,c) VALUES (99,'2020-01-01 00:00:00.723456',991)",
		"ALTER TABLE kd MODIFY COLUMN b TEXT; INSERT INTO kd (id,b,c) VALUES (100,'x',1000)",
	},
	{
		"datetime6-then-timestamp6",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DATETIME, c INT); INSERT INTO kd VALUES (1,'2020-01-01 00:00:01',20),(2,'2020-01-01 00:00:02',21),(3,'2020-01-01 00:00:03',22);",
		"ALTER TABLE kd MODIFY COLUMN b DATETIME(6); INSERT INTO kd (id,b,c) VALUES (99,'2020-01-01 00:00:00.723456',991)",
		"ALTER TABLE kd MODIFY COLUMN b TIMESTAMP(6) NULL; INSERT INTO kd (id,b,c) VALUES (100,'2020-01-01 00:00:00.5',1000)",
	},
	{
		"decimal-scale-then-double",
		"CREATE TABLE kd (id INT PRIMARY KEY, b DECIMAL(10,2), c INT); INSERT INTO kd VALUES (1,1.25,20),(2,1.5,21),(3,1.75,22);",
		"ALTER TABLE kd MODIFY COLUMN b DECIMAL(12,4); INSERT INTO kd (id,b,c) VALUES (99,1.2345,991)",
		"ALTER TABLE kd MODIFY COLUMN b DOUBLE; INSERT INTO kd (id,b,c) VALUES (100,2.5,1000)",
	},
}

// rsfWarmPrimed is a warm resume of a stream that applied a row of the table
// before it stopped, so the table has a retained schema-history version
// (its first-touch boundary) — the typed basis a deferred shape is held to.
// Plain rsfWarm stops straight after the copy: no version, so a deferred
// shape refuses for having no basis at all.
const rsfWarmPrimed rsfEntry = "warm-primed"

// rsfRunDeferredRetypeCell grades rsfDeferredRetype against the source.
//
//   - live: the stream is at the head for each step (row 99 lands before
//     the DROP runs), so every change is its own boundary: it converges.
//   - warm on FULL, folded: refuses DEFERRED-SCHEMA-CHANGE before row 99
//     reaches the target — on the TYPE when primed (a typed basis exists),
//     for having no basis otherwise.
//   - warm on FULL, unfolded: converges.
//   - warm without FULL: Bug 301's look-ahead decides (its own matrix pins
//     which change it refuses); here it must refuse with the target
//     untouched or converge exactly.
func rsfRunDeferredRetypeCell(t *testing.T, srv rsfServer, create, retype, then string, folded bool, entry rsfEntry, idx int) {
	t.Helper()
	tag := fmt.Sprintf("r%d", idx)
	rsfRunDeferredRetypeCellOn(t, srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag), srv.label, srv.full, tag,
		create, retype, then, folded, entry)
}

// rsfRunDeferredRetypeCellOn is rsfRunDeferredRetypeCell for a given source
// and target, which may be different engines.
func rsfRunDeferredRetypeCellOn(t *testing.T, src, tgt twfbDB, label string, full bool, tag, create, retype, then string, folded bool, entry rsfEntry) {
	t.Helper()
	srv := rsfServer{label: label, full: full}
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
	src.exec(t, create)
	what := fmt.Sprintf("%s/deferred-retype %q %q/%s", srv.label, retype, then, entry)
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("%s: the cold start never delivered the seed rows (stream: %v)", what, run.stop(t))
	}
	cell.waitStreaming(t, run)
	if entry == rsfWarmPrimed {
		src.exec(t, "INSERT INTO kd (id) VALUES (50)")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("%s: the priming row never landed (stream: %v)", what, run.stop(t))
		}
	}
	if entry != rsfLiveCDC {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: the clean stop returned %v", what, err)
		}
	}
	before := rsfRead(t, tgt)
	src.exec(t, retype)
	if entry == rsfLiveCDC && !tgt.waitRow(t, "kd", 99, run, 60*time.Second) {
		t.Fatalf("%s: the stream did not apply the retype at the head (stream: %v)", what, run.stop(t))
	}
	src.exec(t, then)
	if entry != rsfLiveCDC {
		run = startTWFBRun(cell.streamer())
	}
	warm := entry != rsfLiveCDC
	mustRefuse := warm && srv.full && folded
	mustConverge := !warm || (srv.full && !folded)
	converged := tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	switch {
	case run.exited() && run.err != nil && !errors.Is(run.err, context.Canceled):
		if mustConverge {
			t.Fatalf("%s: the stream refused a change it should forward: %v", what, run.err)
		}
		if mustRefuse {
			if !errors.Is(run.err, ir.ErrDeferredSchemaChange) {
				t.Fatalf("%s: refused, but not DEFERRED-SCHEMA-CHANGE: %v", what, run.err)
			}
			if entry == rsfWarmPrimed && !strings.Contains(run.err.Error(), `column "b" is`) {
				t.Fatalf("%s: refused, but not on b's type (the primed basis is typed): %v", what, run.err)
			}
		}
		t.Logf("%s refused: %v", what, run.err)
		rsfAssertUntouched(t, tgt, before, what)
		if tgt.hasRow(t, "kd", 99) {
			t.Errorf("%s: the row written under the new type was applied", what)
		}
	case converged:
		if mustRefuse {
			_ = run.stop(t)
			s, g := rsfRead(t, src), rsfRead(t, tgt)
			t.Fatalf("%s: the stream ran past the folded change\nsource %v:\n%s\ntarget %v:\n%s", what, s.columns, s.rows, g.columns, g.rows)
		}
		rsfAssertConvergedOnSourceColumns(t, src, tgt, what)
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("%s: the stream stopped with %v", what, err)
		}
	default:
		_ = run.stop(t)
		t.Fatalf("%s: the stream neither refused nor delivered the last row", what)
	}
}

// rsfAssertConvergedOnSourceColumns: every source column, every row, by
// name, equals the target's — a column the source dropped and the target
// kept (the GC-44 WARN) is not compared.
func rsfAssertConvergedOnSourceColumns(t *testing.T, src, tgt twfbDB, what string) {
	t.Helper()
	s := rsfRead(t, src)
	g := rsfReadColumns(t, tgt, s.columns)
	if s.rows != g {
		t.Errorf("%s: target differs from the source on the source's columns\nsource:\n%s\ntarget:\n%s", what, s.rows, g)
	}
}

// rsfReadColumns renders db's kd rows over cols exactly as rsfRead does.
func rsfReadColumns(t *testing.T, db twfbDB, cols []string) string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	byName := append([]string(nil), cols...)
	sort.Strings(byName)
	data, err := conn.Query("SELECT " + strings.Join(byName, ", ") + " FROM kd ORDER BY id")
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	defer func() { _ = data.Close() }()
	var sb strings.Builder
	for data.Next() {
		vals := make([]sql.NullString, len(byName))
		ptrs := make([]any, len(byName))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := data.Scan(ptrs...); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		for i, v := range vals {
			fmt.Fprintf(&sb, "%s=%s;", byName[i], v.String)
		}
		sb.WriteString("\n")
	}
	if err := data.Err(); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	return sb.String()
}

// rsfRunServer runs every shape's cells on one server.
func rsfRunServer(t *testing.T, srv rsfServer) {
	if srv.dialect == "mysql" {
		k := 0
		for _, s := range rsfDeferredRetype {
			for _, entry := range []rsfEntry{rsfLiveCDC, rsfWarm, rsfWarmPrimed} {
				k++
				n, s := k, s
				t.Run(s.name+"/"+string(entry), func(t *testing.T) {
					rsfRunDeferredRetypeCell(t, srv, s.create, s.retype, s.then, s.folded, entry, n)
				})
			}
		}
		for _, s := range rsfDoubleRetype {
			for _, entry := range []rsfEntry{rsfWarm, rsfWarmPrimed} {
				k++
				n, s := k, s
				t.Run(s.name+"/"+string(entry), func(t *testing.T) {
					rsfRunDeferredRetypeCell(t, srv, s.create, s.retype, s.then, true, entry, n)
				})
			}
		}
		l := 0
		for _, s := range rsfSwapThenLater {
			for _, entry := range []rsfEntry{rsfLiveCDC, rsfWarm} {
				l++
				n, later := l, s.later
				t.Run(s.name+"/"+string(entry), func(t *testing.T) {
					rsfRunSwapThenLaterCell(t, srv, later, entry, n)
				})
			}
		}
	}
	for i, stopped := range []bool{false, true} {
		t.Run(fmt.Sprintf("pre-upgrade/swap-while-stopped=%t", stopped), func(t *testing.T) {
			rsfRunPreUpgradeCell(t, srv, stopped, i+1)
		})
	}
	divergences := []rsfDivergence{rsfAdopted}
	if srv.dialect == "mysql" {
		divergences = append(divergences, rsfAddAfter)
	}
	d := 0
	for _, how := range divergences {
		for _, warm := range []bool{false, true} {
			d++
			n := d
			t.Run(fmt.Sprintf("diverged/%s/warm=%t", how, warm), func(t *testing.T) {
				rsfRunDivergedCell(t, srv, how, warm, n)
			})
		}
	}
	idx := 0
	for _, shape := range rsfShapes(srv.dialect) {
		entries := []rsfEntry{rsfLiveCDC, rsfWarm}
		if shape.liveOnly {
			entries = []rsfEntry{rsfLiveCDC}
		}
		if shape.name == "swap" {
			entries = append(entries, rsfLiveSeed, rsfRefuse)
		}
		if shape.name == "two-columns-widened" {
			entries = append(entries, rsfRefuse)
		}
		for _, entry := range entries {
			idx++
			n := idx
			t.Run(shape.name+"/"+string(entry), func(t *testing.T) {
				if entry == rsfRefuse {
					if shape.name == "two-columns-widened" {
						// Refuse mode judges column by column against the
						// target and already refused a widen (GC-44 F5).
						shape.markers = []string{"SCHEMA-CHANGE-REFUSED"}
					}
					if srv.dialect == "postgres" {
						// On a stream that forwards nothing the Postgres
						// reader's own gate refuses a same-OID rename or
						// retype it sees mid-session, before the pipeline's
						// check; a restart's first relation reaches the
						// pipeline's.
						shape.markers = append(append([]string(nil), shape.markers...), "incompatible schema change mid-stream")
					}
				}
				rsfRunCell(t, srv, shape, entry, n)
			})
		}
	}
}

func TestRenameSwapForward_MySQL80(t *testing.T) {
	t.Parallel()
	dsn, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	rsfRunServer(t, rsfMySQLFamily(t, "mysql-8.0", "mysql", dsn, buildMySQLDSN))
}

func TestRenameSwapForward_MySQL84(t *testing.T) {
	t.Parallel()
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	rsfRunServer(t, rsfMySQLFamily(t, "mysql-8.4", "mysql", dsn, buildMySQLDSN))
}

func TestRenameSwapForward_MariaDB(t *testing.T) {
	t.Parallel()
	dsn, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	rsfRunServer(t, rsfMySQLFamily(t, "mariadb-11.4", "mariadb", dsn, buildMySQLDSN))
}

// rsfFullMetadata switches a MySQL-family server to binlog_row_metadata=FULL
// before any cell runs, and checks the server took it. Under FULL the
// binlog reader decodes each row by the names its own TABLE_MAP recorded
// (Bug 301), not by a catalog read — a different path to the same schema
// boundary, so the whole matrix runs again on it.
func rsfFullMetadata(t *testing.T, rootDSN string) {
	t.Helper()
	conn, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Exec("SET GLOBAL binlog_row_metadata = FULL"); err != nil {
		t.Fatalf("set binlog_row_metadata: %v", err)
	}
	var got string
	if err := conn.QueryRow("SELECT @@global.binlog_row_metadata").Scan(&got); err != nil || got != "FULL" {
		t.Fatalf("binlog_row_metadata = %q (%v), want FULL", got, err)
	}
}

func TestRenameSwapForward_MySQL80_FullMetadata(t *testing.T) {
	t.Parallel()
	dsn, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	rsfFullMetadata(t, dsn)
	rsfRunServer(t, rsfFull(rsfMySQLFamily(t, "mysql-8.0/full", "mysql", dsn, buildMySQLDSN)))
}

func TestRenameSwapForward_MySQL84_FullMetadata(t *testing.T) {
	t.Parallel()
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	rsfFullMetadata(t, dsn)
	rsfRunServer(t, rsfFull(rsfMySQLFamily(t, "mysql-8.4/full", "mysql", dsn, buildMySQLDSN)))
}

func TestRenameSwapForward_MariaDB_FullMetadata(t *testing.T) {
	t.Parallel()
	dsn, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	rsfFullMetadata(t, dsn)
	rsfRunServer(t, rsfFull(rsfMySQLFamily(t, "mariadb-11.4/full", "mariadb", dsn, buildMySQLDSN)))
}

func TestRenameSwapForward_Postgres16(t *testing.T) {
	t.Parallel()
	srcDSN, _, cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 40)
	defer cleanup()
	root := twfbDB{engine: "postgres", dsn: srcDSN}
	rsfRunServer(t, rsfServer{
		label: "postgres-16", engine: "postgres", dialect: "postgres",
		newDB: func(t *testing.T, name string) twfbDB {
			t.Helper()
			root.exec(t, "CREATE DATABASE "+name)
			dsn, err := buildPGDSN(srcDSN, name)
			if err != nil {
				t.Fatalf("dsn for %s: %v", name, err)
			}
			return twfbDB{engine: "postgres", dsn: dsn}
		},
	})
}

// TestRenameSwapForward_ChainRestore is the backup entry point: a full,
// then the swap and a row inside an incremental's window, then a restore.
// The window's schema delta shows only the declared order changing — the
// column-order aspect, which chain replay (and the broker, through the
// same disposition) passed with no DDL until RENAME-SWAP-FORWARD. It must
// now refuse, and before the post-swap row reaches the target.
func TestRenameSwapForward_ChainRestore(t *testing.T) {
	for _, lane := range []acdLane{acdMySQLLane(false), acdPGLane(false)} {
		t.Run(lane.engine, func(t *testing.T) {
			shape := rsfShapes(lane.engine)[0] // the same-type swap
			lane.seed = shape.create
			if lane.engine == "postgres" {
				lane.seed += " ALTER TABLE kd REPLICA IDENTITY FULL; CREATE PUBLICATION sluice_pub FOR ALL TABLES;"
			}
			src, tgt, cleanup := lane.start(t)
			defer cleanup()
			lane.exec(t, src, lane.seed)
			eng, ok := engines.Get(lane.engine)
			if !ok {
				t.Fatalf("%s engine not registered", lane.engine)
			}
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			pos, teardown := lane.startPos(t, src)
			defer teardown()
			ctx := context.Background()
			if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("Backup.Run: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatalf("ReadManifest: %v", err)
			}
			full.Kind = irbackup.BackupKindFull
			full.EndPosition = pos
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("rewrite full: %v", err)
			}

			lane.exec(t, src, shape.change)
			lane.exec(t, src, shape.insert)
			incrCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err = (&IncrementalBackup{
				Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
				Window: 8 * time.Second, ChunkChanges: 3, SluiceVersion: "test",
			}).Run(incrCtx)
			cancel()
			if err != nil {
				t.Fatalf("IncrementalBackup.Run: %v", err)
			}

			restoreErr := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx)
			if restoreErr == nil || !strings.Contains(restoreErr.Error(), "COLUMN-ORDER-CHANGED") {
				t.Fatalf("restore across the swap = %v; want the column-order refusal", restoreErr)
			}
			t.Logf("restore refused: %v", restoreErr)
			if (twfbDB{engine: lane.engine, dsn: tgt}).hasRow(t, "kd", 100) {
				t.Errorf("the post-swap row reached the target before the refusal")
			}
		})
	}
}

// TestRenameSwapForward_MySQLToPostgres_ReorderRemedy pins review F4 on a
// real cross-engine pair: a MySQL source REORDERS a column, and the
// Postgres target cannot — so the refusal must not tell the operator to run
// the same `MODIFY … AFTER` there, and the remedy it does name, the re-copy,
// must converge the target.
// TestRenameSwapForward_MySQLToPostgres_DeferredRetype is the double-retype
// cell on a Postgres target (review H1): a DECIMAL(10,2) column retyped to
// DECIMAL(12,4), written, then to DOUBLE. On a MySQL target the applier's
// DECIMAL-SCALE-EXCEEDED would catch row 99's lost digits after the fact;
// a Postgres NUMERIC(10,2) rounds them without an error, so the reader's
// refusal is the only door. FULL metadata, warm-primed.
func TestRenameSwapForward_MySQLToPostgres_DeferredRetype(t *testing.T) {
	srcDSN, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	rsfFullMetadata(t, srcDSN)
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	s := rsfDoubleRetype[len(rsfDoubleRetype)-1] // decimal-scale-then-double
	rsfRunDeferredRetypeCellOn(t, twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN},
		"mysql-8.4/full→postgres", true, "rxpg", s.create, s.retype, s.then, true, rsfWarmPrimed)
}

func TestRenameSwapForward_MySQLToPostgres_ReorderRemedy(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-x-reorder"}
	src.exec(t, "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22);")
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed rows (stream: %v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	before := rsfRead(t, tgt)
	src.exec(t, "ALTER TABLE kd MODIFY COLUMN c INT AFTER id")
	src.exec(t, "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)")
	err := rsfAwaitRefusal(t, run, []string{"COLUMN-ORDER-CHANGED"}, "mysql→postgres reorder")
	msg := err.Error()
	if strings.Contains(msg, "the same MODIFY") || !strings.Contains(msg, "cannot follow") || !strings.Contains(msg, "--reset-target-data") {
		t.Errorf("the refusal's remedy is not runnable on a Postgres target: %v", err)
	}
	rsfAssertUntouched(t, tgt, before, "mysql→postgres reorder")

	s := cell.streamer()
	s.ResetTargetData = true
	final := startTWFBRun(s)
	if !tgt.waitRow(t, "kd", 100, final, 180*time.Second) {
		t.Fatalf("after --reset-target-data the post-change row never landed (stream: %v)", final.stop(t))
	}
	rsfAssertConverged(t, src, tgt, "mysql→postgres reorder (after --reset-target-data)")
	if err := final.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the re-copied stream stopped with %v", err)
	}
}
