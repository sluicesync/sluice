//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 pin matrix: the target-witnessed first boundary
// (schema_forward_witness.go), per source, across every change kind and
// every way a stream comes to see a table's first boundary with no
// pre-state it can trust.
//
// Change kinds (one table each, so each verdict is unambiguous): a temporal
// precision widen, a decimal scale widen, FLOAT → DOUBLE, a VARCHAR widen,
// ADD COLUMN (with the backfill of the rows the target already held), DROP
// COLUMN (WARN, the target keeps it), a zone swap, RENAME (refuse), a
// two-column change (refuse), and an unchanged control.
//
// Restart kinds: the DDL made while the stream is stopped; the DDL made
// after a restart, before the table's first row; an in-process retry
// (the target's backends killed mid-stream); the ADR-0091 §3 seed-guard
// boundary (the first boundary after a cold start); the drained-model
// recovery of each refusal (the DDL on both sides → no forward, no
// refusal); and, binlog only, the out-of-scope commit persisted past an
// in-scope DDL (GC-44 D3).
//
// The verdict per cell is read off the TARGET — its catalog's declared
// type and the row read back — never off the stream's own logs, except
// where the log is the only witness of a refusal's marker.
//
// Mutation coverage (the commit message records the runs): M1 (blind
// `!hadPre` baseline) fails every widen cell; M4 (first touch disarmed)
// fails the binlog out-of-scope cell; M5 (the blind seed-guard skip)
// fails the seed-guard cell.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// twfbChangeCell is one change kind on its own table.
type twfbChangeCell struct {
	table string
	// create builds the table and its seed row (id 1), in the source's
	// dialect.
	create string
	// ddl is the source change; "" for the control.
	ddl string
	// probe writes row id 2 after the change.
	probe string
	// column / wantType: the target's declared type of column afterwards.
	column, wantType string
	// readBack reads a value of the probe row off the target; want is it.
	readBack, want string
	// alsoRead / alsoWant: a second read (the added column on a row the
	// target already held — the backfill).
	alsoRead, alsoWant string
	// refuse: the first boundary must refuse; recover is the target DDL
	// the drained model applies before the restart that must then pass.
	refuse  bool
	recover string
}

// twfbDialect is one source's spelling of the matrix.
type twfbDialect struct {
	// forward are the cells that must forward (or WARN) after a restart.
	forward []twfbChangeCell
	// refusals are refused at the first boundary, then recovered.
	refusals []twfbChangeCell
	// widen is the precision-widen cell reused by the other restart kinds,
	// on a table named by the argument.
	widen func(table string) twfbChangeCell
}

// mysqlToPGDialect is the MySQL-family source spelling against a Postgres
// target (MySQL 8 file/pos and GTID, MariaDB 11.4).
func mysqlToPGDialect() twfbDialect {
	widen := func(tb string) twfbChangeCell {
		return twfbChangeCell{
			table:  tb,
			create: fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL) ENGINE=InnoDB; INSERT INTO %s VALUES (1, '2026-10-02 10:00:00');", tb, tb),
			ddl:    fmt.Sprintf("ALTER TABLE %s MODIFY ts DATETIME(6) NOT NULL", tb),
			probe:  fmt.Sprintf("INSERT INTO %s VALUES (2, '2026-10-02 10:11:12.345678')", tb),
			column: "ts", wantType: "timestamp(6) without time zone",
			readBack: fmt.Sprintf("SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM %s WHERE id = 2", tb),
			want:     "2026-10-02 10:11:12.345678",
		}
	}
	return twfbDialect{
		widen: widen,
		forward: []twfbChangeCell{
			widen("t_fsp"),
			{
				table:  "t_dec",
				create: "CREATE TABLE t_dec (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(10,2) NOT NULL) ENGINE=InnoDB; INSERT INTO t_dec VALUES (1, 1.25);",
				ddl:    "ALTER TABLE t_dec MODIFY v DECIMAL(10,4) NOT NULL",
				probe:  "INSERT INTO t_dec VALUES (2, 1.2345)",
				column: "v", wantType: "numeric(10,4)",
				readBack: "SELECT v::text FROM t_dec WHERE id = 2", want: "1.2345",
			},
			{
				table:  "t_flt",
				create: "CREATE TABLE t_flt (id BIGINT NOT NULL PRIMARY KEY, v FLOAT NOT NULL) ENGINE=InnoDB; INSERT INTO t_flt VALUES (1, 1.5);",
				ddl:    "ALTER TABLE t_flt MODIFY v DOUBLE NOT NULL",
				probe:  "INSERT INTO t_flt VALUES (2, 1.2345678901234)",
				column: "v", wantType: "double precision",
				readBack: "SELECT v::text FROM t_flt WHERE id = 2", want: "1.2345678901234",
			},
			{
				table:  "t_vc",
				create: "CREATE TABLE t_vc (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL) ENGINE=InnoDB; INSERT INTO t_vc VALUES (1, 'short');",
				ddl:    "ALTER TABLE t_vc MODIFY v VARCHAR(64) NOT NULL",
				probe:  "INSERT INTO t_vc VALUES (2, REPEAT('x', 40))",
				column: "v", wantType: "character varying(64)",
				readBack: "SELECT length(v)::text FROM t_vc WHERE id = 2", want: "40",
			},
			{
				table:  "t_add",
				create: "CREATE TABLE t_add (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB; INSERT INTO t_add VALUES (1, 1);",
				ddl:    "ALTER TABLE t_add ADD COLUMN extra INT NOT NULL DEFAULT 5",
				probe:  "INSERT INTO t_add VALUES (2, 2, 9)",
				column: "extra", wantType: "integer",
				readBack: "SELECT extra::text FROM t_add WHERE id = 2", want: "9",
				alsoRead: "SELECT extra::text FROM t_add WHERE id = 1", alsoWant: "5",
			},
			{
				table:  "t_drop",
				create: "CREATE TABLE t_drop (id BIGINT NOT NULL PRIMARY KEY, gone VARCHAR(8) NULL) ENGINE=InnoDB; INSERT INTO t_drop VALUES (1, 'a');",
				ddl:    "ALTER TABLE t_drop DROP COLUMN gone",
				probe:  "INSERT INTO t_drop VALUES (2)",
				column: "gone", wantType: "character varying(8)",
				readBack: "SELECT coalesce(gone, 'NULL') FROM t_drop WHERE id = 2", want: "NULL",
			},
			{
				table:  "t_ctl",
				create: "CREATE TABLE t_ctl (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL) ENGINE=InnoDB; INSERT INTO t_ctl VALUES (1, '2026-10-02 10:00:00');",
				probe:  "INSERT INTO t_ctl VALUES (2, '2026-10-02 10:11:12')",
				column: "ts", wantType: "timestamp(0) without time zone",
				readBack: "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t_ctl WHERE id = 2", want: "2026-10-02 10:11:12",
			},
		},
		refusals: []twfbChangeCell{
			{
				table:  "t_zone",
				create: "CREATE TABLE t_zone (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NULL) ENGINE=InnoDB; INSERT INTO t_zone VALUES (1, '2026-10-02 10:00:00');",
				ddl:    "ALTER TABLE t_zone MODIFY ts TIMESTAMP NULL",
				probe:  "INSERT INTO t_zone VALUES (2, '2026-10-02 10:11:12')",
				column: "ts", wantType: "timestamp(0) without time zone",
				refuse: true, recover: "ALTER TABLE t_zone ALTER COLUMN ts TYPE timestamp(0) with time zone USING ts AT TIME ZONE 'UTC'",
			},
			{
				table:  "t_ren",
				create: "CREATE TABLE t_ren (id BIGINT NOT NULL PRIMARY KEY, a INT NULL) ENGINE=InnoDB; INSERT INTO t_ren VALUES (1, 1);",
				ddl:    "ALTER TABLE t_ren RENAME COLUMN a TO b",
				probe:  "INSERT INTO t_ren VALUES (2, 2)",
				column: "a", wantType: "integer",
				refuse: true, recover: "ALTER TABLE t_ren RENAME COLUMN a TO b",
			},
			{
				table:  "t_multi",
				create: "CREATE TABLE t_multi (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL, v VARCHAR(16) NOT NULL) ENGINE=InnoDB; INSERT INTO t_multi VALUES (1, '2026-10-02 10:00:00', 'a');",
				ddl:    "ALTER TABLE t_multi MODIFY ts DATETIME(6) NOT NULL, MODIFY v VARCHAR(64) NOT NULL",
				probe:  "INSERT INTO t_multi VALUES (2, '2026-10-02 10:11:12.345678', 'b')",
				column: "ts", wantType: "timestamp(0) without time zone",
				refuse: true, recover: "ALTER TABLE t_multi ALTER COLUMN ts TYPE timestamp(6), ALTER COLUMN v TYPE varchar(64)",
			},
		},
	}
}

// pgToPGDialect is the Postgres source spelling against a Postgres target.
func pgToPGDialect() twfbDialect {
	widen := func(tb string) twfbChangeCell {
		return twfbChangeCell{
			table:  tb,
			create: fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL); INSERT INTO %s VALUES (1, '2026-10-02 10:00:00');", tb, tb),
			ddl:    fmt.Sprintf("ALTER TABLE %s ALTER COLUMN ts TYPE timestamp(6)", tb),
			probe:  fmt.Sprintf("INSERT INTO %s VALUES (2, '2026-10-02 10:11:12.345678')", tb),
			column: "ts", wantType: "timestamp(6) without time zone",
			readBack: fmt.Sprintf("SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM %s WHERE id = 2", tb),
			want:     "2026-10-02 10:11:12.345678",
		}
	}
	return twfbDialect{
		widen: widen,
		forward: []twfbChangeCell{
			widen("t_fsp"),
			{
				table:  "t_dec",
				create: "CREATE TABLE t_dec (id bigint PRIMARY KEY, v numeric(10,2) NOT NULL); INSERT INTO t_dec VALUES (1, 1.25);",
				ddl:    "ALTER TABLE t_dec ALTER COLUMN v TYPE numeric(10,4)",
				probe:  "INSERT INTO t_dec VALUES (2, 1.2345)",
				column: "v", wantType: "numeric(10,4)",
				readBack: "SELECT v::text FROM t_dec WHERE id = 2", want: "1.2345",
			},
			{
				table:  "t_flt",
				create: "CREATE TABLE t_flt (id bigint PRIMARY KEY, v real NOT NULL); INSERT INTO t_flt VALUES (1, 1.5);",
				ddl:    "ALTER TABLE t_flt ALTER COLUMN v TYPE double precision",
				probe:  "INSERT INTO t_flt VALUES (2, 1.2345678901234)",
				column: "v", wantType: "double precision",
				readBack: "SELECT v::text FROM t_flt WHERE id = 2", want: "1.2345678901234",
			},
			{
				table:  "t_vc",
				create: "CREATE TABLE t_vc (id bigint PRIMARY KEY, v varchar(16) NOT NULL); INSERT INTO t_vc VALUES (1, 'short');",
				ddl:    "ALTER TABLE t_vc ALTER COLUMN v TYPE varchar(64)",
				probe:  "INSERT INTO t_vc VALUES (2, repeat('x', 40))",
				column: "v", wantType: "character varying(64)",
				readBack: "SELECT length(v)::text FROM t_vc WHERE id = 2", want: "40",
			},
			{
				table:  "t_add",
				create: "CREATE TABLE t_add (id bigint PRIMARY KEY, v integer NOT NULL); INSERT INTO t_add VALUES (1, 1);",
				ddl:    "ALTER TABLE t_add ADD COLUMN extra integer NOT NULL DEFAULT 5",
				probe:  "INSERT INTO t_add VALUES (2, 2, 9)",
				column: "extra", wantType: "integer",
				readBack: "SELECT extra::text FROM t_add WHERE id = 2", want: "9",
				alsoRead: "SELECT extra::text FROM t_add WHERE id = 1", alsoWant: "5",
			},
			{
				table:  "t_drop",
				create: "CREATE TABLE t_drop (id bigint PRIMARY KEY, gone varchar(8)); INSERT INTO t_drop VALUES (1, 'a');",
				ddl:    "ALTER TABLE t_drop DROP COLUMN gone",
				probe:  "INSERT INTO t_drop VALUES (2)",
				column: "gone", wantType: "character varying(8)",
				readBack: "SELECT coalesce(gone, 'NULL') FROM t_drop WHERE id = 2", want: "NULL",
			},
			{
				table:  "t_ctl",
				create: "CREATE TABLE t_ctl (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL); INSERT INTO t_ctl VALUES (1, '2026-10-02 10:00:00');",
				probe:  "INSERT INTO t_ctl VALUES (2, '2026-10-02 10:11:12')",
				column: "ts", wantType: "timestamp(0) without time zone",
				readBack: "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t_ctl WHERE id = 2", want: "2026-10-02 10:11:12",
			},
		},
		refusals: []twfbChangeCell{
			{
				table:  "t_zone",
				create: "CREATE TABLE t_zone (id bigint PRIMARY KEY, ts timestamp); INSERT INTO t_zone VALUES (1, '2026-10-02 10:00:00');",
				ddl:    "ALTER TABLE t_zone ALTER COLUMN ts TYPE timestamptz USING ts AT TIME ZONE 'UTC'",
				probe:  "INSERT INTO t_zone VALUES (2, '2026-10-02 10:11:12+00')",
				column: "ts", wantType: "timestamp without time zone",
				refuse: true, recover: "ALTER TABLE t_zone ALTER COLUMN ts TYPE timestamptz USING ts AT TIME ZONE 'UTC'",
			},
			{
				table:  "t_ren",
				create: "CREATE TABLE t_ren (id bigint PRIMARY KEY, a integer); INSERT INTO t_ren VALUES (1, 1);",
				ddl:    "ALTER TABLE t_ren RENAME COLUMN a TO b",
				probe:  "INSERT INTO t_ren VALUES (2, 2)",
				column: "a", wantType: "integer",
				refuse: true, recover: "ALTER TABLE t_ren RENAME COLUMN a TO b",
			},
			{
				table:  "t_multi",
				create: "CREATE TABLE t_multi (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL, v varchar(16) NOT NULL); INSERT INTO t_multi VALUES (1, '2026-10-02 10:00:00', 'a');",
				ddl:    "ALTER TABLE t_multi ALTER COLUMN ts TYPE timestamp(6), ALTER COLUMN v TYPE varchar(64)",
				probe:  "INSERT INTO t_multi VALUES (2, '2026-10-02 10:11:12.345678', 'b')",
				column: "ts", wantType: "timestamp(0) without time zone",
				refuse: true, recover: "ALTER TABLE t_multi ALTER COLUMN ts TYPE timestamp(6), ALTER COLUMN v TYPE varchar(64)",
			},
		},
	}
}

// verdict asserts a forwarded/accepted cell on the target.
func (c twfbChangeCell) verdict(t *testing.T, tgt twfbDB, kind string) {
	t.Helper()
	if got := tgt.columnType(t, c.table, c.column); got != c.wantType {
		t.Errorf("[%s] %s.%s on the target is %q, want %q", kind, c.table, c.column, got, c.wantType)
	}
	if c.readBack != "" {
		if got := tgt.scalar(t, c.readBack); got != c.want {
			t.Errorf("[%s] %s probe row read back %q, want %q", kind, c.table, got, c.want)
		}
	}
	if c.alsoRead != "" {
		if got := tgt.scalar(t, c.alsoRead); got != c.alsoWant {
			t.Errorf("[%s] %s pre-existing row read back %q, want %q (the added-column backfill)", kind, c.table, got, c.alsoWant)
		}
	}
}

// runTWFBPinMatrix drives every cell for one source. oosTable, when set,
// is an excluded table on the binlog lane used by the D3 cell.
func runTWFBPinMatrix(t *testing.T, cell twfbCell, d twfbDialect, binlog bool) {
	t.Helper()
	after := d.widen("t_after")
	seed := d.widen("t_seed")
	retry := d.widen("t_retry")
	oos := d.widen("t_d3")
	all := append(append([]twfbChangeCell{}, d.forward...), d.refusals...)
	all = append(all, after, seed, retry)
	if binlog {
		all = append(all, oos)
		cell.src.exec(t, "CREATE TABLE t_oos (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
		cell.exclude = []string{"t_oos"}
	}
	for _, c := range all {
		cell.src.exec(t, c.create)
	}

	// The cold start, and on it the seed-guard boundary (D2): the first
	// boundary after a cold start is classified against the seed.
	run := startTWFBRun(cell.streamer())
	for _, c := range all {
		if !cell.tgt.waitRow(t, c.table, 1, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered %s (stream: %v)", c.table, run.stop(t))
		}
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, seed.ddl)
	cell.src.exec(t, seed.probe)
	if !cell.tgt.waitRow(t, seed.table, 2, run, 90*time.Second) {
		t.Fatalf("[seed-guard] the probe row never landed (stream: %v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the cold start's stop returned %v", err)
	}
	seed.verdict(t, cell.tgt, "seed-guard")

	// DDL while stopped, then the restart.
	for _, c := range d.forward {
		if c.ddl != "" {
			cell.src.exec(t, c.ddl)
		}
	}
	stoppedLogs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	probes := append([]twfbChangeCell{}, d.forward...)
	for _, c := range probes {
		cell.src.exec(t, c.probe)
	}
	// A loud cell (the VARCHAR widen, the ADD COLUMN) that stops the stream
	// must not hide the silent ones probed before it, so a missing row is
	// recorded and every verdict still runs before the test stops.
	landed := true
	for _, c := range probes {
		if !cell.tgt.waitRow(t, c.table, 2, run, 90*time.Second) {
			t.Errorf("[stopped] %s's probe row never landed (stream: %v)\n%s", c.table, run.stop(t), divergenceLines(stoppedLogs.String()))
			landed = false
			break
		}
	}
	if landed {
		// DDL after the restart, before the table's first row.
		cell.src.exec(t, after.ddl)
		cell.src.exec(t, after.probe)
		if !cell.tgt.waitRow(t, after.table, 2, run, 90*time.Second) {
			t.Errorf("[after-restart] the probe row never landed (stream: %v)", run.stop(t))
		}
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the restarted stream returned %v", err)
	}
	for _, c := range probes {
		c.verdict(t, cell.tgt, "stopped")
	}
	if !landed {
		t.FailNow()
	}
	after.verdict(t, cell.tgt, "after-restart")

	// An in-process retry: the table takes a boundary in this process (a
	// warm row), the DDL lands, and the target's backends are killed before
	// the table's next row — the apply fails, the retry loop re-wires the
	// intercept, and that row's boundary is a first boundary again.
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	cell.src.exec(t, strings.Replace(strings.Replace(retry.probe, "(2,", "(3,", 1), "10:11:12.345678", "09:00:00", 1))
	if !cell.tgt.waitRow(t, retry.table, 3, run, 90*time.Second) {
		t.Fatalf("[in-process retry] the warm row never landed (stream: %v)", run.stop(t))
	}
	cell.src.exec(t, retry.ddl)
	cell.tgt.exec(t, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()`)
	cell.src.exec(t, retry.probe)
	if !cell.tgt.waitRow(t, retry.table, 2, run, 180*time.Second) {
		t.Fatalf("[in-process retry] the probe row never landed (stream: %v)\n%s", run.stop(t), divergenceLines(logs.String()))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[in-process retry] the stream returned %v", err)
	}
	retry.verdict(t, cell.tgt, "in-process retry")
	if !strings.Contains(logs.String(), "applier: transient error; retrying") {
		t.Errorf("[in-process retry] no retry was logged — the cell did not exercise a re-wired intercept")
	}
	if len(logLinesFor(stoppedLogs, twfbLogTargetOnly, "t_drop")) == 0 {
		t.Error("[stopped] the DROP COLUMN made while stopped was not WARNed")
	}

	if binlog {
		// D3, isolated: the DDL, then an out-of-scope commit whose position
		// the stream persists past it, then a stop. The restart replays NO
		// DDL — every earlier one is behind the persisted position — so the
		// only boundary t_d3 can take is its first touch. (A restart that
		// does replay a DDL arms every table's first row through the
		// never-cleared pendingDDLActive, which is why this cannot share a
		// phase with the stopped-DDL cells: there it passes with D3 off.)
		run = startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		before := cell.persistedPosition(t)
		cell.src.exec(t, oos.ddl)
		cell.src.exec(t, "INSERT INTO t_oos VALUES (1)")
		deadline := time.Now().Add(30 * time.Second)
		for cell.persistedPosition(t) == before && time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
		}
		if cell.persistedPosition(t) == before {
			t.Fatalf("[D3] the out-of-scope commit never moved the persisted position past %s", before)
		}
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("[D3] the stream returned %v", err)
		}
		run = startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		cell.src.exec(t, oos.probe)
		if !cell.tgt.waitRow(t, oos.table, 2, run, 90*time.Second) {
			t.Errorf("[D3] the probe row never landed (stream: %v)", run.stop(t))
		}
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("[D3] the stream returned %v", err)
		}
		oos.verdict(t, cell.tgt, "out-of-scope commit past the DDL (D3)")
	}

	// Each refusal: refused before anything lands, then the drained-model
	// recovery passes with no forward and no refusal.
	for _, c := range d.refusals {
		cell.src.exec(t, c.ddl)
		logs := twfbCaptureLogs(t)
		run := startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		cell.src.exec(t, c.probe)
		if cell.tgt.waitRow(t, c.table, 2, run, 60*time.Second) {
			t.Errorf("[refuse] %s: the probe row LANDED — the first boundary was not refused", c.table)
		}
		err := run.stop(t)
		if err == nil || errors.Is(err, context.Canceled) {
			t.Errorf("[refuse] %s: the stream did not refuse (err %v)", c.table, err)
		} else {
			t.Logf("[refuse] %s: %v", c.table, err)
		}
		if got := cell.tgt.columnType(t, c.table, c.column); got != c.wantType {
			t.Errorf("[refuse] %s.%s changed on the target to %q before the refusal", c.table, c.column, got)
		}

		cell.tgt.exec(t, c.recover)
		logs = twfbCaptureLogs(t)
		run = startTWFBRun(cell.streamer())
		if !cell.tgt.waitRow(t, c.table, 2, run, 90*time.Second) {
			t.Fatalf("[drained recovery] %s: the probe row never landed (stream: %v)\n%s",
				c.table, run.stop(t), divergenceLines(logs.String()))
		}
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("[drained recovery] %s: the stream returned %v", c.table, err)
		}
		if lines := logLinesFor(logs, twfbLogForwarded, c.table); len(lines) > 0 {
			t.Errorf("[drained recovery] %s: a forward ran after the operator applied the change: %v", c.table, lines)
		}
	}
}

func TestTWFBPinMatrix_MySQLFilePosToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBPinMatrix(t, twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-pin-mysql"}, mysqlToPGDialect(), true)
}

func TestTWFBPinMatrix_MySQLGTIDToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLGTID(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBPinMatrix(t, twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-pin-gtid"}, mysqlToPGDialect(), true)
}

func TestTWFBPinMatrix_MariaDBToPostgres(t *testing.T) {
	srcDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBPinMatrix(t, twfbCell{src: twfbDB{"mariadb", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-pin-mariadb"}, mysqlToPGDialect(), true)
}

func TestTWFBPinMatrix_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	runTWFBPinMatrix(t, twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-pin-pg"}, pgToPGDialect(), false)
}
