//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F5 pins: a stream that forwards no source DDL — --schema-changes=
// refuse, a multi-database stream — stops with SCHEMA-CHANGE-REFUSED at a
// schema change the target cannot hold, instead of rounding every following
// value into the old column at exit 0 (schema_change_refuse.go).
//
// Change kinds, one table each: a temporal precision widen, a decimal widen,
// FLOAT → DOUBLE, a VARCHAR widen and an ADD COLUMN (each refused, then
// recovered by the drained model: the same change applied on the target,
// after which the restart passes and the probe row lands exact); a DROP
// COLUMN (target-only: WARN, passes); an unchanged control; and the target
// widened AHEAD of the source (the drained model run first: passes, and the
// source's later widen passes too).
//
// Restart kinds: the change made while the stream is stopped; the change made
// while it runs, on a table that took no row since the cold start (so on
// Postgres too it is this check that refuses, not the reader's
// cached-relation gate); and, binlog only, the DDL whose position an
// out-of-scope commit persisted past (GC-44 D3).
//
// The verdict per cell is read off the TARGET — its catalog's declared type
// and the probe row read back — and, for a refusal, the stream's error and
// the probe row's absence. The availability half (a restart with no change
// refuses nothing) is TestStreamer_RefuseFamilyMatrix_* and the multi-
// database no-change phase below.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// refusePGTargetRecovery is the drained-model target DDL for a cell of the
// twfb dialects on a Postgres target: the column's post-change type, which
// the cell already states as the Postgres catalog spells it (wantType), so
// a change to a dialect's DDL carries its recovery with it. ADD COLUMN is
// the one shape that is not a retype.
func refusePGTargetRecovery(c twfbChangeCell) string {
	if c.table == "t_add" {
		return "ALTER TABLE t_add ADD COLUMN extra integer NOT NULL DEFAULT 5"
	}
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", c.table, c.column, c.wantType)
}

// waitRefused waits for the run to exit and returns its error.
func waitRefused(t *testing.T, run *twfbRun, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if run.exited() {
			return run.err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// assertRefused asserts a refusal that names table and left the probe row
// off the target with the column's type unchanged. wantMarker requires the
// SCHEMA-CHANGE-REFUSED marker (false only where the Postgres reader's own
// mid-stream gate refuses first).
func assertRefused(t *testing.T, cell twfbCell, c twfbChangeCell, run *twfbRun, wantMarker bool, kind string) {
	t.Helper()
	err := waitRefused(t, run, 60*time.Second)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Errorf("[%s] %s: the stream did not refuse (err %v)", kind, c.table, run.stop(t))
	} else {
		if wantMarker && (!strings.Contains(err.Error(), schemaChangeRefusedMarker) || !strings.Contains(err.Error(), c.table)) {
			t.Errorf("[%s] %s: refused without the %s marker naming the table: %v", kind, c.table, schemaChangeRefusedMarker, err)
		}
		t.Logf("[%s] %s refused: %v", kind, c.table, err)
	}
	if cell.tgt.hasRow(t, c.table, 2) {
		t.Errorf("[%s] %s: the probe row LANDED past the refusal", kind, c.table)
	}
	if got := cell.tgt.columnType(t, c.table, c.column); got == c.wantType {
		t.Errorf("[%s] %s.%s changed on the target to %q — refuse mode must not apply DDL", kind, c.table, c.column, got)
	}
}

// runRefusePinMatrix drives every cell for one source into a Postgres
// target, under --schema-changes=refuse.
func runRefusePinMatrix(t *testing.T, cell twfbCell, d twfbDialect) {
	t.Helper()
	cell.schemaChanges = "refuse"
	byTable := map[string]twfbChangeCell{}
	for _, c := range d.forward {
		byTable[c.table] = c
	}
	refused := []twfbChangeCell{byTable["t_fsp"], byTable["t_dec"], byTable["t_flt"], byTable["t_vc"], byTable["t_add"]}
	passing := []twfbChangeCell{byTable["t_drop"], byTable["t_ctl"]}
	live := d.widen("t_live")
	ahead := d.widen("t_ahead")
	oos := d.widen("t_d3")
	binlog := cell.src.engine != "postgres"
	all := append(append(append([]twfbChangeCell{}, refused...), passing...), live, ahead)
	if binlog {
		all = append(all, oos)
		cell.src.exec(t, "CREATE TABLE t_oos (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
		cell.exclude = []string{"t_oos"}
	}
	for _, c := range all {
		if c.table == "" {
			t.Fatal("the dialect lacks a cell this matrix needs")
		}
		cell.src.exec(t, c.create)
	}

	// The cold start, under refuse mode; then the live widen. The table took
	// no row since the cold start, so on Postgres too it is this check, not
	// the reader's cached-relation gate, that refuses.
	run := startTWFBRun(cell.streamer())
	for _, c := range all {
		if !cell.tgt.waitRow(t, c.table, 1, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered %s (stream: %v)", c.table, run.stop(t))
		}
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, live.ddl)
	cell.src.exec(t, live.probe)
	assertRefused(t, cell, live, run, true, "live")
	_ = run.stop(t)
	cell.tgt.exec(t, refusePGTargetRecovery(live))

	if binlog {
		// GC-44 D3 under refuse mode: the DDL, then an out-of-scope commit
		// whose position the stream persists past it, then a stop. The
		// restart replays no DDL, so the only boundary t_d3 can take is its
		// first touch — armed on this stream since F5.
		run := startTWFBRun(cell.streamer())
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
			t.Fatalf("[D3] the stream returned %v before its probe", err)
		}
		run = startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		cell.src.exec(t, oos.probe)
		assertRefused(t, cell, oos, run, true, "D3")
		_ = run.stop(t)
		cell.tgt.exec(t, refusePGTargetRecovery(oos))
	}

	// Each refused change, made while the stream is stopped: refused at the
	// restart, then recovered on the target before the next.
	for _, c := range refused {
		cell.src.exec(t, c.ddl)
		run := startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		cell.src.exec(t, c.probe)
		assertRefused(t, cell, c, run, true, "stopped")
		_ = run.stop(t)
		cell.tgt.exec(t, refusePGTargetRecovery(c))
	}

	// The passing cells, the target widened ahead of the source, and the
	// recovered restart: every probe row lands exact, nothing refuses.
	for _, c := range passing {
		if c.ddl != "" {
			cell.src.exec(t, c.ddl)
		}
	}
	cell.tgt.exec(t, refusePGTargetRecovery(ahead))
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	for _, c := range passing {
		cell.src.exec(t, c.probe)
	}
	// The target is ahead and the source not yet: a whole-second row.
	cell.src.exec(t, strings.Replace(strings.Replace(ahead.probe, "(2,", "(3,", 1), "10:11:12.345678", "09:00:00", 1))
	probes := append(append(append([]twfbChangeCell{}, refused...), passing...), live)
	if binlog {
		probes = append(probes, oos)
	}
	for _, c := range probes {
		if !cell.tgt.waitRow(t, c.table, 2, run, 90*time.Second) {
			t.Fatalf("[recovered] %s's probe row never landed (stream: %v)\n%s", c.table, run.stop(t), divergenceLines(logs.String()))
		}
	}
	if !cell.tgt.waitRow(t, ahead.table, 3, run, 90*time.Second) {
		t.Fatalf("[ahead] the row before the source widen never landed (stream: %v)\n%s", run.stop(t), divergenceLines(logs.String()))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[recovered] the stream returned %v", err)
	}
	// The source catches up while the stream is stopped (a Postgres source
	// refuses any mid-stream type change under refuse mode at its reader, a
	// pre-existing loud gate), then a fractional row.
	cell.src.exec(t, ahead.ddl)
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	cell.src.exec(t, ahead.probe)
	if !cell.tgt.waitRow(t, ahead.table, 2, run, 90*time.Second) {
		t.Fatalf("[ahead] the probe row never landed after the source caught up (stream: %v)\n%s", run.stop(t), divergenceLines(logs.String()))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[ahead] the stream returned %v", err)
	}
	for _, c := range append(probes, ahead) {
		c.verdict(t, cell.tgt, "recovered")
	}
	if lines := logLinesFor(logs, refuseLogTargetOnly, "t_drop"); len(lines) == 0 {
		t.Error("[recovered] the DROP COLUMN made while stopped was not WARNed")
	}
	if lines := logLinesFor(logs, refuseLogAhead, "t_ahead"); len(lines) == 0 {
		t.Error("[ahead] the target-ahead column was not logged — the cell did not reach the containment arm")
	}
	if lines := divergenceLines(logs.String()); lines != "" {
		t.Errorf("[recovered] a refusal or forward on the recovered restart:\n%s", lines)
	}
}

func TestStreamer_RefusePinMatrix_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runRefusePinMatrix(t, twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "refuse-pin-mysql"}, mysqlToPGDialect())
}

func TestStreamer_RefusePinMatrix_MariaDBToPostgres(t *testing.T) {
	srcDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runRefusePinMatrix(t, twfbCell{src: twfbDB{"mariadb", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "refuse-pin-mariadb"}, mysqlToPGDialect())
}

func TestStreamer_RefusePinMatrix_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	runRefusePinMatrix(t, twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "refuse-pin-pg"}, pgToPGDialect())
}

// multiNamespaceTarget is a multi-database target's dialect for the pin.
type multiNamespaceTarget struct {
	db twfbDB
	// columnType, readTs, rows and widen are per-namespace SQL.
	columnType func(ns string) string
	readTs     func(ns string, id int) string
	rows       func(ns string) string
	widen      func(ns string) string
}

func pgMultiNamespaceTarget(dsn string) multiNamespaceTarget {
	return multiNamespaceTarget{
		db: twfbDB{"postgres", dsn},
		columnType: func(ns string) string {
			return fmt.Sprintf(`SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
				JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relname = 'w' AND a.attname = 'ts' AND n.nspname = '%s'`, ns)
		},
		readTs: func(ns string, id int) string {
			return fmt.Sprintf("SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM %s.w WHERE id = %d", ns, id)
		},
		rows:  func(ns string) string { return fmt.Sprintf("SELECT count(*) FROM %s.w", ns) },
		widen: func(ns string) string { return fmt.Sprintf("ALTER TABLE %s.w ALTER COLUMN ts TYPE timestamp(6)", ns) },
	}
}

func mysqlMultiNamespaceTarget(dsn string) multiNamespaceTarget {
	return multiNamespaceTarget{
		db: twfbDB{"mysql", dsn},
		columnType: func(ns string) string {
			return fmt.Sprintf(`SELECT COLUMN_TYPE FROM information_schema.columns
				WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = 'w' AND COLUMN_NAME = 'ts'`, ns)
		},
		readTs: func(ns string, id int) string {
			return fmt.Sprintf("SELECT DATE_FORMAT(ts, '%%Y-%%m-%%d %%H:%%i:%%s.%%f') FROM %s.w WHERE id = %d", ns, id)
		},
		rows:  func(ns string) string { return fmt.Sprintf("SELECT count(*) FROM %s.w", ns) },
		widen: func(ns string) string { return fmt.Sprintf("ALTER TABLE %s.w MODIFY ts DATETIME(6) NOT NULL", ns) },
	}
}

// waitRows polls a namespace's row count.
func (m multiNamespaceTarget) waitRows(t *testing.T, ns string, n int, run *twfbRun, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.db.scalar(t, m.rows(ns)) == fmt.Sprint(n) {
			return true
		}
		if run != nil && run.exited() {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// runMultiDatabaseRefusePin drives a two-namespace stream (a same-named
// table `w` in each, so a witness keyed by bare name alone would read the
// wrong namespace's catalog): a no-change restart (availability), a widen
// in ONE namespace made while stopped (refused, naming that namespace;
// recovered), and a widen in the other made while the stream runs. src
// executes against the server; srcExec runs SQL in a namespace.
func runMultiDatabaseRefusePin(t *testing.T, cell twfbCell, tgt multiNamespaceTarget, srcExec func(ns, sql string), sourceWiden func(ns string) string, schemaChanges string) {
	t.Helper()
	cell.schemaChanges = schemaChanges
	a, b := cell.databases[0], cell.databases[1]
	insert := func(ns string, id int, ts string) {
		srcExec(ns, fmt.Sprintf("INSERT INTO w VALUES (%d, '%s')", id, ts))
	}

	run := startTWFBRun(cell.streamer())
	for _, ns := range cell.databases {
		if !tgt.waitRows(t, ns, 1, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered %s.w (stream: %v)", ns, run.stop(t))
		}
	}
	cell.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the cold start's stop returned %v", err)
	}

	// Availability: a restart with no schema change refuses nothing, and
	// both namespaces' boundaries were checked (anti-vacuity).
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	insert(a, 2, "2026-10-02 10:00:02")
	insert(b, 2, "2026-10-02 10:00:02")
	for _, ns := range cell.databases {
		if !tgt.waitRows(t, ns, 2, run, 90*time.Second) {
			t.Fatalf("[no change] %s's row never landed (stream: %v)\n%s", ns, run.stop(t), divergenceLines(logs.String()))
		}
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("[no change] the stream returned %v", err)
	}
	if lines := divergenceLines(logs.String()); lines != "" {
		t.Errorf("PHANTOM: a no-change multi-database restart refused:\n%s", lines)
	}
	for _, ns := range cell.databases {
		if len(logLinesFor(logs, refuseLogMatch, ns+".w")) == 0 {
			t.Errorf("VACUOUS: no boundary match was logged for %s.w — the check never ran for it", ns)
		}
		for _, marker := range []string{refuseLogUnwitnessed, refuseLogTargetOnly, refuseLogAhead} {
			if lines := logLinesFor(logs, marker, ns+".w"); len(lines) > 0 {
				t.Errorf("PHANTOM on %s.w: %s", ns, strings.Join(lines, "\n"))
			}
		}
	}

	// A widen in b only, made while stopped: refused naming b, a untouched.
	srcExec(b, sourceWiden(b))
	narrowType := tgt.db.scalar(t, tgt.columnType(b))
	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	insert(b, 3, "2026-10-02 10:11:12.345678")
	err := waitRefused(t, run, 60*time.Second)
	if !errors.Is(err, context.Canceled) && err != nil && strings.Contains(err.Error(), schemaChangeRefusedMarker) {
		if !strings.Contains(err.Error(), b+".w") {
			t.Errorf("[stopped] the refusal does not name %s.w: %v", b, err)
		}
		t.Logf("[stopped] refused: %v", err)
	} else {
		t.Errorf("[stopped] the stream did not refuse with %s (err %v)", schemaChangeRefusedMarker, run.stop(t))
	}
	_ = run.stop(t)
	if got := tgt.db.scalar(t, tgt.readTs(b, 3)); !strings.HasPrefix(got, "<") {
		t.Errorf("[stopped] %s row 3 LANDED past the refusal as %q", b, got)
	}
	if got := tgt.db.scalar(t, tgt.columnType(b)); got != narrowType {
		t.Errorf("[stopped] %s.w.ts changed on the target to %q", b, got)
	}

	// The drained-model recovery: the widen applied on b's target namespace.
	tgt.db.exec(t, tgt.widen(b))
	logs = twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	if !tgt.waitRows(t, b, 3, run, 90*time.Second) {
		t.Fatalf("[recovered] %s row 3 never landed (stream: %v)\n%s", b, run.stop(t), divergenceLines(logs.String()))
	}
	if got, want := tgt.db.scalar(t, tgt.readTs(b, 3)), "2026-10-02 10:11:12.345678"; got != want {
		t.Errorf("[recovered] %s row 3 read back %q, want %q", b, got, want)
	}

	// A widen in a, made while the stream runs.
	srcExec(a, sourceWiden(a))
	insert(a, 3, "2026-10-02 10:11:12.345678")
	err = waitRefused(t, run, 60*time.Second)
	// A Postgres source's reader refuses a mid-stream type change itself
	// when nothing forwards (checkSchemaRace) — loud, before this check.
	wantMarker := cell.src.engine != "postgres"
	if err == nil || (wantMarker && (!strings.Contains(err.Error(), schemaChangeRefusedMarker) || !strings.Contains(err.Error(), a+".w"))) {
		t.Errorf("[live] the stream did not refuse %s.w (err %v)", a, run.stop(t))
	} else {
		t.Logf("[live] refused: %v", err)
	}
	_ = run.stop(t)
	if got := tgt.db.scalar(t, tgt.readTs(a, 3)); !strings.HasPrefix(got, "<") {
		t.Errorf("[live] %s row 3 LANDED past the refusal as %q", a, got)
	}
}

const multiDatabaseRefuseTable = "CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL) ENGINE=InnoDB; INSERT INTO w VALUES (1, '2026-10-02 10:00:00');"

// mysqlMultiDatabaseSource creates source_db (already there) and shop_db,
// each with `w`, and returns the server DSN and a per-database executor.
func mysqlMultiDatabaseSource(t *testing.T, srcDSN string) (string, func(ns, sql string)) {
	t.Helper()
	shopDSN := mustCreateMySQLDatabase(t, srcDSN, "shop_db")
	dsns := map[string]string{"source_db": srcDSN, "shop_db": shopDSN}
	exec := func(ns, q string) { twfbDB{"mysql", dsns[ns]}.exec(t, q) }
	exec("source_db", multiDatabaseRefuseTable)
	exec("shop_db", multiDatabaseRefuseTable)
	return serverDSN(t, srcDSN), exec
}

func mysqlSourceWiden(string) string { return "ALTER TABLE w MODIFY ts DATETIME(6) NOT NULL" }

func TestStreamer_MultiDatabaseRefuse_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	server, exec := mysqlMultiDatabaseSource(t, srcDSN)
	cell := twfbCell{
		src: twfbDB{"mysql", server}, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "multidb-refuse-m2p", databases: []string{"source_db", "shop_db"},
	}
	runMultiDatabaseRefusePin(t, cell, pgMultiNamespaceTarget(tgtDSN), exec, mysqlSourceWiden, "")
}

// The same under --schema-changes=refuse, which a multi-database stream also
// accepts (and which forwards nothing either way).
func TestStreamer_MultiDatabaseRefuse_MySQLToPostgresRefuseMode(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	server, exec := mysqlMultiDatabaseSource(t, srcDSN)
	cell := twfbCell{
		src: twfbDB{"mysql", server}, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "multidb-refuse-m2p-r", databases: []string{"source_db", "shop_db"},
	}
	runMultiDatabaseRefusePin(t, cell, pgMultiNamespaceTarget(tgtDSN), exec, mysqlSourceWiden, "refuse")
}

func TestStreamer_MultiDatabaseRefuse_MySQLToMySQL(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	server, exec := mysqlMultiDatabaseSource(t, srcDSN)
	cell := twfbCell{
		src: twfbDB{"mysql", server}, tgt: twfbDB{"mysql", tgtHomeDSN},
		streamID: "multidb-refuse-m2m", databases: []string{"source_db", "shop_db"},
	}
	runMultiDatabaseRefusePin(t, cell, mysqlMultiNamespaceTarget(tgtHomeDSN), exec, mysqlSourceWiden, "")
}

func TestStreamer_MultiDatabaseRefuse_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	src := twfbDB{"postgres", srcDSN}
	src.exec(t, `CREATE SCHEMA sales; CREATE SCHEMA billing;
		CREATE TABLE sales.w (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL);
		CREATE TABLE billing.w (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL);
		INSERT INTO sales.w VALUES (1, '2026-10-02 10:00:00');
		INSERT INTO billing.w VALUES (1, '2026-10-02 10:00:00');`)
	exec := func(ns, q string) { src.exec(t, "SET search_path TO "+ns+"; "+q) }
	widen := func(ns string) string { return "ALTER TABLE w ALTER COLUMN ts TYPE timestamp(6)" }
	cell := twfbCell{
		src: src, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "multidb-refuse-p2p", databases: []string{"sales", "billing"},
	}
	runMultiDatabaseRefusePin(t, cell, pgMultiNamespaceTarget(tgtDSN), exec, widen, "")
}
