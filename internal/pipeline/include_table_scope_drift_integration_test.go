//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// INCLUDE-TABLE-SCOPE-DRIFT (Bug 321), on real servers and through the real
// CLI binary: the refusal's remedies are only claimed where these tests run
// them.
//
//   - Path 1, pgoutput: a table added to --include-table on a warm restart was
//     never copied and is not in the publication. Before the door the stream
//     came back green and the table stayed empty or absent at exit 0.
//   - Path 2, every source: a `schema add-table` whose copy failed. Before the
//     door a later restart that included the table streamed changes into a
//     table missing its original rows.
//
// Each also carries the no-false-refusal half: an unchanged warm restart with
// writes, and (pgoutput) a narrowed scope, run without the refusal.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// scopeDriftRig drives one stream through the built binary.
type scopeDriftRig struct {
	t         *testing.T
	bin       string
	driver    string // the sluice engine name, both sides
	sqlDriver string // database/sql driver for the test's own queries
	src, tgt  string
	streamID  string
	logDir    string
	runs      int
}

func newScopeDriftRig(t *testing.T, driver, src, tgt string) *scopeDriftRig {
	t.Helper()
	sqlDriver := "pgx"
	if driver == "mysql" {
		sqlDriver = "mysql"
	}
	return &scopeDriftRig{
		t: t, bin: straddleBuildBinary(t), driver: driver, sqlDriver: sqlDriver,
		src: src, tgt: tgt, streamID: "itsd", logDir: t.TempDir(),
	}
}

func (r *scopeDriftRig) connArgs() []string {
	return []string{
		"--source-driver=" + r.driver, "--source=" + r.src,
		"--target-driver=" + r.driver, "--target=" + r.tgt,
	}
}

// startSync launches `sync start` in the background.
func (r *scopeDriftRig) startSync(tables string, extra ...string) *straddleBinaryRun {
	r.t.Helper()
	r.runs++
	args := append([]string{"--log-level=info", "--no-progress", "sync", "start"}, r.connArgs()...)
	args = append(args, "--stream-id="+r.streamID, "--include-table="+tables)
	if r.driver == "postgres" {
		args = append(args, "--slot-name="+r.streamID)
	}
	args = append(args, extra...)
	run := &straddleBinaryRun{t: r.t, bin: r.bin, args: args, logPath: filepath.Join(r.logDir, fmt.Sprintf("sync%d.log", r.runs))}
	run.start()
	r.t.Cleanup(run.end)
	return run
}

// addTable runs `schema add-table` to completion and returns its exit code
// and output.
func (r *scopeDriftRig) addTable(table string) (int, string) {
	r.t.Helper()
	args := append([]string{"--log-level=info", "schema", "add-table", table}, r.connArgs()...)
	args = append(args, "--stream-id="+r.streamID, "--yes")
	stdout, stderr, code := runSluice(r.t, r.bin, args...)
	return code, stdout + stderr
}

func (r *scopeDriftRig) exec(dsn, q string) {
	r.t.Helper()
	db, err := sql.Open(r.sqlDriver, dsn)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, stmt := range strings.Split(q, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			r.t.Fatalf("exec %q: %v", stmt, err)
		}
	}
}

// rows renders a table as "id|v,id|v" in id order; "<absent>" when it cannot
// be read (the table does not exist).
func (r *scopeDriftRig) rows(dsn, table, col string) string {
	db, err := sql.Open(r.sqlDriver, dsn)
	if err != nil {
		return "<absent>"
	}
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf("SELECT COALESCE(string_agg(id::text || '|' || %s, ',' ORDER BY id), '') FROM %s", col, table)
	if r.driver == "mysql" {
		q = fmt.Sprintf("SELECT COALESCE(GROUP_CONCAT(CONCAT(id, '|', %s) ORDER BY id SEPARATOR ','), '') FROM %s", col, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s string
	if err := db.QueryRowContext(ctx, q).Scan(&s); err != nil {
		return "<absent>"
	}
	return s
}

// waitRows polls until table renders exactly want, and reports the last read.
func (r *scopeDriftRig) waitRows(dsn, table, col, want string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	got := r.rows(dsn, table, col)
	for got != want && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		got = r.rows(dsn, table, col)
	}
	return got, got == want
}

// waitExit reports whether run exited within timeout.
func waitExit(run *straddleBinaryRun, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if run.exited() != nil {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return run.exited() != nil
}

func scopeDriftLog(t *testing.T, run *straddleBinaryRun) string {
	t.Helper()
	b, err := os.ReadFile(run.logPath)
	if err != nil {
		t.Fatalf("read %s: %v", run.logPath, err)
	}
	return string(b)
}

// requireScopeDriftRefusal asserts run exits, naming the marker, the table and
// each reason. A run that keeps going is Bug 321 itself.
func (r *scopeDriftRig) requireScopeDriftRefusal(run *straddleBinaryRun, table string, reasons ...string) string {
	r.t.Helper()
	if !waitExit(run, 90*time.Second) {
		r.t.Fatalf("warm restart kept streaming with %q in scope and never brought in — the Bug 321 silent omission%s", table, run.logTail())
	}
	log := scopeDriftLog(r.t, run)
	for _, want := range append([]string{"INCLUDE-TABLE-SCOPE-DRIFT", table + " ("}, reasons...) {
		if !strings.Contains(log, want) {
			r.t.Fatalf("the refusal does not contain %q%s", want, run.logTail())
		}
	}
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT: stream") {
			r.t.Logf("refusal: %s", line)
		}
	}
	return log
}

// requireStreaming asserts run is still running after settle.
func requireStreaming(t *testing.T, run *straddleBinaryRun, settle time.Duration, what string) {
	t.Helper()
	if waitExit(run, settle) {
		t.Fatalf("%s: the stream exited (%v), want it streaming — a false refusal%s", what, run.exited(), run.logTail())
	}
}

// establishKD cold-starts the stream on kd alone, proves CDC, and stops it.
func (r *scopeDriftRig) establishKD() {
	r.t.Helper()
	r.exec(r.src, "CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a')")
	run := r.startSync("kd")
	if got, ok := r.waitRows(r.tgt, "kd", "k", "1|a", 90*time.Second); !ok {
		r.t.Fatalf("cold start: target kd = %q, want 1|a%s", got, run.logTail())
	}
	r.exec(r.src, "INSERT INTO kd VALUES (2, 'b')")
	if got, ok := r.waitRows(r.tgt, "kd", "k", "1|a,2|b", 60*time.Second); !ok {
		r.t.Fatalf("cdc: target kd = %q, want 1|a,2|b%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second) // the source notices the dropped replication connection
}

// TestIncludeTableScopeDrift_PgoutputWidenedTableRefusedThenAddTableConverges
// is path 1 and the add-table remedy, plus the unchanged and narrowed
// restarts that must not refuse.
func TestIncludeTableScopeDrift_PgoutputWidenedTableRefusedThenAddTableConverges(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD()

	// While stopped: a new table with rows, and a kd write the refused run
	// must not apply.
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z'); INSERT INTO kd VALUES (3, 'c')")

	run := r.startSync("kd,nt")
	r.requireScopeDriftRefusal(run, "nt", "not in publication")
	if got := r.rows(tgt, "kd", "k"); got != "1|a,2|b" {
		t.Fatalf("the refused run applied changes: target kd = %q, want 1|a,2|b (refused before the reader opened)", got)
	}

	// The remedy the refusal names, through the CLI.
	if code, out := r.addTable("nt"); code != 0 {
		t.Fatalf("schema add-table nt: exit %d\n%s", code, out)
	}
	run = r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "restart after add-table")
	r.exec(src, "INSERT INTO nt VALUES (4, 'w'); INSERT INTO kd VALUES (4, 'd')")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w", 60*time.Second); !ok {
		t.Fatalf("after add-table: target nt = %q, want 1|x,2|y,3|z,4|w%s", got, run.logTail())
	}
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c,4|d", 60*time.Second); !ok {
		t.Fatalf("after add-table: target kd = %q, want 1|a,2|b,3|c,4|d%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)

	// No false refusal: the same scope again, with writes on both tables.
	r.exec(src, "INSERT INTO nt VALUES (5, 'v'); INSERT INTO kd VALUES (5, 'e')")
	run = r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "unchanged-scope warm restart")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v", 60*time.Second); !ok {
		t.Fatalf("unchanged restart: target nt = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)

	// Narrowing is the operator's choice, not a refusal.
	r.exec(src, "INSERT INTO kd VALUES (6, 'f')")
	run = r.startSync("kd")
	requireStreaming(t, run, 8*time.Second, "narrowed warm restart")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c,4|d,5|e,6|f", 60*time.Second); !ok {
		t.Fatalf("narrowed restart: target kd = %q%s", got, run.logTail())
	}
}

// TestIncludeTableScopeDrift_PgoutputRestartFromScratchRemedy measures the
// refusal's re-copy remedy: on a Postgres source --restart-from-scratch first
// refuses on the stream's existing slot, and converges once `sluice slot
// drop` has removed it — which is how the refusal words it.
func TestIncludeTableScopeDrift_PgoutputRestartFromScratchRemedy(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD()
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y')")

	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", "not in publication")

	run := r.startSync("kd,nt", "--restart-from-scratch")
	if !waitExit(run, 90*time.Second) || !strings.Contains(scopeDriftLog(t, run), "already exists") {
		t.Fatalf("--restart-from-scratch with the stream's slot in place: want the slot-exists refusal the remedy names%s", run.logTail())
	}
	stdout, stderr, code := runSluice(t, r.bin, "slot", "drop", "sluice_"+r.streamID, "--source-driver=postgres", "--source="+src, "--yes")
	if code != 0 {
		t.Fatalf("slot drop: exit %d\n%s%s", code, stdout, stderr)
	}
	run = r.startSync("kd,nt", "--restart-from-scratch")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y", 90*time.Second); !ok {
		t.Fatalf("--restart-from-scratch: target nt = %q, want 1|x,2|y%s", got, run.logTail())
	}
	r.exec(src, "INSERT INTO nt VALUES (3, 'z')")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z", 60*time.Second); !ok {
		t.Fatalf("--restart-from-scratch then CDC: target nt = %q%s", got, run.logTail())
	}
}

// failedAddTableThenRestart is path 2 on either engine with the add-table
// remedy: an add-table whose copy fails on a target CHECK, a restart that
// includes the table and is refused, then the fixed target and a re-run.
func failedAddTableThenRestart(t *testing.T, r *scopeDriftRig, checkDDL, dropCheckDDL string, reasons ...string) {
	t.Helper()
	failAddTableAndRestart(t, r, checkDDL, reasons...)

	// The remedy: make the target table able to hold the rows, empty it, and
	// add it again.
	r.exec(r.tgt, dropCheckDDL+"; DELETE FROM nt")
	if code, out := r.addTable("nt"); code != 0 {
		t.Fatalf("re-run of add-table: exit %d\n%s", code, out)
	}
	run := r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "restart after the re-run add-table")
	r.exec(r.src, "INSERT INTO nt VALUES (5, 'v')")
	if got, ok := r.waitRows(r.tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v", 60*time.Second); !ok {
		t.Fatalf("after the re-run add-table: target nt = %q, want 1|x,2|y,3|z,4|w,5|v%s", got, run.logTail())
	}
}

// failAddTableAndRestart establishes the stream on kd, fails an add-table of
// nt on a target CHECK that refuses source row 3, and asserts the restart
// that includes nt is refused without writing to it.
func failAddTableAndRestart(t *testing.T, r *scopeDriftRig, checkDDL string, reasons ...string) {
	t.Helper()
	r.establishKD()
	r.exec(r.src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	// A pre-created, empty target table whose CHECK refuses source row 3:
	// add-table's preflights pass and its copy fails.
	r.exec(r.tgt, checkDDL)

	code, out := r.addTable("nt")
	if code == 0 {
		t.Fatalf("add-table onto a target CHECK that refuses row 3 succeeded; the test's premise is gone\n%s", out)
	}
	if !strings.Contains(out, "stays recorded as pending") {
		t.Fatalf("the failed add-table does not say it left the table pending\n%s", out)
	}

	// The source keeps writing; the restart must not stream into the partial
	// copy.
	r.exec(r.src, "INSERT INTO nt VALUES (4, 'w')")
	partial := r.rows(r.tgt, "nt", "e")
	run := r.startSync("kd,nt")
	r.requireScopeDriftRefusal(run, "nt", append([]string{"did not complete"}, reasons...)...)
	if got := r.rows(r.tgt, "nt", "e"); got != partial {
		t.Fatalf("the refused run wrote to nt: %q, was %q", got, partial)
	}
}

// TestIncludeTableScopeDrift_PgoutputFailedAddTableRefused is path 2 on a
// pgoutput source. The failed add also took the table back out of the
// publication, so both reasons are named.
func TestIncludeTableScopeDrift_PgoutputFailedAddTableRefused(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	failedAddTableThenRestart(t, r,
		"CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CONSTRAINT nt_lt3",
		"not in publication")
}

// TestIncludeTableScopeDrift_MySQLFailedAddTableRefused is path 2 on a
// MySQL binlog source, which has no publication: the pending record alone
// carries it.
func TestIncludeTableScopeDrift_MySQLFailedAddTableRefused(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	r := newScopeDriftRig(t, "mysql", src, tgt)
	failedAddTableThenRestart(t, r,
		"CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CHECK nt_lt3")
}

// TestIncludeTableScopeDrift_MySQLFailedAddTableRestartFromScratch measures
// the re-copy remedy on a MySQL source (no slot to drop): --restart-from-scratch
// drops and re-copies every in-scope table — the CHECK-bearing nt among them,
// recreated from the source's shape — and clears the pending record, so the
// next plain restart streams on.
func TestIncludeTableScopeDrift_MySQLFailedAddTableRestartFromScratch(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	r := newScopeDriftRig(t, "mysql", src, tgt)
	failAddTableAndRestart(t, r, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))")

	run := r.startSync("kd,nt", "--restart-from-scratch")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w", 90*time.Second); !ok {
		t.Fatalf("--restart-from-scratch: target nt = %q, want 1|x,2|y,3|z,4|w%s", got, run.logTail())
	}
	// A change through CDC proves the cold start handed off — its anchor is
	// written and the pending record cleared, both before the apply loop —
	// so stopping now is not stopping mid-copy.
	r.exec(src, "INSERT INTO nt VALUES (5, 'v')")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v", 60*time.Second); !ok {
		t.Fatalf("--restart-from-scratch then CDC: target nt = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)

	r.exec(src, "INSERT INTO nt VALUES (6, 'u')")
	run = r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "plain restart after --restart-from-scratch (the re-copy clears the pending record)")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v,6|u", 60*time.Second); !ok {
		t.Fatalf("after the re-copy: target nt = %q%s", got, run.logTail())
	}
}
