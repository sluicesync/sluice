//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// INCLUDE-TABLE-SCOPE-DRIFT (Bug 321), on real servers and through the real
// CLI binary: the refusal's remedies are only claimed where these tests run
// them.
//
//   - A table named literally in --include-table that the stream never copied
//     refuses a warm resume, on every source, judged against the stream's
//     established-table record: pgoutput (where the table is not even
//     streamed), a shared publication that does deliver it (S1), MySQL with a
//     pre-created target table (where it streams from the position with its
//     earlier rows missing), and a table the stream narrowed away and came
//     back to.
//   - An add-table whose copy failed refuses until it is redone; a pending
//     name the source has since dropped is ignored (L2), and `sync health`
//     shows the record while it stands (S2).
//   - An unfiltered or glob-admitted new table does NOT refuse (ADR-0021; the
//     L1 regression of round 1) and is named in a WARN.
//   - A stream from before the record (legacy) resumes, and refuses through
//     its unshared publication — a slot on another database of the server
//     does not make it shared (F-B).
//   - A cold start refused before its point of no return (the slot-exists
//     refusal, a schema preflight) leaves the record as it was, so the plain
//     restart after it still refuses (S-A).
//   - A restart racing a real, still-copying add-table defers restartably and
//     converges once the add completes (F-A); `--abandon` drops the dead
//     add's snapshot slot (F-C).
//
// SLUICE_SCOPE_DRIFT_BIN, when set, runs a prebuilt binary instead of
// building this tree — how the pins were shown red against the previous
// HEAD.

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

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// scopeDriftRig drives one stream through the binary.
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

func scopeDriftBinary(t *testing.T) string {
	t.Helper()
	if bin := os.Getenv("SLUICE_SCOPE_DRIFT_BIN"); bin != "" {
		return bin
	}
	return straddleBuildBinary(t)
}

func newScopeDriftRig(t *testing.T, driver, src, tgt string) *scopeDriftRig {
	t.Helper()
	sqlDriver := "pgx"
	if driver == "mysql" {
		sqlDriver = "mysql"
	}
	return &scopeDriftRig{
		t: t, bin: scopeDriftBinary(t), driver: driver, sqlDriver: sqlDriver,
		src: src, tgt: tgt, streamID: "itsd", logDir: t.TempDir(),
	}
}

// sibling is a second stream on the same source, with its own id, slot and
// target.
func (r *scopeDriftRig) sibling(streamID, tgt string) *scopeDriftRig {
	c := *r
	c.streamID, c.tgt, c.runs = streamID, tgt, 100
	return &c
}

func (r *scopeDriftRig) connArgs() []string {
	return []string{
		"--source-driver=" + r.driver, "--source=" + r.src,
		"--target-driver=" + r.driver, "--target=" + r.tgt,
	}
}

// startSync launches `sync start` in the background; tables "" runs
// unfiltered.
func (r *scopeDriftRig) startSync(tables string, extra ...string) *straddleBinaryRun {
	r.t.Helper()
	r.runs++
	args := append([]string{"--log-level=info", "--no-progress", "sync", "start"}, r.connArgs()...)
	args = append(args, "--stream-id="+r.streamID)
	if tables != "" {
		args = append(args, "--include-table="+tables)
	}
	if r.driver == "postgres" {
		args = append(args, "--slot-name="+r.streamID)
	}
	args = append(args, extra...)
	run := &straddleBinaryRun{t: r.t, bin: r.bin, args: args, logPath: filepath.Join(r.logDir, fmt.Sprintf("%s-sync%d.log", r.streamID, r.runs))}
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

// health runs `sync health` and returns its exit code and output.
func (r *scopeDriftRig) health() (int, string) {
	r.t.Helper()
	stdout, stderr, code := runSluice(r.t, r.bin, "sync", "health", "--target-driver="+r.driver, "--target="+r.tgt, "--stream-id="+r.streamID)
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

// requireWarnNames asserts the run logged the not-established WARN naming
// table.
func requireWarnNames(t *testing.T, run *straddleBinaryRun, table string) {
	t.Helper()
	for _, line := range strings.Split(scopeDriftLog(t, run), "\n") {
		if strings.Contains(line, "INCLUDE-TABLE-NOT-ESTABLISHED") && strings.Contains(line, table) {
			t.Logf("warn: %s", line)
			return
		}
	}
	t.Fatalf("no INCLUDE-TABLE-NOT-ESTABLISHED WARN naming %q%s", table, run.logTail())
}

const (
	scopeDriftReasonNotEstablished = "never copied by this stream"
	scopeDriftReasonIncompleteAdd  = "did not complete"
	scopeDriftReasonUnpublished    = "not in publication"
	scopeDriftReasonDropped        = "left this stream's scope earlier"
	scopeDriftReasonRecreated      = "no longer in publication"
)

// establishKD cold-starts the stream on kd alone (tables names its filter;
// "" is unfiltered), proves CDC, and stops it.
func (r *scopeDriftRig) establishKD(tables string) {
	r.t.Helper()
	r.exec(r.src, "CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a')")
	run := r.startSync(tables)
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
// is the literal widening and its add-table remedy, plus the unchanged and
// narrowed restarts that must not refuse, and the re-widen that must.
func TestIncludeTableScopeDrift_PgoutputWidenedTableRefusedThenAddTableConverges(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")

	// While stopped: a new table with rows, and a kd write the refused run
	// must not apply.
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z'); INSERT INTO kd VALUES (3, 'c')")

	run := r.startSync("kd,nt")
	r.requireScopeDriftRefusal(run, "nt", scopeDriftReasonNotEstablished)
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

	// Narrowing is the operator's choice, not a refusal…
	r.exec(src, "INSERT INTO kd VALUES (6, 'f')")
	run = r.startSync("kd")
	requireStreaming(t, run, 8*time.Second, "narrowed warm restart")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c,4|d,5|e,6|f", 60*time.Second); !ok {
		t.Fatalf("narrowed restart: target kd = %q%s", got, run.logTail())
	}
	// …and nt's changes are discarded from here on: a write it will never see.
	r.exec(src, "INSERT INTO nt VALUES (6, 'u')")
	time.Sleep(3 * time.Second)
	run.end()
	time.Sleep(2 * time.Second)

	// Coming back to nt refuses: it stayed in the publication, so only the
	// record moved it to the dropped set (INCLUDE-TABLE-RE-WIDEN).
	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonDropped)
}

// TestIncludeTableScopeDrift_UnfilteredNewTableWarnsAndStreams pins round 1's
// L1 regression: an unfiltered stream whose source gains a table resumes
// (ADR-0021 keeps the new table out) and names it in a WARN.
func TestIncludeTableScopeDrift_UnfilteredNewTableWarnsAndStreams(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'); INSERT INTO kd VALUES (3, 'c')")

	run := r.startSync("")
	requireStreaming(t, run, 10*time.Second, "unfiltered warm restart after a source CREATE TABLE")
	requireWarnNames(t, run, "nt")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c", 60*time.Second); !ok {
		t.Fatalf("unfiltered restart: target kd = %q%s", got, run.logTail())
	}
}

// TestIncludeTableScopeDrift_GlobIncludeWarnsAndStreams: a glob admitting a
// new table is not operator intent for that table.
func TestIncludeTableScopeDrift_GlobIncludeWarnsAndStreams(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("k*")
	r.exec(src, "CREATE TABLE kx (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO kx VALUES (1, 'x'); INSERT INTO kd VALUES (3, 'c')")

	run := r.startSync("k*")
	requireStreaming(t, run, 10*time.Second, "glob warm restart after a source CREATE TABLE")
	requireWarnNames(t, run, "kx")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c", 60*time.Second); !ok {
		t.Fatalf("glob restart: target kd = %q%s", got, run.logTail())
	}
}

// TestIncludeTableScopeDrift_SharedPublicationRefuses is S1: a second stream
// on the same source puts nt in the shared `sluice_pub`, so the publication
// delivers nt to the first stream too — and still nt was never copied by it.
// Before the established record this streamed {4,5} into a pre-created nt
// against the source's {1..5} at exit 0.
func TestIncludeTableScopeDrift_SharedPublicationRefuses(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")

	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	r.exec(tgt, "CREATE DATABASE target_two")
	tgt2 := strings.Replace(tgt, "/target_db", "/target_two", 1)
	s2 := r.sibling("itsd2", tgt2)
	run2 := s2.startSync("kd,nt")
	if got, ok := s2.waitRows(tgt2, "nt", "e", "1|x,2|y,3|z", 90*time.Second); !ok {
		t.Fatalf("second stream: target nt = %q%s", got, run2.logTail())
	}

	r.exec(tgt, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20))")
	r.exec(src, "INSERT INTO nt VALUES (4, 'w')")
	run := r.startSync("kd,nt")
	r.requireScopeDriftRefusal(run, "nt", scopeDriftReasonNotEstablished)
	if got := r.rows(tgt, "nt", "e"); got != "" {
		t.Fatalf("the refused run streamed into nt: %q", got)
	}
}

// TestIncludeTableScopeDrift_MySQLLiteralPrecreatedTableRefused closes
// INCLUDE-TABLE-SCOPE-DRIFT-STREAMED: the binlog carries nt, so with a
// pre-created target table a widened stream used to apply the rows written
// after its position and never the three written before.
func TestIncludeTableScopeDrift_MySQLLiteralPrecreatedTableRefused(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	r := newScopeDriftRig(t, "mysql", src, tgt)
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	r.establishKD("kd")

	r.exec(tgt, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20))")
	r.exec(src, "INSERT INTO nt VALUES (4, 'w')")
	run := r.startSync("kd,nt")
	r.requireScopeDriftRefusal(run, "nt", scopeDriftReasonNotEstablished)
	if got := r.rows(tgt, "nt", "e"); got != "" {
		t.Fatalf("the refused run streamed into nt: %q", got)
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
	r.establishKD("kd")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y')")

	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonNotEstablished)

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
	run.end()
	time.Sleep(2 * time.Second)
	// The re-copy recorded nt as established: a plain restart streams on.
	requireStreaming(t, r.startSync("kd,nt"), 8*time.Second, "plain restart after the re-copy")
}

// TestIncludeTableScopeDrift_PgoutputLegacyStreamFallsBackToItsPublication:
// a stream from before the established record (its column dropped here)
// resumes, and still refuses a literally included table its unshared
// publication does not deliver.
func TestIncludeTableScopeDrift_PgoutputLegacyStreamFallsBackToItsPublication(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")
	r.exec(tgt, "ALTER TABLE sluice_cdc_state DROP COLUMN established_tables")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'); INSERT INTO kd VALUES (3, 'c')")

	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonUnpublished)

	run := r.startSync("kd")
	requireStreaming(t, run, 8*time.Second, "legacy warm restart, unchanged scope")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c", 60*time.Second); !ok {
		t.Fatalf("legacy restart: target kd = %q%s", got, run.logTail())
	}
}

// failedAddTableThenRestart is path 2 on either engine with the add-table
// remedy: an add-table whose copy fails on a target CHECK, a restart that
// includes the table and is refused, then the emptied target and a re-run.
func failedAddTableThenRestart(t *testing.T, r *scopeDriftRig, checkDDL, dropCheckDDL string) {
	t.Helper()
	r.establishKD("kd")
	failAddTable(t, r, checkDDL, dropCheckDDL)
	requireRefusedRestart(t, r, "kd,nt")

	// The remedy: empty the partial copy and add the table again.
	r.exec(r.tgt, "DELETE FROM nt")
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

// failAddTable fails an add-table of nt on a target CHECK that refuses source
// row 3, then removes the CHECK — the cause fixed, the add not re-run — so
// nothing but the door stops a restart from streaming into the partial copy,
// and writes source row 4 for it to stream.
func failAddTable(t *testing.T, r *scopeDriftRig, checkDDL, dropCheckDDL string) {
	t.Helper()
	r.exec(r.src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	r.exec(r.tgt, checkDDL)
	code, out := r.addTable("nt")
	if code == 0 {
		t.Fatalf("add-table onto a target CHECK that refuses row 3 succeeded; the test's premise is gone\n%s", out)
	}
	if !strings.Contains(out, "stays recorded as pending") {
		t.Fatalf("the failed add-table does not say it left the table pending\n%s", out)
	}
	r.exec(r.tgt, dropCheckDDL)
	r.exec(r.src, "INSERT INTO nt VALUES (4, 'w')")
}

// requireRefusedRestart asserts a restart with tables refuses for nt as an
// incomplete add, writing nothing to it.
func requireRefusedRestart(t *testing.T, r *scopeDriftRig, tables string) {
	t.Helper()
	partial := r.rows(r.tgt, "nt", "e")
	r.requireScopeDriftRefusal(r.startSync(tables), "nt", scopeDriftReasonIncompleteAdd)
	if got := r.rows(r.tgt, "nt", "e"); got != partial {
		t.Fatalf("the refused run wrote to nt: %q, was %q", got, partial)
	}
}

// TestIncludeTableScopeDrift_PgoutputFailedAddTableRefused is path 2 on a
// pgoutput source.
func TestIncludeTableScopeDrift_PgoutputFailedAddTableRefused(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	failedAddTableThenRestart(t, newScopeDriftRig(t, "postgres", src, tgt),
		"CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CONSTRAINT nt_lt3")
}

// TestIncludeTableScopeDrift_MySQLFailedAddTableRefused is path 2 on a MySQL
// binlog source.
func TestIncludeTableScopeDrift_MySQLFailedAddTableRefused(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	failedAddTableThenRestart(t, newScopeDriftRig(t, "mysql", src, tgt),
		"CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CHECK nt_lt3")
}

// TestIncludeTableScopeDrift_MySQLFailedAddTableRestartFromScratch measures
// the re-copy remedy on a MySQL source (no slot to drop): --restart-from-scratch
// drops and re-copies every in-scope table — the partial nt among them — and
// records nt as established, so the next plain restart streams on. It also
// pins S2: `sync health` names the pending add while it stands.
func TestIncludeTableScopeDrift_MySQLFailedAddTableRestartFromScratch(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	r := newScopeDriftRig(t, "mysql", src, tgt)
	r.establishKD("kd")
	failAddTable(t, r, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CHECK nt_lt3")
	if code, out := r.health(); code != 1 || !strings.Contains(out, "pending_added_table: nt") {
		t.Fatalf("sync health with a pending add-table: exit %d, want 1 naming nt\n%s", code, out)
	}
	requireRefusedRestart(t, r, "kd,nt")

	run := r.startSync("kd,nt", "--restart-from-scratch")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w", 90*time.Second); !ok {
		t.Fatalf("--restart-from-scratch: target nt = %q, want 1|x,2|y,3|z,4|w%s", got, run.logTail())
	}
	// A change through CDC proves the cold start handed off — its record is
	// written before the apply loop — so stopping now is not stopping mid-copy.
	r.exec(src, "INSERT INTO nt VALUES (5, 'v')")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v", 60*time.Second); !ok {
		t.Fatalf("--restart-from-scratch then CDC: target nt = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)
	if _, out := r.health(); strings.Contains(out, "pending_added_table") {
		t.Fatalf("the re-copy left the pending record\n%s", out)
	}

	r.exec(src, "INSERT INTO nt VALUES (6, 'u')")
	run = r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "plain restart after --restart-from-scratch")
	if got, ok := r.waitRows(tgt, "nt", "e", "1|x,2|y,3|z,4|w,5|v,6|u", 60*time.Second); !ok {
		t.Fatalf("after the re-copy: target nt = %q%s", got, run.logTail())
	}
}

// TestIncludeTableScopeDrift_PendingTableDroppedOnSourceConverges is L2: an
// add-table failed, then the table was dropped on the source, so no add-table
// could ever clear the record. An unfiltered resume ignores the pending name
// (WARN) and streams on.
func TestIncludeTableScopeDrift_PendingTableDroppedOnSourceConverges(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("")
	failAddTable(t, r, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CONSTRAINT nt_lt3")
	r.exec(src, "DROP TABLE nt; INSERT INTO kd VALUES (3, 'c')")

	run := r.startSync("")
	requireStreaming(t, run, 10*time.Second, "unfiltered restart after the pending table was dropped on the source")
	if !strings.Contains(scopeDriftLog(t, run), "names a table the source no longer has") {
		t.Fatalf("no WARN for the ignored pending name%s", run.logTail())
	}
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c", 60*time.Second); !ok {
		t.Fatalf("target kd = %q%s", got, run.logTail())
	}
}

// establishTwo cold-starts the stream over two tables with the given filter,
// proves both copied, and stops it.
func (r *scopeDriftRig) establishTwo(second, tables string, extra ...string) {
	r.t.Helper()
	r.exec(r.src, "CREATE TABLE kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO kd VALUES (1, 'a');"+
		"CREATE TABLE "+second+" (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO "+second+" VALUES (1, 'x'), (2, 'y')")
	run := r.startSync(tables, extra...)
	if got, ok := r.waitRows(r.tgt, second, "e", "1|x,2|y", 90*time.Second); !ok {
		r.t.Fatalf("cold start: target %s = %q%s", second, got, run.logTail())
	}
	if got, ok := r.waitRows(r.tgt, "kd", "k", "1|a", 30*time.Second); !ok {
		r.t.Fatalf("cold start: target kd = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)
}

// narrowThenChange restarts with a narrower scope (proved streaming by a kd
// write), stops, then changes the narrowed-away table on the source — the
// changes this narrowed stream discards.
func (r *scopeDriftRig) narrowThenChange(table, tables string, extra ...string) {
	r.t.Helper()
	r.exec(r.src, "INSERT INTO kd VALUES (2, 'b')")
	run := r.startSync(tables, extra...)
	if got, ok := r.waitRows(r.tgt, "kd", "k", "1|a,2|b", 60*time.Second); !ok {
		r.t.Fatalf("narrowed restart: target kd = %q%s", got, run.logTail())
	}
	r.exec(r.src, "UPDATE "+table+" SET e = 'Y' WHERE id = 2; DELETE FROM "+table+" WHERE id = 1; INSERT INTO "+table+" VALUES (3, 'z')")
	time.Sleep(3 * time.Second)
	run.end()
	time.Sleep(2 * time.Second)
}

// TestIncludeTableScopeDrift_UnfilteredRewidenRefuses is N1: an unfiltered
// stream narrowed with --exclude-table and widened back. nt never left the
// publication, so the narrowed run discarded its changes; before the dropped
// set the re-widened stream only WARNed and applied the rest onto the stale
// copy (target 1:x,2:y,4:w against source 2:Y,3:z,4:w) at exit 0.
func TestIncludeTableScopeDrift_UnfilteredRewidenRefuses(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishTwo("nt", "")
	r.narrowThenChange("nt", "", "--exclude-table=nt")
	r.requireScopeDriftRefusal(r.startSync(""), "nt", scopeDriftReasonDropped)
}

// TestIncludeTableScopeDrift_GlobRewidenRefuses is N1 through a glob.
func TestIncludeTableScopeDrift_GlobRewidenRefuses(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishTwo("kx", "k*")
	r.narrowThenChange("kx", "kd")
	r.requireScopeDriftRefusal(r.startSync("k*"), "kx", scopeDriftReasonDropped)
}

// TestIncludeTableScopeDrift_MySQLExcludedPendingRefuses is N1's MySQL arm:
// a failed add-table, a --restart-from-scratch that excludes the table, then
// the exclude dropped. A cold start clears pending only for what it copied,
// so the partial table still refuses.
func TestIncludeTableScopeDrift_MySQLExcludedPendingRefuses(t *testing.T) {
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()
	r := newScopeDriftRig(t, "mysql", src, tgt)
	r.establishKD("")
	failAddTable(t, r, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), CONSTRAINT nt_lt3 CHECK (id < 3))",
		"ALTER TABLE nt DROP CHECK nt_lt3")

	run := r.startSync("", "--restart-from-scratch", "--exclude-table=nt")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b", 90*time.Second); !ok {
		t.Fatalf("--restart-from-scratch: target kd = %q%s", got, run.logTail())
	}
	// A row written after the copy finished can only arrive through CDC,
	// after the apply loop starts — which is after the cold start's record
	// writes and pending clears. So its arrival proves those ran.
	waitLog(t, run, "entering CDC mode")
	r.exec(src, "INSERT INTO kd VALUES (3, 'c')")
	if got, ok := r.waitRows(tgt, "kd", "k", "1|a,2|b,3|c", 60*time.Second); !ok {
		t.Fatalf("--restart-from-scratch then CDC: target kd = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)
	if code, out := r.health(); code != 1 || !strings.Contains(out, "pending_added_table: nt") {
		t.Fatalf("a re-copy that did not copy nt cleared its pending record (exit %d)\n%s", code, out)
	}

	r.requireScopeDriftRefusal(r.startSync(""), "nt", scopeDriftReasonIncompleteAdd)
}

// TestIncludeTableScopeDrift_RecreatedTableRefusesThenConverges is N2: a
// table dropped and recreated while the stream was stopped leaves the FOR
// TABLE publication; before, the stream kept going with zero log lines while
// the target held the old table's rows. The remedy — empty the target table,
// add-table — converges.
func TestIncludeTableScopeDrift_RecreatedTableRefusesThenConverges(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishTwo("nt", "kd,nt")
	r.exec(src, "DROP TABLE nt; CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (10, 'a'), (11, 'b')")

	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonRecreated)

	r.exec(tgt, "DELETE FROM nt")
	if code, out := r.addTable("nt"); code != 0 {
		t.Fatalf("schema add-table nt: exit %d\n%s", code, out)
	}
	run := r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "restart after the remedy")
	r.exec(src, "INSERT INTO nt VALUES (12, 'c')")
	if got, ok := r.waitRows(tgt, "nt", "e", "10|a,11|b,12|c", 60*time.Second); !ok {
		t.Fatalf("after the remedy: target nt = %q, want 10|a,11|b,12|c%s", got, run.logTail())
	}
}

// waitLog waits until run has logged substr.
func waitLog(t *testing.T, run *straddleBinaryRun, substr string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(run.logPath); err == nil && strings.Contains(string(b), substr) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("the run never logged %q%s", substr, run.logTail())
}

// setPending writes a pending add-table entry for table straight into the
// control row — the state a killed add leaves (stale), or a running one
// (fresh) for the `sync health` rendering. The container shares this host's
// kernel clock, so a host-clock stamp is a server-clock stamp here.
func (r *scopeDriftRig) setPending(table string, heartbeat time.Time) {
	r.t.Helper()
	db, err := sql.Open(r.sqlDriver, r.tgt)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "ALTER TABLE sluice_cdc_state ADD COLUMN IF NOT EXISTS pending_added_tables TEXT"); err != nil {
		r.t.Fatal(err)
	}
	entry := ir.PendingAddedTable{Table: table, Token: "injected", StartedAt: heartbeat.Add(-time.Hour), HeartbeatAt: heartbeat}
	value := appliershared.EncodeTableSet([]string{appliershared.EncodePendingEntry(entry)})
	if _, err := db.ExecContext(ctx, "UPDATE sluice_cdc_state SET pending_added_tables = $1 WHERE stream_id = $2", value, r.streamID); err != nil {
		r.t.Fatal(err)
	}
}

// TestIncludeTableScopeDrift_PendingLivenessInHealthAndAbandon is F1: `sync
// health` reports an add-table that is still running as in progress (exit 0),
// one whose heartbeat went stale as ADD-TABLE INCOMPLETE (exit 1), and
// `schema add-table --abandon` clears it.
func TestIncludeTableScopeDrift_PendingLivenessInHealthAndAbandon(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20))")

	r.setPending("nt", time.Now())
	if code, out := r.health(); code != 0 || !strings.Contains(out, "pending_added_table: nt in progress") {
		t.Fatalf("health with an add in progress: exit %d, want 0 reporting it\n%s", code, out)
	}
	r.setPending("nt", time.Now().Add(-ir.PendingAddStaleAfter-time.Minute))
	if code, out := r.health(); code != 1 || !strings.Contains(out, "ADD-TABLE INCOMPLETE") || !strings.Contains(out, "--abandon") {
		t.Fatalf("health with a stale add: exit %d, want 1 naming --abandon\n%s", code, out)
	}
	// F-C: the snapshot slot a killed add leaves behind, which pins WAL and
	// makes the next add-table of nt refuse "slot already exists".
	r.exec(src, "SELECT pg_catalog.pg_create_logical_replication_slot('sluice_addtable_nt', 'pgoutput')")
	args := append([]string{"schema", "add-table", "nt", "--abandon", "--yes"}, r.connArgs()...)
	stdout, stderr, code := runSluice(t, r.bin, append(args, "--stream-id="+r.streamID)...)
	if code != 0 || !strings.Contains(stdout+stderr, "may hold a partial copy") {
		t.Fatalf("add-table --abandon: exit %d\n%s%s", code, stdout, stderr)
	}
	if got, _ := r.waitScalar(src, "SELECT COUNT(*)::text FROM pg_replication_slots WHERE slot_name = 'sluice_addtable_nt'", "0", 0); got != "0" {
		t.Fatalf("--abandon left the dead add's snapshot slot behind (count %s)\n%s%s", got, stdout, stderr)
	}
	if code, out := r.health(); code != 0 || strings.Contains(out, "pending_added_table") {
		t.Fatalf("health after --abandon: exit %d\n%s", code, out)
	}
}

// TestIncludeTableScopeDrift_RestartDuringAddTableIsRestartable is F-A with a
// REAL add-table: `sync start --include-table=kd,nt` issued while `schema
// add-table nt` is still copying a large nt. nt is literally named and not yet
// established, which on its own refuses terminally — but the live add is
// exactly what cures that, so the restart defers with the restartable
// INCLUDE-TABLE-ADD-IN-PROGRESS (a supervisor retries it), and once the add
// completes the stream resumes and streams nt. Before F-A the restart was the
// terminal INCLUDE-TABLE-SCOPE-DRIFT: the operator was told to redo an add
// that was about to succeed.
func TestIncludeTableScopeDrift_RestartDuringAddTableIsRestartable(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20), pad TEXT); "+
		"INSERT INTO nt SELECT g, 'r', repeat('x', 100) FROM generate_series(1, 1000000) g")

	add := r.startAddTable("nt")
	r.waitPendingInProgress(add, "nt", 120*time.Second)
	run := r.startSync("kd,nt")
	if !waitExit(run, 90*time.Second) {
		t.Fatalf("a restart during a running add-table kept streaming%s", run.logTail())
	}
	log := scopeDriftLog(t, run)
	if !strings.Contains(log, "INCLUDE-TABLE-ADD-IN-PROGRESS") || strings.Contains(log, "warm resume refused: INCLUDE-TABLE-SCOPE-DRIFT") {
		t.Fatalf("want the restartable INCLUDE-TABLE-ADD-IN-PROGRESS only%s", run.logTail())
	}

	if !waitExit(add, 300*time.Second) || !strings.Contains(add.exited().Error(), "exit status 0") {
		t.Fatalf("schema add-table nt: %v%s", add.exited(), add.logTail())
	}
	run = r.startSync("kd,nt")
	requireStreaming(t, run, 8*time.Second, "restart after the add-table finished")
	r.exec(src, "INSERT INTO nt VALUES (1000001, 'z', '')")
	if got, ok := r.waitScalar(tgt, "SELECT COUNT(*)::text || '/' || COALESCE(MAX(e) FILTER (WHERE id = 1000001), '-') FROM nt",
		"1000001/z", 90*time.Second); !ok {
		t.Fatalf("target nt = %q, want 1000001 rows ending in the streamed one%s", got, run.logTail())
	}
}

// startAddTable launches `schema add-table` in the background.
func (r *scopeDriftRig) startAddTable(table string) *straddleBinaryRun {
	r.t.Helper()
	r.runs++
	args := append([]string{"--log-level=info", "schema", "add-table", table}, r.connArgs()...)
	args = append(args, "--stream-id="+r.streamID, "--yes")
	run := &straddleBinaryRun{t: r.t, bin: r.bin, args: args, logPath: filepath.Join(r.logDir, fmt.Sprintf("%s-add%d.log", r.streamID, r.runs))}
	run.start()
	r.t.Cleanup(run.end)
	return run
}

// waitPendingInProgress polls `sync health` until it reports table's add as
// in progress — read from the control row the add-table process writes.
func (r *scopeDriftRig) waitPendingInProgress(add *straddleBinaryRun, table string, timeout time.Duration) {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, out := r.health(); strings.Contains(out, "pending_added_table: "+table+" in progress") {
			return
		}
		if add.exited() != nil {
			r.t.Fatalf("the add-table exited (%v) before its pending entry was seen in progress%s", add.exited(), add.logTail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.t.Fatalf("no in-progress pending entry for %q within %s%s", table, timeout, add.logTail())
}

// waitScalar polls a one-value query on dsn until it returns want.
func (r *scopeDriftRig) waitScalar(dsn, q, want string, timeout time.Duration) (string, bool) {
	db, err := sql.Open(r.sqlDriver, dsn)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	got := ""
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := db.QueryRowContext(ctx, q).Scan(&got); err != nil {
			got = "<" + err.Error() + ">"
		}
		cancel()
		if got == want || time.Now().After(deadline) {
			return got, got == want
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestIncludeTableScopeDrift_RefusedRestartFromScratchKeepsTheRecord is S-A,
// the exact repro: the stream established kd and dropped nt (narrowed away,
// so its changes since were discarded). The re-widen refuses; a
// --restart-from-scratch then refuses on the stream's existing slot — after
// its EnsurePublication had already run. A plain restart must still refuse:
// the refused cold start changed nothing, so the record must still say nt was
// dropped. Before the fix the cold start reset the record to "unrecorded" as
// its first step, the plain restart fell back to the publication (which
// delivers nt), and streamed onto nt's stale copy at exit 0.
func TestIncludeTableScopeDrift_RefusedRestartFromScratchKeepsTheRecord(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishTwo("nt", "kd,nt")
	r.narrowThenChange("nt", "kd")
	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonDropped)

	run := r.startSync("kd,nt", "--restart-from-scratch")
	if !waitExit(run, 90*time.Second) || !strings.Contains(scopeDriftLog(t, run), "already exists") {
		t.Fatalf("--restart-from-scratch with the stream's slot in place: want the slot-exists refusal%s", run.logTail())
	}
	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonDropped)
	if got := r.rows(tgt, "nt", "e"); got != "1|x,2|y" {
		t.Fatalf("target nt = %q, want the stale 1|x,2|y untouched (refused before anything was applied)", got)
	}
}

// TestIncludeTableScopeDrift_PreflightRefusedColdStartKeepsTheRecord is the
// S-A preflight variant: the --restart-from-scratch is refused by a schema
// preflight (a --type-override naming a column nt does not have) that runs
// after the publication is ensured and before the snapshot. Same verdict: the
// record is untouched and the plain restart still refuses.
func TestIncludeTableScopeDrift_PreflightRefusedColdStartKeepsTheRecord(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishTwo("nt", "kd,nt")
	r.narrowThenChange("nt", "kd")

	run := r.startSync("kd,nt", "--restart-from-scratch", "--type-override=nt.no_such_column=text")
	if !waitExit(run, 90*time.Second) || !strings.Contains(scopeDriftLog(t, run), "no_such_column") {
		t.Fatalf("--restart-from-scratch with a bad --type-override: want its preflight refusal%s", run.logTail())
	}
	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonDropped)
}

// TestIncludeTableScopeDrift_LegacySlotOnAnotherDatabaseIsNotAPeer is F-B
// through the binary: a stream with no record (legacy) whose source server
// also hosts a sluice slot on ANOTHER database. That slot cannot read this
// database's publication, so the publication is not shared and the no-record
// fallback still refuses a literally included table it does not deliver.
// Before the filter the other database's slot made the publication read as
// shared, and the fallback stood down with a WARN.
func TestIncludeTableScopeDrift_LegacySlotOnAnotherDatabaseIsNotAPeer(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	r := newScopeDriftRig(t, "postgres", src, tgt)
	r.establishKD("kd")
	r.exec(tgt, "ALTER TABLE sluice_cdc_state DROP COLUMN established_tables")
	r.exec(src, "CREATE TABLE nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO nt VALUES (1, 'x')")

	r.exec(src, "CREATE DATABASE other_db")
	other := strings.Replace(src, "/source_db", "/other_db", 1)
	if other == src {
		t.Fatalf("could not derive a second database's DSN from %q", src)
	}
	r.exec(other, "SELECT pg_catalog.pg_create_logical_replication_slot('sluice_elsewhere', 'pgoutput')")
	defer r.exec(other, "SELECT pg_catalog.pg_drop_replication_slot('sluice_elsewhere')")

	r.requireScopeDriftRefusal(r.startSync("kd,nt"), "nt", scopeDriftReasonUnpublished)
}

// TestIncludeTableScopeDrift_MultiDatabaseRemedyIsTheRecopy is F3: on a
// multi-database stream add-table cannot record a table, so the refusal names
// --restart-from-scratch instead of an add-table that would loop.
func TestIncludeTableScopeDrift_MultiDatabaseRemedyIsTheRecopy(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtHome, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	r := newScopeDriftRig(t, "mysql", serverDSN(t, srcServer), tgtHome)
	r.exec(r.src, "CREATE DATABASE shop_db; CREATE TABLE source_db.kd (id INT PRIMARY KEY, k VARCHAR(20)); "+
		"INSERT INTO source_db.kd VALUES (1, 'a'); CREATE TABLE shop_db.kd (id INT PRIMARY KEY, k VARCHAR(20)); INSERT INTO shop_db.kd VALUES (1, 'b')")
	run := r.startSync("kd", "--include-database=source_db,shop_db")
	if got, ok := r.waitRows(r.tgt, "shop_db.kd", "k", "1|b", 90*time.Second); !ok {
		t.Fatalf("multi-database cold start: target shop_db.kd = %q%s", got, run.logTail())
	}
	r.exec(r.src, "INSERT INTO shop_db.kd VALUES (2, 'c')")
	if got, ok := r.waitRows(r.tgt, "shop_db.kd", "k", "1|b,2|c", 60*time.Second); !ok {
		t.Fatalf("multi-database cdc: target shop_db.kd = %q%s", got, run.logTail())
	}
	run.end()
	time.Sleep(2 * time.Second)

	r.exec(r.src, "CREATE TABLE source_db.nt (id INT PRIMARY KEY, e VARCHAR(20)); INSERT INTO source_db.nt VALUES (1, 'x')")
	log := r.requireScopeDriftRefusal(r.startSync("kd,nt", "--include-database=source_db,shop_db"), "source_db.nt", scopeDriftReasonNotEstablished)
	if !strings.Contains(log, "re-copy the stream with --restart-from-scratch") || strings.Contains(log, "run `sluice schema add-table") {
		t.Fatal("the multi-database refusal must name the re-copy, not add-table")
	}
}
