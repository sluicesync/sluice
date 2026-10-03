//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 review item 1: the target-witnessed first boundary forwards a type
// change only when the TARGET is the narrower side. A first boundary cannot
// tell an old shape from a new one, so a target WIDER than the snapshot is
// kept (WARN). Two shapes are pinned here:
//
//   - a target an operator widened on purpose while the stream was stopped:
//     narrowing it changes every later value that needs the wider type
//     (measured under the mutated rule: a numeric(14,6) target narrowed to
//     (10,2), a bigint to integer) — the value gate for the rule;
//   - the Postgres crash replay: a source transaction
//     `UPDATE; ALTER … TYPE numeric(12,4); INSERT …` whose apply dies part
//     way through, after the forwarded ALTER and some committed batches of
//     rows written under it. The retry replays from the PRE-ALTER relation
//     against the widened target; the cell pins that it converges exactly
//     (see its doc for what the mutated rule did there).
//
// The independent expected values are the target's catalog type and the
// rows read back from it, compared against the values the source wrote.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestTWFB_PostgresCrashMidTransaction_KeepsTheWiderTarget is the crash
// replay, on the serial apply path (the one that writes ADR-0190 apply
// marks). It is a CONVERGENCE pin, not a value gate for the direction rule:
// with the rule mutated (a wider target forwarded as an ALTER) the replay
// narrowed the target and the transaction's next relation widened it again,
// and every row still read back 1.2345 — on the serial path, on
// --exactly-once-lanes and on the default lanes alike (measured
// 2026-10-02). The committed rows were re-delivered, not skipped, so the
// review's "rounded and then skipped by the marks" mechanism did not
// reproduce here; the mutant is caught by this cell's WIDER-warning
// assertion and, on values, by TestTWFB_OperatorWidenedTarget_*.
func TestTWFB_PostgresCrashMidTransaction_KeepsTheWiderTarget(t *testing.T) {
	runTWFBCrashMidTransaction(t, twfbCrashCell{
		table:  "t_crash",
		from:   "numeric(10,2)",
		to:     "numeric(12,4)",
		seed:   "1.25",
		update: "2.50",
		insert: "1.2345",
		// format_type of the widened column, the row-1 read-back, and the
		// predicate every inserted row must satisfy.
		wantType: "numeric(12,4)", wantRow1: "2.5000", exact: "v = 1.2345",
	})
}

// TestTWFB_PostgresCrashMidTransaction_VarcharToTextConverges is the same
// replay across families (the second review): a forwarded
// varchar(16) → text, then a crash in the middle of the transaction. The
// replay's PRE-ALTER relation (varchar(16)) meets a target already holding
// text. Before the cross-family relations in witnessWidthOrder that was a
// RESUME-SCHEMA-DIVERGENCE on every start — a permanent wedge on a stream
// that had converged before GC-44; now the target is wider (text holds
// every varchar(16) value), WARNed and kept, and the replay converges.
func TestTWFB_PostgresCrashMidTransaction_VarcharToTextConverges(t *testing.T) {
	runTWFBCrashMidTransaction(t, twfbCrashCell{
		table:    "t_crash_vt",
		from:     "varchar(16)",
		to:       "text",
		seed:     "'short'",
		update:   "'updated'",
		insert:   "repeat('x', 40)",
		wantType: "text", wantRow1: "updated", exact: "v = repeat('x', 40)",
	})
}

// twfbCrashCell is one column type change made inside a large source
// transaction: UPDATE row 1, ALTER from → to, INSERT many rows needing to.
type twfbCrashCell struct {
	table, from, to           string
	seed, update, insert      string // SQL value expressions
	wantType, wantRow1, exact string
}

// runTWFBCrashMidTransaction drives c: the transaction, the target's
// backends killed once it is part-applied, the in-process retry, and the
// verdict read off the target.
func runTWFBCrashMidTransaction(t *testing.T, c twfbCrashCell) {
	t.Helper()
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-crash-pg"}
	const rows = 50000
	cell.src.exec(t, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY, v %s NOT NULL);
		INSERT INTO %s VALUES (1, %s);`, c.table, c.from, c.table, c.seed))
	cell.coldStartAndStop(t, c.table, 1)

	logs := twfbCaptureLogs(t)
	s := cell.streamer()
	s.ApplyConcurrency = 1
	run := startTWFBRun(s)
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf(`BEGIN;
		UPDATE %[1]s SET v = %[2]s WHERE id = 1;
		ALTER TABLE %[1]s ALTER COLUMN v TYPE %[3]s;
		INSERT INTO %[1]s SELECT g, %[4]s FROM generate_series(10, %[5]d) g;
		COMMIT;`, c.table, c.update, c.to, c.insert, rows+9))

	// Kill the target's backends once some, not all, of the transaction's
	// rows are committed there: the forwarded ALTER has landed and batches
	// written under it are durable.
	count := "SELECT count(*) FROM " + c.table
	partial := func() bool {
		n, err := strconv.Atoi(cell.tgt.scalar(t, count+" WHERE id >= 10"))
		return err == nil && n > 0 && n < rows
	}
	deadline := time.Now().Add(120 * time.Second)
	for !partial() && time.Now().Before(deadline) && !run.exited() {
		time.Sleep(20 * time.Millisecond)
	}
	if !partial() {
		t.Fatalf("VACUOUS: never observed the transaction part-applied (count %s, stream %v)",
			cell.tgt.scalar(t, count+" WHERE id >= 10"), run.err)
	}
	cell.tgt.exec(t, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()`)

	deadline = time.Now().Add(600 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		if cell.tgt.scalar(t, count) == strconv.Itoa(rows+1) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the stream returned %v\n%s", err, divergenceLines(logs.String()))
	}

	if !strings.Contains(logs.String(), "applier: transient error; retrying") {
		t.Fatal("VACUOUS: no retry was logged — the kill did not land mid-apply")
	}
	if len(logLinesFor(logs, twfbLogTargetWider, c.table)) == 0 {
		t.Errorf("VACUOUS: the retry's first boundary was not the pre-ALTER relation against the widened target "+
			"(no WIDER warning)\n%s", divergenceLines(logs.String()))
	}
	if got := cell.tgt.columnType(t, c.table, "v"); got != c.wantType {
		t.Errorf("target %s.v is %q, want %s", c.table, got, c.wantType)
	}
	if got := cell.tgt.scalar(t, count); got != strconv.Itoa(rows+1) {
		t.Errorf("target holds %s rows, want %d\n%s", got, rows+1, twfbNoticeLines(logs.String()))
	}
	if got := cell.tgt.scalar(t, count+" WHERE id >= 10 AND NOT ("+c.exact+")"); got != "0" {
		t.Errorf("SILENT LOSS: %s of the transaction's rows differ from what the source wrote (e.g. %s)", got,
			cell.tgt.scalar(t, "SELECT v::text FROM "+c.table+" WHERE id >= 10 AND NOT ("+c.exact+") LIMIT 1"))
	}
	if got := cell.tgt.scalar(t, "SELECT v::text FROM "+c.table+" WHERE id = 1"); got != c.wantRow1 {
		t.Errorf("target row 1 = %q, want %s", got, c.wantRow1)
	}
}

// twfbNoticeLines is every WARN/ERROR line and every retry line of a
// captured log, for a failure message.
func twfbNoticeLines(out string) string {
	var b strings.Builder
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") || strings.Contains(line, "retrying") {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// twfbWidenedTargetCell is a column an operator widens on the target while
// the stream is stopped.
type twfbWidenedTargetCell struct {
	table, create, widen, probe string
	column, wantType            string
	readBack, want              string
}

// runTWFBOperatorWidenedTarget: cold start, stop, widen the target, restart,
// one row. The target keeps its type (WARN, no ALTER) and the row lands
// exactly.
func runTWFBOperatorWidenedTarget(t *testing.T, cell twfbCell, cells []twfbWidenedTargetCell) {
	t.Helper()
	for _, c := range cells {
		cell.src.exec(t, c.create)
	}
	cell.coldStartAndStop(t, cells[0].table, 1)
	for _, c := range cells {
		if !cell.tgt.waitRow(t, c.table, 1, nil, 60*time.Second) {
			t.Fatalf("the cold start never delivered %s", c.table)
		}
		cell.tgt.exec(t, c.widen)
	}
	logs := twfbCaptureLogs(t)
	run := startTWFBRun(cell.streamer())
	for _, c := range cells {
		cell.src.exec(t, c.probe)
	}
	for _, c := range cells {
		if !cell.tgt.waitRow(t, c.table, 2, run, 120*time.Second) {
			t.Fatalf("%s: the probe row never landed (stream: %v)\n%s", c.table, run.stop(t), divergenceLines(logs.String()))
		}
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the restarted stream returned %v", err)
	}
	if lines := divergenceLines(logs.String()); lines != "" {
		t.Errorf("a widened target was altered or refused:\n%s", lines)
	}
	for _, c := range cells {
		if got := cell.tgt.columnType(t, c.table, c.column); got != c.wantType {
			t.Errorf("%s.%s on the target is %q, want the operator's %q", c.table, c.column, got, c.wantType)
		}
		if got := cell.tgt.scalar(t, c.readBack); got != c.want {
			t.Errorf("%s probe row read back %q, want %q", c.table, got, c.want)
		}
		if len(logLinesFor(logs, twfbLogTargetWider, c.table)) == 0 {
			t.Errorf("VACUOUS: no WIDER warning for %s — the witness never saw the widened target", c.table)
		}
	}
}

func TestTWFB_OperatorWidenedTarget_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	runTWFBOperatorWidenedTarget(t, twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-owiden-pg",
	}, []twfbWidenedTargetCell{
		{
			table:  "t_ow_dec",
			create: "CREATE TABLE t_ow_dec (id bigint PRIMARY KEY, v numeric(10,2) NOT NULL); INSERT INTO t_ow_dec VALUES (1, 1.25);",
			widen:  "ALTER TABLE t_ow_dec ALTER COLUMN v TYPE numeric(14,6)",
			probe:  "INSERT INTO t_ow_dec VALUES (2, 12345678.12)",
			column: "v", wantType: "numeric(14,6)",
			readBack: "SELECT v::text FROM t_ow_dec WHERE id = 2", want: "12345678.120000",
		},
		{
			table:  "t_ow_vc",
			create: "CREATE TABLE t_ow_vc (id bigint PRIMARY KEY, v varchar(16) NOT NULL); INSERT INTO t_ow_vc VALUES (1, 'a');",
			widen:  "ALTER TABLE t_ow_vc ALTER COLUMN v TYPE varchar(64)",
			probe:  "INSERT INTO t_ow_vc VALUES (2, 'sixteen-chars-ok')",
			column: "v", wantType: "character varying(64)",
			readBack: "SELECT v FROM t_ow_vc WHERE id = 2", want: "sixteen-chars-ok",
		},
		{
			table:  "t_ow_ts",
			create: "CREATE TABLE t_ow_ts (id bigint PRIMARY KEY, ts timestamp(0) NOT NULL); INSERT INTO t_ow_ts VALUES (1, '2026-10-02 10:00:00');",
			widen:  "ALTER TABLE t_ow_ts ALTER COLUMN ts TYPE timestamp(6)",
			probe:  "INSERT INTO t_ow_ts VALUES (2, '2026-10-02 10:11:12')",
			column: "ts", wantType: "timestamp(6) without time zone",
			readBack: "SELECT to_char(ts, 'YYYY-MM-DD HH24:MI:SS.US') FROM t_ow_ts WHERE id = 2", want: "2026-10-02 10:11:12.000000",
		},
	})
}

func TestTWFB_OperatorWidenedTarget_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runTWFBOperatorWidenedTarget(t, twfbCell{
		src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-owiden-mysql",
	}, []twfbWidenedTargetCell{
		{
			table:  "t_ow_dec",
			create: "CREATE TABLE t_ow_dec (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(10,2) NOT NULL) ENGINE=InnoDB; INSERT INTO t_ow_dec VALUES (1, 1.25);",
			widen:  "ALTER TABLE t_ow_dec ALTER COLUMN v TYPE numeric(14,6)",
			probe:  "INSERT INTO t_ow_dec VALUES (2, 12345678.12)",
			column: "v", wantType: "numeric(14,6)",
			readBack: "SELECT v::text FROM t_ow_dec WHERE id = 2", want: "12345678.120000",
		},
		{
			table:  "t_ow_int",
			create: "CREATE TABLE t_ow_int (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB; INSERT INTO t_ow_int VALUES (1, 1);",
			widen:  "ALTER TABLE t_ow_int ALTER COLUMN v TYPE bigint",
			probe:  "INSERT INTO t_ow_int VALUES (2, 2147483647)",
			column: "v", wantType: "bigint",
			readBack: "SELECT v::text FROM t_ow_int WHERE id = 2", want: "2147483647",
		},
	})
}
