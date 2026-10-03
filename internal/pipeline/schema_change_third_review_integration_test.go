//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F5 third review pins, on real servers.
//
//   - HIGH-1: a source NARROWING (`numeric(12,4)` → `(10,2)` and its
//     siblings) on a stream that forwards no DDL was accepted with a WARN
//     while the target kept the source's old, unrounded values at exit 0
//     (reproduced on postgres:16 by the review: the target held 1.2345
//     where the source held 1.23). It refuses now — live (at the Postgres
//     reader's own gate, restored 2026-10-03), and again after a restart
//     from the retained history (AMBIGUOUS on Postgres: the one-version
//     history cannot order a replay against a source change) — and the
//     drained model recovers it. The forward path's first boundary forwards
//     the same narrowing made while a MySQL stream was stopped.
//   - CHAR across families: a Postgres source sends bpchar padded, so a
//     varchar key changed to char while stopped lost every key-scoped
//     UPDATE and DELETE and padded every INSERT. It refuses now.
//   - Array element modifiers changed while stopped were rounded at exit 0
//     with no log line. They refuse now.
//
// The independent expected value is the SOURCE's own read-back of each
// row, compared with the target's.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
)

// narrowingCell is one source narrowing: the column's type before and
// after, a value the wide type holds and the narrow one converts, and a
// value written after the change.
type narrowingCell struct {
	name         string
	wide, narrow string
	before, post string
}

// narrowingCells are the PG families the review named: decimal on both
// axes, temporal precision, float width, and numeric to integer.
func narrowingCells() []narrowingCell {
	return []narrowingCell{
		{"numeric", "numeric(12,4)", "numeric(10,2)", "1.2345", "3.45"},
		{"timestamp precision", "timestamp(6)", "timestamp(0)", "2020-01-02 10:11:12.345678", "2020-01-03 10:11:12"},
		{"double to real", "double precision", "real", "1.23456789012", "2.5"},
		{"numeric to integer", "numeric(12,2)", "integer", "12.75", "7"},
	}
}

// runRefuseNarrowingPin: cold start, a row through the stream (the table's
// first boundary — the prior), then the live narrowing and a row. Refused
// live and on a restart; the drained model then converges every row.
func runRefuseNarrowingPin(t *testing.T, cell twfbCell, table string, c narrowingCell) {
	t.Helper()
	cell.src.exec(t, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s NOT NULL); INSERT INTO %s VALUES (1, '%s');", table, c.wide, table, c.before))
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, table, 1, run, 180*time.Second) {
		t.Fatalf("[%s] the cold start never delivered %s (stream: %v)", c.name, table, run.stop(t))
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (2, '%s')", table, c.before))
	if !cell.tgt.waitRow(t, table, 2, run, 90*time.Second) {
		t.Fatalf("[%s] row 2 never landed (stream: %v)", c.name, run.stop(t))
	}

	cell.src.exec(t, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN v TYPE %s", table, c.narrow))
	cell.src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (3, '%s')", table, c.post))
	for _, kind := range []string{"live", "restart"} {
		err := waitRefused(t, run, 60*time.Second)
		// Live, the table's relation is cached in this reader session, so the
		// Postgres reader's own gate refuses the type change first (restored
		// 2026-10-03). After the restart the narrowed relation is the
		// session's first: this check judges it against the one-version
		// history, which cannot order it — a replay or a source change — so
		// it refuses AMBIGUOUS (GC-44 fourth review).
		want := []string{schemaChangeRefusedMarker, "narrowed from", ambiguousBoundaryMarker}
		if kind == "live" {
			want = []string{"incompatible schema change mid-stream", "Drained-model recovery"}
		}
		for _, w := range want {
			if err == nil || !strings.Contains(err.Error(), w) {
				t.Errorf("[%s/%s] the narrowing was not refused with %q (err %v)", c.name, kind, w, err)
			}
		}
		if err == nil {
			t.Errorf("[%s/%s] not refused (stream: %v)", c.name, kind, run.stop(t))
		}
		_ = run.stop(t)
		if cell.tgt.hasRow(t, table, 3) {
			t.Errorf("[%s/%s] row 3 LANDED past the refusal", c.name, kind)
		}
		if kind == "live" {
			run = startTWFBRun(cell.streamer())
		}
	}

	// The drained model: the target takes the same narrowing.
	cell.tgt.exec(t, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN v TYPE %s", table, c.narrow))
	run = startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, table, 3, run, 90*time.Second) {
		t.Fatalf("[%s] the drained-model restart did not resume (stream: %v)", c.name, run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the resumed stream returned %v", c.name, err)
	}
	for id := 1; id <= 3; id++ {
		q := fmt.Sprintf("SELECT v::text FROM %s WHERE id = %d", table, id)
		if src, tgt := cell.src.scalar(t, q), cell.tgt.scalar(t, q); src != tgt {
			t.Errorf("[%s] row %d: source %q, target %q", c.name, id, src, tgt)
		}
	}
}

func TestStreamer_RefuseSourceNarrowing_PostgresToPostgres(t *testing.T) {
	for _, c := range narrowingCells() {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			runRefuseNarrowingPin(t, twfbCell{
				src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "refuse-narrow", schemaChanges: "refuse",
			}, "t_nar", c)
		})
	}
}

func TestStreamer_MultiSchemaSourceNarrowing_PostgresToPostgres(t *testing.T) {
	c := narrowingCells()[0]
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	src := twfbDB{"postgres", srcDSN}
	src.exec(t, "CREATE SCHEMA sales; CREATE SCHEMA billing; CREATE TABLE billing.other (id bigint PRIMARY KEY); INSERT INTO billing.other VALUES (1);")
	runRefuseNarrowingPin(t, twfbCell{
		src: src, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "multischema-narrow", databases: []string{"sales", "billing"},
	}, "sales.t_nar", c)
}

// forwardNarrowingCell is a narrowing made while a forward-mode stream was
// stopped, in a source dialect; readV renders a row's value comparably on
// either engine.
type forwardNarrowingCell struct {
	name, create, alter, insert string
}

// runForwardNarrowedWhileStopped: cold start, a row through the stream (the
// table's retained history version), stop, narrow the source and write a
// row, restart. forwarded says whether the first boundary is expected to
// forward the narrowing (the retained history PROVES it) or keep the target
// with the WARN.
func runForwardNarrowedWhileStopped(t *testing.T, cell twfbCell, c forwardNarrowingCell, readV func(db twfbDB, id int) string, forwarded bool) {
	t.Helper()
	cell.src.exec(t, c.create)
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_nar", 1, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered t_nar (stream: %v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf(c.insert, 2))
	if !cell.tgt.waitRow(t, "t_nar", 2, run, 90*time.Second) {
		t.Fatalf("row 2 never landed (stream: %v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}

	cell.src.exec(t, c.alter)
	cell.src.exec(t, fmt.Sprintf(c.insert, 3))
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_nar", 3, run, 90*time.Second) {
		t.Fatalf("row 3 never landed (stream: %v)\n%s", run.stop(t), divergenceLines(logs.String()))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream returned %v", err)
	}
	if !forwarded {
		// The Postgres residual (GC-44 F23): the WARN keeps the target, and
		// says the source may have narrowed it.
		lines := logLinesFor(logs, twfbLogTargetWider, "t_nar")
		if len(lines) == 0 || !strings.Contains(strings.Join(lines, "\n"), "the SOURCE narrowed") {
			t.Errorf("VACUOUS: no WIDER warning naming a possible source narrowing\n%s", divergenceLines(logs.String()))
		}
		return
	}
	if lines := logLinesFor(logs, twfbLogForwarded, "t_nar"); len(lines) == 0 || !strings.Contains(strings.Join(lines, "\n"), "narrowed from") {
		t.Errorf("VACUOUS: no forwarded narrowing was logged\n%s", divergenceLines(logs.String()))
	}
	for id := 1; id <= 3; id++ {
		if src, tgt := readV(cell.src, id), readV(cell.tgt, id); src != tgt {
			t.Errorf("SILENT DIVERGENCE row %d: source %q, target %q", id, src, tgt)
		}
	}
}

// TestTWFB_SourceNarrowedWhileStopped_MySQLToPostgres is the forward path's
// sibling of HIGH-1: a narrowing made while the stream was stopped, against
// a target the retained history proves still holds the source's old type,
// is forwarded at the first boundary — the target's rows are converted as
// the source's were, and every row reads back equal. Before the fix the
// first boundary kept the target with a WARN and rows 1–2 kept their
// unrounded values.
func TestTWFB_SourceNarrowedWhileStopped_MySQLToPostgres(t *testing.T) {
	for _, c := range []forwardNarrowingCell{
		{
			"decimal", "CREATE TABLE t_nar (id BIGINT PRIMARY KEY, v DECIMAL(12,4) NOT NULL) ENGINE=InnoDB; INSERT INTO t_nar VALUES (1, 1.2345);",
			"ALTER TABLE t_nar MODIFY v DECIMAL(10,2) NOT NULL", "INSERT INTO t_nar VALUES (%d, 1.2345)",
		},
		{
			"datetime fsp", "CREATE TABLE t_nar (id BIGINT PRIMARY KEY, v DATETIME(6) NOT NULL) ENGINE=InnoDB; INSERT INTO t_nar VALUES (1, '2020-01-02 10:11:12.345678');",
			"ALTER TABLE t_nar MODIFY v DATETIME(0) NOT NULL", "INSERT INTO t_nar VALUES (%d, '2020-01-02 10:11:12.345678')",
		},
		{
			"decimal to integer", "CREATE TABLE t_nar (id BIGINT PRIMARY KEY, v DECIMAL(12,2) NOT NULL) ENGINE=InnoDB; INSERT INTO t_nar VALUES (1, 12.75);",
			"ALTER TABLE t_nar MODIFY v INT NOT NULL", "INSERT INTO t_nar VALUES (%d, 12.75)",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, _, srcCleanup := startMySQLBinlog(t)
			defer srcCleanup()
			_, tgtDSN, tgtCleanup := startPostgres(t)
			defer tgtCleanup()
			cell := twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-narrow-my"}
			readV := func(db twfbDB, id int) string {
				if db.engine == "postgres" {
					return db.scalar(t, fmt.Sprintf("SELECT v::text FROM t_nar WHERE id = %d", id))
				}
				return db.scalar(t, fmt.Sprintf("SELECT CAST(v AS CHAR) FROM t_nar WHERE id = %d", id))
			}
			runForwardNarrowedWhileStopped(t, cell, c, readV, true)
		})
	}
}

// TestTWFB_SourceNarrowedWhileStopped_PostgresToPostgres pins the stated
// residual on a Postgres source (GC-44 F23): its boundaries are anchored at
// LSN 0/0, so the retained history can never prove a boundary lies after
// the shape it holds, and a crash replay's pre-ALTER relation looks exactly
// like a narrowing (TestTWFB_PostgresCrashMidTransaction_* pin that the
// replay converges). The first boundary keeps the target with a WARN that
// says the source may have narrowed the column.
func TestTWFB_SourceNarrowedWhileStopped_PostgresToPostgres(t *testing.T) {
	c := narrowingCells()[0]
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-narrow-pg"}
	runForwardNarrowedWhileStopped(t, cell, forwardNarrowingCell{
		name:   c.name,
		create: fmt.Sprintf("CREATE TABLE t_nar (id bigint PRIMARY KEY, v %s NOT NULL); INSERT INTO t_nar VALUES (1, '%s');", c.wide, c.before),
		alter:  fmt.Sprintf("ALTER TABLE t_nar ALTER COLUMN v TYPE %s", c.narrow),
		insert: "INSERT INTO t_nar VALUES (%d, '" + c.before + "')",
	}, nil, false)
}

// TestStreamer_RefuseNarrowingOverrideColdStart_PostgresToPostgres is
// MEDIUM-2 through the real wiring: a deliberately narrowing --type-override
// (unconstrained numeric → numeric(18,4)) on a cold-started refuse-mode
// stream. Before the fix the table's first boundary had no prior and
// refused, so every such stream stopped at its first change; the cold
// start's raw source read is the prior now. A source narrowing under the
// override still refuses.
func TestStreamer_RefuseNarrowingOverrideColdStart_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{
		src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN},
		streamID: "refuse-ovr-cold", schemaChanges: "refuse",
		mappings: []config.Mapping{{
			Table: "t_ovc", Column: "amount", TargetType: "numeric",
			TargetTypeOptions: map[string]any{"precision": 18, "scale": 4},
		}},
	}
	cell.src.exec(t, "CREATE TABLE t_ovc (id bigint PRIMARY KEY, amount numeric NOT NULL); INSERT INTO t_ovc VALUES (1, 1.5);")
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_ovc", 1, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered t_ovc (stream: %v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	if got := cell.tgt.columnType(t, "t_ovc", "amount"); got != "numeric(18,4)" {
		t.Fatalf("the override did not land: target amount is %q", got)
	}
	cell.src.exec(t, "INSERT INTO t_ovc VALUES (2, 2.25)")
	if !cell.tgt.waitRow(t, "t_ovc", 2, run, 90*time.Second) {
		t.Fatalf("the first boundary under a narrowing override refused or stalled (stream: %v)", run.stop(t))
	}
	cell.src.exec(t, "ALTER TABLE t_ovc ALTER COLUMN amount TYPE numeric(10,1); INSERT INTO t_ovc VALUES (3, 3.5);")
	// Live, the relation is cached in this reader session: the Postgres
	// reader's own gate refuses the type change first (restored
	// 2026-10-03). The restart's first relation is this check's, judged
	// against the one-version history: AMBIGUOUS on Postgres.
	err := waitRefused(t, run, 60*time.Second)
	_ = run.stop(t)
	if err == nil || !strings.Contains(err.Error(), "incompatible schema change mid-stream") {
		t.Errorf("[live] the source narrowing was not refused at the reader (err %v)", err)
	}
	run = startTWFBRun(cell.streamer())
	err = waitRefused(t, run, 60*time.Second)
	_ = run.stop(t)
	for _, w := range []string{schemaChangeRefusedMarker, "narrowed from", ambiguousBoundaryMarker} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("[restart] a source narrowing under the override was not refused with %q (err %v)", w, err)
		}
	}
	if cell.tgt.hasRow(t, "t_ovc", 3) {
		t.Error("row 3 LANDED past the refusal")
	}
}

// TestTWFB_PostgresCharKeyChangedWhileStopped_Refuses: a varchar key
// changed to char while the stream was stopped. pgoutput sends the padded
// bpchar ('ab   '), the target's rows hold 'ab'. Before the fix the first
// boundary kept the target (char ⊂ varchar) and the next UPDATE / DELETE
// by that key matched no row while the INSERT landed padded, at exit 0.
func TestTWFB_PostgresCharKeyChangedWhileStopped_Refuses(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-charkey"}
	cell.src.exec(t, `CREATE TABLE t_ck (code varchar(5) PRIMARY KEY, n integer NOT NULL, id integer NOT NULL);
		INSERT INTO t_ck VALUES ('ab', 1, 1), ('cd', 2, 2);`)
	run := startTWFBRun(cell.streamer())
	deadline := time.Now().Add(180 * time.Second)
	for cell.tgt.scalar(t, "SELECT count(*) FROM t_ck") != "2" && time.Now().Before(deadline) && !run.exited() {
		time.Sleep(200 * time.Millisecond)
	}
	cell.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}
	cell.src.exec(t, `ALTER TABLE t_ck ALTER COLUMN code TYPE char(5);
		UPDATE t_ck SET n = 10 WHERE code = 'ab'; DELETE FROM t_ck WHERE code = 'cd'; INSERT INTO t_ck VALUES ('ef', 3, 3);`)
	run = startTWFBRun(cell.streamer())
	err := waitRefused(t, run, 90*time.Second)
	_ = run.stop(t)
	if err == nil || !strings.Contains(err.Error(), resumeDivergenceMarker) {
		t.Fatalf("the stopped-time char change was not refused with %s (err %v)", resumeDivergenceMarker, err)
	}
	if got := cell.tgt.scalar(t, "SELECT string_agg(code || ':' || n, ',' ORDER BY code) FROM t_ck"); got != "ab:1,cd:2" {
		t.Errorf("the target moved past the refusal: %q, want the pre-change rows untouched", got)
	}
}

// TestTWFB_PostgresArrayElementChangedWhileStopped_Refuses: an array
// column's element modifier changed while the stream was stopped. Before
// the fix the boundary carried no element modifier and the lens erased the
// target's, so the change matched and every later element was rounded into
// the old modifier at exit 0, with no log line (the review measured
// `{1.23}` and `10:00:00`).
func TestTWFB_PostgresArrayElementChangedWhileStopped_Refuses(t *testing.T) {
	for _, c := range []struct {
		name, from, to, value string
	}{
		{"numeric element", "numeric(10,2)[]", "numeric(10,4)[]", "{1.2345}"},
		{"timestamp element", "timestamp(0)[]", "timestamp(6)[]", "{\"2020-01-02 10:00:00.123456\"}"},
	} {
		for _, mode := range []string{"", "refuse"} {
			t.Run(c.name+"/"+map[string]string{"": "forward", "refuse": "refuse"}[mode], func(t *testing.T) {
				srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
				defer cleanup()
				cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "twfb-arr", schemaChanges: mode}
				cell.src.exec(t, fmt.Sprintf("CREATE TABLE t_arr (id bigint PRIMARY KEY, a %s); INSERT INTO t_arr VALUES (1, NULL);", c.from))
				cell.coldStartAndStop(t, "t_arr", 1)
				// Anti-vacuity: an UNCHANGED restart passes. Without it a
				// refusal below could be a phantom (a projection that drops
				// the element modifier reads as a change on every start)
				// rather than the change this cell makes.
				run := startTWFBRun(cell.streamer())
				cell.src.exec(t, "INSERT INTO t_arr VALUES (5, NULL)")
				if !cell.tgt.waitRow(t, "t_arr", 5, run, 90*time.Second) {
					t.Fatalf("an unchanged restart did not resume — a phantom element change (stream: %v)", run.stop(t))
				}
				if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("the unchanged restart returned %v", err)
				}
				cell.src.exec(t, fmt.Sprintf("ALTER TABLE t_arr ALTER COLUMN a TYPE %s; INSERT INTO t_arr VALUES (2, '%s');", c.to, c.value))
				run = startTWFBRun(cell.streamer())
				err := waitRefused(t, run, 90*time.Second)
				_ = run.stop(t)
				want := resumeDivergenceMarker
				if mode == "refuse" {
					want = schemaChangeRefusedMarker
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("the stopped-time element change was not refused with %s (err %v)", want, err)
				}
				if cell.tgt.hasRow(t, "t_arr", 2) {
					t.Errorf("row 2 LANDED past the refusal as %s", cell.tgt.scalar(t, "SELECT a::text FROM t_arr WHERE id = 2"))
				}
			})
		}
	}
}
