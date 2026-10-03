//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F5 fourth review pins, on real servers — the reviewer's repros
// (zz_review_integration_test.go overlay) ported to pins.
//
//   - Finding 1, the narrow-back: the drained model widens a column (target
//     first, then the source), and the source then narrows it BACK to the
//     shape it held before. Its own ALTER converts the rows it stores; the
//     target's copies were never converted. On MySQL the old recorded-shape
//     exemption accepted the narrow-back with a WARN — live, and after a
//     restart against a history holding both versions — and the target kept
//     1.2345 where the source holds 1.23, at exit 0. It refuses now, as a
//     proven narrowing. On Postgres the reader's gate refuses it live, and
//     after a restart the one-version history makes it AMBIGUOUS.
//   - Finding 2, the crash replay: a refuse-mode Postgres transaction that
//     part-applied before the target's backends were killed is re-delivered
//     from its start by the ADR-0038 retry, pre-ALTER relation first. For an
//     ADD COLUMN that read as "DROP COLUMN a — drop it on the target", which
//     destroys the column's values. It now refuses AMBIGUOUS, and the
//     acknowledgement it offers lands every row exact. For a type change the
//     reader's gate refuses the transaction at its first delivery, before
//     anything written after the ALTER lands, and every restart after
//     (GC-44 F24, loud); one start under --schema-changes=forward recovers
//     it.
//
// The independent expected value is the SOURCE's own read-back of each
// row, compared with the target's.

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

// narrowBackDialect is one source's spelling of the narrow-back pin.
type narrowBackDialect struct {
	create, widen, narrow, readV string
}

// runRefuseNarrowBackPin: a refuse-mode stream; rows 1–2 at numeric(10,2);
// the drained-model widen to (12,4) and row 3 = 1.2345; the source narrows
// back to (10,2) (rounding row 3 to 1.23 on the source) and writes row 4.
// liveWant / restartWant are what the live and restarted refusals must
// carry. The drained model for the narrowing (the target narrowed too)
// then converges every row.
func runRefuseNarrowBackPin(t *testing.T, cell twfbCell, d narrowBackDialect, liveWant, restartWant []string) {
	t.Helper()
	cell.schemaChanges = "refuse"
	cell.src.exec(t, d.create)
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_nb", 1, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered t_nb (stream: %v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, "INSERT INTO t_nb VALUES (2, 2.50)")
	if !cell.tgt.waitRow(t, "t_nb", 2, run, 90*time.Second) {
		t.Fatalf("row 2 never landed (stream: %v)", run.stop(t))
	}

	// The drained-model widen: the target first, then the source.
	cell.tgt.exec(t, "ALTER TABLE t_nb ALTER COLUMN v TYPE numeric(12,4)")
	cell.src.exec(t, d.widen)
	cell.src.exec(t, "INSERT INTO t_nb VALUES (3, 1.2345)")
	if !cell.tgt.waitRow(t, "t_nb", 3, run, 90*time.Second) {
		// A Postgres reader refuses the live type change at its own gate;
		// the restart takes it as the session's first relation.
		t.Logf("the widen stopped the stream (%v); restarting", run.stop(t))
		run = startTWFBRun(cell.streamer())
		if !cell.tgt.waitRow(t, "t_nb", 3, run, 90*time.Second) {
			t.Fatalf("row 3 never landed after the widen (stream: %v)", run.stop(t))
		}
	}
	readV := func(db twfbDB, id int) string { return db.scalar(t, fmt.Sprintf(d.readV, id)) }
	if got := readV(cell.tgt, 3); got != "1.2345" {
		t.Fatalf("row 3 on the target after the widen = %q, want 1.2345", got)
	}

	// The narrow-back.
	cell.src.exec(t, d.narrow)
	cell.src.exec(t, "INSERT INTO t_nb VALUES (4, 4.50)")
	for _, step := range []struct {
		kind string
		want []string
	}{{"live", liveWant}, {"restart", restartWant}} {
		err := waitRefused(t, run, 60*time.Second)
		if err == nil || errors.Is(err, context.Canceled) {
			t.Errorf("[%s] the narrow-back was NOT refused (err %v); row 4 landed=%v, row 3 source %q target %q",
				step.kind, run.stop(t), cell.tgt.hasRow(t, "t_nb", 4), readV(cell.src, 3), readV(cell.tgt, 3))
		} else {
			for _, w := range step.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("[%s] the refusal lacks %q: %v", step.kind, w, err)
				}
			}
			t.Logf("[%s] refused: %v", step.kind, err)
		}
		_ = run.stop(t)
		if cell.tgt.hasRow(t, "t_nb", 4) {
			t.Errorf("[%s] row 4 LANDED past the narrow-back", step.kind)
		}
		if step.kind == "live" {
			run = startTWFBRun(cell.streamer())
		}
	}

	// The source changed it: narrow the target the same way, then restart.
	cell.tgt.exec(t, "ALTER TABLE t_nb ALTER COLUMN v TYPE numeric(10,2)")
	run = startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_nb", 4, run, 90*time.Second) {
		t.Fatalf("[recovered] row 4 never landed (stream: %v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[recovered] the stream returned %v", err)
	}
	for id := 1; id <= 4; id++ {
		if src, tgt := readV(cell.src, id), readV(cell.tgt, id); src != tgt {
			t.Errorf("[recovered] row %d: source %q, target %q", id, src, tgt)
		}
	}
}

func TestStreamer_RefuseNarrowBack_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	cell := twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "narrowback-mysql"}
	// Positions order every boundary here, so both refusals are proven
	// narrowings — never AMBIGUOUS — and the restart is judged against a
	// history holding both the (10,2) and (12,4) versions.
	proven := []string{schemaChangeRefusedMarker, "narrowed from", "NARROWED"}
	runRefuseNarrowBackPin(t, cell, narrowBackDialect{
		create: "CREATE TABLE t_nb (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(10,2) NOT NULL) ENGINE=InnoDB; INSERT INTO t_nb VALUES (1, 1.25);",
		widen:  "ALTER TABLE t_nb MODIFY v DECIMAL(12,4) NOT NULL",
		narrow: "ALTER TABLE t_nb MODIFY v DECIMAL(10,2) NOT NULL",
		readV:  "SELECT CAST(v AS DECIMAL(12,4)) FROM t_nb WHERE id = %d",
	}, proven, proven)
}

func TestStreamer_RefuseNarrowBack_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "narrowback-pg"}
	runRefuseNarrowBackPin(
		t, cell, narrowBackDialect{
			create: "CREATE TABLE t_nb (id bigint PRIMARY KEY, v numeric(10,2) NOT NULL); INSERT INTO t_nb VALUES (1, 1.25);",
			widen:  "ALTER TABLE t_nb ALTER COLUMN v TYPE numeric(12,4)",
			narrow: "ALTER TABLE t_nb ALTER COLUMN v TYPE numeric(10,2)",
			readV:  "SELECT v::numeric(12,4)::text FROM t_nb WHERE id = %d",
		},
		// Live: the relation is cached in this reader session.
		[]string{"incompatible schema change mid-stream", "Drained-model recovery"},
		// Restarted: the session's first relation, judged against the
		// one-version history at 0/0 — a replay or a source change.
		[]string{schemaChangeRefusedMarker, "narrowed from", ambiguousBoundaryMarker, "CURRENT definition"},
	)
}

// crashReplayCell runs the reviewer's crash cell: under refuse mode, with
// the target already holding the drained-model change, one source
// transaction — UPDATE row 1; the ALTER; INSERT rows 10…rows+9 — is applied
// while the target's backends are killed once it has part-applied. It
// returns the stream's error and the run (stopped).
func crashReplayCell(t *testing.T, cell twfbCell, table, alter, insert string, rows int) error {
	t.Helper()
	s := cell.streamer()
	s.ApplyConcurrency = 1
	run := startTWFBRun(s)
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf(`BEGIN;
		UPDATE %[1]s SET v = 2 WHERE id = 1;
		%[2]s;
		%[3]s;
		COMMIT;`, table, alter, fmt.Sprintf(insert, table, rows+9)))
	count := func(where string) int {
		n, _ := strconv.Atoi(cell.tgt.scalar(t, "SELECT count(*) FROM "+table+where))
		return n
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		if n := count(" WHERE id >= 10"); n > 0 && n < rows {
			cell.tgt.exec(t, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	deadline = time.Now().Add(300 * time.Second)
	for time.Now().Before(deadline) && !run.exited() && count("") != rows+1 {
		time.Sleep(250 * time.Millisecond)
	}
	return run.stop(t)
}

// TestStreamer_RefuseCrashReplayAddColumn_PostgresToPostgres is Finding 2's
// ADD COLUMN cell. The retry re-delivers the transaction's pre-ADD relation
// against a history that holds only the post-ADD shape: AMBIGUOUS, and the
// refusal must not tell the operator to drop the column. The operator
// confirms the source's current definition equals the target's and passes
// the fingerprint back: every row lands exact.
func TestStreamer_RefuseCrashReplayAddColumn_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "crash-add", schemaChanges: "refuse"}
	const table, rows = "t_ca", 50000
	cell.src.exec(t, "CREATE TABLE t_ca (id bigint PRIMARY KEY, v int NOT NULL); INSERT INTO t_ca VALUES (1, 1);")
	cell.coldStartAndStop(t, table, 1)
	cell.tgt.exec(t, "ALTER TABLE t_ca ADD COLUMN a int")
	err := crashReplayCell(t, cell, table, "ALTER TABLE t_ca ADD COLUMN a int",
		"INSERT INTO %s SELECT g, g, 7 FROM generate_series(10, %d) g", rows)
	if err == nil || !strings.Contains(err.Error(), ambiguousBoundaryMarker) || !strings.Contains(err.Error(), "DROP COLUMN a") {
		n := cell.tgt.scalar(t, "SELECT count(*) FROM t_ca")
		if n == strconv.Itoa(rows+1) {
			t.Fatalf("VACUOUS: the transaction applied whole before the kill landed (err %v) — no replay to judge", err)
		}
		t.Fatalf("the replay was not refused AMBIGUOUS naming the DROP (err %v, target count %s)", err, n)
	}
	if two, drop := strings.Index(err.Error(), "it is (2):"), strings.Index(err.Error(), "apply the same change"); two < 0 || drop < two {
		t.Errorf("the destructive remedy is not confined to the source-change reading: %v", err)
	}
	m := ackRE.FindStringSubmatch(err.Error())
	if m == nil {
		t.Fatalf("the refusal offers no acknowledgement: %v", err)
	}

	// Telling them apart, as the refusal says: the source's CURRENT
	// definition holds the column, as the target does — a replay.
	if cell.src.columnType(t, table, "a") != "integer" || cell.tgt.columnType(t, table, "a") != "integer" {
		t.Fatalf("source a %q / target a %q: the cell is not the replay case", cell.src.columnType(t, table, "a"), cell.tgt.columnType(t, table, "a"))
	}
	// A restart without the acknowledgement refuses again (nothing persisted
	// it; the evidence is re-derived) — and prints the same fingerprint.
	run := startTWFBRun(cell.streamer())
	err = waitRefused(t, run, 90*time.Second)
	_ = run.stop(t)
	if err == nil || !strings.Contains(err.Error(), "--accept-unforwarded-schema-change="+m[1]) {
		t.Fatalf("the restart did not repeat the refusal with fingerprint %s: %v", m[1], err)
	}

	s := cell.streamer()
	s.AcceptUnforwardedSchemaChange = m[1]
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(s)
	deadline := time.Now().Add(300 * time.Second)
	for time.Now().Before(deadline) && !run.exited() && cell.tgt.scalar(t, "SELECT count(*) FROM t_ca") != strconv.Itoa(rows+1) {
		time.Sleep(250 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("[acknowledged] the stream returned %v", err)
	}
	if !strings.Contains(logs.String(), "accepted an "+ambiguousBoundaryMarker) {
		t.Error("[acknowledged] the acceptance was not logged")
	}
	q := "SELECT count(*) FROM t_ca WHERE id >= 10 AND v = id AND a = 7"
	if got := cell.tgt.scalar(t, q); got != strconv.Itoa(rows) {
		t.Errorf("[acknowledged] %s rows of the transaction read back exact; want %d", got, rows)
	}
	if got := cell.tgt.scalar(t, "SELECT v::text || '/' || coalesce(a::text, 'null') FROM t_ca WHERE id = 1"); got != "2/null" {
		t.Errorf("[acknowledged] row 1 = %q, want 2/null", got)
	}

	// The stream resumes past the transaction without the acknowledgement.
	run = startTWFBRun(cell.streamer())
	cell.src.exec(t, "INSERT INTO t_ca VALUES (5, 5, 5)")
	if !cell.tgt.waitRow(t, table, 5, run, 90*time.Second) {
		t.Fatalf("the restart after the acknowledged run did not resume (stream: %v)", run.stop(t))
	}
	_ = run.stop(t)

	// GC-44 F5 fifth review, Finding 1: the acknowledgement left in the unit
	// file. While the stream is stopped the source really drops the column —
	// the same table, the same refused column, the same detail as the replay
	// the fingerprint was printed for. The stream has persisted past that
	// replay, so the fingerprint (bound to the position it resumed from) no
	// longer names this boundary: it refuses, and the target keeps column a.
	// Before the binding this start accepted the DROP silently.
	cell.src.exec(t, "ALTER TABLE t_ca DROP COLUMN a; INSERT INTO t_ca VALUES (6, 6)")
	stale := cell.streamer()
	stale.AcceptUnforwardedSchemaChange = m[1]
	run = startTWFBRun(stale)
	err = waitRefused(t, run, 90*time.Second)
	_ = run.stop(t)
	if err == nil || !strings.Contains(err.Error(), ambiguousBoundaryMarker) || !strings.Contains(err.Error(), "does not name this boundary") {
		t.Fatalf("[stale acknowledgement] a genuine DROP COLUMN after the stream moved past the replay was not refused "+
			"(err %v; target row 6 present: %v)", err, cell.tgt.hasRow(t, table, 6))
	}
	if cell.tgt.hasRow(t, table, 6) {
		t.Error("[stale acknowledgement] row 6, written after the genuine DROP, landed")
	}
}

// TestStreamer_RefuseCrashReplayWiden_PostgresToPostgres is the reviewer's
// other crash cell, a drained-model widen inside one large transaction. With
// the Postgres reader's gate restored no insert of it is ever applied: its
// post-ALTER relation is refused at the reader on its first delivery — the
// kill never finds a part-applied insert to interrupt — and on the restart
// after (GC-44 F24, loud). One
// start under --schema-changes=forward, the documented single-database
// recovery, lands every row exact.
func TestStreamer_RefuseCrashReplayWiden_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "crash-widen", schemaChanges: "refuse"}
	const table, rows = "t_cr", 50000
	cell.src.exec(t, "CREATE TABLE t_cr (id bigint PRIMARY KEY, v numeric(10,2) NOT NULL); INSERT INTO t_cr VALUES (1, 1.25);")
	cell.coldStartAndStop(t, table, 1)
	cell.tgt.exec(t, "ALTER TABLE t_cr ALTER COLUMN v TYPE numeric(12,4)")
	err := crashReplayCell(t, cell, table, "ALTER TABLE t_cr ALTER COLUMN v TYPE numeric(12,4)",
		"INSERT INTO %s SELECT g, 1.2345 FROM generate_series(10, %d) g", rows)
	if err == nil || !strings.Contains(err.Error(), "incompatible schema change mid-stream") {
		t.Fatalf("the transaction was not refused at the reader (err %v)", err)
	}
	// Nothing after the ALTER landed. The UPDATE before it may have (a
	// source transaction can span target transactions; measured: row 1 read
	// back 2.0000) — the source's committed value, re-applied by the
	// recovery.
	if got := cell.tgt.scalar(t, "SELECT count(*) FROM t_cr"); got != "1" {
		t.Errorf("target after the refusal holds %s rows, want 1 (no row inserted after the ALTER)", got)
	}
	// The wedge: the restart replays the transaction and refuses again.
	run := startTWFBRun(cell.streamer())
	if err := waitRefused(t, run, 90*time.Second); err == nil || !strings.Contains(err.Error(), "incompatible schema change mid-stream") {
		t.Errorf("the restart did not repeat the reader's refusal: %v", err)
	}
	_ = run.stop(t)

	// The recovery.
	fwd := cell
	fwd.schemaChanges = "forward"
	run = startTWFBRun(fwd.streamer())
	deadline := time.Now().Add(300 * time.Second)
	for time.Now().Before(deadline) && !run.exited() && cell.tgt.scalar(t, "SELECT count(*) FROM t_cr") != strconv.Itoa(rows+1) {
		time.Sleep(250 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("[forward] the recovery returned %v", err)
	}
	if got := cell.tgt.scalar(t, "SELECT count(*) FROM t_cr WHERE id >= 10 AND v = 1.2345"); got != strconv.Itoa(rows) {
		t.Errorf("[forward] %s rows read back exact; want %d", got, rows)
	}
	if got := cell.tgt.scalar(t, "SELECT v::text FROM t_cr WHERE id = 1"); got != "2.0000" {
		t.Errorf("[forward] row 1 = %q, want 2.0000", got)
	}
}
