//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 332: what `schema add-table --no-drain` (AddTable.LiveMode, the
// publication-add live path of ADR-0030/0033/0036) does on a POSTGRES source
// to changes made to the added table while the add runs.
//
// Two arms, one cause — the running stream does not apply the added table's
// post-snapshot changes in order AFTER the copy:
//
//   - filtered (the stream has --include-table and the added table is not in
//     it): the dispatch filter drops the table until the live-added poll
//     admits it;
//   - unfiltered: the stream applies the table's changes concurrently with
//     the add's bulk copy, so a DELETE that lands before the copy writes the
//     row is a no-op and the copy then resurrects it.
//
// Phases, as in the binlog rig (add_table_no_drain_window_binlog_integration_test.go):
// A right after the add's snapshot opened (copy not started), B mid-copy
// after ndwMidCopyAfter rows (ids 21..30 already read, 15001..15020 not yet),
// C after the add returned, D after the live-added poll interval.
// The hooks hold the add for pgndwHold after each phase so the running stream
// gets to apply the phase before the copy moves on, which is what makes the
// overlap deterministic rather than a scheduling accident.
//
// MEASURED 2026-10-23 at v0.163.8 (bce1057d, before the refusal), 20,000
// seeded rows, PG 16 source; per phase "deletes kept / updates stale /
// inserts missing", the add exiting nil with 0 WARN/ERROR lines every time:
//
//   - pg->pg filtered:      A 20/10/10, B 30/10/10, C and D exact
//     (whole table 50 kept, 20 stale, 20 missing)
//   - pg->pg unfiltered:    A 20/10/0,  B 20/5/0 (the not-yet-copied
//     15001..15020 and 15101..15105), C and D exact (40 kept, 15 stale)
//   - pg->mysql filtered:   A 20/10/10, B 30/10/10 (50 kept, 20 stale, 20 missing)
//   - pg->mysql unfiltered: A 20/10/0,  B 30/0/0 (50 kept, 10 stale)
//   - drained add against a stopped stream, pg->pg and pg->mysql: exact.
//
// The live arms are now pins: --no-drain on a Postgres source refuses
// NO-DRAIN-POSTGRES-SOURCE before it reads the source, opens a snapshot,
// extends the publication or records anything, and the running stream is
// unaffected. The drained controls (filtered pg->pg and pg->mysql,
// unfiltered pg->pg) stay exact pins across every phase. The RecopyRemedy
// cells measure the CHANGELOG's re-copy on a Postgres source, including its
// foreign-key caveat (runRecopyRemedy, shared with the binlog rig).

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// pgndwHold is how long a hook holds the add after applying its phase.
const pgndwHold = 2 * time.Second

// pgndwSource is the Postgres engine whose add-table snapshot (the temp-slot
// open the add uses) runs the rig's hooks. It survives the add's
// re-scoping to the stream's recorded publication (WithPublicationScope),
// which would otherwise hand the add the bare engine and drop the hooks.
type pgndwSource struct {
	postgres.Engine
	onSnapshot *func()
	onMidCopy  *func()
}

func (e *pgndwSource) WithPublicationScope(publication, ownSlot string) ir.Engine {
	inner, ok := e.Engine.WithPublicationScope(publication, ownSlot).(postgres.Engine)
	if !ok {
		panic("RIG: postgres WithPublicationScope no longer returns a postgres.Engine")
	}
	return &pgndwSource{Engine: inner, onSnapshot: e.onSnapshot, onMidCopy: e.onMidCopy}
}

func (e *pgndwSource) OpenSnapshotStreamWithSlot(ctx context.Context, dsn, slotName string) (*ir.SnapshotStream, error) {
	stream, err := e.Engine.OpenSnapshotStreamWithSlot(ctx, dsn, slotName)
	if err != nil {
		return stream, err
	}
	if f := *e.onSnapshot; f != nil {
		f()
	}
	if f := *e.onMidCopy; f != nil {
		stream.Rows = &ndwMidCopyReader{inner: stream.Rows, fire: f}
	}
	return stream, nil
}

// pgndwArm is one configuration of the Postgres-source rig.
type pgndwArm struct {
	name      string
	tgtEngine string
	src, tgt  ndwDB
	// filtered starts the stream with --include-table=users (the staged-wave
	// shape); otherwise the stream is unfiltered.
	filtered bool
	// noDrain runs the add with --no-drain against the running stream;
	// otherwise the stream is stopped (`sync stop --wait`), the add runs
	// drained, and the stream restarts.
	noDrain bool
}

// pgndwOutcome is what the arm measured.
type pgndwOutcome struct {
	addErr                  error
	phases                  []*ndwPhase
	results                 []ndwPhaseResult
	stale, changed, missing int
	warnLines               int
	snapshotOpened          bool
}

func runPGNoDrainArm(t *testing.T, arm pgndwArm) pgndwOutcome {
	t.Helper()
	logs := captureSlog(t)
	srcEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	tgtEng, ok := engines.Get(arm.tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", arm.tgtEngine)
	}

	arm.src.exec(t,
		"CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		"INSERT INTO users VALUES (1, 'u1')")

	streamID := "pgndw-" + strings.ReplaceAll(arm.name, " ", "-")
	var filter migcore.TableFilter
	if arm.filtered {
		filter = mustNewFilter(t, []string{"users"}, nil)
	}
	mk := func() *Streamer {
		return &Streamer{
			Source: srcEng, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
			StreamID: streamID, Filter: filter, ApplyRetryAttempts: 8,
		}
	}
	cell := twfbCell{tgt: twfbDB{engine: arm.tgtEngine, dsn: arm.tgt.dsn}, streamID: streamID}
	waitUser := func(id int64, run *twfbRun) {
		t.Helper()
		if !cell.tgt.waitRow(t, "users", int(id), run, 120*time.Second) {
			if run.exited() {
				t.Fatalf("the stream exited: %v", run.err)
			}
			t.Fatalf("users %d never reached the target", id)
		}
	}

	run := startTWFBRun(mk())
	defer func() { _ = run.stop(t) }()
	cell.waitStreaming(t, run)
	arm.src.exec(t, "INSERT INTO users VALUES (2, 'u2')")
	waitUser(2, run)

	// The table to add is created after the stream started, so neither arm's
	// cold start copied it.
	arm.src.exec(t,
		"CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL)",
		fmt.Sprintf("INSERT INTO orders SELECT g, 'v1' FROM generate_series(1, %d) g", ndwSeedRows))

	phaseA := &ndwPhase{name: "A", deletes: ndwRange(1, 20), updates: ndwRange(101, 110), inserts: ndwRange(100001, 100010)}
	phaseB := &ndwPhase{
		name:    "B",
		deletes: append(ndwRange(21, 30), ndwRange(15001, 15020)...),
		updates: append(ndwRange(121, 125), ndwRange(15101, 15105)...),
		inserts: ndwRange(100011, 100020),
	}
	phaseC := &ndwPhase{name: "C", deletes: ndwRange(31, 40), updates: ndwRange(131, 140), inserts: ndwRange(100021, 100030)}
	phaseD := &ndwPhase{name: "D", deletes: ndwRange(41, 45), updates: ndwRange(141, 145), inserts: ndwRange(100031, 100035)}
	out := pgndwOutcome{phases: []*ndwPhase{phaseA, phaseB, phaseC, phaseD}}

	if !arm.noDrain {
		ndwStopWait(t, tgtEng, arm.tgt.dsn, streamID, run)
	}

	var onSnapshot, onMidCopy func()
	onSnapshot = func() {
		out.snapshotOpened = true
		phaseA.apply(t, arm.src)
		time.Sleep(pgndwHold)
	}
	onMidCopy = func() {
		phaseB.apply(t, arm.src)
		time.Sleep(pgndwHold)
	}
	src := &pgndwSource{Engine: srcEng.(postgres.Engine), onSnapshot: &onSnapshot, onMidCopy: &onMidCopy}
	logOffset := len(logs.String())
	out.addErr = (&AddTable{
		Source: src, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
		StreamID: streamID, TableName: "orders", LiveMode: arm.noDrain,
	}).Run(context.Background())
	if arm.noDrain {
		// The pin: the add that lost the window's changes now refuses before
		// it reads the source or writes anything, and the running stream is
		// unaffected.
		requirePGNoDrainRefusal(t, arm, out, tgtEng, streamID)
		arm.src.exec(t, "INSERT INTO users VALUES (3, 'u3')")
		waitUser(3, run)
		return out
	}
	if out.addErr != nil {
		t.Fatalf("[%s] drained AddTable: %v", arm.name, out.addErr)
	}
	if phaseA.at.IsZero() || phaseB.at.IsZero() {
		t.Fatalf("RIG [%s]: the hooks did not run (A=%v B=%v)", arm.name, phaseA.at, phaseB.at)
	}
	phaseC.apply(t, arm.src)
	if !arm.noDrain {
		run = startTWFBRun(mk())
	}
	// Past the live-added poll interval, so D lands after any admission.
	time.Sleep(8 * time.Second)
	phaseD.apply(t, arm.src)
	arm.src.exec(t, fmt.Sprintf("INSERT INTO orders VALUES (%d, 'marker')", ndwMarkerID), "INSERT INTO users VALUES (3, 'u3')")
	waitUser(3, run)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := arm.tgt.rows(t)[ndwMarkerID]; ok {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	srcRows, tgtRows := arm.src.rows(t), arm.tgt.rows(t)
	if _, ok := tgtRows[ndwMarkerID]; !ok {
		t.Fatalf("RIG [%s]: the post-add marker never reached the target", arm.name)
	}
	if want := ndwSeedRows - 20 - 30 - 10 - 5 + 10 + 10 + 10 + 5 + 1; len(srcRows) != want {
		t.Fatalf("RIG [%s]: source holds %d orders rows; want %d", arm.name, len(srcRows), want)
	}
	if !strings.Contains(logs.String()[logOffset:], "add-table: snapshot captured") {
		t.Errorf("RIG [%s]: the log capture does not hold the add's own INFO line", arm.name)
	}
	for _, p := range out.phases {
		r := p.judge(tgtRows)
		out.results = append(out.results, r)
		t.Logf("[%s] phase %s: deletes %d -> still on target %d | updates %d -> old value %d | inserts %d -> missing %d",
			arm.name, p.name, len(p.deletes), r.staleDeletes, len(p.updates), r.staleUpdates, len(p.inserts), r.missingInserts)
	}
	out.stale, out.changed, out.missing = ndwDiff(srcRows, tgtRows)
	out.warnLines = len(ndwWarnLines(logs.String(), logOffset))
	t.Logf("[%s] whole table: %d deleted rows still on target, %d rows with old values, %d rows missing (source %d, target %d); %d WARN/ERROR lines",
		arm.name, out.stale, out.changed, out.missing, len(srcRows), len(tgtRows), out.warnLines)
	return out
}

// requirePGNoDrainRefusal is the pin that replaced the measurement: the
// --no-drain add refuses NO-DRAIN-POSTGRES-SOURCE, names the drained flow,
// and left nothing behind — no snapshot, no target table, no live-added or
// pending record, and no publication membership for the table.
func requirePGNoDrainRefusal(t *testing.T, arm pgndwArm, out pgndwOutcome, tgtEng ir.Engine, streamID string) {
	t.Helper()
	if out.addErr == nil {
		t.Fatalf("[%s] --no-drain on a Postgres source succeeded; want the %s refusal", arm.name, noDrainPostgresSourceMarker)
	}
	for _, want := range []string{noDrainPostgresSourceMarker, "sluice sync stop --wait", "without --no-drain", "Keep the stream stopped"} {
		if !strings.Contains(out.addErr.Error(), want) {
			t.Errorf("[%s] refusal = %v; want it to contain %q", arm.name, out.addErr, want)
		}
	}
	if out.snapshotOpened {
		t.Errorf("[%s] the refused add opened a snapshot", arm.name)
	}
	if rows := arm.tgt.rows(t); rows != nil {
		t.Errorf("[%s] the refused add left orders on the target (%d rows)", arm.name, len(rows))
	}
	ndwAssertNoAddRecorded(t, tgtEng, arm.tgt.dsn, streamID)
	src := twfbDB{engine: "postgres", dsn: arm.src.dsn}
	if n := src.scalar(t, `SELECT count(*) FROM pg_publication_rel pr JOIN pg_class c ON c.oid = pr.prrelid WHERE c.relname = 'orders'`); n != "0" {
		t.Errorf("[%s] the refused add put orders in a publication (%s memberships)", arm.name, n)
	}
}

func requirePGDrainedExact(t *testing.T, arm pgndwArm) {
	out := runPGNoDrainArm(t, arm)
	if out.stale+out.changed+out.missing > 0 {
		t.Errorf("CONTROL [%s]: drained add against a stopped stream is not exact: %d stale, %d changed, %d missing", arm.name, out.stale, out.changed, out.missing)
	}
}

func pgndwPGPG(t *testing.T) (src, tgt ndwDB, cleanup func()) {
	t.Helper()
	s, d, c := startPostgresLogical(t)
	return ndwDB{"pgx", s}, ndwDB{"pgx", d}, c
}

func pgndwPGMySQL(t *testing.T) (src, tgt ndwDB, cleanup func()) {
	t.Helper()
	s, _, c1 := startPostgresLogical(t)
	_, m, c2 := startMySQLBinlog(t)
	return ndwDB{"pgx", s}, ndwDB{"mysql", m}, func() { c2(); c1() }
}

func TestAddTableNoDrainWindow_PG_PGToPG_FilteredLive(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	runPGNoDrainArm(t, pgndwArm{name: "pg-pg-filtered-live", tgtEngine: "postgres", src: src, tgt: tgt, filtered: true, noDrain: true})
}

func TestAddTableNoDrainWindow_PG_PGToPG_UnfilteredLive(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	runPGNoDrainArm(t, pgndwArm{name: "pg-pg-unfiltered-live", tgtEngine: "postgres", src: src, tgt: tgt, noDrain: true})
}

func TestAddTableNoDrainWindow_PG_PGToMySQL_FilteredLive(t *testing.T) {
	src, tgt, cleanup := pgndwPGMySQL(t)
	defer cleanup()
	runPGNoDrainArm(t, pgndwArm{name: "pg-mysql-filtered-live", tgtEngine: "mysql", src: src, tgt: tgt, filtered: true, noDrain: true})
}

func TestAddTableNoDrainWindow_PG_PGToMySQL_UnfilteredLive(t *testing.T) {
	src, tgt, cleanup := pgndwPGMySQL(t)
	defer cleanup()
	runPGNoDrainArm(t, pgndwArm{name: "pg-mysql-unfiltered-live", tgtEngine: "mysql", src: src, tgt: tgt, noDrain: true})
}

func TestAddTableNoDrainWindow_PG_PGToPG_DrainedStoppedControl(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	requirePGDrainedExact(t, pgndwArm{name: "pg-pg-drained-stopped", tgtEngine: "postgres", src: src, tgt: tgt, filtered: true})
}

func TestAddTableNoDrainWindow_PG_PGToPG_UnfilteredDrainedStoppedControl(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	requirePGDrainedExact(t, pgndwArm{name: "pg-pg-unfiltered-drained-stopped", tgtEngine: "postgres", src: src, tgt: tgt})
}

// TestAddTableNoDrainWindow_PG_RecopyRemedy* measure the remedy the
// CHANGELOG gives an operator whose table an earlier --no-drain add on a
// Postgres source left diverged (runRecopyRemedy: stop the stream, empty the
// target table with its foreign keys off for that session, add it again
// drained, restart), including the foreign-key caveat.
func TestAddTableNoDrainWindow_PG_RecopyRemedy(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	runRecopyRemedy(t, "postgres", "postgres", src, tgt, "SET LOCAL session_replication_role = replica")
}

func TestAddTableNoDrainWindow_PG_RecopyRemedyMySQLTarget(t *testing.T) {
	src, tgt, cleanup := pgndwPGMySQL(t)
	defer cleanup()
	runRecopyRemedy(t, "postgres", "mysql", src, tgt, "SET FOREIGN_KEY_CHECKS = 0")
}

func TestAddTableNoDrainWindow_PG_PGToMySQL_DrainedStoppedControl(t *testing.T) {
	src, tgt, cleanup := pgndwPGMySQL(t)
	defer cleanup()
	requirePGDrainedExact(t, pgndwArm{name: "pg-mysql-drained-stopped", tgtEngine: "mysql", src: src, tgt: tgt, filtered: true})
}
