//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// NO-DRAIN-BINLOG-SOURCE (was BINLOG-NO-DRAIN-WINDOW-UNMEASURED): what
// `schema add-table --no-drain` (AddTable.LiveMode, ADR-0034's filter-flip)
// did on a MySQL-family binlog source, and the refusal that replaced it.
//
// ADR-0034 documented a "best-effort caveat for writes landing during the
// streamer's poll lag". The window was not the poll lag: the add's copy
// reads a snapshot fixed when it opens, the table joins the running
// stream's scope only after the copy, and the stream drops every change to
// it until its poll admits the table, while its position moves past them.
//
// MEASURED 2026-10-23 at e803bcc7 (before the refusal), seeding orders with
// ndwSeedRows rows and changing it right after the add's snapshot opened
// (A), mid-copy after ndwMidCopyAfter rows (B), after the add returned but
// before the stream admitted the table (C), and after admission (D):
//
//   - mysql->mysql, 5s poll: A, B and C all lost: 60 deletes still on the
//     target, 30 updates at the old value, 30 inserts missing; the add exited
//     nil, the stream kept running, 0 WARN/ERROR lines. D applied.
//   - mysql->mysql, 200ms poll: A and B still all lost (C landed after
//     admission): the window scales with the copy, not the poll interval.
//   - mariadb->mariadb: identical to mysql (60/30/30, 0 WARN).
//   - mysql->postgres --no-drain: already refused (the filter-flip was
//     MySQL-target-only).
//   - drained add against a stopped stream (mysql and postgres targets):
//     exact.
//
// The live arms are now pins: --no-drain on a binlog source refuses
// NO-DRAIN-BINLOG-SOURCE before it opens a snapshot or records the table,
// and the running stream is unaffected. The short-poll arm went with the
// path it measured (a refusal does not depend on the poll). The drained
// controls stay exact pins across every phase.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

const (
	ndwSeedRows     = 20000
	ndwMidCopyAfter = 5000
	ndwMarkerID     = 9000001
)

// ndwSource is a MySQL-family engine whose add-table snapshot runs two hooks:
// onSnapshot once the snapshot is open (consistent view fixed, copy not
// started) and onMidCopy after ndwMidCopyAfter rows have been forwarded to
// the writer. Embedding the concrete engine keeps every optional surface.
type ndwSource struct {
	mysql.Engine
	onSnapshot func()
	onMidCopy  func()
}

func (e *ndwSource) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	stream, err := e.Engine.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		return stream, err
	}
	if e.onSnapshot != nil {
		e.onSnapshot()
	}
	if e.onMidCopy != nil {
		stream.Rows = &ndwMidCopyReader{inner: stream.Rows, fire: e.onMidCopy}
	}
	return stream, nil
}

// ndwMidCopyReader forwards the snapshot's rows and, after ndwMidCopyAfter
// of them, runs fire synchronously (holding the copy there while it runs).
type ndwMidCopyReader struct {
	inner ir.RowReader
	fire  func()
	// copied counts rows forwarded; read by the test after the add.
	copied atomic.Int64
}

func (r *ndwMidCopyReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	in, err := r.inner.ReadRows(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		fired := false
		for row := range in {
			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
			if n := r.copied.Add(1); n == ndwMidCopyAfter && !fired {
				fired = true
				r.fire()
			}
		}
	}()
	return out, nil
}

func (r *ndwMidCopyReader) Err() error { return r.inner.Err() }

// ndwDB is one side of an arm: a database/sql driver name and DSN.
type ndwDB struct {
	driver string // "mysql" or "pgx"
	dsn    string
}

func (d ndwDB) exec(t *testing.T, stmts ...string) {
	t.Helper()
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// rows reads every (id, payload) of orders; nil when the table is absent.
func (d ndwDB) rows(t *testing.T) map[int64]string {
	t.Helper()
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rs, err := db.Query("SELECT id, payload FROM orders")
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		t.Fatalf("read orders: %v", err)
	}
	defer func() { _ = rs.Close() }()
	out := map[int64]string{}
	for rs.Next() {
		var id int64
		var payload string
		if err := rs.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		out[id] = payload
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (d ndwDB) count(t *testing.T) int {
	t.Helper()
	return len(d.rows(t))
}

// ndwPhase is one batch of source changes and when it landed.
type ndwPhase struct {
	name    string
	deletes []int64
	updates []int64 // payload becomes "upd"+name
	inserts []int64 // payload "ins"+name
	at      time.Time
	note    string
}

func ndwRange(lo, hi int64) []int64 {
	out := make([]int64, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, i)
	}
	return out
}

func ndwJoin(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(id)
	}
	return strings.Join(parts, ",")
}

// apply runs the phase on the source (MySQL dialect: every source here is
// MySQL-family) and stamps its time.
func (p *ndwPhase) apply(t *testing.T, src ndwDB) {
	t.Helper()
	var stmts []string
	if len(p.deletes) > 0 {
		stmts = append(stmts, "DELETE FROM orders WHERE id IN ("+ndwJoin(p.deletes)+")")
	}
	if len(p.updates) > 0 {
		stmts = append(stmts, fmt.Sprintf("UPDATE orders SET payload = 'upd%s' WHERE id IN (%s)", p.name, ndwJoin(p.updates)))
	}
	if len(p.inserts) > 0 {
		vals := make([]string, len(p.inserts))
		for i, id := range p.inserts {
			vals[i] = fmt.Sprintf("(%d,'ins%s')", id, p.name)
		}
		stmts = append(stmts, "INSERT INTO orders VALUES "+strings.Join(vals, ","))
	}
	src.exec(t, stmts...)
	p.at = time.Now()
}

// ndwPhaseResult is what the target holds wrong for one phase's changes.
type ndwPhaseResult struct {
	staleDeletes, staleUpdates, missingInserts int
}

func (p *ndwPhase) judge(tgt map[int64]string) ndwPhaseResult {
	var r ndwPhaseResult
	for _, id := range p.deletes {
		if _, ok := tgt[id]; ok {
			r.staleDeletes++
		}
	}
	for _, id := range p.updates {
		if tgt[id] != "upd"+p.name {
			r.staleUpdates++
		}
	}
	for _, id := range p.inserts {
		if _, ok := tgt[id]; !ok {
			r.missingInserts++
		}
	}
	return r
}

// ndwDiff is the whole-table comparison, independent of the phase bookkeeping.
func ndwDiff(src, tgt map[int64]string) (stale, changed, missing int) {
	for id, tv := range tgt {
		sv, ok := src[id]
		switch {
		case !ok:
			stale++
		case sv != tv:
			changed++
		}
	}
	for id := range src {
		if _, ok := tgt[id]; !ok {
			missing++
		}
	}
	return stale, changed, missing
}

// ndwArm is one configuration: a --no-drain add against the running stream
// (which must refuse, NO-DRAIN-BINLOG-SOURCE), or the drained control.
type ndwArm struct {
	name      string
	srcEngine string
	tgtEngine string
	src, tgt  ndwDB
	// noDrain runs the add with --no-drain against the running stream,
	// which must refuse before anything is written; otherwise the stream is
	// stopped with `sync stop --wait`'s protocol, the add runs drained, the
	// stream restarts, and the target must be exact.
	noDrain bool
}

// ndwStopWait is `sluice sync stop --wait`: request the stop on the control
// row, wait until the draining stream clears it, then collect the Run.
func ndwStopWait(t *testing.T, tgtEng ir.Engine, tgtDSN, streamID string, run *twfbRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	applier, err := tgtEng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("RIG: open target applier: %v", err)
	}
	defer func() { _ = applier.(io.Closer).Close() }()
	if err := applier.RequestStop(ctx, streamID); err != nil {
		t.Fatalf("RIG: request stop: %v", err)
	}
	reader := applier.(stopFlagReader)
	for {
		pending, err := reader.ReadStopRequested(ctx, streamID)
		if err != nil {
			t.Fatalf("RIG: read stop flag: %v", err)
		}
		if !pending {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("RIG: the stream never drained after `sync stop`")
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: the stopped stream's Run: %v", err)
	}
}

// ndwAssertNoAddRecorded fails if the stream's control row carries orders
// in its live-added set or as a pending add: a refused add records neither.
func ndwAssertNoAddRecorded(t *testing.T, tgtEng ir.Engine, tgtDSN, streamID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	applier, err := tgtEng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("RIG: open target applier: %v", err)
	}
	defer func() { _ = applier.(io.Closer).Close() }()
	live, ok := applier.(liveAddedTablesReader)
	if !ok {
		t.Fatalf("RIG: the %s applier has no live-added set to check", tgtEng.Name())
	}
	added, err := live.ReadLiveAddedTables(ctx, streamID)
	if err != nil {
		t.Fatalf("RIG: read live-added set: %v", err)
	}
	for _, tbl := range added {
		if tbl == "orders" {
			t.Errorf("the refused add recorded orders in the live-added set %v", added)
		}
	}
	pending, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		t.Fatalf("RIG: the %s applier keeps no pending-add record to check", tgtEng.Name())
	}
	entries, _, err := pending.ReadPendingAddedTables(ctx, streamID)
	if err != nil {
		t.Fatalf("RIG: read pending adds: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the refused add left pending-add entries %+v", entries)
	}
}

// ndwWarnLines returns the WARN/ERROR lines logged since offset.
func ndwWarnLines(all string, offset int) []string {
	if offset > len(all) {
		offset = len(all)
	}
	var out []string
	for _, line := range strings.Split(all[offset:], "\n") {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") {
			out = append(out, line)
		}
	}
	return out
}

func runNoDrainArm(t *testing.T, arm ndwArm) {
	logs := captureSlog(t)
	srcEng, ok := engines.Get(arm.srcEngine)
	if !ok {
		t.Fatalf("%s engine not registered", arm.srcEngine)
	}
	tgtEng, ok := engines.Get(arm.tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", arm.tgtEngine)
	}

	arm.src.exec(t,
		"CREATE TABLE users (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"INSERT INTO users VALUES (1, 'u1')",
		"CREATE TABLE orders (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	for lo := 1; lo <= ndwSeedRows; lo += 5000 {
		var b strings.Builder
		b.WriteString("INSERT INTO orders VALUES ")
		for id := lo; id < lo+5000 && id <= ndwSeedRows; id++ {
			if id > lo {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,'v1')", id)
		}
		arm.src.exec(t, b.String())
	}

	streamID := "ndw-" + strings.ReplaceAll(arm.name, " ", "-")
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mk := func() *Streamer {
		return &Streamer{Source: srcEng, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn, StreamID: streamID, Filter: filter}
	}
	tgtCell := twfbCell{tgt: twfbDB{engine: arm.tgtEngine, dsn: arm.tgt.dsn}, streamID: streamID}
	if arm.tgtEngine != "postgres" {
		tgtCell.tgt.engine = "mysql"
	}
	waitUser := func(id int64, run *twfbRun) {
		t.Helper()
		deadline := time.Now().Add(120 * time.Second)
		for time.Now().Before(deadline) {
			db, err := sql.Open(arm.tgt.driver, arm.tgt.dsn)
			if err != nil {
				t.Fatal(err)
			}
			var n int
			q := fmt.Sprintf("SELECT COUNT(*) FROM users WHERE id = %d", id)
			err = db.QueryRow(q).Scan(&n)
			_ = db.Close()
			if err == nil && n == 1 {
				return
			}
			if run.exited() {
				t.Fatalf("the stream exited: %v", run.err)
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("users %d never reached the target", id)
	}

	run := startTWFBRun(mk())
	defer func() { _ = run.stop(t) }()
	tgtCell.waitStreaming(t, run)
	arm.src.exec(t, "INSERT INTO users VALUES (2, 'u2')")
	waitUser(2, run)

	if arm.noDrain {
		// The pin: the same add that lost every in-window change now refuses
		// before it opens a snapshot or records anything, and the running
		// stream is unaffected.
		src := &ndwSource{Engine: srcEng.(mysql.Engine)}
		snapshotOpened := false
		src.onSnapshot = func() { snapshotOpened = true }
		addErr := (&AddTable{
			Source: src, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
			StreamID: streamID, TableName: "orders", LiveMode: true,
		}).Run(context.Background())
		if addErr == nil {
			t.Fatalf("[%s] --no-drain on a %s binlog source succeeded; want the %s refusal", arm.name, arm.srcEngine, noDrainBinlogSourceMarker)
		}
		for _, want := range []string{noDrainBinlogSourceMarker, "sluice sync stop --wait", "without --no-drain", "Keep the stream stopped"} {
			if !strings.Contains(addErr.Error(), want) {
				t.Errorf("[%s] refusal = %v; want it to contain %q", arm.name, addErr, want)
			}
		}
		if rows := arm.tgt.rows(t); rows != nil {
			t.Errorf("[%s] the refused add left orders on the target (%d rows)", arm.name, len(rows))
		}
		if snapshotOpened {
			t.Errorf("[%s] the refused add opened a snapshot", arm.name)
		}
		ndwAssertNoAddRecorded(t, tgtEng, arm.tgt.dsn, streamID)
		arm.src.exec(t, "INSERT INTO users VALUES (3, 'u3')")
		waitUser(3, run)
		return
	}

	// The drained control: phases A and B during the add's copy, C while
	// the stream is still stopped after it, D after the restart.
	phaseA := &ndwPhase{name: "A", deletes: ndwRange(1, 20), updates: ndwRange(101, 110), inserts: ndwRange(100001, 100010)}
	phaseB := &ndwPhase{
		name: "B",
		// 21..30 were already copied when B runs; 15001..15020 were not yet read.
		deletes: append(ndwRange(21, 30), ndwRange(15001, 15020)...),
		updates: append(ndwRange(121, 125), ndwRange(15101, 15105)...),
		inserts: ndwRange(100011, 100020),
	}
	phaseC := &ndwPhase{name: "C", deletes: ndwRange(31, 40), updates: ndwRange(131, 140), inserts: ndwRange(100021, 100030)}
	phaseD := &ndwPhase{name: "D", deletes: ndwRange(41, 45), updates: ndwRange(141, 145), inserts: ndwRange(100031, 100035)}

	ndwStopWait(t, tgtEng, arm.tgt.dsn, streamID, run)

	src := &ndwSource{Engine: srcEng.(mysql.Engine)}
	tgtAtMidCopy := -1
	src.onSnapshot = func() { phaseA.apply(t, arm.src) }
	src.onMidCopy = func() {
		// The target holds only part of the table here: the copy is in
		// progress (counted after A, the copy writes in batches).
		tgtAtMidCopy = arm.tgt.count(t)
		phaseB.apply(t, arm.src)
	}
	logOffset := len(logs.String())
	if err := (&AddTable{
		Source: src, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
		StreamID: streamID, TableName: "orders",
	}).Run(context.Background()); err != nil {
		t.Fatalf("[%s] drained AddTable: %v", arm.name, err)
	}
	if phaseA.at.IsZero() || phaseB.at.IsZero() {
		t.Fatalf("RIG [%s]: the hooks did not run (A=%v B=%v)", arm.name, phaseA.at, phaseB.at)
	}
	if tgtAtMidCopy < 0 || tgtAtMidCopy >= ndwSeedRows {
		t.Errorf("RIG [%s]: the target held %d orders rows when phase B ran; want a partial copy (< %d)", arm.name, tgtAtMidCopy, ndwSeedRows)
	}
	phaseC.apply(t, arm.src)
	run = startTWFBRun(mk())

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
		t.Fatalf("RIG [%s]: the post-restart marker never reached the target", arm.name)
	}
	if want := ndwSeedRows - 20 - 30 - 10 - 5 + 10 + 10 + 10 + 5 + 1; len(srcRows) != want {
		t.Fatalf("RIG [%s]: source holds %d orders rows; want %d", arm.name, len(srcRows), want)
	}
	// The log capture is live (the add's own INFO line is in it), so the
	// WARN count below is evidence rather than a dead sink.
	if !strings.Contains(logs.String()[logOffset:], "add-table: snapshot captured") {
		t.Errorf("RIG [%s]: the log capture does not hold the add's own INFO line", arm.name)
	}
	for _, p := range []*ndwPhase{phaseA, phaseB, phaseC, phaseD} {
		r := p.judge(tgtRows)
		t.Logf("phase %s: deletes %d -> still on target %d | updates %d -> old value %d | inserts %d -> missing %d",
			p.name, len(p.deletes), r.staleDeletes, len(p.updates), r.staleUpdates, len(p.inserts), r.missingInserts)
	}
	stale, changed, missing := ndwDiff(srcRows, tgtRows)
	t.Logf("whole table: %d deleted rows still on target, %d rows with old values, %d rows missing (source %d, target %d); %d WARN/ERROR lines",
		stale, changed, missing, len(srcRows), len(tgtRows), len(ndwWarnLines(logs.String(), logOffset)))
	if stale+changed+missing > 0 {
		t.Errorf("CONTROL [%s]: drained add against a stopped stream is not exact: %d stale, %d changed, %d missing", arm.name, stale, changed, missing)
	}
}

// TestAddTableNoDrainWindow_Binlog_RecopyRemedy measures the remedy the
// CHANGELOG gives an operator whose table an earlier --no-drain add left
// diverged: stop the stream, empty the target table, add it again drained,
// restart. The damage is planted directly on the target in the shape the
// live path left (deleted rows kept, updates at the old value, inserts
// missing), so the pin does not depend on the refused path; the verdict
// compares the source and the target by id and payload.
func TestAddTableNoDrainWindow_Binlog_RecopyRemedy(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	captureSlog(t)
	src, tgt := ndwDB{"mysql", srcDSN}, ndwDB{"mysql", tgtDSN}
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src.exec(t,
		"CREATE TABLE users (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"INSERT INTO users VALUES (1, 'u1')",
		"CREATE TABLE orders (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"INSERT INTO orders VALUES "+ndwValues(1, 2000, "v1"))
	const streamID = "ndw-recopy-remedy"
	filter := mustNewFilter(t, []string{"users"}, nil)
	mk := func() *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter}
	}
	cell := twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}
	run := startTWFBRun(mk())
	defer func() { _ = run.stop(t) }()
	cell.waitStreaming(t, run)
	// stopAddRestart is the drained flow: stop, add, restart.
	stopAddRestart := func(beforeAdd func()) {
		t.Helper()
		ndwStopWait(t, eng, tgtDSN, streamID, run)
		if beforeAdd != nil {
			beforeAdd()
		}
		if err := (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).Run(context.Background()); err != nil {
			t.Fatalf("drained AddTable: %v", err)
		}
		run = startTWFBRun(mk())
	}
	stopAddRestart(nil)
	if n := tgt.count(t); n != 2000 {
		t.Fatalf("RIG: the first add copied %d rows; want 2000", n)
	}

	// The damage the lost window left, planted on the target alone (no
	// source change touches these rows, so the running stream leaves them):
	// rows the source deleted still present, updates at the old value,
	// inserts missing.
	tgt.exec(t, "INSERT INTO orders VALUES "+ndwValues(50001, 50020, "deleted-on-source"),
		"UPDATE orders SET payload = 'stale' WHERE id BETWEEN 101 AND 110",
		"DELETE FROM orders WHERE id BETWEEN 1500 AND 1509")
	stale, changed, missing := ndwDiff(src.rows(t), tgt.rows(t))
	if stale != 20 || changed != 10 || missing != 10 {
		t.Fatalf("RIG: planted damage reads %d stale, %d changed, %d missing; want 20/10/10", stale, changed, missing)
	}
	t.Logf("planted: %d deleted rows still on target, %d rows with old values, %d rows missing; row counts source %d target %d",
		stale, changed, missing, src.count(t), tgt.count(t))

	// The remedy: stop, empty the target table, add it again drained, restart.
	stopAddRestart(func() { tgt.exec(t, "DELETE FROM orders") })
	src.exec(t, "DELETE FROM orders WHERE id = 1000", "INSERT INTO orders VALUES (9000001, 'marker')")
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := tgt.rows(t)[9000001]; ok {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, ok := tgt.rows(t)[9000001]; !ok {
		t.Fatal("RIG: the post-remedy marker never reached the target")
	}
	stale, changed, missing = ndwDiff(src.rows(t), tgt.rows(t))
	t.Logf("after the re-copy: %d deleted rows still on target, %d rows with old values, %d rows missing", stale, changed, missing)
	if stale+changed+missing > 0 {
		t.Errorf("the re-copy remedy left the table diverged: %d stale, %d changed, %d missing", stale, changed, missing)
	}
}

// ndwValues renders rows lo..hi with one payload as an INSERT VALUES list.
func ndwValues(lo, hi int64, payload string) string {
	parts := make([]string, 0, hi-lo+1)
	for id := lo; id <= hi; id++ {
		parts = append(parts, fmt.Sprintf("(%d,'%s')", id, payload))
	}
	return strings.Join(parts, ",")
}

func TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_Live(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-mysql-live", srcEngine: "mysql", tgtEngine: "mysql",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, noDrain: true,
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_DrainedStoppedControl(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-mysql-drained-stopped", srcEngine: "mysql", tgtEngine: "mysql",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN},
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToPostgres_Live(t *testing.T) {
	srcDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-postgres-live", srcEngine: "mysql", tgtEngine: "postgres",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"pgx", pgDSN}, noDrain: true,
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToPostgres_DrainedStoppedControl(t *testing.T) {
	srcDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-postgres-drained-stopped", srcEngine: "mysql", tgtEngine: "postgres",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"pgx", pgDSN},
	})
}

func TestAddTableNoDrainWindow_Binlog_MariaDBToMariaDB_Live(t *testing.T) {
	srcDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	ndwDB{"mysql", srcDSN}.exec(t, "CREATE DATABASE target_db")
	tgtDSN := strings.Replace(srcDSN, "/source_db?", "/target_db?", 1)
	runNoDrainArm(t, ndwArm{
		name: "mariadb-mariadb-live", srcEngine: "mariadb", tgtEngine: "mariadb",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, noDrain: true,
	})
}
