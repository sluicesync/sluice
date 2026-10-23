//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// BINLOG-NO-DRAIN-WINDOW-UNMEASURED: what `schema add-table --no-drain`
// (AddTable.LiveMode, ADR-0034's filter-flip) leaves on the target when the
// source is a MySQL-family binlog and the new table changes while the add
// runs.
//
// ADR-0034 documents a "best-effort caveat for writes landing during the
// streamer's poll lag". The suspicion this test measures is that the window
// is not the poll lag but everything from the add's snapshot to the poll
// tick that admits the table: the running stream's dispatch filter (and the
// binlog reader's scope predicate) drop every change to the table until
// then, while the copy reads a snapshot fixed before those changes.
//
// Each arm seeds `orders` with ndwSeedRows rows and changes it in three
// phases, every change recorded with its own wall-clock time:
//
//	A  right after the add's snapshot opens, before its copy reads a row;
//	B  mid-copy, after ndwMidCopyAfter rows have been handed to the writer
//	   (deletes both of rows already copied and of rows not yet read);
//	C  after AddTable.Run returns and before the stream admits the table
//	   (live arms; on the drained control, while the stream is stopped).
//
// Then a change after admission (D) and a marker row, and the target is
// compared with the source by id and payload, per phase.
//
// Arms:
//
//	live MySQL→MySQL     the real --no-drain path against a running stream
//	live MariaDB→MariaDB same, MariaDB flavor
//	live MySQL→Postgres  --no-drain must refuse (the filter-flip path is
//	                     MySQL-target-only); measured as a refusal
//	drained-stopped MySQL→MySQL and MySQL→Postgres: the control — the
//	stream stopped with `sync stop --wait`'s protocol, a drained add, the
//	stream restarted; must be exact.
//
// The live arms REPORT (t.Logf + a summary line) rather than assert
// exactness, because they are a measurement of a suspected defect; the
// control arms assert exactness. When the defect is fixed, flip the live
// arms' report into the same exactness assertion — the per-phase table is
// already the pin's shape.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
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

// ndwArm is one measured configuration.
type ndwArm struct {
	name        string
	srcEngine   string
	tgtEngine   string
	src, tgt    ndwDB
	live        bool // --no-drain against the running stream; else drained against a stopped one
	wantRefusal bool // --no-drain on a pair the filter-flip path refuses
	// pollInterval, when set, overrides the stream's control-row poll (the
	// live-added poll rides it): a short poll separates the copy-length
	// part of the window from the poll-lag part.
	pollInterval time.Duration
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
	if arm.pollInterval > 0 {
		setPollIntervalForTest(t, arm.pollInterval)
	}
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

	streamer := mk()
	run := startTWFBRun(streamer)
	defer func() { _ = run.stop(t) }()
	tgtCell.waitStreaming(t, run)
	arm.src.exec(t, "INSERT INTO users VALUES (2, 'u2')")
	waitUser(2, run)

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

	if !arm.live {
		ndwStopWait(t, tgtEng, arm.tgt.dsn, streamID, run)
	}

	src := &ndwSource{Engine: srcEng.(mysql.Engine)}
	var (
		snapshotAt   time.Time
		tgtAtMidCopy = -1
	)
	src.onSnapshot = func() {
		snapshotAt = time.Now()
		phaseA.apply(t, arm.src)
	}
	src.onMidCopy = func() {
		// The target holds only part of the table here: the copy is in
		// progress (counted after A, the copy writes in batches).
		tgtAtMidCopy = arm.tgt.count(t)
		phaseB.apply(t, arm.src)
	}

	// Admission watcher: the first moment the running streamer's live-added
	// filter contains `orders`.
	var admittedAt atomic.Pointer[time.Time]
	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()
	if arm.live {
		go func() {
			for watchCtx.Err() == nil {
				if f := streamer.liveFilterRef.Load(); f != nil && f.Contains("orders") {
					now := time.Now()
					admittedAt.Store(&now)
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	logOffset := len(logs.String())
	addStart := time.Now()
	addErr := (&AddTable{
		Source: src, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
		StreamID: streamID, TableName: "orders", LiveMode: arm.live,
	}).Run(context.Background())
	addEnd := time.Now()

	if arm.wantRefusal {
		if addErr == nil {
			t.Fatalf("[%s] --no-drain on a %s→%s pair succeeded; the filter-flip path is MySQL-target-only and must refuse", arm.name, arm.srcEngine, arm.tgtEngine)
		}
		t.Logf("MEASURED [%s]: --no-drain refused loudly (non-zero exit): %v", arm.name, addErr)
		if rows := arm.tgt.rows(t); rows != nil {
			t.Errorf("[%s] the refused add left orders on the target (%d rows)", arm.name, len(rows))
		}
		if !snapshotAt.IsZero() {
			t.Errorf("[%s] the refused add opened a snapshot", arm.name)
		}
		return
	}
	if addErr != nil {
		t.Fatalf("[%s] AddTable (live=%v): %v", arm.name, arm.live, addErr)
	}
	if snapshotAt.IsZero() || phaseB.at.IsZero() {
		t.Fatalf("RIG [%s]: the hooks did not run (snapshot=%v midcopy=%v)", arm.name, snapshotAt, phaseB.at)
	}
	if tgtAtMidCopy < 0 || tgtAtMidCopy >= ndwSeedRows {
		t.Errorf("RIG [%s]: the target held %d orders rows when phase B ran; want a partial copy (< %d)", arm.name, tgtAtMidCopy, ndwSeedRows)
	}

	// Phase C: after the add returns, before the stream admits the table.
	admittedBeforeC := admittedAt.Load() != nil
	phaseC.apply(t, arm.src)
	if arm.live {
		phaseC.note = fmt.Sprintf("stream admitted orders before C began: %v", admittedBeforeC)
		if a := admittedAt.Load(); a != nil && a.Before(phaseC.at) {
			phaseC.note += fmt.Sprintf("; admitted %s before C committed", phaseC.at.Sub(*a))
		}
	} else {
		phaseC.note = "stream stopped"
	}

	if arm.live {
		deadline := time.Now().Add(60 * time.Second)
		for admittedAt.Load() == nil && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		if admittedAt.Load() == nil {
			t.Fatalf("RIG [%s]: the running stream never admitted orders into its live-added filter", arm.name)
		}
	} else {
		run = startTWFBRun(mk())
		streamer = nil
	}

	// D: after admission, then a marker; wait for the marker and a users row
	// that follows it, so the stream has applied past everything above.
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
		t.Fatalf("RIG [%s]: the post-admission marker never reached the target", arm.name)
	}
	if want := ndwSeedRows - 20 - 30 - 10 - 5 + 10 + 10 + 10 + 5 + 1; len(srcRows) != want {
		t.Fatalf("RIG [%s]: source holds %d orders rows; want %d", arm.name, len(srcRows), want)
	}
	streamAlive := !run.exited()

	// verify --depth count over orders.
	vres, verr := (&Verifier{
		Source: srcEng, Target: tgtEng, SourceDSN: arm.src.dsn, TargetDSN: arm.tgt.dsn,
		Depth: VerifyDepthCount, Filter: mustNDWFilter(t, "orders"), Out: io.Discard,
	}).Run(context.Background())
	verifyVerdict := "error: " + fmt.Sprint(verr)
	if verr == nil {
		var parts []string
		for _, tr := range vres.Tables {
			parts = append(parts, fmt.Sprintf("%s=%s(src %d, tgt %d)", tr.Name, tr.Status, tr.SourceRowCount, tr.TargetRowCount))
		}
		verifyVerdict = strings.Join(parts, " ")
	}

	stale, changed, missing := ndwDiff(srcRows, tgtRows)
	warns := ndwWarnLines(logs.String(), logOffset)
	// The capture is live (so "0 WARN lines" is evidence, not a dead sink):
	// the add's own INFO line is in it.
	if !strings.Contains(logs.String()[logOffset:], "add-table: snapshot captured") {
		t.Errorf("RIG [%s]: the log capture does not hold the add's own INFO line; the WARN count is unproven", arm.name)
	}

	t.Logf("=== [%s] live=%v src=%s tgt=%s ===", arm.name, arm.live, arm.srcEngine, arm.tgtEngine)
	t.Logf("timeline: add start %s | snapshot +%s | phase A +%s | phase B +%s (target held %d/%d rows) | add returned +%s | phase C +%s",
		addStart.Format("15:04:05.000"), snapshotAt.Sub(addStart), phaseA.at.Sub(addStart), phaseB.at.Sub(addStart),
		tgtAtMidCopy, ndwSeedRows, addEnd.Sub(addStart), phaseC.at.Sub(addStart))
	if a := admittedAt.Load(); a != nil {
		t.Logf("admission: stream's live-added filter contained orders at +%s (%s after the add returned; poll interval %s)",
			a.Sub(addStart), a.Sub(addEnd), loadPollIntervalForTest())
	}
	total := ndwPhaseResult{}
	for _, p := range []*ndwPhase{phaseA, phaseB, phaseC, phaseD} {
		r := p.judge(tgtRows)
		total.staleDeletes += r.staleDeletes
		total.staleUpdates += r.staleUpdates
		total.missingInserts += r.missingInserts
		t.Logf("phase %s @+%s: deletes %d -> still on target %d | updates %d -> old value %d | inserts %d -> missing %d %s",
			p.name, p.at.Sub(addStart), len(p.deletes), r.staleDeletes, len(p.updates), r.staleUpdates, len(p.inserts), r.missingInserts, p.note)
	}
	t.Logf("whole table: %d deleted rows still on target, %d rows with old values, %d rows missing (source %d, target %d)",
		stale, changed, missing, len(srcRows), len(tgtRows))
	t.Logf("verify --depth count: %s", verifyVerdict)
	t.Logf("stream still running at the end: %v; WARN/ERROR lines logged since the add began: %d", streamAlive, len(warns))
	sort.Strings(warns)
	for i, w := range warns {
		if i >= 15 {
			t.Logf("  ... %d more", len(warns)-i)
			break
		}
		t.Logf("  %s", w)
	}

	if !arm.live {
		// The control: exact.
		if stale+changed+missing > 0 {
			t.Errorf("CONTROL [%s]: drained add against a stopped stream is not exact: %d stale, %d changed, %d missing", arm.name, stale, changed, missing)
		}
		return
	}
	if r := phaseD.judge(tgtRows); r.staleDeletes+r.staleUpdates+r.missingInserts > 0 {
		t.Errorf("[%s] changes made AFTER the stream admitted orders did not all apply: %+v", arm.name, r)
	}
	t.Logf("MEASURED [%s]: add exit=nil, stream running=%v, %d WARN/ERROR lines; lost: %d stale deletes, %d stale updates, %d missing inserts",
		arm.name, streamAlive, len(warns), total.staleDeletes, total.staleUpdates, total.missingInserts)
}

func mustNDWFilter(t *testing.T, tables ...string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(tables, nil)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_Live(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-mysql-live", srcEngine: "mysql", tgtEngine: "mysql",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, live: true,
	})
}

// The same arm with a 200ms poll: if phases A and B are still lost, the
// window is the copy, not the poll lag.
func TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_LiveShortPoll(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-mysql-live-200ms-poll", srcEngine: "mysql", tgtEngine: "mysql",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, live: true, pollInterval: 200 * time.Millisecond,
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_DrainedStoppedControl(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-mysql-drained-stopped", srcEngine: "mysql", tgtEngine: "mysql",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, live: false,
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToPostgres_Live(t *testing.T) {
	srcDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-postgres-live", srcEngine: "mysql", tgtEngine: "postgres",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"pgx", pgDSN}, live: true, wantRefusal: true,
	})
}

func TestAddTableNoDrainWindow_Binlog_MySQLToPostgres_DrainedStoppedControl(t *testing.T) {
	srcDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runNoDrainArm(t, ndwArm{
		name: "mysql-postgres-drained-stopped", srcEngine: "mysql", tgtEngine: "postgres",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"pgx", pgDSN}, live: false,
	})
}

func TestAddTableNoDrainWindow_Binlog_MariaDBToMariaDB_Live(t *testing.T) {
	srcDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	ndwDB{"mysql", srcDSN}.exec(t, "CREATE DATABASE target_db")
	tgtDSN := strings.Replace(srcDSN, "/source_db?", "/target_db?", 1)
	runNoDrainArm(t, ndwArm{
		name: "mariadb-mariadb-live", srcEngine: "mariadb", tgtEngine: "mariadb",
		src: ndwDB{"mysql", srcDSN}, tgt: ndwDB{"mysql", tgtDSN}, live: true,
	})
}
