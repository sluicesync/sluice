//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED on a MySQL binlog source writing
// to a MySQL target (the VStream twin is the DrainedRunning cell of
// TestStreamer_VStreamAddTableKeyspaceWideCopy, which runs only under the
// vstream tag and in no CI job; this one runs in the pipeline-rest-streamer
// shard).
//
// One stream, scoped to `users`, and a table `orders` brought in with a
// drained `schema add-table`:
//
//  1. Against the RUNNING stream the add refuses
//     (DRAINED-ADD-TABLE-STREAM-NOT-STOPPED) before anything is written: no
//     target table, no pending or live-added record. Its snapshot hook would
//     have deleted and updated orders rows right after the snapshot opened.
//  2. `sync stop --wait`'s protocol stops the stream, and the add runs for
//     real. At its snapshot the hook deletes 20 rows and updates 10 on the
//     source, then restarts the stream while the add is still copying (a
//     supervisor restart, a fleet reload): the restart must defer with
//     INCLUDE-TABLE-ADD-IN-PROGRESS, because `orders` joins the stream's
//     scope only when the add finishes.
//  3. The stream is started again after the add, and `orders` on the target
//     must equal the source exactly: the restart replays the add's window
//     from the stream's pre-add position.
//
// When the refusal in (1) does not fire (the mutation run that measured the
// binlog arm of the defect), the test reports what the add left instead of
// stopping, and so measures the loss.

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

// drainHBSource is the mysql engine with a hook that runs once the add's
// snapshot is open — its consistent view fixed, its copy not yet started.
// Embedding the concrete engine keeps every optional surface AddTable asserts.
type drainHBSource struct {
	mysql.Engine
	onSnapshot func()
}

func (e *drainHBSource) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	stream, err := e.Engine.OpenSnapshotStream(ctx, dsn)
	if err == nil && e.onSnapshot != nil {
		e.onSnapshot()
	}
	return stream, err
}

// syncStopWait is `sluice sync stop --wait` against the stream: request the
// stop on its control row, wait until the drained stream clears it, then
// collect the Run, which has returned on its own.
func syncStopWait(t *testing.T, tgtEng ir.Engine, tgtDSN, streamID string, stream *twfbRun) {
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
	if err := stream.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: the stopped stream's Run: %v", err)
	}
}

// drainHBRows reads every (id, payload) of table; nil when it is absent.
func drainHBRows(t *testing.T, dsn, table string) map[int64]string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query("SELECT id, payload FROM " + table)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return nil
		}
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		out[id] = payload
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// drainHBControl reads one of the stream's control-row columns, "" when NULL.
func drainHBControl(t *testing.T, dsn, streamID, column string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v sql.NullString
	if err := db.QueryRow("SELECT CAST("+column+" AS CHAR) FROM sluice_cdc_state WHERE stream_id = ?", streamID).Scan(&v); err != nil {
		t.Fatalf("read %s: %v", column, err)
	}
	return v.String
}

// drainHBWaitStamped waits until the running stream's liveness stamp is on
// its control row, so the refusal below is the running-stream one ("is
// running"), not the never-stamped one.
func drainHBWaitStamped(t *testing.T, dsn, streamID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if v := drainHBControl(t, dsn, streamID, "stream_alive_ms"); v != "" && v != "0" {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("the running stream never stamped its liveness")
}

// drainHBDiff counts what the target holds wrong against the source: rows
// the source deleted, rows whose payload differs, rows the target lacks.
func drainHBDiff(src, tgt map[int64]string) (stale, changed, missing int) {
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

func TestStreamer_AddTableDrainedAgainstRunningStream_Binlog(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO users VALUES (1, 'u1');
		CREATE TABLE orders (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;`)
	var seed strings.Builder
	seed.WriteString("INSERT INTO orders VALUES ")
	for id := 1; id <= 2000; id++ {
		if id > 1 {
			seed.WriteString(",")
		}
		fmt.Fprintf(&seed, "(%d,'v1')", id)
	}
	applyDDLMySQL(t, srcDSN, seed.String())

	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	const streamID = "drained-running"
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mk := func() *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter}
	}
	waitUser := func(id int, run *twfbRun) {
		t.Helper()
		deadline := time.Now().Add(120 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := drainHBRows(t, tgtDSN, "users")[int64(id)]; ok {
				return
			}
			if run.exited() {
				t.Fatalf("the stream exited: %v", run.err)
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("users %d never reached the target", id)
	}
	stream := startTWFBRun(mk())
	defer func() { _ = stream.stop(t) }()
	// In CDC: its cold start has persisted a position, and a change made
	// after that reaches the target.
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)
	applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (2, 'u2')")
	waitUser(2, stream)
	drainHBWaitStamped(t, tgtDSN, streamID)

	// The changes made while an add copies: 20 deletes, 10 updates.
	changeOrders := func() {
		applyDDLMySQL(t, srcDSN, "DELETE FROM orders WHERE id BETWEEN 1 AND 20; UPDATE orders SET payload = 'upd' WHERE id BETWEEN 101 AND 110;")
	}
	src := &drainHBSource{Engine: eng.(mysql.Engine)}
	add := func() error {
		return (&AddTable{Source: src, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
			Run(context.Background())
	}

	// (1) Against the running stream: refused, nothing written.
	liveBefore := drainHBControl(t, tgtDSN, streamID, "live_added_tables")
	pendingBefore := drainHBControl(t, tgtDSN, streamID, "pending_added_tables")
	src.onSnapshot = changeOrders
	err = add()
	if err == nil {
		// The mutation run's measurement: the add ran against the running
		// stream. Let the stream's poll admit the table, push a marker
		// through, and report what the target holds.
		t.Errorf("a drained add-table against a running stream succeeded; want the %s refusal", DrainedAddTableStreamNotStoppedMarker)
		time.Sleep(15 * time.Second)
		applyDDLMySQL(t, srcDSN, "INSERT INTO orders VALUES (9000001, 'marker')")
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := drainHBRows(t, tgtDSN, "orders")[9000001]; ok {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		stale, changed, missing := drainHBDiff(drainHBRows(t, srcDSN, "orders"), drainHBRows(t, tgtDSN, "orders"))
		t.Errorf("MEASURED (binlog source, add against the running stream): %d deleted rows still on the target, %d rows with old values, %d rows missing", stale, changed, missing)
		return
	}
	for _, want := range []string{DrainedAddTableStreamNotStoppedMarker, "has not recorded that it stopped", "sluice sync stop --wait --stream-id " + streamID, "must not run while the stream applies changes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal = %v; want it to contain %q", err, want)
		}
	}
	if rows := drainHBRows(t, tgtDSN, "orders"); rows != nil {
		t.Errorf("the refused add left orders on the target (%d rows)", len(rows))
	}
	if got := drainHBControl(t, tgtDSN, streamID, "live_added_tables"); got != liveBefore {
		t.Errorf("live_added_tables = %q after the refusal; want %q", got, liveBefore)
	}
	if got := drainHBControl(t, tgtDSN, streamID, "pending_added_tables"); got != pendingBefore {
		t.Errorf("pending_added_tables = %q after the refusal; want %q", got, pendingBefore)
	}

	// (2) Stopped with `sync stop --wait`, the add runs; the stream is
	// restarted while it copies and must defer.
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	var revived *twfbRun
	src.onSnapshot = func() {
		changeOrders()
		revived = startTWFBRun(mk())
		deadline := time.Now().Add(120 * time.Second)
		for !revived.exited() && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if err := add(); err != nil {
		t.Fatalf("the drained add after `sync stop --wait`: %v", err)
	}
	if !revived.exited() || !errors.Is(revived.err, ir.ErrIncludeTableAddInProgress) {
		t.Errorf("the stream restarted mid-add did not defer with INCLUDE-TABLE-ADD-IN-PROGRESS (returned=%v err=%v)", revived.exited(), revived.err)
		_ = revived.stop(t)
	}

	// (3) Started again after the add: exact.
	stream = startTWFBRun(mk())
	applyDDLMySQL(t, srcDSN, "INSERT INTO orders VALUES (9000001, 'marker'); INSERT INTO users VALUES (3, 'u3');")
	waitUser(3, stream)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := drainHBRows(t, tgtDSN, "orders")[9000001]; ok {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	srcRows, tgtRows := drainHBRows(t, srcDSN, "orders"), drainHBRows(t, tgtDSN, "orders")
	if stale, changed, missing := drainHBDiff(srcRows, tgtRows); stale+changed+missing > 0 {
		t.Errorf("orders on the target differs from the source after the drained add: %d deleted rows still present, %d old values, %d missing (source %d rows, target %d)",
			stale, changed, missing, len(srcRows), len(tgtRows))
	}
	if len(srcRows) != 2000-20+1 {
		t.Fatalf("RIG: the source holds %d orders rows; want %d", len(srcRows), 2000-20+1)
	}
}

// drainHBSetAlive overwrites the stream's liveness column on the target (nil
// = NULL), standing in for a record the stream's own stamping did not keep up.
func drainHBSetAlive(t *testing.T, dsn, streamID string, ms int64) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// The rig's source and target share one server, so a binlogged write here
	// would reach the stream as a (filtered) event, and its position write
	// would stamp the record straight back.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "SET SESSION sql_log_bin = 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "UPDATE sluice_cdc_state SET stream_alive_ms = ? WHERE stream_id = ?", ms, streamID); err != nil {
		t.Fatal(err)
	}
}

// drainHBStoppedTarget is the mysql engine whose change applier answers its
// first stoppedReads liveness reads with a recorded stop, then reads the real
// record. Embedding the concrete applier keeps every surface AddTable asserts.
type drainHBStoppedTarget struct {
	mysql.Engine
	stoppedReads int
}

type drainHBStoppedApplier struct {
	*mysql.ChangeApplier
	left *int
}

func (e *drainHBStoppedTarget) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	a, err := e.Engine.OpenChangeApplier(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return drainHBStoppedApplier{ChangeApplier: a.(*mysql.ChangeApplier), left: &e.stoppedReads}, nil
}

func (a drainHBStoppedApplier) ReadStreamLiveness(ctx context.Context, streamID string) (ir.StreamLiveness, error) {
	if *a.left > 0 {
		*a.left--
		return ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: time.Now()}, nil
	}
	return a.ChangeApplier.ReadStreamLiveness(ctx, streamID)
}

// drainHBWaitUser waits until users row id is on the target.
func drainHBWaitUser(t *testing.T, tgtDSN string, id int, stream *twfbRun) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := drainHBRows(t, tgtDSN, "users")[int64(id)]; ok {
			return
		}
		if stream != nil && stream.exited() {
			t.Fatalf("the stream exited: %v", stream.err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("users %d never reached the target", id)
}

// drainHBRig starts a binlog source and target holding users and orders and
// returns what a cell needs.
func drainHBRig(t *testing.T) (srcDSN, tgtDSN string, eng ir.Engine, filter migcore.TableFilter, cleanup func()) {
	t.Helper()
	srcDSN, tgtDSN, cleanup = startMySQLBinlog(t)
	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO users VALUES (1, 'u1');
		CREATE TABLE orders (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO orders VALUES (1, 'v1'), (2, 'v1'), (3, 'v1');`)
	var ok bool
	if eng, ok = engines.Get("mysql"); !ok {
		t.Fatal("mysql engine not registered")
	}
	f, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return srcDSN, tgtDSN, eng, f, cleanup
}

// TestStreamer_AddTableDrainedLivenessFromPositionWrites_Binlog: every
// position write stamps the record, and the add's last check, after its copy,
// reads both the record and the stream's position.
//
//  1. The record is overwritten with the stopped marker (as if nothing had
//     stamped since a previous stop), then the stream applies one change: its
//     position write stamps the record, and the drained add refuses with
//     nothing written. The stream cannot record its stop on exit in this
//     cell (a liveness store that always fails), which changes nothing while
//     it runs.
//  2. The add's two checks before its copy read a recorded stop (forced: the
//     stream is in fact running); during the copy the stream applies a
//     change, and the add refuses before handing the table to the stream.
//  3. The stream is stopped with `sync stop --wait`. During the add's copy a
//     stream applies and stops cleanly (simulated through the target's own
//     applier: a position write, then the stopped marker), so the record
//     says stopped at the end; the moved position refuses.
func TestStreamer_AddTableDrainedLivenessFromPositionWrites_Binlog(t *testing.T) {
	srcDSN, tgtDSN, eng, filter, cleanup := drainHBRig(t)
	defer cleanup()
	const streamID = "liveness-from-positions"
	var opens atomic.Int32
	stream := startTWFBRun(&Streamer{
		Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter,
		livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
			opens.Add(1)
			return nil, func() {}, errors.New("liveness store unavailable (test)")
		},
	})
	defer func() { _ = stream.stop(t) }()
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)

	// (1) Stopped marker, then one applied change: the position write
	// stamps, and the add refuses.
	drainHBSetAlive(t, tgtDSN, streamID, 0)
	applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (2, 'u2')")
	drainHBWaitUser(t, tgtDSN, 2, stream)
	err := (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
		Run(context.Background())
	if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "has not recorded that it stopped") {
		t.Fatalf("a drained add against an applying stream over a stopped marker: %v; want the %s refusal",
			err, DrainedAddTableStreamNotStoppedMarker)
	}
	if rows := drainHBRows(t, tgtDSN, "orders"); rows != nil {
		t.Errorf("the refused add left orders on the target (%d rows)", len(rows))
	}

	// (2) A forced recorded stop lets the add start; the stream applies
	// during the copy, and the add refuses before recording the table.
	src := &drainHBSource{Engine: eng.(mysql.Engine), onSnapshot: func() {
		applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (3, 'u3')")
		drainHBWaitUser(t, tgtDSN, 3, stream)
	}}
	tgt := &drainHBStoppedTarget{Engine: eng.(mysql.Engine), stoppedReads: 2}
	err = (&AddTable{Source: src, Target: tgt, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
		Run(context.Background())
	if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "NOT handed to the stream") {
		t.Fatalf("a drained add during which the stream applied: %v; want the %s refusal before the table is recorded",
			err, DrainedAddTableStreamNotStoppedMarker)
	}
	if got := drainHBControl(t, tgtDSN, streamID, "live_added_tables"); strings.Contains(got, "orders") {
		t.Errorf("live_added_tables = %q after the refusal; the table was handed to the stream", got)
	}

	// (3) Stopped properly; a stream applies and stops cleanly during the
	// copy. The record reads stopped at the end; the position moved.
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("RIG: after `sync stop --wait` the record is %q; want the stopped marker", got)
	}
	applyDDLMySQL(t, tgtDSN, "DROP TABLE IF EXISTS orders")
	src.onSnapshot = func() {
		ctx := context.Background()
		applier, err := eng.OpenChangeApplier(ctx, tgtDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = applier.(io.Closer).Close() }()
		if err := applier.EnsureControlTable(ctx); err != nil {
			t.Fatal(err)
		}
		if err := applier.(ir.PositionWriter).WritePosition(ctx, streamID, ir.Position{Engine: "mysql", Token: "advanced-during-the-add"}); err != nil {
			t.Fatal(err)
		}
		if err := applier.(ir.StreamLivenessStore).RecordStreamStopped(ctx, streamID); err != nil {
			t.Fatal(err)
		}
	}
	err = (&AddTable{Source: src, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
		Run(context.Background())
	if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "position moved during the add") {
		t.Fatalf("a drained add during which a stream applied and stopped cleanly: %v; want the %s refusal on the moved position",
			err, DrainedAddTableStreamNotStoppedMarker)
	}
}

// TestStreamer_AddTableDrainedCrashRemedy_Binlog: a stream that crashed
// records no stop (simulated: a running stamp an hour old over a stopped
// stream), and the drained add refuses it however old the stamp; the remedy
// it names records the stop. Both halves of the remedy are measured: a start
// that fails after reaching the target records the stop as it exits, and so
// does a start followed by `sync stop --wait`, after which the add passes.
func TestStreamer_AddTableDrainedCrashRemedy_Binlog(t *testing.T) {
	srcDSN, tgtDSN, eng, filter, cleanup := drainHBRig(t)
	defer cleanup()
	const streamID = "crash-remedy"
	mk := func(src string) *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: src, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter}
	}
	stream := startTWFBRun(mk(srcDSN))
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	crash := func() { drainHBSetAlive(t, tgtDSN, streamID, time.Now().Add(-time.Hour).UnixMilli()) }
	add := func() error {
		return (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
			Run(context.Background())
	}

	crash()
	err := add()
	if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "has not recorded that it stopped") ||
		!strings.Contains(err.Error(), "`sluice sync start` with the same --stream-id") {
		t.Fatalf("a drained add against a stream whose last stamp is an hour old: %v; want the %s refusal naming the start-then-stop remedy",
			err, DrainedAddTableStreamNotStoppedMarker)
	}
	if rows := drainHBRows(t, tgtDSN, "orders"); rows != nil {
		t.Errorf("the refused add left orders on the target (%d rows)", len(rows))
	}

	// A start that reaches the target and then fails (its source refuses the
	// connection) records the stop as it exits.
	refused := mk("root:rootpw@tcp(127.0.0.1:1)/source_db?timeout=5s")
	if err := refused.Run(context.Background()); err == nil {
		t.Fatal("RIG: a start against an unreachable source succeeded")
	}
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("after a start that failed past the target, the record is %q; want the stopped marker", got)
	}

	// The remedy as the refusal states it: start, then `sync stop --wait`.
	crash()
	stream = startTWFBRun(mk(srcDSN))
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreamingPast(t, stream, "")
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("after start then `sync stop --wait`, the record is %q; want the stopped marker", got)
	}
	if err := add(); err != nil {
		t.Fatalf("the drained add after the remedy: %v", err)
	}
}
