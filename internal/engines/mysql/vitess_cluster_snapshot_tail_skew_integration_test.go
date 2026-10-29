//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The snapshot stream's CDC tail must not stall behind vtgate's skew hold
// after a skewed multi-shard cold start (Bug 334, VSTREAM-TAIL-SKEW-HOLD).
//
// THE SHAPE: a four-shard keyspace where two shards (-40, 40-80) hold a
// few hundred rows and two (80-c0, c0-) hold tens of thousands, copied
// through a throttled proxy. The small shards finish their copy early and
// take changes while the big ones are still copying, -40 first and 40-80
// after a gap. Each small shard's tail position is the GTID it finished its
// copy at (Bug 331's shardCopyBoundary), so the CDC tail opens with two
// shards replaying windows seconds old, and seconds apart, while the other
// two start at "now".
//
// THE HARM (through v0.163.10): that tail asked vtgate for
// MinimizeSkew=true. vtgate named the oldest rewound shard the laggard and
// paused every other shard on its next event (vendored vitess
// vstream_manager.go mustPause compares against lowestTS, which nothing
// assigns, so every non-laggard pauses). The second rewound shard paused on
// its first replayed event, freezing its clock seconds in the past, while
// the two big shards' clocks sat at "now". The hold clears only when every
// clock is within 2s (MaxSkew) of the others, which a frozen old clock
// never is, so three of four shards delivered nothing until vtgate's
// 10-minute skew timeout, and then re-held. Zero rows lost (a restart
// converged exactly) but the stream sat "alive" with a stale target and no
// per-shard WARN. Measured with this test against v0.163.10's tail:
// -40 4/4 markers, the other shards 0/4, vtgate logging "Skew found,
// laggard is commerce/-40" for the tail it opened with minimize_skew=true.
//
// THE ORACLE: one INSERT on EVERY shard every few seconds after the copy;
// every one must reach the stream within 30s of its commit, and the
// stream-applied target must then equal the source exactly (read back
// through vtgate, never from sluice's state). The rig proves its own shape
// first ([tskWindow.requireShape]): both small shards finished and took
// their windows while the copy still had seconds to run, so a run where the
// shape did not form fails as a RIG error instead of passing vacuously.
//
// Usage (Windows; see CLAUDE.md / docs/dev/development.md):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vitessreshard' -v -count=1 -timeout=30m \
//	  -run 'TestVitessReshard_SnapshotTailSkew' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

const (
	tskTable = "tskew"

	// tskMarkerBase is the first marker id. Markers are inserted through
	// shard-targeted sessions, so each lands on the shard it names
	// whatever the hash vindex would pick; the seed ids stay far below.
	tskMarkerBase = int64(50_000_000)

	// tskArrivalBudget is the per-marker deadline: a marker committed on
	// any shard must reach the stream within this long.
	tskArrivalBudget = 30 * time.Second
)

// tskShards is the four-shard layout; the first two are the small,
// early-finishing shards.
var tskShards = []string{"-40", "40-80", "80-c0", "c0-"}

// TestVitessReshard_SnapshotTailSkew_EveryShardDelivers is Bug 334's
// real-cluster pin: a skewed four-shard cold start whose CDC tail must keep
// delivering every shard's changes.
func TestVitessReshard_SnapshotTailSkew_EveryShardDelivers(t *testing.T) {
	c := startVitessReshardCluster(t, strings.Join(tskShards, " "))
	defer c.terminate()

	if got := vrShowShards(t, c.mysqlDSN); len(got) != len(tskShards) {
		t.Fatalf("keyspace has shards %v; want %v — the four-shard bring-up did not take", got, tskShards)
	}

	// A keyspace booted with several shards has no vschema yet, and vtgate
	// refuses DDL on an unsharded-looking keyspace with more than one shard,
	// so mark it sharded (with the table's hash vindex) before the DDL.
	if out, err := c.vtctldExec(t, "ApplyVSchema", "--vschema",
		`{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{"`+tskTable+`":{"column_vindexes":[{"column":"id","name":"hash"}]}}}`,
		vrKeyspace); err != nil {
		t.Fatalf("ApplyVSchema: %v\n%s", err, out)
	}
	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE `+tskTable+` (
		id  BIGINT       NOT NULL,
		v   BIGINT       NOT NULL,
		pad VARCHAR(200) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	time.Sleep(3 * time.Second) // schema-tracker settle

	shardDB := tskOpenShardDBs(t, c)
	defer func() {
		for _, db := range shardDB {
			_ = db.Close()
		}
	}()

	tskSeed(t, c, shardDB)
	smallIDs := map[string]int64{}
	for i, sh := range tskShards[:2] {
		smallIDs[sh] = tskFirstID(i)
	}

	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// The stream reaches vtgate through a downstream-throttled proxy, so the
	// big shards take many seconds to copy while the small ones finish in the
	// first moments (the rig TestStreamer_VStreamShardSkewCopy uses).
	// Throttling the consumer instead would not do it: the snapshot pump
	// buffers the whole copy ahead of a slow reader.
	proxy := startTskProxy(t, c.grpcAddr)
	sluiceDSN := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none",
		c.mysqlDSN, proxy.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	stream, err := Engine{Flavor: FlavorPlanetScale}.OpenSnapshotStreamForTables(ctx, sluiceDSN, []string{tskTable})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Window writer: once both small shards have finished their copy (their
	// per-shard COPY_COMPLETED lines), change rows on them while the big
	// shards still copy, one autocommit transaction at a time. Each small
	// shard's tail position is its copy-finish GTID, so the tail replays all
	// of these. The two shards write in separate phases, -40 first and 40-80
	// after a gap, so the two replays sit more than vtgate's 2s MaxSkew
	// apart: whichever shard vtgate hears from first, one rewound shard is
	// named the laggard and the other pauses on a clock far behind the big
	// shards'.
	win := &tskWindow{}
	windowCtx, stopWindow := context.WithCancel(ctx)
	var windowWG sync.WaitGroup
	windowWG.Add(1)
	go func() {
		defer windowWG.Done()
		win.run(windowCtx, logs, shardDB, smallIDs)
	}()

	target := &tskTarget{rows: map[int64]int64{}, arrived: map[int64]time.Time{}}
	copied := tskDrainCopy(ctx, t, stream, target)
	stopWindow()
	windowWG.Wait()
	t.Logf("copy delivered %d rows; window commits %v", copied, win.commits)
	win.requireShape(t, logs.String())
	// PHASEA-DEBUG (Bug 334 CI triage): watch each small shard's window row
	// reach its final source value on the tail.
	for _, sh := range tskShards[:2] {
		var v int64
		if err := shardDB[sh].QueryRowContext(ctx, "SELECT v FROM "+tskTable+" WHERE id = ?", smallIDs[sh]).Scan(&v); err != nil {
			t.Fatalf("PHASEA: read final window value on %q: %v", sh, err)
		}
		target.watch(smallIDs[sh], v)
	}

	// Markers: one INSERT per shard per round, rounds a few seconds apart,
	// so every shard has writes after the copy (the real-cluster check's
	// shape: busy shards stalled too, not only an idle laggard). The first
	// round lands BEFORE the tail opens, as it does in a real cold start
	// where index builds run between the copy and the tail, so the big
	// shards' streams start with events seconds newer than the held windows.
	committedAt := map[int64]time.Time{}
	markerShard := map[int64]string{}
	const rounds = 4
	markerRound := func(r int) {
		for i, sh := range tskShards {
			id := tskMarkerBase + int64(r*10+i)
			if _, err := shardDB[sh].ExecContext(ctx, "INSERT INTO "+tskTable+" (id, v, pad) VALUES (?, ?, 'marker')", id, r); err != nil {
				t.Fatalf("marker insert on %q: %v", sh, err)
			}
			committedAt[id] = time.Now()
			markerShard[id] = sh
		}
	}
	time.Sleep(3 * time.Second)
	markerRound(0)

	tailOpenAt := time.Now()
	tailOpenBytes := tskProxyDownBytes.Load()
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	// The handoff must have taken the rewound reopen, relaxed: this is the
	// path Bug 334 reproduced, so a run that reached the tail some other way
	// (in place, or held) would not be testing the fix.
	if !tskHandoffLogged(logs.String(), "reason=shard_finished_copy_early", "minimize_skew=false") {
		t.Fatalf("RIG: the CDC handoff line does not carry reason=shard_finished_copy_early and minimize_skew=false. Logs:\n%s",
			tskVStreamLines(logs.String()))
	}
	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		for ch := range changes {
			target.apply(t, ch)
		}
	}()
	for r := 1; r < rounds; r++ {
		time.Sleep(3 * time.Second)
		markerRound(r)
	}

	var lastCommit time.Time
	for _, at := range committedAt {
		if at.After(lastCommit) {
			lastCommit = at
		}
	}
	deadline := lastCommit.Add(tskArrivalBudget)
	for time.Now().Before(deadline) && target.arrivedCount(committedAt) < len(committedAt) {
		time.Sleep(250 * time.Millisecond)
	}
	// PHASEA-DEBUG: keep waiting (forensics only) so the eventual arrival
	// of a late marker is recorded, then dump the timeline.
	for extra := time.Now().Add(150 * time.Second); time.Now().Before(extra) && target.arrivedCount(committedAt) < len(committedAt); {
		time.Sleep(250 * time.Millisecond)
	}
	{
		var b strings.Builder
		fmt.Fprintf(&b, "PHASEA timeline: tail opened %s; proxy down bytes at tail open %d, now %d; tail changes %d\n",
			tailOpenAt.Format(time.RFC3339Nano), tailOpenBytes, tskProxyDownBytes.Load(), target.tailChanges())
		for _, sh := range tskShards[:2] {
			at, ok := target.reached(smallIDs[sh])
			fmt.Fprintf(&b, "  window %s (%d commits): drained=%v at %s (+%s after tail open)\n",
				sh, win.commits[sh], ok, at.Format(time.RFC3339Nano), at.Sub(tailOpenAt))
		}
		ids := make([]int64, 0, len(committedAt))
		for id := range committedAt {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			at, ok := target.arrivedAt(id)
			fmt.Fprintf(&b, "  marker %s:%d committed %s arrived=%v %s latency %s (+%s after tail open)\n",
				markerShard[id], id, committedAt[id].Format(time.RFC3339Nano), ok, at.Format(time.RFC3339Nano),
				at.Sub(committedAt[id]), at.Sub(tailOpenAt))
		}
		t.Log(b.String())
	}
	perShard := map[string]string{}
	late := []string{}
	for _, sh := range tskShards {
		got := 0
		for id, s := range markerShard {
			if s != sh {
				continue
			}
			at, ok := target.arrivedAt(id)
			if ok && at.Sub(committedAt[id]) <= tskArrivalBudget {
				got++
				continue
			}
			late = append(late, fmt.Sprintf("%s:%d", sh, id))
		}
		perShard[sh] = fmt.Sprintf("%d/%d", got, rounds)
	}
	if len(late) > 0 {
		sort.Strings(late)
		t.Fatalf("markers did not reach the CDC tail within %s of their commit (per shard %v; late %v) — "+
			"the snapshot tail stalled behind vtgate's skew hold (Bug 334).\nsluice logs:\n%s\nvtgate skew lines:\n%s",
			tskArrivalBudget, perShard, late, tskVStreamLines(logs.String()), tskVtgateSkewLines(c))
	}
	t.Logf("every shard delivered every marker within %s: %v\nvtgate skew lines:\n%s", tskArrivalBudget, perShard, tskVtgateSkewLines(c))

	// Exact: the stream-applied target equals the source, key and value.
	want := tskSourceRows(t, c)
	var diff string
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
		if diff = target.diff(want); diff == "" {
			break
		}
	}
	if diff != "" {
		t.Fatalf("target != source after the tail drained: %s", diff)
	}
	t.Logf("EXACT: target == source (%d rows)", len(want))

	_ = stream.Close()
	select {
	case <-applyDone:
	case <-time.After(30 * time.Second):
		t.Error("CDC tail channel did not close after stream.Close")
	}
}

// tskOpenShardDBs opens one shard-targeted vtgate session per shard.
func tskOpenShardDBs(t *testing.T, c *vitessReshardCluster) map[string]*sql.DB {
	t.Helper()
	out := map[string]*sql.DB{}
	for _, sh := range tskShards {
		dsn := strings.Replace(c.mysqlDSN, "/"+vrKeyspace+"?", "/"+vrKeyspace+":"+sh+"?", 1)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open shard %q: %v", sh, err)
		}
		out[sh] = db
	}
	return out
}

// tskSeedRows is each shard's seed size: a few hundred rows on the two
// small shards, tens of thousands on the two big ones.
var tskSeedRows = map[string]int{"-40": 300, "40-80": 300, "80-c0": 25000, "c0-": 25000}

// tskFirstID is shard i's first seed id. Every shard gets its own id
// range and is written through its shard-targeted session, so the seed's
// skew is exact rather than whatever the hash vindex spreads (and no
// statement crosses vttablet's 10000-row DML cap, which a trim by DELETE
// would).
func tskFirstID(shardIdx int) int64 { return int64(shardIdx+1) * 1_000_000 }

// tskSeed fills each shard with its tskSeedRows and checks the skew took.
func tskSeed(t *testing.T, c *vitessReshardCluster, shardDB map[string]*sql.DB) {
	t.Helper()
	const batch = 2000
	pad := strings.Repeat("p", 150)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i, sh := range tskShards {
		first := tskFirstID(i)
		end := first + int64(tskSeedRows[sh])
		for lo := first; lo < end; lo += batch {
			var sb strings.Builder
			sb.WriteString("INSERT INTO " + tskTable + " (id, v, pad) VALUES ")
			for id := lo; id < lo+batch && id < end; id++ {
				if id > lo {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, "(%d,%d,'%s')", id, id, pad)
			}
			if _, err := shardDB[sh].ExecContext(ctx, sb.String()); err != nil {
				t.Fatalf("seed shard %q: %v", sh, err)
			}
		}
	}
	for _, sh := range tskShards {
		if n := shardScopedCount(t, c, sh, tskTable); n != tskSeedRows[sh] {
			t.Fatalf("shard %q holds %d rows after the seed; want %d", sh, n, tskSeedRows[sh])
		}
	}
}

// tskDrainCopy reads the copy into target.
func tskDrainCopy(ctx context.Context, t *testing.T, stream *ir.SnapshotStream, target *tskTarget) int {
	t.Helper()
	tbl := &ir.Table{Name: tskTable, Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "v", Type: ir.Integer{Width: 64}},
		{Name: "pad", Type: ir.Varchar{Length: 200}},
	}}
	rows, err := stream.Rows.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	n := 0
	for row := range rows {
		target.put(t, row)
		n++
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("copy error: %v", err)
	}
	return n
}

// tskShardFinishedLine is the snapshot's DEBUG record of one shard's
// per-scope COPY_COMPLETED (logShardCopyFinished).
const tskShardFinishedLine = `msg="mysql/vstream: snapshot: shard finished its copy"`

// tskHeldLine is the snapshot's INFO record, written at the stream-wide
// COPY_COMPLETED, of the changes a shard that finished early sent inside the
// copy (shardCopyBoundary.logHeld).
const tskHeldLine = "replayed from its copy position"

// tskShardFinishedAt is when the copy logged shard's per-scope
// COPY_COMPLETED, or the zero time when it has not.
func tskShardFinishedAt(logs, shard string) time.Time {
	return tskLoggedAt(logs, tskShardFinishedLine, " shard="+shard+" ")
}

// tskHandoffLogged reports whether the CDC handoff's INFO line
// (reopenForCDC) carries every one of attrs.
func tskHandoffLogged(logs string, attrs ...string) bool {
	return !tskLoggedAt(logs, append([]string{`msg="mysql/vstream: snapshot: CDC handoff (tail opened at the snapshot position)"`}, attrs...)...).IsZero()
}

// tskLoggedAt is the time of the first log line containing every one of
// parts, or the zero time.
func tskLoggedAt(logs string, parts ...string) time.Time {
next:
	for _, line := range strings.Split(logs, "\n") {
		for _, p := range parts {
			if !strings.Contains(line, p) {
				continue next
			}
		}
		stamp, _, _ := strings.Cut(strings.TrimPrefix(line, "time="), " ")
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			return time.Time{}
		}
		return at
	}
	return time.Time{}
}

// tskWindow phases are the window writer's timing: -40 writes for
// tskPhaseA, nothing for tskPhaseGap, then 40-80 writes until the copy ends.
const (
	tskPhaseA   = 3 * time.Second
	tskPhaseGap = 2500 * time.Millisecond
)

// tskWindow writes the held windows on the two small shards and records
// what it did, so the test can prove the shape formed.
type tskWindow struct {
	commits     map[string]int
	phaseBStart time.Time
	startErr    string
}

func (w *tskWindow) run(ctx context.Context, logs *logcapture.Buffer, shardDB map[string]*sql.DB, smallIDs map[string]int64) {
	w.commits = map[string]int{}
	for tskShardFinishedAt(logs.String(), tskShards[0]).IsZero() || tskShardFinishedAt(logs.String(), tskShards[1]).IsZero() {
		select {
		case <-ctx.Done():
			w.startErr = "the copy ended before both small shards logged their per-shard COPY_COMPLETED"
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	write := func(sh string, until time.Time) {
		for ctx.Err() == nil && (until.IsZero() || time.Now().Before(until)) {
			if _, err := shardDB[sh].ExecContext(ctx, "UPDATE "+tskTable+" SET v = v + 1 WHERE id = ?", smallIDs[sh]); err == nil {
				w.commits[sh]++
			}
		}
	}
	write(tskShards[0], time.Now().Add(tskPhaseA))
	select {
	case <-ctx.Done():
		return
	case <-time.After(tskPhaseGap):
	}
	w.phaseBStart = time.Now()
	write(tskShards[1], time.Time{})
}

// requireShape fails the test as a RIG error unless the shape Bug 334 needs
// formed: both small shards wrote a window after their copy finished, and
// the copy was still running a few seconds into the second window.
func (w *tskWindow) requireShape(t *testing.T, logs string) {
	t.Helper()
	if w.startErr != "" {
		t.Fatalf("RIG: %s; the shape did not form. Logs:\n%s", w.startErr, tskVStreamLines(logs))
	}
	for _, sh := range tskShards[:2] {
		if w.commits[sh] < 30 {
			t.Fatalf("RIG: the window writer committed %d changes on %q after its copy finished; want at least 30", w.commits[sh], sh)
		}
	}
	// The last shard's per-shard COPY_COMPLETED arrives with the stream-wide
	// one and is not logged on its own, so the copy's end is read from the
	// held-changes line finishCopy writes at the stream-wide COPY_COMPLETED.
	end := tskLoggedAt(logs, tskHeldLine)
	if end.IsZero() || end.Before(w.phaseBStart.Add(3*time.Second)) {
		t.Fatalf("RIG: the copy ended at %v, less than 3s into the second window (started %v), so the two held windows are not "+
			"far enough apart to exercise Bug 334; slow the proxy or grow the big shards. Logs:\n%s",
			end, w.phaseBStart, tskVStreamLines(logs))
	}
}

// tskDownstreamBytesPerSec caps vtgate→sluice throughput so the big shards
// copy for well over ten seconds (the rate TestStreamer_VStreamShardSkewCopy's
// proxy uses).
const tskDownstreamBytesPerSec = 512 << 10

// tskProxyDownBytes is PHASEA-DEBUG: total vtgate->sluice bytes.
var tskProxyDownBytes atomic.Int64

// startTskProxy is a pass-through TCP proxy to target whose downstream
// (vtgate → sluice) direction is throttled to tskDownstreamBytesPerSec.
func startTskProxy(t *testing.T, target string) net.Listener {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", target)
			if err != nil {
				_ = down.Close()
				continue
			}
			track(down)
			track(up)
			go func() { _, _ = io.Copy(up, down); _ = up.Close() }()
			go func() { tskThrottledCopy(down, up); _ = down.Close() }()
		}
	}()
	return ln
}

// tskThrottledCopy copies src to dst at no more than
// tskDownstreamBytesPerSec on average.
func tskThrottledCopy(dst io.Writer, src io.Reader) {
	buf := make([]byte, 16<<10)
	start := time.Now()
	var sent int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
			sent += int64(n)
			tskProxyDownBytes.Add(int64(n))
			due := start.Add(time.Duration(float64(sent) / tskDownstreamBytesPerSec * float64(time.Second)))
			if d := time.Until(due); d > 0 {
				time.Sleep(d)
			}
		}
		if err != nil {
			return
		}
	}
}

// tskVtgateSkewLines returns vtgate's skew-hold log lines (the cluster
// container's stdout tails vtgate.log), the ground truth for whether a hold
// formed.
func tskVtgateSkewLines(c *vitessReshardCluster) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := c.vitess.Logs(ctx)
	if err != nil {
		return "(container logs unavailable: " + err.Error() + ")"
	}
	defer func() { _ = rc.Close() }()
	var keep []string
	for _, line := range strings.Split(vrDrain(rc), "\n") {
		if strings.Contains(strings.ToLower(line), "skew") {
			keep = append(keep, line)
		}
	}
	if len(keep) > 20 {
		keep = append(keep[:10], append([]string{"..."}, keep[len(keep)-10:]...)...)
	}
	return strings.Join(keep, "\n")
}

// tskVStreamLines keeps the mysql/vstream lines of a log dump.
func tskVStreamLines(logs string) string {
	var keep []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "mysql/vstream") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, "\n")
}

// tskSourceRows reads id -> v from every shard through vtgate.
func tskSourceRows(t *testing.T, c *vitessReshardCluster) map[int64]int64 {
	t.Helper()
	db, err := sql.Open("mysql", c.mysqlDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	// The streaming executor lifts vttablet's 10000-row result cap.
	if _, err := conn.ExecContext(ctx, "set workload='olap'"); err != nil {
		t.Fatalf("set workload=olap: %v", err)
	}
	rows, err := conn.QueryContext(ctx, "SELECT id, v FROM "+tskTable)
	if err != nil {
		t.Fatalf("source select: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]int64{}
	for rows.Next() {
		var id, v int64
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("source scan: %v", err)
		}
		out[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("source rows: %v", err)
	}
	return out
}

// tskTarget is the stream-applied target: id -> v, plus each id's first
// arrival time on the CDC tail.
type tskTarget struct {
	mu      sync.Mutex
	rows    map[int64]int64
	arrived map[int64]time.Time

	// PHASEA-DEBUG
	watched   map[int64]int64
	reachedAt map[int64]time.Time
	nTail     int
}

// PHASEA-DEBUG
func (tg *tskTarget) watch(id, v int64) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	if tg.watched == nil {
		tg.watched = map[int64]int64{}
		tg.reachedAt = map[int64]time.Time{}
	}
	tg.watched[id] = v
	if got, ok := tg.rows[id]; ok && got >= v {
		tg.reachedAt[id] = time.Now()
	}
}

// PHASEA-DEBUG
func (tg *tskTarget) reached(id int64) (time.Time, bool) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	at, ok := tg.reachedAt[id]
	return at, ok
}

// PHASEA-DEBUG
func (tg *tskTarget) tailChanges() int {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return tg.nTail
}

func (tg *tskTarget) put(t *testing.T, row ir.Row) {
	t.Helper()
	id, okID := vrAsInt64(row["id"])
	v, okV := vrAsInt64(row["v"])
	if !okID || !okV {
		t.Errorf("row with non-integer id/v: %#v", row)
		return
	}
	tg.mu.Lock()
	tg.rows[id] = v
	if want, ok := tg.watched[id]; ok && v >= want {
		if _, done := tg.reachedAt[id]; !done {
			tg.reachedAt[id] = time.Now()
		}
	}
	tg.mu.Unlock()
}

func (tg *tskTarget) apply(t *testing.T, ch ir.Change) {
	t.Helper()
	tg.mu.Lock()
	tg.nTail++
	tg.mu.Unlock()
	switch c := ch.(type) {
	case ir.Insert:
		tg.put(t, c.Row)
		tg.markArrived(c.Row)
	case ir.Update:
		tg.put(t, c.After)
		tg.markArrived(c.After)
	case ir.Delete:
		if id, ok := vrAsInt64(c.Before["id"]); ok {
			tg.mu.Lock()
			delete(tg.rows, id)
			tg.mu.Unlock()
		}
	}
}

func (tg *tskTarget) markArrived(row ir.Row) {
	id, ok := vrAsInt64(row["id"])
	if !ok {
		return
	}
	tg.mu.Lock()
	if _, seen := tg.arrived[id]; !seen {
		tg.arrived[id] = time.Now()
	}
	tg.mu.Unlock()
}

func (tg *tskTarget) arrivedAt(id int64) (time.Time, bool) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	at, ok := tg.arrived[id]
	return at, ok
}

func (tg *tskTarget) arrivedCount(ids map[int64]time.Time) int {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	n := 0
	for id := range ids {
		if _, ok := tg.arrived[id]; ok {
			n++
		}
	}
	return n
}

// diff compares the target with want, naming the first few mismatches.
func (tg *tskTarget) diff(want map[int64]int64) string {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	var missing, extra, stale []string
	for id, v := range want {
		got, ok := tg.rows[id]
		switch {
		case !ok:
			missing = append(missing, fmt.Sprint(id))
		case got != v:
			stale = append(stale, fmt.Sprintf("%d(%d!=%d)", id, got, v))
		}
	}
	for id := range tg.rows {
		if _, ok := want[id]; !ok {
			extra = append(extra, fmt.Sprint(id))
		}
	}
	if len(missing)+len(extra)+len(stale) == 0 {
		return ""
	}
	clip := func(s []string) []string {
		sort.Strings(s)
		if len(s) > 10 {
			return s[:10]
		}
		return s
	}
	return fmt.Sprintf("target %d rows, source %d: missing %d %v, extra %d %v, stale %d %v",
		len(tg.rows), len(want), len(missing), clip(missing), len(extra), clip(extra), len(stale), clip(stale))
}
