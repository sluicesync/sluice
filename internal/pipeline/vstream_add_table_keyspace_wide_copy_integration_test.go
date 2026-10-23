//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Measurement of VSTREAM-ADD-TABLE-KEYSPACE-WIDE-COPY (docs/dev/audit-backlog.md):
// `schema add-table` from a VStream source opens its snapshot with
// a.Source.OpenSnapshotStream (no table list), which on a VStream flavor is
// one keyspace-wide COPY stream. Two questions:
//
//   - (a) does the add's copy read, and buffer, every OTHER table of the
//     keyspace while it drains only the new one?
//   - (b) does a delete made while that copy runs, on the new table, land
//     stale on the target? In a single multi-table VStream, uvstreamer
//     replays already-copied rows' changes as copy-phase ROW events, and
//     bufferCopyRow keeps after images only.
//
// The keyspace per run (copy order is name order):
//
//   - <p>_1est: 1,000 rows, the stream's established table (--include-table).
//   - <p>_2add: 100,000 rows, the table added mid-stream.
//   - <p>_3big: 150,000 rows, outside the stream's scope and never added.
//
// The add reaches vtgate through a throttling TCP proxy so its copy is in
// flight while the source changes (the stream itself dials vtgate directly).
// Two rounds of changes are made during the add:
//
//   - round 1, while the target holds atkPhase1Rows of _2add: in _2add,
//     20 deletes + 10 updates below the copy (ids 1.., 101..), at the frontier
//     (just past what the target held) and far above it (ids 90,001..); in
//     _1est, 20 deletes + 10 updates (ids 1.., 101..).
//   - round 2, after _2add has fully landed on the target while vtgate is
//     still copying _3big: in _2add 20 deletes + 10 updates (ids 50,001..,
//     50,101..); in _1est 20 deletes + 10 updates (ids 201.., 301..).
//
// Then the add returns, the stream runs (drained: it is restarted from its
// pre-add position; --no-drain, before NO-DRAIN-VSTREAM-SOURCE: it was never
// stopped, and the test waited out the live-added poll), a marker row goes
// into both tables, and the verdict compares the SOURCE's (id, payload) set
// read off vtgate with the TARGET's, band by band, never consulting
// sluice's own state. Rows the add's copy received for the tables it does
// NOT write are counted by draining the snapshot's own buffer for them just
// before it closes (atkCountingEngine).
//
// MEASURED 2026-10-22 (vttestserver mysql80 = Vitess 24, one shard, 3 runs
// per cell; Postgres 16 target drained, MySQL 8.0 target --no-drain):
//
//   - (a) holds, every run: the add's copy received the whole keyspace,
//     1,011-1,012 rows of _1est and 150,000 of _3big besides _2add's
//     100,030, 40.3 MB on the wire, and the add took 78s, ~45s of it after
//     _2add had fully landed. Beside an out-of-scope 40,000-row table of
//     2 KB rows (cell BufferCap) the add refused loudly in 3-8s ("table
//     cap1_3fat would buffer 67110728 bytes, exceeding the --max-buffer-bytes
//     cap"); `schema add-table` has no --max-buffer-bytes to raise.
//   - (b) as the add leaves the target, every run, every mode: _2add's 20
//     round-1 deletes below, at and ABOVE the copy frontier are all stale
//     (the copy reads the table from one snapshot taken before them, and
//     their catch-up DELETEs are dropped), as are the 20 round-2 deletes AND
//     the 10 round-2 updates (changes after the copy's own position, which
//     the add never streams); round-1 updates are exact (the catch-up's
//     after images).
//   - Drained: exact after the restart, every run (60+20 deletes, 10 updates
//     replayed from the stream's pre-add position).
//   - --no-drain after a warm resume: all of the above stays stale (80 rows,
//     10 values), and stays so across a later restart. The running stream
//     dropped those changes as out of scope while the add copied.
//   - --no-drain in the process that cold-started the stream: the same, and
//     the running stream never applies the added table at all (its
//     one-table cold start carries its CDC on the copy stream, scoped to
//     _1est): the post-add marker never lands, and after a restart the
//     first marker is missing for good while a second one lands.
//
// SINCE NO-DRAIN-VSTREAM-SOURCE (`schema add-table --no-drain` refuses on a
// VStream source): the two NoDrain cells require that refusal, with nothing
// written to the target and the running stream unaffected (the measuring
// code for those modes was removed with them; commit 8c9baaee carries it).
// The Drained cells stay the exact pins: into Postgres 16 (measured 3 runs)
// and, since 2026-10-23, into MySQL 8.0 (MySQLTarget/Drained, 1 run), one
// shard each. (a) is still open: the BufferCap cell stays a measurement.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	SLUICE_ATK_MEASURE=1 SLUICE_ATK_RUNS=3 go test -tags='integration vstream' \
//	  -count=1 -timeout=120m -v -run 'TestStreamer_VStreamAddTableKeyspaceWideCopy' ./internal/pipeline/
//
// The BufferCap measurement skips unless SLUICE_ATK_MEASURE=1 (the item is
// open); the Drained cells run SLUICE_ATK_RUNS times (default 1).

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

const (
	atkEstRows    = 1_000
	atkAddRows    = 100_000
	atkBigRows    = 150_000
	atkPhase1Rows = 20_000
	atkAboveBase  = 90_000
	atkFinBase    = 50_000
	atkMarkerID   = 9_000_001
)

// atkDownstreamBytesPerSec caps vtgate→sluice throughput on the add's
// connection, so its copy (about 16 MB for _2add, 24 MB for _3big) takes
// tens of seconds instead of a few.
const atkDownstreamBytesPerSec = 512 << 10

// atkProxy is a throttling TCP proxy to vtgate's gRPC port that counts the
// bytes it forwards vtgate→sluice.
type atkProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	down atomic.Int64
}

func startATKProxy(t *testing.T, target string) *atkProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &atkProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
	})
	return p
}

func (p *atkProxy) addr() string { return p.ln.Addr().String() }

func (p *atkProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serve(c)
	}
}

func (p *atkProxy) serve(c net.Conn) {
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	buf := make([]byte, 16<<10)
	start := time.Now()
	var sent int64
	for {
		n, err := up.Read(buf)
		if n > 0 {
			if _, werr := c.Write(buf[:n]); werr != nil {
				break
			}
			sent += int64(n)
			p.down.Add(int64(n))
			if ahead := time.Duration(float64(sent)/atkDownstreamBytesPerSec*float64(time.Second)) - time.Since(start); ahead > 0 {
				time.Sleep(ahead)
			}
		}
		if err != nil {
			break
		}
	}
	_ = c.Close()
}

// atkCountingEngine is the planetscale engine with one change: just before
// the add's snapshot stream closes, it drains the stream's buffer for every
// table in others and records how many rows the copy had received for each.
// Embedding the concrete engine keeps every optional surface AddTable asserts.
type atkCountingEngine struct {
	mysql.Engine
	others []string

	mu       sync.Mutex
	received map[string]int
	drainErr error
}

func (e *atkCountingEngine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	stream, err := e.Engine.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		return nil, err
	}
	inner := stream.CloseFn
	stream.CloseFn = func() error {
		e.drainOthers(stream)
		if inner == nil {
			return nil
		}
		return inner()
	}
	return stream, nil
}

func (e *atkCountingEngine) drainOthers(stream *ir.SnapshotStream) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	got := map[string]int{}
	var derr error
	for _, tbl := range e.others {
		ch, err := stream.Rows.ReadRows(ctx, &ir.Table{Name: tbl})
		if err != nil {
			derr = errors.Join(derr, fmt.Errorf("drain %s: %w", tbl, err))
			continue
		}
		for range ch {
			got[tbl]++
		}
	}
	e.mu.Lock()
	e.received, e.drainErr = got, derr
	e.mu.Unlock()
}

func (e *atkCountingEngine) counts() (map[string]int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.received, e.drainErr
}

// atkRows reads every (id, payload) of table, in keyset pages (vtgate caps a
// result at 10,000 rows). nil when the table is absent.
func atkRows(t *testing.T, db twfbDB, table string) map[int64]string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	q := "SELECT id, payload FROM " + table + " WHERE id > ? ORDER BY id LIMIT 5000"
	if db.engine == "postgres" {
		q = "SELECT id, payload FROM " + table + " WHERE id > $1 ORDER BY id LIMIT 5000"
	}
	out := map[int64]string{}
	after := int64(-1 << 62)
	for {
		rows, err := conn.QueryContext(ctx, q, after)
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "does not exist") {
				return nil
			}
			t.Fatalf("read %s.%s: %v", db.engine, table, err)
		}
		n := 0
		for rows.Next() {
			var id int64
			var payload string
			if err := rows.Scan(&id, &payload); err != nil {
				t.Fatalf("scan %s.%s: %v", db.engine, table, err)
			}
			out[id] = payload
			after = id
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %s.%s: %v", db.engine, table, err)
		}
		_ = rows.Close()
		if n < 5000 {
			return out
		}
	}
}

// atkBand is one id set and the changes made to it.
type atkBand struct {
	arm     string
	table   string
	deleted []int64
	updated []int64
}

type atkBandResult struct {
	arm                    string
	staleRows, staleVals   int
	staleRowEx, staleValEx []int64
}

// atkResult is one run's outcome.
type atkResult struct {
	addErr        error
	addTook       time.Duration
	phase1At      int // _2add rows on the target at round 1
	phase2At      int
	wireBytes     int64           // vtgate→sluice bytes on the add's connection
	received      map[string]int  // rows the add's copy buffered for tables it does not write
	drainErr      error           // from counting them
	bands         []atkBandResult // per band, after the stream carried the table
	copyOnly      []atkBandResult // per band of _2add, as the add left the target (before the stream applied anything to it)
	extra         map[string]int  // on the target, not on the source, outside the bands
	missing       map[string]int  // on the source, absent or different on the target, outside the bands
	bigOnTarget   bool            // the out-of-scope table reached the target
	markersLanded map[string]bool // the post-add marker reached the target
	note          string
}

func (r atkResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "add-err=%v add-took=%v add-rows-on-target-at-round1=%d at-round2=%d add-wire-bytes=%d copy-received-other-tables=%v (drain-err=%v) big-on-target=%v markers=%v",
		r.addErr, r.addTook.Round(time.Millisecond), r.phase1At, r.phase2At, r.wireBytes, r.received, r.drainErr, r.bigOnTarget, r.markersLanded)
	for _, a := range r.copyOnly {
		fmt.Fprintf(&b, "\n    as-the-add-left-it %-16s stale-rows=%d (e.g. %v) stale-values=%d (e.g. %v)", a.arm, a.staleRows, a.staleRowEx, a.staleVals, a.staleValEx)
	}
	for _, a := range r.bands {
		fmt.Fprintf(&b, "\n    final              %-16s stale-rows=%d (e.g. %v) stale-values=%d (e.g. %v)", a.arm, a.staleRows, a.staleRowEx, a.staleVals, a.staleValEx)
	}
	fmt.Fprintf(&b, "\n    outside the bands: extra-on-target=%v missing-or-different-on-target=%v", r.extra, r.missing)
	if r.note != "" {
		fmt.Fprintf(&b, "\n    note: %s", r.note)
	}
	return b.String()
}

func (r atkResult) diverged() bool {
	if r.note != "" {
		return true
	}
	for _, a := range r.bands {
		if a.staleRows > 0 || a.staleVals > 0 {
			return true
		}
	}
	for _, m := range []map[string]int{r.extra, r.missing} {
		for _, n := range m {
			if n > 0 {
				return true
			}
		}
	}
	return r.bigOnTarget
}

// atkStale grades table's bands against the target alone: a deleted id still
// present, an updated id not carrying its new value.
func atkStale(t *testing.T, r vcrRig, table string, bands []atkBand) []atkBandResult {
	t.Helper()
	tgt := atkRows(t, r.tgt(), table)
	var out []atkBandResult
	for _, bd := range bands {
		if bd.table != table {
			continue
		}
		br := atkBandResult{arm: bd.arm}
		for _, id := range bd.deleted {
			if _, ok := tgt[id]; ok {
				br.staleRows++
				if len(br.staleRowEx) < 5 {
					br.staleRowEx = append(br.staleRowEx, id)
				}
			}
		}
		for _, id := range bd.updated {
			if got, ok := tgt[id]; !ok || got != fmt.Sprintf("upd-%d", id) {
				br.staleVals++
				if len(br.staleValEx) < 5 {
					br.staleValEx = append(br.staleValEx, id)
				}
			}
		}
		out = append(out, br)
	}
	return out
}

// atkCountOnTarget is table's row count on the target, 0 while it is absent.
func atkCountOnTarget(t *testing.T, r vcrRig, table string) int {
	t.Helper()
	n, err := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+table))
	if err != nil {
		return 0
	}
	return n
}

func atkApply(t *testing.T, dsn string, bands []atkBand) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, bd := range bands {
		for _, id := range bd.deleted {
			if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = %d", bd.table, id)); err != nil {
				t.Fatalf("source delete: %v", err)
			}
		}
		for _, id := range bd.updated {
			if _, err := db.Exec(fmt.Sprintf("UPDATE %s SET payload = 'upd-%d' WHERE id = %d", bd.table, id, id)); err != nil {
				t.Fatalf("source update: %v", err)
			}
		}
	}
}

// atkEstablish starts a stream scoped to est and waits until it applies a
// change. warm restarts it once first, so the running stream is a warm
// resume (its tail the standalone VStream CDC reader) rather than the
// process that cold-started (its tail the cold start's snapshot stream
// carried on into CDC). The caller stops the returned run.
func atkEstablish(t *testing.T, r vcrRig, p, est string, warm bool) (*twfbRun, func() *Streamer) {
	t.Helper()
	mk := func() *Streamer { return r.streamer(p, []string{est}, "") }
	cell := twfbCell{tgt: r.tgt(), streamID: p}
	stream := startTWFBRun(mk())
	cell.waitStreaming(t, stream)
	vcrSeed(t, r.mysqlDSN, est, "pre", []int64{atkMarkerID - 1})
	if !r.tgt().waitRow(t, est, atkMarkerID-1, stream, 120*time.Second) {
		_ = stream.stop(t)
		t.Fatalf("RIG: the established stream never applied a change (exited=%v err=%v)", stream.exited(), stream.err)
	}
	if warm {
		if err := stream.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("RIG: stop before the warm resume: %v", err)
		}
		stream = startTWFBRun(mk())
		vcrSeed(t, r.mysqlDSN, est, "pre", []int64{atkMarkerID - 2})
		if !r.tgt().waitRow(t, est, atkMarkerID-2, stream, 120*time.Second) {
			_ = stream.stop(t)
			t.Fatalf("RIG: the warm-resumed stream never applied a change (exited=%v err=%v)", stream.exited(), stream.err)
		}
	}
	return stream, mk
}

// atkRunCell runs one drained add of _2add into a stream established on
// _1est, with two rounds of source changes during the add's copy.
func atkRunCell(t *testing.T, r vcrRig, proxyDSN string, proxy *atkProxy, prefix string, run int) atkResult {
	t.Helper()
	p := fmt.Sprintf("%s%d", prefix, run)
	est, add, big := p+"_1est", p+"_2add", p+"_3big"
	vcrCreate(t, r, est, add, big)
	defer func() {
		for _, tbl := range []string{est, add, big} {
			r.src().exec(t, "DROP TABLE IF EXISTS "+tbl)
		}
	}()
	vcrSeed(t, r.mysqlDSN, est, "v1", vcrRange(1, 1, atkEstRows))
	vcrSeed(t, r.mysqlDSN, add, "v1", vcrRange(1, 1, atkAddRows))
	vcrSeed(t, r.mysqlDSN, big, "v1", vcrRange(1, 1, atkBigRows))
	time.Sleep(3 * time.Second) // the schema tracker

	var res atkResult
	logStart := r.logs.Len()
	defer func() {
		if dir := os.Getenv("SLUICE_ATK_LOGDIR"); dir != "" {
			_ = os.WriteFile(filepath.Join(dir, p+".log"), []byte(r.logs.String()[logStart:]), 0o600)
		}
	}()
	stream, mk := atkEstablish(t, r, p, est, false)
	if err := stream.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: drained stop: %v", err)
	}

	pe, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("engine not registered: planetscale")
	}
	counter := &atkCountingEngine{Engine: pe.(mysql.Engine), others: []string{est, big}}
	tgtEng, _ := engines.Get(r.targetEngine())
	addTable := &AddTable{
		Source: counter, Target: tgtEng,
		SourceDSN: proxyDSN, TargetDSN: r.tgtDSN,
		StreamID:  p,
		TableName: add,
	}
	proxy.down.Store(0)
	addStart := time.Now()
	addDone := make(chan error, 1)
	go func() { addDone <- addTable.Run(context.Background()) }()
	var addErr error
	addReturned := false
	pollAdd := func() bool {
		if addReturned {
			return true
		}
		select {
		case addErr = <-addDone:
			addReturned = true
			res.addTook = time.Since(addStart)
		default:
		}
		return addReturned
	}

	// Round 1: _2add mid-copy.
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if pollAdd() || time.Now().After(deadline) {
			t.Fatalf("RIG: the add returned (%v) or timed out before round 1; %s held %d rows", addErr, add, atkCountOnTarget(t, r, add))
		}
		if n := atkCountOnTarget(t, r, add); n >= atkPhase1Rows {
			res.phase1At = n
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	front := int64(res.phase1At)
	if front+200 >= atkAboveBase {
		t.Fatalf("RIG: %s held %d rows at round 1; the above band would not be above", add, res.phase1At)
	}
	round1 := []atkBand{
		{arm: "r1-add-below", table: add, deleted: vcrRange(1, 1, 20), updated: vcrRange(101, 1, 10)},
		{arm: "r1-add-frontier", table: add, deleted: vcrRange(front+1, 1, 20), updated: vcrRange(front+101, 1, 10)},
		{arm: "r1-add-above", table: add, deleted: vcrRange(atkAboveBase+1, 1, 20), updated: vcrRange(atkAboveBase+101, 1, 10)},
		{arm: "r1-est", table: est, deleted: vcrRange(1, 1, 20), updated: vcrRange(101, 1, 10)},
	}
	atkApply(t, r.mysqlDSN, round1)

	// Round 2: _2add fully on the target, vtgate still copying _3big.
	want := atkAddRows - 60
	for {
		if pollAdd() {
			res.note = fmt.Sprintf("the add returned before round 2 (%s held %d)", add, atkCountOnTarget(t, r, add))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("RIG: %s never reached %d rows on the target during the add", add, want)
		}
		if n := atkCountOnTarget(t, r, add); n >= want {
			time.Sleep(3 * time.Second)
			res.phase2At = atkCountOnTarget(t, r, add)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var round2 []atkBand
	if !pollAdd() {
		round2 = []atkBand{
			{arm: "r2-add-finished", table: add, deleted: vcrRange(atkFinBase+1, 1, 20), updated: vcrRange(atkFinBase+101, 1, 10)},
			{arm: "r2-est", table: est, deleted: vcrRange(201, 1, 20), updated: vcrRange(301, 1, 10)},
		}
		atkApply(t, r.mysqlDSN, round2)
	} else if res.note == "" {
		res.note = "the add returned before round 2"
	}

	// The add finishes.
	if !addReturned {
		select {
		case addErr = <-addDone:
			res.addTook = time.Since(addStart)
		case <-time.After(10 * time.Minute):
			t.Fatal("RIG: the add did not return within 10 minutes")
		}
	}
	res.addErr = addErr
	res.wireBytes = proxy.down.Load()
	res.received, res.drainErr = counter.counts()
	if addErr != nil {
		return res
	}
	// The add's own result, with the stream stopped: the copy's work alone.
	res.copyOnly = atkStale(t, r, add, append(round1, round2...))

	// The stream carries the table from here, restarted from its pre-add
	// position.
	stream = startTWFBRun(mk())
	defer func() { _ = stream.stop(t) }()
	res.markersLanded = map[string]bool{}
	for _, tbl := range []string{est, add} {
		vcrSeed(t, r.mysqlDSN, tbl, "marker", []int64{atkMarkerID})
	}
	for _, tbl := range []string{est, add} {
		res.markersLanded[tbl] = r.tgt().waitRow(t, tbl, atkMarkerID, stream, 180*time.Second)
		if !res.markersLanded[tbl] {
			res.note = fmt.Sprintf("%s's marker never reached the target (exited=%v err=%v)", tbl, stream.exited(), stream.err)
		}
	}
	time.Sleep(2 * time.Second)

	// The verdict, from the source and the target only.
	src, tgt, inBand := map[string]map[int64]string{}, map[string]map[int64]string{}, map[string]map[int64]bool{}
	for _, tbl := range []string{est, add} {
		src[tbl], tgt[tbl], inBand[tbl] = atkRows(t, r.src(), tbl), atkRows(t, r.tgt(), tbl), map[int64]bool{}
	}
	for _, bd := range append(round1, round2...) {
		br := atkBandResult{arm: bd.arm}
		for _, id := range bd.deleted {
			inBand[bd.table][id] = true
			if _, onSrc := src[bd.table][id]; onSrc {
				t.Fatalf("RIG: %s id %d still on the source after its delete", bd.table, id)
			}
			if _, onTgt := tgt[bd.table][id]; onTgt {
				br.staleRows++
				if len(br.staleRowEx) < 5 {
					br.staleRowEx = append(br.staleRowEx, id)
				}
			}
		}
		for _, id := range bd.updated {
			inBand[bd.table][id] = true
			w := fmt.Sprintf("upd-%d", id)
			if src[bd.table][id] != w {
				t.Fatalf("RIG: %s id %d reads %q on the source, want %q", bd.table, id, src[bd.table][id], w)
			}
			if got, ok := tgt[bd.table][id]; !ok || got != w {
				br.staleVals++
				if len(br.staleValEx) < 5 {
					br.staleValEx = append(br.staleValEx, id)
				}
			}
		}
		res.bands = append(res.bands, br)
	}
	res.extra, res.missing = map[string]int{}, map[string]int{}
	for _, tbl := range []string{est, add} {
		var missing []int64
		for id, sv := range src[tbl] {
			if inBand[tbl][id] {
				continue
			}
			if tv, ok := tgt[tbl][id]; !ok || tv != sv {
				missing = append(missing, id)
			}
		}
		for id := range tgt[tbl] {
			if _, ok := src[tbl][id]; !ok && !inBand[tbl][id] {
				res.extra[tbl]++
			}
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		res.missing[tbl] = len(missing)
		if len(missing) > 0 {
			t.Logf("  %s: %d source rows missing or different on the target outside the bands, e.g. %v", tbl, len(missing), missing[:min(5, len(missing))])
		}
	}
	res.bigOnTarget = atkRows(t, r.tgt(), big) != nil
	return res
}

func atkRuns() int {
	if n, err := strconv.Atoi(os.Getenv("SLUICE_ATK_RUNS")); err == nil && n > 0 {
		return n
	}
	return 1
}

func atkCell(t *testing.T, r vcrRig, proxyDSN string, proxy *atkProxy, prefix string) {
	t.Helper()
	runs := atkRuns()
	for i := 1; i <= runs; i++ {
		res := atkRunCell(t, r, proxyDSN, proxy, prefix, i)
		t.Logf("RUN %d/%d: %s", i, runs, res)
		if res.addErr != nil {
			t.Errorf("RUN %d: the add failed: %v", i, res.addErr)
			continue
		}
		if res.diverged() {
			t.Errorf("RUN %d: SILENT DIVERGENCE (add succeeded, stream running): %s", i, res)
		}
	}
}

// atkControlColumn reads one of the stream's control-row table-set columns
// on the MySQL target, "" when NULL.
func atkControlColumn(t *testing.T, r vcrRig, streamID, column string) string {
	t.Helper()
	v := r.tgt().scalar(t, "SELECT COALESCE(CAST("+column+" AS CHAR), '') FROM "+appliershared.ControlTableName+" WHERE stream_id = ?", streamID)
	if strings.HasPrefix(v, "<") {
		t.Fatalf("RIG: read %s of stream %s: %s", column, streamID, v)
	}
	return v
}

// atkNoDrainRefusalCell runs `schema add-table --no-drain` of _2add into a
// RUNNING stream established on _1est, on a MySQL target. Before
// NO-DRAIN-VSTREAM-SOURCE this pair took the filter-flip live path, which
// left 80 deletes and 10 updates of the added table stale (the header's
// measurement). Now the add must refuse, naming the marker and the drained
// flow, having written nothing to the target: no table, no live-added or
// pending record. The running stream must be unaffected.
func atkNoDrainRefusalCell(t *testing.T, r vcrRig, prefix string, warm bool) {
	t.Helper()
	p := prefix + "1"
	est, add := p+"_1est", p+"_2add"
	vcrCreate(t, r, est, add)
	defer func() {
		for _, tbl := range []string{est, add} {
			r.src().exec(t, "DROP TABLE IF EXISTS "+tbl)
		}
	}()
	vcrSeed(t, r.mysqlDSN, est, "v1", vcrRange(1, 1, atkEstRows))
	vcrSeed(t, r.mysqlDSN, add, "v1", vcrRange(1, 1, 1000))
	time.Sleep(3 * time.Second) // the schema tracker

	stream, _ := atkEstablish(t, r, p, est, warm)
	defer func() { _ = stream.stop(t) }()
	liveBefore := atkControlColumn(t, r, p, "live_added_tables")
	pendingBefore := atkControlColumn(t, r, p, "pending_added_tables")

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("engine not registered: planetscale")
	}
	tgtEng, _ := engines.Get(r.targetEngine())
	err := (&AddTable{
		Source: srcEng, Target: tgtEng,
		SourceDSN: r.streamDSN, TargetDSN: r.tgtDSN,
		StreamID: p, TableName: add,
		LiveMode: true,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("schema add-table --no-drain on a VStream source succeeded; want the %s refusal", noDrainVStreamSourceMarker)
	}
	for _, want := range []string{noDrainVStreamSourceMarker, "sluice sync stop --wait", "without --no-drain", "sluice sync start"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal = %v; want it to contain %q", err, want)
		}
	}
	if rows := atkRows(t, r.tgt(), add); rows != nil {
		t.Errorf("the refused add left %s on the target (%d rows); want it absent", add, len(rows))
	}
	if got := atkControlColumn(t, r, p, "live_added_tables"); got != liveBefore {
		t.Errorf("live_added_tables = %q after the refusal; want it unchanged (%q)", got, liveBefore)
	}
	if got := atkControlColumn(t, r, p, "pending_added_tables"); got != pendingBefore {
		t.Errorf("pending_added_tables = %q after the refusal; want it unchanged (%q)", got, pendingBefore)
	}

	// The running stream is unaffected: it still applies its own table.
	vcrSeed(t, r.mysqlDSN, est, "post", []int64{atkMarkerID})
	if !r.tgt().waitRow(t, est, atkMarkerID, stream, 120*time.Second) {
		t.Errorf("the running stream stopped applying %s after the refusal (exited=%v err=%v)", est, stream.exited(), stream.err)
	}
}

// atkBufferCapCell adds a small table to a stream whose keyspace also holds
// a table wider than the snapshot's 64 MiB default buffer cap, outside the
// stream's scope. A table-scoped add copies the small table alone; the
// keyspace-wide one also streams the wide table and buffers it.
func atkBufferCapCell(t *testing.T, r vcrRig, run int) (addErr error, took time.Duration) {
	t.Helper()
	p := fmt.Sprintf("cap%d", run)
	est, add, fat := p+"_1est", p+"_2add", p+"_3fat"
	vcrCreate(t, r, est, add)
	r.src().exec(t, "CREATE TABLE "+fat+" (id BIGINT NOT NULL, payload VARCHAR(4000) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	defer func() {
		for _, tbl := range []string{est, add, fat} {
			r.src().exec(t, "DROP TABLE IF EXISTS "+tbl)
		}
	}()
	vcrSeed(t, r.mysqlDSN, est, "v1", vcrRange(1, 1, 100))
	vcrSeed(t, r.mysqlDSN, add, "v1", vcrRange(1, 1, 1000))
	db, err := sql.Open("mysql", r.mysqlDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	wide := strings.Repeat("w", 2000)
	for start := 1; start <= 40_000; start += 200 {
		var b strings.Builder
		b.WriteString("INSERT INTO " + fat + " (id, payload) VALUES ")
		for id := start; id < start+200; id++ {
			if id > start {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,'%s')", id, wide)
		}
		if _, err := db.Exec(b.String()); err != nil {
			t.Fatalf("seed %s: %v", fat, err)
		}
	}
	_ = db.Close()
	time.Sleep(3 * time.Second)

	cell := twfbCell{tgt: r.tgt(), streamID: p}
	stream := startTWFBRun(r.streamer(p, []string{est}, ""))
	cell.waitStreaming(t, stream)
	if err := stream.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: drained stop: %v", err)
	}
	srcEng, _ := engines.Get("planetscale")
	tgtEng, _ := engines.Get(r.targetEngine())
	start := time.Now()
	err = (&AddTable{
		Source: srcEng, Target: tgtEng,
		SourceDSN: r.streamDSN, TargetDSN: r.tgtDSN,
		StreamID: p, TableName: add,
	}).Run(context.Background())
	return err, time.Since(start)
}

func TestStreamer_VStreamAddTableKeyspaceWideCopy(t *testing.T) {
	var logs logcapture.Buffer
	prev := slog.Default()
	level := slog.LevelInfo
	if os.Getenv("SLUICE_ATK_LOGDIR") != "" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	const keyspace = "commerce"

	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	proxy := startATKProxy(t, grpcEndpoint)
	vparams := "&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"
	streamDSN := fmt.Sprintf("%s&vstream_endpoint=%s%s", mysqlDSN, grpcEndpoint, vparams)
	proxyDSN := fmt.Sprintf("%s&vstream_endpoint=%s%s", mysqlDSN, proxy.addr(), vparams)

	// Exact in every measured run: the stream's restart replays the add's
	// window from its pre-add position.
	t.Run("Drained/PostgresTarget", func(t *testing.T) {
		tgtDSN, cleanupTgt := startPGTarget(t)
		defer cleanupTgt()
		r := vcrRig{mysqlDSN: mysqlDSN, streamDSN: streamDSN, tgtDSN: tgtDSN, logs: &logs}
		atkCell(t, r, proxyDSN, proxy, "dr")
	})
	t.Run("MySQLTarget", func(t *testing.T) {
		tgtDSN, cleanupTgt := startMySQLTarget(t)
		defer cleanupTgt()
		r := vcrRig{mysqlDSN: mysqlDSN, streamDSN: streamDSN, tgtDSN: tgtDSN, logs: &logs, tgtEngine: "mysql"}
		// The drained flow into the target the --no-drain cells use: exact,
		// the same pin as the Postgres cell.
		t.Run("Drained", func(t *testing.T) {
			atkCell(t, r, proxyDSN, proxy, "dm")
		})
		// --no-drain refuses (NO-DRAIN-VSTREAM-SOURCE). Before the refusal
		// both cells left 80 deletes and 10 updates of the added table stale
		// in every measured run, the cold-start one never delivering the
		// post-add marker either (see the header).
		t.Run("NoDrain/AfterColdStart", func(t *testing.T) {
			atkNoDrainRefusalCell(t, r, "nd", false)
		})
		t.Run("NoDrain/AfterWarmResume", func(t *testing.T) {
			atkNoDrainRefusalCell(t, r, "nw", true)
		})
	})
	// A measurement, not a pin (a loud failure, filed separately): refused in
	// every measured run, because the keyspace-wide copy buffers the
	// out-of-scope table past the 64 MiB default cap, and `schema add-table`
	// has no --max-buffer-bytes to raise it.
	t.Run("BufferCap", func(t *testing.T) {
		if os.Getenv("SLUICE_ATK_MEASURE") != "1" {
			t.Skip("reproduces VSTREAM-ADD-TABLE-KEYSPACE-WIDE-COPY (docs/dev/audit-backlog.md); set SLUICE_ATK_MEASURE=1 to measure")
		}
		tgtDSN, cleanupTgt := startPGTarget(t)
		defer cleanupTgt()
		r := vcrRig{mysqlDSN: mysqlDSN, streamDSN: streamDSN, tgtDSN: tgtDSN, logs: &logs}
		for i := 1; i <= atkRuns(); i++ {
			err, took := atkBufferCapCell(t, r, i)
			t.Logf("RUN %d: add of a 1,000-row table beside an 80 MB out-of-scope table: took=%v err=%v", i, took.Round(time.Millisecond), err)
			if err != nil {
				t.Errorf("RUN %d: the add refused: %v", i, err)
			}
		}
	})
}
