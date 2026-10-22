//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Measurement of VSTREAM-COPY-RECONNECT-STALE-ROWS (docs/dev/audit-backlog.md):
// does the in-place mid-COPY reconnect (reconnectCopy, ADR-0072 Phase C),
// which re-opens the VStream from the last-observed VGTID and its TablePKs,
// leave rows on the target that the source deleted during the reconnect
// window?
//
// The fault is real and needs no product hook: the Streamer reaches vtgate's
// gRPC port through a TCP proxy defined here, which severs every open
// connection mid-copy. Recv fails with gRPC Unavailable (retriable), and
// reconnectCopy re-opens on the same ClientConn, which redials through the
// proxy. The proxy HOLDS that redial (accepts the TCP connection, forwards
// nothing) until the source changes are made, so every change lands after
// the resume position and before the resumed stream exists. It also caps
// vtgate→sluice throughput: unthrottled, a local vtgate pushes the whole copy
// into the client's gRPC buffers within seconds, and a severed connection
// then surfaces no Recv error at all.
//
// The source changes, per arm (big = the 200,000-row table the fault lands
// in, small = the 1,000-row one):
//
//   - arm 1-finished: small, already copied: ids 1..20 deleted, 101..110
//     updated. (0-not-started when small copies after big.)
//   - arm 2-below: big, rows already on the target: ids 1..20 deleted,
//     101..110 updated.
//   - arm 2-frontier: big's ids just past what the target held at the fault
//     (received or not; the target cannot tell): 20 deleted, 10 updated.
//   - arm 2-above: big's ids from 150,001, far past the frontier: 20
//     deleted, 10 updated.
//
// The verdict compares the SOURCE (id, payload) set read off vtgate with the
// TARGET's, after the CDC hand-off and after a marker written behind the
// changes reached every table. It never consults sluice's own state.
//
// MEASURED 2026-10-22 (vttestserver mysql80 = Vitess 24, Postgres 16 target,
// 3 runs per cell, reconnect window 2.2s: 200ms backoff + the held redial).
// Exact in every run: the default path with the small table finished first
// (one and two shards), and every no-fault control. Stale (deleted on the
// source, still on the target, at exit 0) in every run, updates always exact:
//
//   - single-stream opt-out, one shard: 1-finished 20/20, 2-below 20/20,
//     2-frontier 20/20, 2-above 0 (the finished table was re-received from
//     the start: 1,980 rows for a 1,000-row table);
//   - single-stream opt-out, two shards: 20/20, 20/20, 11-12/20 or 0, 0;
//   - default path, fault in the FIRST table: 2-below 20/20, 2-frontier
//     20/20 (two shards: 12-20/20), 2-above 0;
//   - one-table scope: 2-below 20/20, 2-frontier 20/20 (two shards: 8-20/20),
//     2-above 0.
//
// So the in-progress table's catch-up deletes are lost too (arm 2), not only
// a finished table's. The default path's small-first cell is exact only
// because the CDC tail resumes from the stitched minimum of the per-table
// snapshots (the finished table's, from before the fault), which replays the
// window's deletes.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	SLUICE_VRS_MEASURE=1 SLUICE_VRS_RUNS=3 go test -tags='integration vstream' \
//	  -count=1 -timeout=120m -v -run 'TestStreamer_VStreamCopyReconnectStaleRows' ./internal/pipeline/
//
// The reproducing cells skip unless SLUICE_VRS_MEASURE=1 (the item is open);
// each cell runs SLUICE_VRS_RUNS times (default 1).

package pipeline

import (
	"context"
	"database/sql"
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

	"sluicesync.dev/sluice/internal/logcapture"
)

// vrsBigRows sizes the in-progress table: large enough that the fault lands
// mid-copy on vttestserver.
const vrsBigRows = 200_000

// vrsFaultAtRows is how many of b's rows the target must hold before the
// fault, and vrsAboveBase is where the not-yet-copied band starts.
const (
	vrsFaultAtRows = 20_000
	vrsAboveBase   = 150_000
)

// vrsFaultBytes is the single-stream cells' trigger: wire bytes vtgate has
// sent, about 1,000 rows of a and 40,000 of b at vrsWireBytesPerRow.
const (
	vrsFaultBytes      = 6_500_000
	vrsWireBytesPerRow = 160 // default path: 3.43-3.62 MB sent at 20,000 target rows (rows buffered past the target included)
)

// vrsHold is how long the proxy holds the reconnect's redial after it
// arrives, on top of the time the source changes take.
const vrsHold = 2 * time.Second

// vrsProxy is a TCP proxy to vtgate's gRPC port that can sever every open
// connection and hold new ones until released.
type vrsProxy struct {
	ln     net.Listener
	target string

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	hold     chan struct{} // non-nil while holding; closed on release
	accepts  int
	heldSeen time.Time // first connection accepted while holding

	down atomic.Int64 // bytes forwarded vtgate→sluice since the last reset
}

func startVRSProxy(t *testing.T, target string) *vrsProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &vrsProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	t.Cleanup(func() { _ = ln.Close(); p.sever() })
	return p
}

func (p *vrsProxy) addr() string { return p.ln.Addr().String() }

func (p *vrsProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.accepts++
		hold := p.hold
		if hold != nil && p.heldSeen.IsZero() {
			p.heldSeen = time.Now()
		}
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		go p.serve(c, hold)
	}
}

func (p *vrsProxy) serve(c net.Conn, hold chan struct{}) {
	if hold != nil {
		<-hold
	}
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns[up] = struct{}{}
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	vrsThrottledCopy(c, up, &p.down)
	_ = c.Close()
}

// vrsDownstreamBytesPerSec caps vtgate→sluice throughput below the target's
// write rate. Unthrottled, a local vtgate pushes the whole copy into the
// client's gRPC buffers (BDP-sized windows) within seconds, so a severed
// connection surfaces no Recv error: the buffered stream, COPY_COMPLETED
// included, drains first (measured: the first cut faulted at 20,000 rows
// and the copy completed without a reconnect).
const vrsDownstreamBytesPerSec = 512 << 10

func vrsThrottledCopy(dst io.Writer, src io.Reader, counter *atomic.Int64) {
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
			counter.Add(int64(n))
			if ahead := time.Duration(float64(sent)/vrsDownstreamBytesPerSec*float64(time.Second)) - time.Since(start); ahead > 0 {
				time.Sleep(ahead)
			}
		}
		if err != nil {
			return
		}
	}
}

// faultAndHold makes every NEW connection wait, then severs every open one.
func (p *vrsProxy) faultAndHold() {
	p.mu.Lock()
	p.hold = make(chan struct{})
	p.heldSeen = time.Time{}
	p.mu.Unlock()
	p.sever()
}

func (p *vrsProxy) sever() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// release lets held and new connections through.
func (p *vrsProxy) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hold != nil {
		close(p.hold)
		p.hold = nil
	}
}

func (p *vrsProxy) firstHeld() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heldSeen
}

// vrsRows reads every (id, payload) of table (nil when the table is absent).
func vrsRows(t *testing.T, db twfbDB, table string) map[int64]string {
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

// vrsBand is one arm's id set and the changes made to it.
type vrsBand struct {
	arm     string
	table   string
	deleted []int64
	updated []int64
}

// vrsArmResult is one arm's outcome.
type vrsArmResult struct {
	arm                    string
	staleRows, staleVals   int // deleted on source but on target / updated but old value on target
	staleRowEx, staleValEx []int64
}

// vrsCellResult is one run's outcome.
type vrsCellResult struct {
	arms                     []vrsArmResult
	extraOther, missingOther map[string]int // differences outside the arms, per table
	bAtFault                 int
	downAtFault              int64
	byteTrigger              bool
	redialAfter, window      time.Duration
	changesTook              time.Duration
	reconnects               int
	handedOff                bool
	note                     string
}

func (r vrsCellResult) String() string {
	var b strings.Builder
	frontier := "b-on-target-at-fault"
	if r.byteTrigger {
		frontier = "b-received-at-fault(estimated)"
	}
	fmt.Fprintf(&b, "%s=%d wire-bytes-at-fault=%d reconnect-WARNs=%d changes-took=%v redial-after-fault=%v window(fault->release)=%v handed-off=%v",
		frontier, r.bAtFault, r.downAtFault, r.reconnects, r.changesTook.Round(time.Millisecond), r.redialAfter.Round(time.Millisecond), r.window.Round(time.Millisecond), r.handedOff)
	for _, a := range r.arms {
		fmt.Fprintf(&b, "\n    %-14s stale-rows=%d (e.g. %v) stale-values=%d (e.g. %v)", a.arm, a.staleRows, a.staleRowEx, a.staleVals, a.staleValEx)
	}
	fmt.Fprintf(&b, "\n    outside the arms: extra-on-target=%v missing-on-target=%v", r.extraOther, r.missingOther)
	if r.note != "" {
		fmt.Fprintf(&b, "\n    note: %s", r.note)
	}
	return b.String()
}

func (r vrsCellResult) diverged() bool {
	for _, a := range r.arms {
		if a.staleRows > 0 || a.staleVals > 0 {
			return true
		}
	}
	for _, n := range r.extraOther {
		if n > 0 {
			return true
		}
	}
	for _, n := range r.missingOther {
		if n > 0 {
			return true
		}
	}
	return !r.handedOff
}

const vrsReconnectLine = "COPY stream dropped; reconnecting in place from cursor"

// vrsLayout is where the small (1,000-row) table sits relative to the big
// one in the copy order (tables copy in name order, _a before _b).
type vrsLayout int

const (
	// vrsSmallFirst: the small table is finished when the fault lands in the
	// big one, the shape the item names.
	vrsSmallFirst vrsLayout = iota
	// vrsBigFirst: the fault lands in the first table; the small one has not
	// started. On the default path no earlier finished table pulls the
	// stitched CDC position back before the fault.
	vrsBigFirst
	// vrsBigOnly: one table, which takes the single-stream path.
	vrsBigOnly
)

// vrsRunCell runs one cold start with a fault mid-copy of the big table and
// measures the target against the source.
func vrsRunCell(t *testing.T, r vcrRig, proxy *vrsProxy, mk func() *Streamer, a, b string, layout vrsLayout, byteTrigger, noFault bool, sharded func(t *testing.T, tables ...string)) vrsCellResult {
	t.Helper()
	small, big := a, b
	switch layout {
	case vrsBigFirst:
		small, big = b, a
	case vrsBigOnly:
		small = ""
	}
	tables := []string{big}
	if small != "" {
		tables = []string{a, b}
	}
	vcrCreate(t, r, tables...)
	if sharded != nil {
		sharded(t, tables...)
	}
	if small != "" {
		vcrSeed(t, r.mysqlDSN, small, "v1", vcrRange(1, 1, 1000))
	}
	vcrSeed(t, r.mysqlDSN, big, "v1", vcrRange(1, 1, vrsBigRows))
	time.Sleep(3 * time.Second) // the schema tracker

	var res vrsCellResult
	res.byteTrigger = byteTrigger
	s := mk()
	proxy.down.Store(0)
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()

	// Wait until a is fully copied and b is mid-copy. On the default path
	// the target shows it. On the single-stream path it does not: sluice
	// buffers b's rows while a's read stays open until the global
	// COPY_COMPLETED, so a target-count trigger fires after vtgate has
	// finished (measured: the fault then reached only the CDC phase). There
	// the trigger is the bytes vtgate has sent, and b's received rows are
	// estimated from them.
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if run.exited() || time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			t.Fatalf("RIG: the copy exited or timed out before the fault point (%v)", run.err)
		}
		if byteTrigger {
			if down := proxy.down.Load(); down >= vrsFaultBytes {
				res.bAtFault = int(down / vrsWireBytesPerRow)
				if layout == vrsSmallFirst {
					res.bAtFault -= 1000
				}
				break
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		smallDone := true
		if layout == vrsSmallFirst {
			ns, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+small))
			smallDone = ns == 1000
		}
		nb, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+big))
		if smallDone && nb >= vrsFaultAtRows {
			res.bAtFault = nb
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	res.downAtFault = proxy.down.Load()
	if res.bAtFault >= vrsAboveBase-1000 {
		t.Fatalf("RIG: %s held about %d rows at the fault point; the above band would not be above", big, res.bAtFault)
	}
	front := int64(res.bAtFault)

	var bands []vrsBand
	switch layout {
	case vrsSmallFirst:
		bands = append(bands, vrsBand{arm: "1-finished", table: small, deleted: vcrRange(1, 1, 20), updated: vcrRange(101, 1, 10)})
	case vrsBigFirst:
		bands = append(bands, vrsBand{arm: "0-not-started", table: small, deleted: vcrRange(1, 1, 20), updated: vcrRange(101, 1, 10)})
	}
	bands = append(
		bands,
		vrsBand{arm: "2-below", table: big, deleted: vcrRange(1, 1, 20), updated: vcrRange(101, 1, 10)},
		vrsBand{arm: "2-frontier", table: big, deleted: vcrRange(front+1, 1, 20), updated: vcrRange(front+101, 1, 10)},
		vrsBand{arm: "2-above", table: big, deleted: vcrRange(vrsAboveBase+1, 1, 20), updated: vcrRange(vrsAboveBase+101, 1, 10)},
	)

	// The fault, then the changes while the redial is held. A control run
	// (noFault) makes the same changes at the same point with no fault.
	faultAt := time.Now()
	if !noFault {
		proxy.faultAndHold()
	}
	db, err := sql.Open("mysql", r.mysqlDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
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
	_ = db.Close()
	res.changesTook = time.Since(faultAt)
	// Wait for the reconnect's redial to arrive, then hold it vrsHold more.
	for !noFault && proxy.firstHeld().IsZero() {
		if time.Since(faultAt) > 60*time.Second || run.exited() {
			proxy.release()
			vcrLogTail(t, r, logStart)
			t.Fatalf("RIG: no reconnect redial within 60s of the fault (exited=%v err=%v)", run.exited(), run.err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !noFault {
		res.redialAfter = proxy.firstHeld().Sub(faultAt)
		time.Sleep(vrsHold)
		proxy.release()
		res.window = time.Since(faultAt)
	}

	// Hand-off, then a marker in each table written after the changes.
	cell := twfbCell{tgt: r.tgt(), streamID: s.StreamID}
	handoff := time.Now().Add(5 * time.Minute)
	for cell.persistedPosition(t) == "" {
		if run.exited() || time.Now().After(handoff) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	res.handedOff = cell.persistedPosition(t) != ""
	if res.handedOff {
		for _, tbl := range tables {
			vcrSeed(t, r.mysqlDSN, tbl, "marker", []int64{9_000_001})
		}
		for _, tbl := range tables {
			if !r.tgt().waitRow(t, tbl, 9_000_001, run, 120*time.Second) {
				res.note = fmt.Sprintf("%s's marker never reached the target (exited=%v err=%v)", tbl, run.exited(), run.err)
			}
		}
		time.Sleep(2 * time.Second)
	} else {
		res.note = fmt.Sprintf("no CDC hand-off within 5 minutes of the release (exited=%v err=%v); %s on target=%s of %d",
			run.exited(), run.err, big, r.tgt().scalar(t, "SELECT count(*) FROM "+big), vrsBigRows-40)
	}
	res.reconnects = strings.Count(r.logs.String()[logStart:], vrsReconnectLine)
	if dir := os.Getenv("SLUICE_VRS_LOGDIR"); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, s.StreamID+".log"), []byte(r.logs.String()[logStart:]), 0o600)
	}

	// The verdict, from the source and the target only.
	src, tgt := map[string]map[int64]string{}, map[string]map[int64]string{}
	inArm := map[string]map[int64]bool{}
	for _, tbl := range tables {
		src[tbl], tgt[tbl], inArm[tbl] = vrsRows(t, r.src(), tbl), vrsRows(t, r.tgt(), tbl), map[int64]bool{}
	}
	for _, bd := range bands {
		ar := vrsArmResult{arm: bd.arm + "(" + bd.table + ")"}
		for _, id := range bd.deleted {
			inArm[bd.table][id] = true
			if _, onSrc := src[bd.table][id]; onSrc {
				t.Fatalf("RIG: %s id %d still on the source after its delete", bd.table, id)
			}
			if _, onTgt := tgt[bd.table][id]; onTgt {
				ar.staleRows++
				if len(ar.staleRowEx) < 5 {
					ar.staleRowEx = append(ar.staleRowEx, id)
				}
			}
		}
		for _, id := range bd.updated {
			inArm[bd.table][id] = true
			want := fmt.Sprintf("upd-%d", id)
			if src[bd.table][id] != want {
				t.Fatalf("RIG: %s id %d reads %q on the source, want %q", bd.table, id, src[bd.table][id], want)
			}
			if got, ok := tgt[bd.table][id]; !ok || got != want {
				ar.staleVals++
				if len(ar.staleValEx) < 5 {
					ar.staleValEx = append(ar.staleValEx, id)
				}
			}
		}
		res.arms = append(res.arms, ar)
	}
	res.extraOther, res.missingOther = map[string]int{}, map[string]int{}
	for _, tbl := range tables {
		var extra, missing []int64
		for id, sv := range src[tbl] {
			if inArm[tbl][id] {
				continue
			}
			if tv, ok := tgt[tbl][id]; !ok {
				missing = append(missing, id)
			} else if tv != sv {
				missing = append(missing, id) // a value difference outside the arms
			}
		}
		for id := range tgt[tbl] {
			if _, ok := src[tbl][id]; !ok && !inArm[tbl][id] {
				extra = append(extra, id)
			}
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		res.extraOther[tbl], res.missingOther[tbl] = len(extra), len(missing)
		if len(missing) > 0 {
			t.Logf("  %s: %d source rows missing or different on the target outside the arms, e.g. %v", tbl, len(missing), missing[:min(5, len(missing))])
		}
	}
	if res.reconnects == 0 && !noFault {
		vcrLogTail(t, r, logStart)
	}
	return res
}

// vrsCell runs one mode vrsRuns times and reports per run. A cell whose runs
// reproduce a divergence fails, or skips (pointing at the item) unless
// SLUICE_VRS_MEASURE=1 — decided by the measurement recorded in knownStale.
func vrsCell(t *testing.T, r vcrRig, proxy *vrsProxy, m vrsMode, mk func(streamID string, include []string) *Streamer, sharded func(t *testing.T, tables ...string)) {
	t.Helper()
	if m.knownStale && os.Getenv("SLUICE_VRS_MEASURE") != "1" {
		t.Skip("reproduces VSTREAM-COPY-RECONNECT-STALE-ROWS (docs/dev/audit-backlog.md); set SLUICE_VRS_MEASURE=1 to measure")
	}
	runs := vrsRuns()
	for i := 1; i <= runs; i++ {
		a, b := fmt.Sprintf("%s%d_a", m.prefix, i), fmt.Sprintf("%s%d_b", m.prefix, i)
		include := []string{a, b}
		if m.layout == vrsBigOnly {
			include = []string{b}
		}
		streamID := fmt.Sprintf("%s%d", m.prefix, i)
		res := vrsRunCell(t, r, proxy, func() *Streamer { return mk(streamID, include) }, a, b, m.layout, m.byteTrigger, m.noFault, sharded)
		t.Logf("RUN %d/%d: %s", i, runs, res)
		switch {
		case m.noFault && res.reconnects != 0:
			t.Errorf("RUN %d: RIG: the no-fault control reconnected", i)
		case !m.noFault && res.reconnects == 0:
			t.Errorf("RUN %d: RIG: no in-place reconnect was logged, so the run did not measure reconnectCopy", i)
		}
		if res.diverged() {
			t.Errorf("RUN %d: SILENT DIVERGENCE (stream running, no-fault control=%v): %s", i, m.noFault, res)
		}
	}
}

// vrsRuns is how many times each cell runs: SLUICE_VRS_RUNS, default 1 (the
// weekly leg's budget); the measurement recorded in the item used 3.
func vrsRuns() int {
	if n, err := strconv.Atoi(os.Getenv("SLUICE_VRS_RUNS")); err == nil && n > 0 {
		return n
	}
	return 1
}

// vrsMode is one cell's shape.
type vrsMode struct {
	prefix string
	layout vrsLayout
	// knownStale skips the cell (unless SLUICE_VRS_MEASURE=1): it reproduced
	// the item in every measured run.
	knownStale bool
	// byteTrigger faults on vtgate's sent bytes (the single-stream cells).
	byteTrigger bool
	// noFault is the control: the same changes at the same point, no fault.
	noFault bool
}

func TestStreamer_VStreamCopyReconnectStaleRows(t *testing.T) {
	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	const keyspace = "commerce"
	const single = "&vstream_copy_single_stream=true"

	t.Run("OneShard", func(t *testing.T) {
		mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
		defer cleanupSrc()
		tgtDSN, cleanupTgt := startPGTarget(t)
		defer cleanupTgt()
		proxy := startVRSProxy(t, grpcEndpoint)
		r := vcrRig{
			mysqlDSN: mysqlDSN,
			streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
				mysqlDSN, proxy.addr()),
			tgtDSN: tgtDSN,
			logs:   &logs,
		}
		plain := func(extra string) func(id string, inc []string) *Streamer {
			return func(id string, inc []string) *Streamer { return r.streamer(id, inc, extra) }
		}
		t.Run("DefaultPath", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "rd", layout: vrsSmallFirst}, plain(""), nil)
		})
		t.Run("FaultInFirstTable", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "rf", layout: vrsBigFirst, knownStale: true}, plain(""), nil)
		})
		t.Run("SingleTableScope", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "ro", layout: vrsBigOnly, knownStale: true}, plain(""), nil)
		})
		t.Run("SingleStream", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "rs", layout: vrsSmallFirst, knownStale: true, byteTrigger: true}, plain(single), nil)
		})
		// Controls: the same changes at the same point with no fault.
		t.Run("NoFaultControl/FirstTable", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "cf", layout: vrsBigFirst, noFault: true}, plain(""), nil)
		})
		t.Run("NoFaultControl/SingleTableScope", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "co", layout: vrsBigOnly, noFault: true}, plain(""), nil)
		})
		t.Run("NoFaultControl/SingleStream", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "cs", layout: vrsSmallFirst, byteTrigger: true, noFault: true}, plain(single), nil)
		})
	})

	t.Run("TwoShards", func(t *testing.T) {
		mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 2)
		defer cleanupSrc()
		tgtDSN, cleanupTgt := startPGTarget(t)
		defer cleanupTgt()
		proxy := startVRSProxy(t, grpcEndpoint)
		r := vcrRig{
			mysqlDSN: mysqlDSN,
			streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=-80,80-",
				mysqlDSN, proxy.addr()),
			tgtDSN: tgtDSN,
			logs:   &logs,
		}
		sharded := func(t *testing.T, tables ...string) {
			for _, tbl := range tables {
				r.src().exec(t, "ALTER VSCHEMA ON "+keyspace+"."+tbl+" ADD VINDEX hash(id) USING hash")
			}
		}
		merged := func(extra string) func(id string, inc []string) *Streamer {
			return func(id string, inc []string) *Streamer {
				s := r.streamer(id, inc, extra)
				s.AllowCrossShardMerge = true
				return s
			}
		}
		t.Run("DefaultPath", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "td", layout: vrsSmallFirst}, merged(""), sharded)
		})
		t.Run("FaultInFirstTable", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "tf", layout: vrsBigFirst, knownStale: true}, merged(""), sharded)
		})
		t.Run("SingleTableScope", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "to", layout: vrsBigOnly, knownStale: true}, merged(""), sharded)
		})
		t.Run("SingleStream", func(t *testing.T) {
			vrsCell(t, r, proxy, vrsMode{prefix: "ts", layout: vrsSmallFirst, knownStale: true, byteTrigger: true}, merged(single), sharded)
		})
	})
}
