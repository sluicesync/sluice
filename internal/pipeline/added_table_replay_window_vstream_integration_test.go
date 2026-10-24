//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 333 on a VStream source: the drained `schema add-table` replay window.
//
// A VStream source's add copies the whole keyspace in one stream, table by
// table, and vttablet replays the changes made to an already-copied table
// inside that copy, before the next table. v0.163.9 made the copy upsert
// such an INSERT and DEFER a key change or DELETE to the restarted stream,
// whose replay addresses an UPDATE by its whole before-image. So a key change
// X→-X followed by an INSERT reusing X, both inside the copy, met the reused
// row at X, missed (a tolerated zero-row miss, DEBUG only), and the moved row
// was never written: measured 22/24/28 rows lost at exit 0 into Postgres and
// MySQL targets (sluice-testing workspace/b333/cell.sh, v0.163.9). A key
// change the copy had already seen, on a table created while the stream was
// stopped, wedged the restart on a duplicate key.
//
// Every cell drives the real Streamer and AddTable against vttestserver, the
// add through a throttling proxy so its copy is in flight when the source
// changes, and grades the target against the SOURCE's own (id, payload) set.
// Each cell also requires its rig evidence: an in-copy cell must see the
// copy's deferral line count some of its changes (or none reached the copy's
// replay), and every cell must see the window open and close (or nothing was
// replayed through it).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=90m -v \
//	  -run 'TestStreamer_AddedTableReplayWindow_VStream' ./internal/pipeline/

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
)

const (
	vrwEstRows = 500
	vrwAddRows = 20_000
	vrwBigRows = 100_000
	vrwOps     = 50
)

// vrwCell is one drained add on the VStream source.
type vrwCell struct {
	name string
	// late: the added table is created and filled while the stream is
	// stopped; otherwise it exists, filled, before the stream starts.
	late bool
	ops  string // "reuse", "kc", "delreins" or "seq336" (see vrwStatements)
	// inCopy runs the changes, one key's group every 50 ms, for as long as
	// the add's copy runs (some land in vttablet's in-copy replay, which the
	// copy defers, the rest before or after it — the measured writer's
	// shape); otherwise they all run while the stream is stopped, before the
	// add.
	inCopy bool
}

var vrwCells = []vrwCell{
	{name: "ReuseInCopy", ops: "reuse", inCopy: true}, // the silent arm
	{name: "KeyChangeInCopy", ops: "kc", inCopy: true},
	{name: "DeleteReinsertInCopy", ops: "delreins", inCopy: true},
	{name: "LateKeyChangeBeforeCopy", late: true, ops: "kc"}, // the VStream wedge
	{name: "ReuseBeforeCopy", ops: "reuse"},
	{name: "Bug336InCopy", ops: "seq336", inCopy: true}, // exact before the fix too; pinned so it stays so
	{name: "Bug336BeforeCopy", ops: "seq336"},
}

// vrwGroups renders a cell's source changes, one group per key X = i*300+7.
func vrwGroups(kind, table string) [][]string {
	var out [][]string
	for i := 1; i <= vrwOps; i++ {
		x := int64(i*300 + 7)
		switch kind {
		case "reuse":
			out = append(out, []string{
				fmt.Sprintf("UPDATE %s SET id = %d, payload = 'kc-%d' WHERE id = %d", table, -x, x, x),
				fmt.Sprintf("INSERT INTO %s (id, payload) VALUES (%d, 'reuse-%d')", table, x, x),
			})
		case "kc":
			out = append(out, []string{fmt.Sprintf("UPDATE %s SET id = %d, payload = 'kc-%d' WHERE id = %d", table, -x, x, x)})
		case "delreins":
			out = append(out, []string{
				fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, x),
				fmt.Sprintf("INSERT INTO %s (id, payload) VALUES (%d, 'again-%d')", table, x, x),
			})
		case "seq336":
			w := x + 150
			out = append(out, []string{
				fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", table, -x, x),
				fmt.Sprintf("UPDATE %s SET id = %d, payload = 'm2-%d' WHERE id = %d", table, x, x, w),
				fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, -x),
			})
		default:
			panic("unknown op kind " + kind)
		}
	}
	return out
}

// vrwWriter runs groups one every 50 ms until stop closes or they run out,
// returning how many ran and the first error.
func vrwWriter(dsn string, groups [][]string, stop <-chan struct{}) (ran int, err error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	for _, g := range groups {
		select {
		case <-stop:
			return ran, nil
		default:
		}
		for _, s := range g {
			if _, err := db.Exec(s); err != nil {
				return ran, fmt.Errorf("source %q: %w", s, err)
			}
		}
		ran++
		time.Sleep(50 * time.Millisecond)
	}
	return ran, nil
}

// vrwExec runs statements one at a time through vtgate.
func vrwExec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("source %q: %v", s, err)
		}
	}
}

// vrwDeferred is the count the copy's deferral line reports for table, 0
// when there is none.
func vrwDeferred(logs, table string) int {
	re := regexp.MustCompile(`left them to the change stream[^\n]*table=` + regexp.QuoteMeta(table) + ` changes=(\d+)`)
	m := re.FindStringSubmatch(logs)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

func vrwRunCell(t *testing.T, r vcrRig, proxyDSN string, proxy *atkProxy, c vrwCell, p string) {
	t.Helper()
	est, add, big := p+"_1est", p+"_2add", p+"_3big"
	vcrCreate(t, r, est, big)
	if !c.late {
		vcrCreate(t, r, add)
	}
	defer func() {
		for _, tbl := range []string{est, add, big} {
			r.src().exec(t, "DROP TABLE IF EXISTS "+tbl)
		}
	}()
	vcrSeed(t, r.mysqlDSN, est, "v1", vcrRange(1, 1, vrwEstRows))
	vcrSeed(t, r.mysqlDSN, big, "v1", vcrRange(1, 1, vrwBigRows))
	if !c.late {
		vcrSeed(t, r.mysqlDSN, add, "v1", vcrRange(1, 1, vrwAddRows))
	}
	time.Sleep(3 * time.Second) // the schema tracker
	logStart := r.logs.Len()
	defer func() {
		if !t.Failed() {
			return
		}
		shown := 0
		for _, line := range strings.Split(r.logs.String()[logStart:], "\n") {
			if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") || strings.Contains(line, "ADDED-TABLE-") ||
				strings.Contains(line, "left them to the change stream") {
				if shown++; shown > 40 {
					return
				}
				t.Logf("  log: %s", line[:min(len(line), 500)])
			}
		}
	}()

	stream, mk := atkEstablish(t, r, p, est, false)
	if err := stream.stop(t); err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("RIG: drained stop: %v", err)
	}
	if c.late {
		vcrCreate(t, r, add)
		vcrSeed(t, r.mysqlDSN, add, "v1", vcrRange(1, 1, vrwAddRows))
		time.Sleep(3 * time.Second)
	}
	groups := vrwGroups(c.ops, add)
	if !c.inCopy {
		for _, g := range groups {
			vrwExec(t, r.mysqlDSN, g...)
		}
	}

	pe, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("engine not registered: planetscale")
	}
	tgtEng, _ := engines.Get(r.targetEngine())
	addDone := make(chan error, 1)
	stopWriter := make(chan struct{})
	type writerResult struct {
		ran int
		err error
	}
	writerDone := make(chan writerResult, 1)
	if c.inCopy {
		go func() {
			ran, err := vrwWriter(r.mysqlDSN, groups, stopWriter)
			writerDone <- writerResult{ran, err}
		}()
		time.Sleep(300 * time.Millisecond) // the measured writer started just before the add
	}
	go func() {
		addDone <- (&AddTable{Source: pe, Target: tgtEng, SourceDSN: proxyDSN, TargetDSN: r.tgtDSN, StreamID: p, TableName: add}).Run(context.Background())
	}()
	select {
	case err := <-addDone:
		close(stopWriter)
		if err != nil {
			t.Fatalf("drained AddTable: %v", err)
		}
	case <-time.After(10 * time.Minute):
		t.Fatal("RIG: the add did not return within 10 minutes")
	}
	if c.inCopy {
		w := <-writerDone
		if w.err != nil {
			t.Fatalf("RIG: writer: %v", w.err)
		}
		t.Logf("the writer ran %d of %d groups during the add", w.ran, len(groups))
	}
	deferred := vrwDeferred(r.logs.String()[logStart:], add)
	t.Logf("the copy deferred %d replayed change(s) of %s", deferred, add)
	// Every in-copy op set holds a key change or a DELETE, which the copy
	// defers; none deferred means the changes missed the copy's replay.
	if c.inCopy && deferred == 0 {
		t.Fatalf("RIG: the copy deferred none of the in-copy changes; they did not reach the copy's replay")
	}

	stream = startTWFBRun(func() *Streamer {
		// Batched apply, as the binary runs it (a late table's whole seed replays).
		s := mk()
		s.ApplyBatchSize = 100
		return s
	}())
	defer func() { _ = stream.stop(t) }()
	for _, tbl := range []string{est, add} {
		vcrSeed(t, r.mysqlDSN, tbl, "marker", []int64{atkMarkerID})
	}
	for _, tbl := range []string{est, add} {
		if !r.tgt().waitRow(t, tbl, atkMarkerID, stream, 180*time.Second) {
			t.Fatalf("WEDGED: %s's post-add marker never reached the target (exited=%v err=%v)", tbl, stream.exited(), stream.err)
		}
	}
	time.Sleep(2 * time.Second)

	src, tgt := atkRows(t, r.src(), add), atkRows(t, r.tgt(), add)
	missing, extra, stale := rwVerdict(src, tgt)
	t.Logf("%s: source %d rows, target %d: missing %d %v, extra %d %v, stale %d %v",
		c.name, len(src), len(tgt), len(missing), rwHead(missing), len(extra), rwHead(extra), len(stale), rwHead(stale))
	if len(missing)+len(extra)+len(stale) > 0 {
		t.Errorf("DIVERGED after the drained add's restart: missing %d, extra %d, stale %d", len(missing), len(extra), len(stale))
	}
	tail := r.logs.String()[logStart:]
	opened := strings.Contains(tail, AddedTableReplayWindowMarker+": the stream resumes from before this added table's copy")
	closed := strings.Contains(tail, AddedTableReplayWindowMarker+": the stream is past this added table's copy position")
	if !opened || !closed {
		t.Errorf("RIG: the replay window did not open and close (opened=%v closed=%v)", opened, closed)
	}
}

func TestStreamer_AddedTableReplayWindow_VStream(t *testing.T) {
	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	const keyspace = "commerce"

	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	proxy := startATKProxy(t, grpcEndpoint)
	vparams := "&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"
	streamDSN := fmt.Sprintf("%s&vstream_endpoint=%s%s", mysqlDSN, grpcEndpoint, vparams)
	proxyDSN := fmt.Sprintf("%s&vstream_endpoint=%s%s", mysqlDSN, proxy.addr(), vparams)

	for _, target := range []string{"postgres", "mysql"} {
		t.Run(target, func(t *testing.T) {
			var tgtDSN string
			var cleanupTgt func()
			if target == "postgres" {
				tgtDSN, cleanupTgt = startPGTarget(t)
			} else {
				tgtDSN, cleanupTgt = startMySQLTarget(t)
			}
			defer cleanupTgt()
			r := vcrRig{mysqlDSN: mysqlDSN, streamDSN: streamDSN, tgtDSN: tgtDSN, logs: &logs}
			if target == "mysql" {
				r.tgtEngine = "mysql"
			}
			for i, c := range vrwCells {
				t.Run(c.name, func(t *testing.T) {
					vrwRunCell(t, r, proxyDSN, proxy, c, fmt.Sprintf("rw%s%d", target[:1], i))
				})
			}
		})
	}
}
