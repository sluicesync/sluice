//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the withdrawal of the VStream mid-COPY resume
// (VSTREAM-COPY-RESUME-WITHDRAWN, v0.163.3), against vttestserver with a
// Postgres target.
//
// A resumed VStream copy could not make the target match the source
// (VSTREAM-COPY-RESUME-RECREATED-TABLE, measured here before the
// withdrawal): Vitess copies again from the start every table with no
// cursor, the finished ones included, and drops their stopped-window
// changes, and the re-copy writes by upsert, which removes nothing. So a
// row deleted on the source while the stream was stopped stayed on the
// target, and so did every old row of a table dropped and re-created
// (rc_b: source 701, target 6701; pd_a: source 1156, target 1176). The
// cursor-less checkpoints before that were worse
// (VSTREAM-COPY-CURSORLESS-CHECKPOINT-WARM-RESUMES, fixed in v0.163.2: up to
// 298,000 of 300,000 rows never copied).
//
// Every cell drives the REAL Streamer through three phases:
//
//  1. A cold start, stopped once the big table holds vcrStopAfterRows of
//     its rows but not all. Throughout, and after the stop, the target holds
//     no persisted position: no mid-COPY checkpoint is written, so there is
//     nothing a next start could resume from.
//  2. With the stream stopped, the source changes (rows deleted, or every
//     table dropped and re-created), and a plain re-run of the same command
//     must refuse loudly: it exits with an error and never streams.
//  3. The refusal's remedy, the same command with --restart-from-scratch,
//     must leave every table exactly equal to the source while traffic
//     streams, the stopped-window deletes included.
//
// The verdict compares the SOURCE's ids (read off vtgate) against the
// TARGET's ids, never sluice's own state, logs or counters.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=60m -v \
//	  -run 'TestStreamer_VStreamCopyResume' ./internal/pipeline/

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// vcrBigRows sizes the table the stop lands in: large enough that the
// 50,000-row checkpoint cadence persists several mid-table cursors.
const vcrBigRows = 300_000

// vcrRig is one vttestserver source and one Postgres target shared by every
// cell; each cell uses its own table names and stream id.
type vcrRig struct {
	mysqlDSN  string // vtgate, no VStream params
	streamDSN string // vtgate + VStream params
	tgtDSN    string
	logs      *logcapture.Buffer
}

func (r vcrRig) src() twfbDB { return twfbDB{engine: "mysql", dsn: r.mysqlDSN} }
func (r vcrRig) tgt() twfbDB { return twfbDB{engine: "postgres", dsn: r.tgtDSN} }

// streamer builds the Streamer a `sync start --include-table ...` run would,
// with extraDSN appended to the source DSN (e.g. the single-stream opt-out).
func (r vcrRig) streamer(streamID string, include []string, extraDSN string) *Streamer {
	srcEng, ok := engines.Get("planetscale")
	if !ok {
		panic("engine not registered: planetscale")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		panic("engine not registered: postgres")
	}
	filter, err := migcore.NewTableFilter(include, nil)
	if err != nil {
		panic(err)
	}
	return &Streamer{
		Source: srcEng, Target: tgtEng,
		SourceDSN: r.streamDSN + extraDSN, TargetDSN: r.tgtDSN,
		StreamID:           streamID,
		Filter:             filter,
		ApplyRetryAttempts: 8,
	}
}

// vcrSeed inserts rows with the given explicit ids into table (id, payload).
func vcrSeed(t *testing.T, dsn, table, tag string, ids []int64) {
	t.Helper()
	const filler = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" // 80 chars
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = db.Close() }()
	const per = 1000
	for start := 0; start < len(ids); start += per {
		end := min(start+per, len(ids))
		var b strings.Builder
		b.WriteString("INSERT INTO " + table + " (id, payload) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,'%s-%s-%d')", ids[i], tag, filler, ids[i])
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, err := db.ExecContext(ctx, b.String())
		cancel()
		if err != nil {
			t.Fatalf("seed %s [%d,%d): %v", table, start, end, err)
		}
	}
}

// vcrRange returns ids from, from+step, ... for n ids.
func vcrRange(from, step int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = from + int64(i)*step
	}
	return out
}

func vcrCreate(t *testing.T, r vcrRig, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		r.src().exec(t, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, payload VARCHAR(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	}
}

// vcrIDs reads every id of table from db (nil when the table is absent).
func vcrIDs(t *testing.T, db twfbDB, table string) map[int64]bool {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// Keyset pages: vtgate caps one result at 10,000 rows by default, and a
	// swallowed cap error would read as an empty source.
	q := "SELECT id FROM " + table + " WHERE id > ? ORDER BY id LIMIT 5000"
	if db.engine == "postgres" {
		q = "SELECT id FROM " + table + " WHERE id > $1 ORDER BY id LIMIT 5000"
	}
	out := map[int64]bool{}
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
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan %s.%s: %v", db.engine, table, err)
			}
			out[id] = true
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

// vcrLogTail logs phase-2's WARN/ERROR lines and the resume/anchor lines,
// so a cell's report carries what the run itself said.
func vcrLogTail(t *testing.T, r vcrRig, from int) {
	t.Helper()
	shown := 0
	for _, line := range strings.Split(r.logs.String()[from:], "\n") {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") ||
			strings.Contains(line, "mid-COPY") || strings.Contains(line, "anchor persisted") ||
			strings.Contains(line, "resum") {
			if shown++; shown > 40 {
				t.Log("  ... (truncated)")
				return
			}
			t.Logf("  log: %s", line[:min(len(line), 500)])
		}
	}
}

// vcrDiff is one table's source-vs-target comparison.
type vcrDiff struct {
	table                 string
	source, target        int
	missing, extra        int     // ids on the source only / target only
	missingEx, extraEx    []int64 // a few examples of each
	missingMin, missingMx int64
}

func (d vcrDiff) exact() bool { return d.missing == 0 && d.extra == 0 }

func (d vcrDiff) String() string {
	return fmt.Sprintf("%s: source=%d target=%d missing-on-target=%d (e.g. %v, range %d..%d) extra-on-target=%d (e.g. %v)",
		d.table, d.source, d.target, d.missing, d.missingEx, d.missingMin, d.missingMx, d.extra, d.extraEx)
}

func vcrCompare(t *testing.T, r vcrRig, table string) vcrDiff {
	t.Helper()
	s, g := vcrIDs(t, r.src(), table), vcrIDs(t, r.tgt(), table)
	d := vcrDiff{table: table, source: len(s), target: len(g), missingMin: -1, missingMx: -1}
	var miss, extra []int64
	for id := range s {
		if !g[id] {
			miss = append(miss, id)
		}
	}
	for id := range g {
		if !s[id] {
			extra = append(extra, id)
		}
	}
	sort.Slice(miss, func(i, j int) bool { return miss[i] < miss[j] })
	sort.Slice(extra, func(i, j int) bool { return extra[i] < extra[j] })
	d.missing, d.extra = len(miss), len(extra)
	if len(miss) > 0 {
		d.missingMin, d.missingMx = miss[0], miss[len(miss)-1]
	}
	d.missingEx = miss[:min(5, len(miss))]
	d.extraEx = extra[:min(5, len(extra))]
	return d
}

// vcrStopAfterRows is where phase 1 stops: past the first COPY checkpoint
// tick (1,500–2,500 rows, measured), which until v0.163.3 could persist a
// position, so the no-position assertion is not vacuous.
const vcrStopAfterRows = 5000

// vcrStopMidCopyPersistingNothing runs phase 1 and stops it once big holds
// vcrStopAfterRows of its rows but not all. While the copy runs, and after
// the stop, the target must hold no persisted position for the stream.
func vcrStopMidCopyPersistingNothing(t *testing.T, r vcrRig, s *Streamer, big string, sourceRows int) {
	t.Helper()
	cell := twfbCell{tgt: r.tgt(), streamID: s.StreamID}
	run := startTWFBRun(s)
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if run.exited() || time.Now().After(deadline) {
			_ = run.stop(t)
			t.Fatalf("RIG: phase 1 exited or timed out before %s held part of its rows (%v)", big, run.err)
		}
		if tok := cell.persistedPosition(t); tok != "" {
			_ = run.stop(t)
			t.Fatalf("a position was persisted during the copy (%s holds %s of %d rows): %s",
				big, r.tgt().scalar(t, "SELECT count(*) FROM "+big), sourceRows, tok)
		}
		n := r.tgt().scalar(t, "SELECT count(*) FROM "+big)
		if got, err := strconv.Atoi(n); err == nil && got >= vcrStopAfterRows {
			if n == fmt.Sprint(sourceRows) {
				_ = run.stop(t)
				t.Fatalf("RIG: %s was fully copied before the stop", big)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: phase 1's stop returned %v", err)
	}
	if tok := cell.persistedPosition(t); tok != "" {
		t.Fatalf("the stopped copy left a persisted position: %s", tok)
	}
	t.Logf("phase 1 stopped mid-copy with no persisted position; %s target holds %d of %d rows",
		big, len(vcrIDs(t, r.tgt(), big)), sourceRows)
}

// vcrRequireRefusal runs phase 2, a plain re-run, which must exit with an
// error within three minutes without the stream ever persisting a position.
func vcrRequireRefusal(t *testing.T, r vcrRig, s *Streamer) {
	t.Helper()
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()
	deadline := time.Now().Add(3 * time.Minute)
	for !run.exited() {
		if time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			t.Fatal("the re-run after a stopped copy neither refused nor exited in 3 minutes")
		}
		time.Sleep(250 * time.Millisecond)
	}
	if run.err == nil {
		t.Fatal("the re-run after a stopped copy exited nil; want a loud refusal")
	}
	// The populated-target refusal by its code, so a rig failure (a boot or
	// connection error) cannot pass as "refused loudly".
	if ce, ok := sluicecode.FromError(run.err); !ok || ce.Code != sluicecode.CodeColdStartTargetNotEmpty {
		t.Fatalf("the re-run failed, but not with %s: %v", sluicecode.CodeColdStartTargetNotEmpty, run.err)
	}
	if tok := (twfbCell{tgt: r.tgt(), streamID: s.StreamID}).persistedPosition(t); tok != "" {
		t.Fatalf("the refused re-run persisted a position: %s", tok)
	}
	t.Logf("REFUSED (loud): %v", run.err)
}

// vcrRecopyExact runs phase 3, the refusal's remedy (s with
// RestartFromScratch), writing one row a second into traffic. Every compare
// table must match the source exactly within five minutes, with the run
// still going; an exit or a lasting difference fails the cell.
func vcrRecopyExact(t *testing.T, r vcrRig, s *Streamer, traffic string, compare []string) {
	t.Helper()
	if !s.RestartFromScratch {
		t.Fatal("RIG: phase 3 must run with --restart-from-scratch")
	}
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()
	const trafficBase = 8_000_000
	deadline := time.Now().Add(5 * time.Minute)
	for i := int64(0); ; i++ {
		if run.exited() {
			vcrLogTail(t, r, logStart)
			t.Fatalf("the --restart-from-scratch re-copy exited: %v", run.err)
		}
		vcrSeed(t, r.mysqlDSN, traffic, "traffic", []int64{trafficBase + i})
		exact := true
		for _, tbl := range compare {
			if !vcrCompare(t, r, tbl).exact() {
				exact = false
				break
			}
		}
		if exact {
			for _, tbl := range compare {
				t.Logf("EXACT: %s", vcrCompare(t, r, tbl))
			}
			return
		}
		if time.Now().After(deadline) {
			vcrLogTail(t, r, logStart)
			for _, tbl := range compare {
				if d := vcrCompare(t, r, tbl); !d.exact() {
					t.Errorf("after the --restart-from-scratch re-copy: %s", d)
				}
			}
			return
		}
		time.Sleep(time.Second)
	}
}

// vcrStopRefuseRecopy is one cell: phase 1 on mk(), whileStopped against
// the source, then the refused plain re-run and the exact re-copy.
func vcrStopRefuseRecopy(t *testing.T, r vcrRig, mk func() *Streamer, big string, sourceRows int, whileStopped func(), traffic string, compare []string) {
	t.Helper()
	vcrStopMidCopyPersistingNothing(t, r, mk(), big, sourceRows)
	if whileStopped != nil {
		whileStopped()
		time.Sleep(5 * time.Second) // the schema tracker
	}
	vcrRequireRefusal(t, r, mk())
	recopy := mk()
	recopy.RestartFromScratch = true
	vcrRecopyExact(t, r, recopy, traffic, compare)
}

func TestStreamer_VStreamCopyResume(t *testing.T) {
	const keyspace = "commerce"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r := vcrRig{
		mysqlDSN: mysqlDSN,
		streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
			mysqlDSN, grpcEndpoint),
		tgtDSN: tgtDSN,
		logs:   &logs,
	}
	const single = "&vstream_copy_single_stream=true"

	// The stopped-window DELETE that a resumed copy kept (pd_a: source
	// 1156, target 1176 before the withdrawal), on the default path: rows
	// deleted from the finished small table and from the copied part of the
	// big one.
	t.Run("DefaultPath/delete-while-stopped", func(t *testing.T) {
		const a, b = "pd_a", "pd_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopRefuseRecopy(t, r, func() *Streamer { return r.streamer("vcrpd", []string{a, b}, "") }, b, vcrBigRows,
			func() {
				r.src().exec(t, "DELETE FROM "+a+" WHERE id <= 20")
				r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 20")
			}, a, []string{a, b})
	})

	// Both tables dropped and re-created with disjoint ids while stopped:
	// the resumed copy kept every old row (rc_b: source 701, target 6701).
	t.Run("RecreatedTable/default-path", func(t *testing.T) {
		const a, b = "rc_a", "rc_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "old", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "old", vcrRange(2, 2, vcrBigRows)) // even ids 2..600000
		time.Sleep(3 * time.Second)
		vcrStopRefuseRecopy(t, r, func() *Streamer { return r.streamer("vcrrc", []string{a, b}, "") }, b, vcrBigRows,
			func() {
				r.src().exec(t, "DROP TABLE "+a)
				r.src().exec(t, "DROP TABLE "+b)
				vcrCreate(t, r, a, b)
				vcrSeed(t, r.mysqlDSN, a, "new", vcrRange(5001, 1, 500))
				vcrSeed(t, r.mysqlDSN, b, "new", vcrRange(1, 1000, 700)) // odd ids, none an old one
			}, a, []string{a, b})
	})

	// --restart-from-scratch with --schema-already-applied cannot clear the
	// target, so over a stopped copy's rows it must refuse
	// (RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED, review finding F2) rather
	// than re-copy by upsert and keep the rows deleted while stopped.
	t.Run("SchemaAlreadyApplied/restart-refuses-uncleared", func(t *testing.T) {
		const b = "sa_b"
		vcrCreate(t, r, b)
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopMidCopyPersistingNothing(t, r, r.streamer("vcrsa", []string{b}, ""), b, vcrBigRows)
		r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 20")
		before := len(vcrIDs(t, r.tgt(), b))
		s := r.streamer("vcrsa", []string{b}, "")
		s.RestartFromScratch, s.SchemaAlreadyApplied = true, true
		run := startTWFBRun(s)
		defer func() { _ = run.stop(t) }()
		deadline := time.Now().Add(3 * time.Minute)
		for !run.exited() {
			if time.Now().After(deadline) {
				t.Fatal("--restart-from-scratch --schema-already-applied over a populated target neither refused nor exited in 3 minutes")
			}
			time.Sleep(250 * time.Millisecond)
		}
		if run.err == nil || !strings.Contains(run.err.Error(), restartUnclearedMarker) {
			t.Fatalf("want the %s refusal; got %v", restartUnclearedMarker, run.err)
		}
		if after := len(vcrIDs(t, r.tgt(), b)); after != before {
			t.Fatalf("the refused run changed the target: %d rows before, %d after", before, after)
		}
		t.Logf("REFUSED (loud): %v", run.err)
	})

	// Changes made DURING an uninterrupted cold copy to rows already copied
	// (review finding F5 of the v0.163.3 pre-tag review): Vitess replays
	// them in the copy's catch-up, a DELETE carries only a before image, and
	// the copy-phase decoder reads only after images. Rows 1..100 are
	// deleted and row 200's key moved to 900000 once the target holds
	// 50,000 rows; after the handoff the target must equal the source.
	t.Run("ChangesDuringCopy/default-path", func(t *testing.T) {
		const b = "dc_b"
		vcrCreate(t, r, b)
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		s := r.streamer("vcrdc", []string{b}, "")
		logStart := r.logs.Len()
		run := startTWFBRun(s)
		defer func() { _ = run.stop(t) }()
		deadline := time.Now().Add(8 * time.Minute)
		for {
			if run.exited() || time.Now().After(deadline) {
				t.Fatalf("RIG: the copy exited or timed out before %s held 50,000 rows (%v)", b, run.err)
			}
			n, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+b))
			if n >= 50_000 {
				if n == vcrBigRows {
					t.Fatalf("RIG: %s was fully copied before the changes", b)
				}
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 100")
		r.src().exec(t, "UPDATE "+b+" SET id = 900000 WHERE id = 200")
		cell := twfbCell{tgt: r.tgt(), streamID: s.StreamID}
		if cell.persistedPosition(t) == "" {
			t.Log("the changes landed before the CDC handoff (no position persisted yet)")
		}
		for cell.persistedPosition(t) == "" {
			if run.exited() || time.Now().After(deadline) {
				vcrLogTail(t, r, logStart)
				t.Fatalf("the cold start never handed off to CDC (%v)", run.err)
			}
			time.Sleep(250 * time.Millisecond)
		}
		vcrSeed(t, r.mysqlDSN, b, "marker", []int64{9_000_001})
		if !r.tgt().waitRow(t, b, 9_000_001, run, 120*time.Second) {
			t.Fatalf("the marker never reached the target (%v)", run.err)
		}
		if d := vcrCompare(t, r, b); d.exact() {
			t.Logf("EXACT: %s", d)
		} else {
			t.Errorf("SILENT DIVERGENCE after an uninterrupted cold copy (stream running): %s", d)
		}
	})

	// The single-stream path (vstream_copy_single_stream=true) with two
	// tables, and a one-table scope, which takes the single-stream path too.
	t.Run("SingleStream/delete-while-stopped", func(t *testing.T) {
		const a, b = "ss_a", "ss_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopRefuseRecopy(t, r, func() *Streamer { return r.streamer("vcrss", []string{a, b}, single) }, b, vcrBigRows,
			func() { r.src().exec(t, "DELETE FROM "+a+" WHERE id <= 20") }, a, []string{a, b})
	})
	t.Run("SingleTableScope/delete-while-stopped", func(t *testing.T) {
		const b = "st_b"
		vcrCreate(t, r, b)
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopRefuseRecopy(t, r, func() *Streamer { return r.streamer("vcrst", []string{b}, "") }, b, vcrBigRows,
			func() { r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 20") }, b, []string{b})
	})
}

// TestStreamer_VStreamCopyResume_TwoShards runs the same phases on a
// two-shard keyspace (-80, 80-) with a hash vindex, where Vitess resumed
// each shard on its own (a started shard with no cursor skipped its share:
// 152,960 of 300,000 rows on the target before v0.163.2).
func TestStreamer_VStreamCopyResume_TwoShards(t *testing.T) {
	const keyspace = "commerce"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 2)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r := vcrRig{
		mysqlDSN: mysqlDSN,
		streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=-80,80-",
			mysqlDSN, grpcEndpoint),
		tgtDSN: tgtDSN,
		logs:   &logs,
	}
	createSharded := func(t *testing.T, tables ...string) {
		t.Helper()
		vcrCreate(t, r, tables...)
		for _, tbl := range tables {
			r.src().exec(t, "ALTER VSCHEMA ON "+keyspace+"."+tbl+" ADD VINDEX hash(id) USING hash")
		}
		time.Sleep(3 * time.Second) // schema-tracker settle
	}
	// The keys are globally unique (one id space, hash-sharded), so the
	// shards merge into one target table: the Bug 152 door's opt-out.
	merged := func(streamID string, include ...string) func() *Streamer {
		return func() *Streamer {
			s := r.streamer(streamID, include, "")
			s.AllowCrossShardMerge = true
			return s
		}
	}

	t.Run("DefaultPath/delete-while-stopped", func(t *testing.T) {
		const a, b = "sh_a", "sh_b"
		createSharded(t, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		vcrStopRefuseRecopy(t, r, merged("vcrsh", a, b), b, vcrBigRows,
			func() {
				r.src().exec(t, "DELETE FROM "+a+" WHERE id <= 20")
				r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 20")
			}, a, []string{a, b})
	})
	t.Run("SingleTableScope/delete-while-stopped", func(t *testing.T) {
		const b = "sh_t"
		createSharded(t, b)
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		vcrStopRefuseRecopy(t, r, merged("vcrsht", b), b, vcrBigRows,
			func() { r.src().exec(t, "DELETE FROM "+b+" WHERE id <= 20") }, b, []string{b})
	})
}
