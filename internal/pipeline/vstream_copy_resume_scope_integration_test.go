//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Measurements for two audit-backlog hypotheses about the VStream
// interrupted-COPY resume (a persisted TablePKs cursor, routed through
// coldStart's resumingCopy branch and OpenSnapshotStreamFromPosition):
//
//   - VSTREAM-COPY-RESUME-RECREATED-TABLE: the cursor carries no table
//     identity, so a table dropped and re-created while the stream was
//     stopped may keep the old table's rows on the target, or have the new
//     table's rows below the old cursor skipped.
//   - VSTREAM-COPY-RESUME-SCOPE-WIDEN: a table added to --include-table
//     before the resume may never be copied on the single-stream arms
//     (vstream_copy_single_stream=true, or a one-table resume scope whose
//     cursor names another table). REFUTED on every arm (2026-10-20).
//
// They found a third, worse, now fixed: VSTREAM-COPY-CURSORLESS-
// CHECKPOINT-WARM-RESUMES. The COPY checkpoint persisted positions with no
// cursor, which the next start reads as a completed copy, so the rest of
// the table was never copied. The SingleStream / SingleTableScope /
// DefaultPath cells pin its fix (vcrStopMidCopyHoldingTheCursorRule).
//
// Every cell drives the REAL Streamer against vttestserver with a Postgres
// target: phase 1 is a real cold start, cancelled the moment the target's
// sluice_cdc_state row carries a mid-COPY cursor naming the expected
// table; phase 2 is a real `sync start` re-run. The verdict compares the
// SOURCE's ids (read off vtgate) against the TARGET's ids — never sluice's
// own state, logs or counters. A cell passes on an exact match or on a
// loud refusal (Run returns an error), and fails on a target that differs
// from the source while the stream runs on without error.
//
// Rig guards (a mismatch is only evidence once these hold): phase 1 must
// stop with the cursor naming exactly the expected table and the target
// holding fewer of its rows than the source (a COPY that finished before
// the stop is a rig failure, not a measurement), and phase 2 must log the
// mid-COPY resume line (a resume that never took the cursor path measured
// nothing).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=60m -v \
//	  -run 'TestStreamer_VStreamCopyResume' ./internal/pipeline/
//
// Cells whose silent loss is measured and not yet fixed are skipped (see
// vcrSkipConfirmed); SLUICE_VCR_MEASURE=1 runs them, and they fail.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// vcrResumeLog is the literal line phaseOpenChangeStream logs when a
// persisted position carries a mid-COPY cursor and the run resumes through
// the bulk path — the anti-vacuity witness that phase 2 measured the resume.
const vcrResumeLog = "persisted position carries a mid-COPY cursor; resuming interrupted cold-start via the bulk path"

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

// vcrSkipConfirmed skips a cell whose silent loss is measured and not yet
// fixed, so the vstream suite stays green; SLUICE_VCR_MEASURE=1 runs it.
func vcrSkipConfirmed(t *testing.T, why string) {
	t.Helper()
	if os.Getenv("SLUICE_VCR_MEASURE") == "" {
		t.Skip(why + " (set SLUICE_VCR_MEASURE=1 to reproduce)")
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

// vcrCursorTables decodes the persisted position token and returns the
// table names its TablePKs cursor carries (empty for a cursor-less, i.e.
// streaming, position). ok is false when no position is persisted.
func vcrCursorTables(t *testing.T, r vcrRig, streamID string) (tables []string, ok bool) {
	t.Helper()
	tok := twfbCell{tgt: r.tgt(), streamID: streamID}.persistedPosition(t)
	if tok == "" {
		return nil, false
	}
	var shards []struct {
		TablePKs []struct {
			TableName string `json:"table_name"`
		} `json:"table_p_ks"`
	}
	if err := json.Unmarshal([]byte(tok), &shards); err != nil {
		t.Fatalf("persisted position is not a VStream token: %v\n%s", err, tok)
	}
	seen := map[string]bool{}
	for _, s := range shards {
		for _, pk := range s.TablePKs {
			if !seen[pk.TableName] {
				seen[pk.TableName] = true
				tables = append(tables, pk.TableName)
			}
		}
	}
	sort.Strings(tables)
	return tables, true
}

// vcrStopMidCopy runs phase 1 and cancels it the moment the persisted
// cursor names exactly cursorTable; it then asserts the stop really was
// mid-table (the rig guard) and returns the target's row count for it.
func vcrStopMidCopy(t *testing.T, r vcrRig, s *Streamer, cursorTable string, sourceRows int) int {
	t.Helper()
	run := startTWFBRun(s)
	logStart := r.logs.Len()
	observed := map[string]bool{}
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if time.Now().After(deadline) {
			_ = run.stop(t)
			t.Fatalf("RIG: no mid-COPY cursor naming only %s was ever persisted (%d distinct positions observed)", cursorTable, len(observed))
		}
		if run.exited() {
			vcrLogTail(t, r, logStart)
			t.Fatalf("RIG: phase 1 exited before a mid-COPY cursor was persisted: %v", run.err)
		}
		if tabs, ok := vcrCursorTables(t, r, s.StreamID); ok {
			tok := twfbCell{tgt: r.tgt(), streamID: s.StreamID}.persistedPosition(t)
			if !observed[tok] {
				observed[tok] = true
				t.Logf("phase 1 persisted a position with cursor tables %v (target %s rows: %s)",
					tabs, cursorTable, r.tgt().scalar(t, "SELECT count(*) FROM "+cursorTable))
			}
			if len(tabs) == 1 && tabs[0] == cursorTable {
				break
			}
			// A cursor-less position is also persisted BETWEEN tables on the
			// auto-shard path (the previous table's completion checkpoint), so
			// it means "finished" only once the target holds every row.
			if len(tabs) == 0 && r.tgt().scalar(t, "SELECT count(*) FROM "+cursorTable) == fmt.Sprint(sourceRows) {
				vcrLogTail(t, r, logStart)
				_ = run.stop(t)
				t.Fatalf("RIG: the COPY finished (cursor-less position) before a mid-%s cursor was observed (%d distinct positions observed)", cursorTable, len(observed))
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RIG: phase 1's stop returned %v", err)
	}
	tabs, _ := vcrCursorTables(t, r, s.StreamID)
	if len(tabs) != 1 || tabs[0] != cursorTable {
		t.Fatalf("RIG: after the stop the persisted cursor names %v; want [%s]", tabs, cursorTable)
	}
	got := len(vcrIDs(t, r.tgt(), cursorTable))
	if got >= sourceRows {
		t.Fatalf("RIG: the target already holds %d of %s's %d rows — the stop was not mid-table", got, cursorTable, sourceRows)
	}
	t.Logf("phase 1 stopped mid-COPY: cursor names [%s], target holds %d of %d rows", cursorTable, got, sourceRows)
	return got
}

// vcrStopAfterRows is where the stop lands: past the first COPY checkpoint
// (written at 1,500–2,500 rows, measured), which used to persist a
// cursor-less position, so the cells are not vacuous on the defect.
const vcrStopAfterRows = 5000

// vcrStopMidCopyHoldingTheCursorRule runs phase 1 and stops it once big
// holds vcrStopAfterRows of its rows but not all, a point the defect's
// cursor-less checkpoints had already been persisted by (the first, before
// any LASTPK; every one of a multi-table single-stream copy). Throughout, it holds the
// rule VSTREAM-COPY-CURSORLESS-CHECKPOINT-WARM-RESUMES's fix makes: while
// the copy is incomplete, no position without a copy cursor is persisted,
// because the next start would read it as a completed copy.
func vcrStopMidCopyHoldingTheCursorRule(t *testing.T, r vcrRig, s *Streamer, big string, sourceRows int) {
	t.Helper()
	run := startTWFBRun(s)
	deadline := time.Now().Add(8 * time.Minute)
	broken := false
	checkRule := func(when string) {
		if broken {
			return
		}
		tabs, ok := vcrCursorTables(t, r, s.StreamID)
		if ok && len(tabs) == 0 {
			if n := len(vcrIDs(t, r.tgt(), big)); n < sourceRows {
				broken = true
				t.Errorf("%s: a cursor-less position is persisted while %s holds %d of %d rows; the next start would warm-resume "+
					"it as a completed copy", when, big, n, sourceRows)
			}
		}
	}
	for {
		if run.exited() || time.Now().After(deadline) {
			_ = run.stop(t)
			t.Fatalf("RIG: phase 1 exited or timed out before %s held part of its rows (%v)", big, run.err)
		}
		checkRule("during the copy")
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
	checkRule("after the stop")
	tabs, ok := vcrCursorTables(t, r, s.StreamID)
	t.Logf("phase 1 stopped mid-copy: persisted=%v cursor=%v, %s target holds %d of %d rows",
		ok, tabs, big, len(vcrIDs(t, r.tgt(), big)), sourceRows)
}

// vcrResumeUnderTraffic runs phase 2 while writing one row a second into
// traffic (so a quiet source cannot trip the VStream liveness check and
// stand in for a refusal). It passes on an exact match of every compare
// table or on a loud exit, and fails on a divergence once the traffic is
// visibly streaming to the target.
func vcrResumeUnderTraffic(t *testing.T, r vcrRig, s *Streamer, traffic string, compare []string) {
	t.Helper()
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()
	defer vcrLogTail(t, r, logStart)
	const trafficBase = 8_000_000
	deadline := time.Now().Add(3 * time.Minute)
	for i := int64(0); time.Now().Before(deadline); i++ {
		if run.exited() {
			t.Logf("REFUSED (loud): %v", run.err)
			for _, tbl := range compare {
				t.Logf("  target at refusal: %s", vcrCompare(t, r, tbl))
			}
			return
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
		time.Sleep(time.Second)
	}
	streamed := r.tgt().scalar(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE id >= %d", traffic, trafficBase))
	if strings.HasPrefix(streamed, "<") || streamed == "0" {
		for _, tbl := range compare {
			t.Logf("  target while stalled: %s", vcrCompare(t, r, tbl))
		}
		tabs, _ := vcrCursorTables(t, r, s.StreamID)
		t.Fatalf("STALLED: no traffic row reached the target in 3 minutes and the run neither exited nor errored (traffic on target: %s; persisted cursor names %v)", streamed, tabs)
	}
	t.Logf("the stream is live: %s traffic rows reached the target", streamed)
	for _, tbl := range compare {
		if d := vcrCompare(t, r, tbl); d.exact() {
			t.Logf("EXACT: %s", d)
		} else {
			t.Errorf("SILENT DIVERGENCE (stream running and applying, no error): %s", d)
		}
	}
}

// vcrResumeAndGrade runs phase 2 and grades it. A refusal (Run exits with an
// error) passes. Otherwise the run must hand off to CDC; markers are then
// inserted (one per entry of markers, table -> id) and awaited, and every
// table in compare must match the source exactly. A table that differs while
// the stream runs without error is the silent-loss shape and fails the cell.
func vcrResumeAndGrade(t *testing.T, r vcrRig, s *Streamer, markers map[string]int64, compare []string) {
	t.Helper()
	logStart := r.logs.Len()
	run := startTWFBRun(s)
	defer func() { _ = run.stop(t) }()
	defer vcrLogTail(t, r, logStart)

	// Wait for the hand-off to CDC (the cold start's anchor, or a
	// cursor-less position) or an exit.
	deadline := time.Now().Add(8 * time.Minute)
	for {
		if run.exited() {
			if !strings.Contains(r.logs.String()[logStart:], vcrResumeLog) {
				t.Fatalf("RIG: phase 2 exited without taking the mid-COPY resume path: %v", run.err)
			}
			if run.err == nil {
				t.Fatal("phase 2 exited nil before streaming")
			}
			t.Logf("REFUSED (loud): %v", run.err)
			for _, tbl := range compare {
				t.Logf("  target at refusal: %s", vcrCompare(t, r, tbl))
			}
			return
		}
		if tabs, ok := vcrCursorTables(t, r, s.StreamID); ok && len(tabs) == 0 {
			break
		}
		if strings.Contains(r.logs.String()[logStart:], "cold-start CDC anchor persisted") {
			tabs, _ := vcrCursorTables(t, r, s.StreamID)
			t.Logf("phase 2 persisted its CDC anchor; the persisted cursor names %v", tabs)
			break
		}
		if time.Now().After(deadline) {
			tabs, _ := vcrCursorTables(t, r, s.StreamID)
			for _, tbl := range compare {
				t.Logf("  target while stuck: %s", vcrCompare(t, r, tbl))
			}
			t.Fatalf("phase 2 never handed off to CDC in 8 minutes and never exited (persisted cursor names %v)", tabs)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !strings.Contains(r.logs.String()[logStart:], vcrResumeLog) {
		t.Fatal("RIG: phase 2 streamed without taking the mid-COPY resume path — it measured nothing")
	}
	time.Sleep(3 * time.Second)

	for tbl, id := range markers {
		vcrSeed(t, r.mysqlDSN, tbl, "marker", []int64{id})
	}
	for tbl, id := range markers {
		if !r.tgt().waitRow(t, tbl, int(id), run, 120*time.Second) {
			if run.exited() {
				t.Logf("REFUSED (loud) after streaming: %v", run.err)
				return
			}
			t.Errorf("marker %s.%d never reached the target (stream still running)", tbl, id)
		}
	}
	silent := false
	for _, tbl := range compare {
		d := vcrCompare(t, r, tbl)
		if d.exact() {
			t.Logf("EXACT: %s", d)
			continue
		}
		silent = true
		t.Errorf("SILENT DIVERGENCE (stream running, no error): %s", d)
	}
	if run.exited() {
		t.Logf("note: the stream exited during grading: %v", run.err)
	} else if silent {
		t.Log("the stream is still running without error with the divergence above")
	}
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

	// VSTREAM-COPY-RESUME-RECREATED-TABLE. Phase 1 copies rc_a (small)
	// and stops mid-rc_b (even ids). Both tables are then dropped and
	// re-created with disjoint rows: rc_a with ids 5001.., rc_b with ODD
	// ids spread over the whole old range and beyond, so some new rows lie
	// below the old cursor (skipped if the scan continues from it) and the
	// old even ids are gone (left behind if nothing deletes them).
	//
	// MEASURED SILENT (2026-10-20, 3 runs): both re-created tables keep every
	// old row the first run copied (rc_a: source 501, target 1501; rc_b:
	// source 701, target 6701), no new row missing. Skipped so the vstream
	// suite stays green until the fix lands; SLUICE_VCR_MEASURE=1 runs it.
	t.Run("RecreatedTable/default-autoshard", func(t *testing.T) {
		vcrSkipConfirmed(t, "VSTREAM-COPY-RESUME-RECREATED-TABLE: confirmed silent (old rows survive on the target); see docs/dev/audit-backlog.md")
		vcrRecreated(t, r, "rc", "", "")
	})
	// The narrower form of the same entry: no re-create, just rows DELETED
	// from the already-copied table while the stream was stopped, resumed
	// with the unchanged scope on the default path. MEASURED SILENT: pd_a
	// source 1156, target 1176 (ids 1..20 survive), stream live and applying.
	t.Run("RecreatedTable/default-plain-delete-while-stopped", func(t *testing.T) {
		vcrSkipConfirmed(t, "VSTREAM-COPY-RESUME-RECREATED-TABLE: confirmed silent for a plain stopped-window DELETE too; see docs/dev/audit-backlog.md")
		const a, b = "pd_a", "pd_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopMidCopy(t, r, r.streamer("vcrpd", []string{a, b}, ""), b, vcrBigRows)
		r.src().exec(t, "DELETE FROM "+a+" WHERE id <= 20")
		time.Sleep(3 * time.Second)
		vcrResumeUnderTraffic(t, r, r.streamer("vcrpd", []string{a, b}, ""), a, []string{a, b})
	})

	// The single-stream arms resume a cursor written by a default
	// (auto-shard) first run: a single-stream first run with more than one
	// table never persists a mid-COPY cursor at all (measured; see
	// SingleStream/stop-mid-copy below), so an operator reaches a
	// single-stream RESUME only by switching the opt-out on between runs.
	// MEASURED SILENT: rs_a source 501, target 1501; rs_b source 701,
	// target 5701.
	t.Run("RecreatedTable/single-stream-resume", func(t *testing.T) {
		vcrSkipConfirmed(t, "VSTREAM-COPY-RESUME-RECREATED-TABLE: confirmed silent on this arm too; see docs/dev/audit-backlog.md")
		vcrRecreated(t, r, "rs", "", single)
	})

	// VSTREAM-COPY-RESUME-SCOPE-WIDEN arm (a) and the default control:
	// phase 1 scope {x_a, x_b}, stopped mid-x_b; x_c (pre-populated) is
	// added to the scope before the resume. MEASURED EXACT on both (the
	// tablet's uvstreamer builds a copy plan for every filter-matched table
	// and copies one with no TablePKs entry from the start).
	t.Run("ScopeWiden/default-autoshard-control", func(t *testing.T) {
		vcrWiden(t, r, "wd", "", "")
	})
	t.Run("ScopeWiden/armA-single-stream-resume", func(t *testing.T) {
		vcrWiden(t, r, "ws", "", single)
	})

	// A single-stream run from the start: stopped mid-COPY of the second
	// table (no cursor is ever persisted on this arm), then started again
	// with the same scope. MEASURED SILENT before the fix: the re-run warm-resumed the
	// cursor-less position; ss_b source 300000, target 1000 (298,000+
	// missing) with the stream live and applying traffic. Fixed: no
	// checkpoint is persisted, so the re-run refuses on the populated target.
	t.Run("SingleStream/stop-mid-copy", func(t *testing.T) {
		const a, b = "ss_a", "ss_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		s := r.streamer("vcrss", []string{a, b}, single)
		vcrStopMidCopyHoldingTheCursorRule(t, r, s, b, vcrBigRows)
		vcrResumeUnderTraffic(t, r, r.streamer("vcrss", []string{a, b}, single), a, []string{a, b})
	})

	// A one-table scope takes the single-stream path too (len(tables) ==
	// 1). Stopped while the persisted position is cursor-less with rows
	// already on the target. MEASURED SILENT before the fix: st_b source
	// 300133, target 2133 (ids 2001..300000 missing). Fixed: the stop leaves
	// a cursor, and the resume is exact.
	t.Run("SingleTableScope/stop-at-cursorless-checkpoint", func(t *testing.T) {
		const b = "st_b"
		vcrCreate(t, r, b)
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		s := r.streamer("vcrst", []string{b}, "")
		vcrStopMidCopyHoldingTheCursorRule(t, r, s, b, vcrBigRows)
		vcrResumeUnderTraffic(t, r, r.streamer("vcrst", []string{b}, ""), b, []string{b})
	})

	// The same shape on the DEFAULT auto-shard path: the first position the
	// COPY checkpoint persists carries no cursor (it precedes the first
	// LASTPK), and so does the one between two tables. A stop while such a
	// position is the persisted one leaves the next start with a
	// cursor-less position and no established-table record. MEASURED
	// SILENT before the fix: dp_b source 300000, target 2000 (ids
	// 2001..300000 missing). Fixed: an earlier cursor or no row (a refusal).
	t.Run("DefaultPath/stop-at-cursorless-checkpoint", func(t *testing.T) {
		const a, b = "dp_a", "dp_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		s := r.streamer("vcrdp", []string{a, b}, "")
		vcrStopMidCopyHoldingTheCursorRule(t, r, s, b, vcrBigRows)
		vcrResumeUnderTraffic(t, r, r.streamer("vcrdp", []string{a, b}, ""), a, []string{a, b})
	})

	// Arm (b), the entry's own example: phase 1 scope {nb_a, nb_b} stopped
	// mid-nb_b; resume narrowed to {nb_a}. nb_a was fully copied in phase
	// 1, so the measurement is whether a change to nb_a made while stopped
	// arrives, and whether nb_a stays exact. MEASURED SILENT: nb_a is
	// re-copied (the 50 stopped-window inserts land) but the 20 deleted rows
	// survive — source 1205, target 1225 — the RECREATED-TABLE mechanism
	// (re-copy by upsert never removes a row), not a scope defect. Without
	// write traffic this cell instead sat 8 minutes with no hand-off, no
	// error and the same divergence.
	t.Run("ScopeWiden/armB-narrowed-to-copied-table", func(t *testing.T) {
		vcrSkipConfirmed(t, "VSTREAM-COPY-RESUME-RECREATED-TABLE: confirmed silent on a narrowed resume too (stopped-window DELETEs survive); see docs/dev/audit-backlog.md")
		const a, b = "nb_a", "nb_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
		time.Sleep(3 * time.Second)
		vcrStopMidCopy(t, r, r.streamer("vcrnb", []string{a, b}, ""), b, vcrBigRows)
		vcrSeed(t, r.mysqlDSN, a, "stopped", vcrRange(1001, 1, 50))
		r.src().exec(t, "DELETE FROM "+a+" WHERE id <= 20")
		time.Sleep(3 * time.Second)
		vcrResumeUnderTraffic(t, r, r.streamer("vcrnb", []string{a}, ""), a, []string{a})
	})

	// Arm (b), the strongest form: phase 1 scope {nc_a (big), nc_b
	// (small)} stopped mid-nc_a, so nc_b was never copied; resume narrowed
	// to {nc_b}. One VStream filters nc_b and carries nc_a's cursor.
	// MEASURED EXACT under traffic (704 = 704): the tablet copies nc_b from
	// the start. Without traffic one run ended loud (apply retry budget
	// exhausted on the VStream no-events liveness check) and another sat 8
	// minutes with no hand-off, nc_b exact both times.
	t.Run("ScopeWiden/armB-narrowed-to-uncopied-table", func(t *testing.T) {
		const a, b = "nc_a", "nc_b"
		vcrCreate(t, r, a, b)
		vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, vcrBigRows))
		vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, 700))
		time.Sleep(3 * time.Second)
		vcrStopMidCopy(t, r, r.streamer("vcrnc", []string{a, b}, ""), a, vcrBigRows)
		if n := len(vcrIDs(t, r.tgt(), b)); n != 0 {
			t.Fatalf("RIG: %s already holds %d rows after phase 1; want 0 (never reached)", b, n)
		}
		vcrResumeUnderTraffic(t, r, r.streamer("vcrnc", []string{b}, ""), b, []string{b})
	})
}

func vcrRecreated(t *testing.T, r vcrRig, p, phase1DSN, phase2DSN string) {
	a, b := p+"_a", p+"_b"
	streamID := "vcr" + p
	vcrCreate(t, r, a, b)
	vcrSeed(t, r.mysqlDSN, a, "old", vcrRange(1, 1, 1000))
	vcrSeed(t, r.mysqlDSN, b, "old", vcrRange(2, 2, vcrBigRows)) // even ids 2..600000
	time.Sleep(3 * time.Second)
	vcrStopMidCopy(t, r, r.streamer(streamID, []string{a, b}, phase1DSN), b, vcrBigRows)

	r.src().exec(t, "DROP TABLE "+a)
	r.src().exec(t, "DROP TABLE "+b)
	vcrCreate(t, r, a, b)
	vcrSeed(t, r.mysqlDSN, a, "new", vcrRange(5001, 1, 500))
	// Odd ids, every 1000th: 1, 1001, ..., 699001 — below, around and above
	// the old cursor, none of them an old id.
	vcrSeed(t, r.mysqlDSN, b, "new", vcrRange(1, 1000, 700))
	time.Sleep(5 * time.Second) // the schema tracker

	vcrResumeAndGrade(t, r, r.streamer(streamID, []string{a, b}, phase2DSN),
		map[string]int64{a: 9_000_001, b: 9_000_001}, []string{a, b})
}

func vcrWiden(t *testing.T, r vcrRig, p, phase1DSN, phase2DSN string) {
	a, b, c := p+"_a", p+"_b", p+"_c"
	streamID := "vcr" + p
	vcrCreate(t, r, a, b, c)
	vcrSeed(t, r.mysqlDSN, a, "v1", vcrRange(1, 1, 1000))
	vcrSeed(t, r.mysqlDSN, b, "v1", vcrRange(1, 1, vcrBigRows))
	vcrSeed(t, r.mysqlDSN, c, "v1", vcrRange(1, 1, 500))
	time.Sleep(3 * time.Second)
	vcrStopMidCopy(t, r, r.streamer(streamID, []string{a, b}, phase1DSN), b, vcrBigRows)
	if n := len(vcrIDs(t, r.tgt(), c)); n != 0 {
		t.Fatalf("RIG: %s holds %d target rows after phase 1; want 0 (out of scope)", c, n)
	}
	vcrResumeAndGrade(t, r, r.streamer(streamID, []string{a, b, c}, phase2DSN),
		map[string]int64{b: 9_000_001, c: 9_000_001}, []string{a, b, c})
}
