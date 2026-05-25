//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0036 (Path D Phase A) diagnostic integration test for the
// v0.24.0 mid-stream live add-table flow's residual loss surface.
//
// This is a Phase A *characterization* test, NOT a correctness fix.
// Path A (slot-pause) was empirically falsified in ADR-0033; H2
// (temp-slot snapshot covers pre-publication-add rows) holds. Where
// does the ~36% loss observed in TestStreamer_AddTable_LiveMode_PG_UnderLoad
// at high burst rates actually come from?
//
// Phase A.2 (committed on main as f8fc85c) falsified reframed M3
// conclusively across 10/10 Vultr runs: every missing row's commit
// LSN lands STRICTLY AFTER `lsn_pubadd_after`. A fifth mechanism
// (M5) is in play. Phase A.3 distinguishes the three candidate M5
// shapes:
//
//   M5a. pgoutput's per-slot catalog cache lazily refreshes after
//        the catalog-effective LSN; events in the gap are filtered.
//   M5b. Streamer-snapshot-handoff race: the temp-slot snapshot's
//        consistent point and the active stream's confirmed_flush_lsn
//        don't intersect cleanly, so events between them slip
//        through.
//   M5c. Applier-side drop: pgoutput delivered the event but the
//        applier failed to commit it (e.g. early termination on
//        AddTable.Run return).
//
// Phase A.3 distinguishes M5c from M5a/M5b by capturing every
// Insert reaching the PG applier's dispatch site (the new
// `addtable.diag: applier insert received` DEBUG-level probe in
// internal/engines/postgres/change_applier.go::dispatch) and
// cross-referencing missing-row bodies against the captured set.
// If a missing row's body appears → M5c (applier received and
// dropped); if absent → M5a or M5b (pgoutput filtered or
// streamer-handoff race, applier never saw it).
//
// M5a vs M5b requires server-side log_min_messages=DEBUG2 and is
// deferred to Phase A.4.
//
// ADR-0033 § "What we still don't know" enumerates four candidate
// mechanisms:
//
//   M1. Long-running transactions across the publication-add boundary
//       (txn_start_lsn < LSN_pubadd < commit_lsn).
//   M2. Snapshot consistent-point race (LSN_S < LSN_pubadd).
//   M3. pgoutput catalog-snapshot lag between publication-add commit
//       and the active stream's slot's view of catalog membership.
//   M4. Test-side counter artifact — finalInserted not perfectly
//       synchronized with bulk-copy completion / loader cancellation.
//
// This test runs the same burst-writer scenario the v0.24.0 under-
// load test runs, but:
//
//  - captures every "cdc.diag" / "addtable.diag" DEBUG slog line into
//    an in-memory buffer the test then introspects;
//  - drives an explicit set-diff between source-side committed loader
//    rows (queried back via SELECT body FROM events) and target-side
//    delivered rows;
//  - PHASE A.2: captures source-side pg_current_wal_lsn() immediately
//    after each loader INSERT commits and stores the (body, lsn) pair
//    in an in-memory map. At set-diff time, every missing row's
//    captured LSN is classified against the
//    [lsn_pubadd_before, lsn_pubadd_after] window and totals emitted.
//  - emits four VERDICT_M[1-4] log lines naming the empirical result
//    per mechanism, copying the discipline ADR-0033 used for H1/H2.
//
// VERDICT line shape (so the ADR can quote them post-run):
//
//   VERDICT_M1: long_txn_observed=N affected_rows=K [HOLDS|FAILS|INCONCLUSIVE]
//   VERDICT_M2: lsn_snapshot=X lsn_pubadd_after=Y ordering=before|after|equal
//   VERDICT_M3: rel_first_event_lsn=X lsn_pubadd_after=Y delta_bytes=N
//                missing_before=B missing_inside=I missing_after=A missing_unknown=U
//                [HOLDS_reframed|FAILS|INCONCLUSIVE]
//   VERDICT_M4: source_committed=N target_delivered=K missing_ids=[...]
//   VERDICT_M5_ATTRIBUTION: missing_total=T applier_received=A pgoutput_filtered=F
//                preview_applier=[...] preview_filtered=[...]
//                [M5c_HOLDS|M5a_OR_M5b_HOLDS|MIXED|INCONCLUSIVE]
//
// **Phase A is non-negotiable: this test only OBSERVES. No production
// fix code is gated on its outcome.** When the run completes, the
// captured log + the VERDICT lines drive the next iteration's
// mechanism choice (ADR-0036's "Decision" + "Forward options"
// sections).
//
// Pure observability — does NOT enforce zero-loss assertions. The
// existing TestStreamer_AddTable_LiveMode_PG_UnderLoad continues to enforce
// snapshot + post-add CDC correctness; this test enforces only that
// the diagnostic markers are emitted.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_AddTable_LiveMode_PG_DiagnoseLossSurface is the ADR-0036 Phase A
// instrumentation run. Captures cdc.diag + addtable.diag DEBUG-level
// slog lines emitted by the v0.24.0 mid-stream add-table flow under
// the same burst-writer conditions that exhibit ~36% loss in the
// original under-load test, then introspects them to render four
// VERDICT_M[1-4] lines. See the file-level comment for the structure
// and what each VERDICT means.
func TestStreamer_AddTable_LiveMode_PG_DiagnoseLossSurface(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// Capture DEBUG-level slog lines from the v0.24.0 path. The
	// goroutine-safe buffer + mutex pattern matches what the broker
	// integration tests use for log scraping. Slog's JSONHandler keeps
	// the parser deterministic across runs so verdict-rendering can
	// rely on field shapes.
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(prevDefault)

	const seedDDL = `
		CREATE TABLE customers (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE customers REPLICA IDENTITY FULL;
		INSERT INTO customers (email) VALUES ('alice@example.com');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-live-add-diagnose"
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCount(t, targetDSN, "customers", 1, 30*time.Second) {
		t.Fatalf("bulk copy did not deliver customers seed row")
	}

	// Warm CDC + cdc-state row. Same rationale as the under-load test —
	// AddTable preflight needs an active stream row on the target before
	// it can proceed.
	applyDDL(t, sourceDSN, "INSERT INTO customers (email) VALUES ('warmup@example.com');")
	if !waitForRowCount(t, targetDSN, "customers", 2, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-snapshot warmup insert; cdc-state row may not be written yet")
	}

	const newTableDDL = `
		CREATE TABLE events (
			id   BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			body TEXT
		);
		ALTER TABLE events REPLICA IDENTITY FULL;
	`
	applyDDL(t, sourceDSN, newTableDDL)

	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	const seedRowCount = 50
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i := 0; i < seedRowCount; i++ {
		if _, err := srcDB.ExecContext(seedCtx, `INSERT INTO events (body) VALUES ($1)`, fmt.Sprintf("seed-%d", i)); err != nil {
			seedCancel()
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	seedCancel()

	// Burst loader. The shape mirrors the under-load test exactly so
	// the diagnostic captures the same scheduler dynamics that produce
	// the ~36% loss in the original. Counter increments AFTER successful
	// commit (no off-by-one on cancellation).
	//
	// PHASE A.2: after each INSERT commits, also SELECT pg_current_wal_lsn()
	// and record (body, lsn) into lsnByBody. The captured LSN is the WAL
	// flush position *after* the implicit txn commit — a conservative upper
	// bound on the commit LSN. For Phase A.2's classification (does the
	// commit fall inside [pubadd_before, pubadd_after]?) an upper bound is
	// fine: if lsn_after_insert <= pubadd_after, the commit definitely
	// landed at-or-before pubadd_after. The map is keyed by row body so
	// the set-diff cross-reference is trivial.
	loadCtx, loadCancel := context.WithCancel(context.Background())
	var inserted atomic.Int64
	var loadWG sync.WaitGroup
	loadWG.Add(1)
	lsnByBody := make(map[string]string)
	var lsnMu sync.Mutex
	go func() {
		defer loadWG.Done()
		var local int64
		for {
			select {
			case <-loadCtx.Done():
				inserted.Store(local)
				return
			default:
			}
			body := fmt.Sprintf("load-%d", local+1)
			if _, err := srcDB.ExecContext(context.Background(), `INSERT INTO events (body) VALUES ($1)`, body); err != nil {
				inserted.Store(local)
				return
			}
			// Best-effort post-commit LSN capture. A failure here just
			// leaves the body un-classified (M3 emits missing_unknown=N
			// rather than a verdict). Do NOT skip incrementing the counter
			// or aborting the loader on this — it's purely diagnostic.
			var commitLSN string
			if err := srcDB.QueryRowContext(context.Background(), `SELECT pg_current_wal_lsn()::text`).Scan(&commitLSN); err == nil {
				lsnMu.Lock()
				lsnByBody[body] = commitLSN
				lsnMu.Unlock()
			}
			local++
			select {
			case <-loadCtx.Done():
				inserted.Store(local)
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	// Live add. The orchestrator + cdc_reader emit the diag log lines
	// during this call.
	addCtx, addCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer addCancel()
	add := &AddTable{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		TableName: "events",
		LiveMode:  true,
	}
	if err := add.Run(addCtx); err != nil {
		loadCancel()
		loadWG.Wait()
		t.Fatalf("AddTable (live mode under load): %v", err)
	}

	// Stop the loader, fire a sentinel, wait for the stream to drain.
	loadCancel()
	loadWG.Wait()
	finalInserted := inserted.Load()

	if _, err := srcDB.ExecContext(context.Background(), `INSERT INTO events (body) VALUES ($1)`, "post-add-sentinel"); err != nil {
		t.Fatalf("post-add insert: %v", err)
	}

	// Wait for the snapshot rows + post-add sentinel to land on the
	// target — Phase 2's strict pin (mirrors the under-load test).
	minTotal := seedRowCount + 1
	if !waitForRowCount(t, targetDSN, "events", minTotal, 60*time.Second) {
		got := pollRowCount(targetDSN, "events")
		t.Fatalf("under-load events row count = %d; want at least %d", got, minTotal)
	}

	// Give the stream extra time to deliver in-flight burst events
	// before the loss assessment. We use a fixed pause + a sentinel
	// confirmation so the verdict isn't biased by a too-short window.
	time.Sleep(3 * time.Second)
	if !sentinelDelivered(t, targetDSN, "events", "post-add-sentinel") {
		t.Errorf("post-add-sentinel row NOT delivered to target — Phase 2 post-add CDC contract regressed (verdicts below run regardless)")
	}

	// ---- Set-diff: source-side committed loader rows vs target-side
	// delivered rows. The set-diff is the ground truth for M4 (test-
	// counter artifact) — if finalInserted disagrees with what the
	// source actually committed, M4 holds; if it agrees but the target
	// is missing rows, M4 fails and the loss is real.
	sourceCommitted, err := loadRowsByPattern(sourceDSN, "events", "load-")
	if err != nil {
		t.Fatalf("query source committed: %v", err)
	}
	targetDelivered, err := loadRowsByPattern(targetDSN, "events", "load-")
	if err != nil {
		t.Fatalf("query target delivered: %v", err)
	}

	missing := stringSetDiff(sourceCommitted, targetDelivered)

	// ---- Parse captured logs into structured records.
	logRecs := parseDiagLogs(t, logBuf.Bytes())

	// ---- Render VERDICT lines.
	t.Logf("DIAG: finalInserted=%d source_committed=%d target_delivered=%d missing=%d",
		finalInserted, len(sourceCommitted), len(targetDelivered), len(missing))

	// Snapshot the LSN map so the verdict helper doesn't race the
	// loader goroutine (it has already returned via loadWG.Wait, but
	// taking a copy keeps the data flow obvious and the lint clean).
	lsnMu.Lock()
	lsnSnapshot := make(map[string]string, len(lsnByBody))
	for k, v := range lsnByBody {
		lsnSnapshot[k] = v
	}
	lsnMu.Unlock()

	renderVerdictM1(t, logRecs)
	renderVerdictM2(t, logRecs)
	renderVerdictM3(t, logRecs, missing, lsnSnapshot)
	renderVerdictM4(t, finalInserted, sourceCommitted, targetDelivered, missing)
	renderVerdictM5Attribution(t, logRecs, missing)

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned err: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// lockedBuffer is a goroutine-safe wrapper around bytes.Buffer.
// slog.JSONHandler writes from arbitrary goroutines (the streamer's
// pump, the orchestrator's main goroutine, the test goroutine).
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.buf.Len())
	copy(out, b.buf.Bytes())
	return out
}

// diagRecord is the parsed shape of one slog JSON line we care about
// for verdict rendering. Untyped fields (snapshots / LSNs / etc.) are
// kept in attrs as a generic map so the verdict helpers can extract
// what they need without a long struct of optional fields.
type diagRecord struct {
	level string
	msg   string
	phase string // the "phase" field we tag every diag line with
	attrs map[string]any
}

// parseDiagLogs scans the captured slog JSON stream and returns the
// records whose msg starts with "cdc.diag" or "addtable.diag". Other
// log lines (engine info / warn output / unrelated tests) are dropped.
func parseDiagLogs(t *testing.T, data []byte) []diagRecord {
	t.Helper()
	var out []diagRecord
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			// Non-JSON lines (rare; unstructured log output): skip.
			continue
		}
		msg, _ := raw["msg"].(string)
		if !strings.HasPrefix(msg, "cdc.diag") && !strings.HasPrefix(msg, "addtable.diag") {
			continue
		}
		level, _ := raw["level"].(string)
		phase, _ := raw["phase"].(string)
		out = append(out, diagRecord{
			level: level,
			msg:   msg,
			phase: phase,
			attrs: raw,
		})
	}
	return out
}

// renderVerdictM1 inspects the captured logs for transactions whose
// txn_start_lsn lands BEFORE lsn_after_pub_add but whose
// txn_commit_lsn lands AFTER. Those events on the new table at LSN
// inside the transaction would be filtered by pgoutput's per-LSN
// catalog snapshot at decode-time, even though the publication
// already includes the table at commit.
func renderVerdictM1(t *testing.T, recs []diagRecord) {
	t.Helper()

	pubAddAfter := lookupSinglePubAddLSN(recs, "lsn_after_pub_add")
	if pubAddAfter == 0 {
		t.Logf("VERDICT_M1: INCONCLUSIVE — no addtable.diag pub-add-window record captured (lsn_after_pub_add empty); cannot test long-txn straddle hypothesis")
		return
	}

	type txn struct {
		startLSN  pglogrepl.LSN
		commitLSN pglogrepl.LSN
	}
	var txns []txn
	for _, r := range recs {
		if r.phase != "begin" {
			continue
		}
		startStr, _ := r.attrs["txn_start_lsn"].(string)
		commitStr, _ := r.attrs["txn_commit_lsn"].(string)
		s, err1 := pglogrepl.ParseLSN(startStr)
		c, err2 := pglogrepl.ParseLSN(commitStr)
		if err1 != nil || err2 != nil {
			continue
		}
		txns = append(txns, txn{startLSN: s, commitLSN: c})
	}

	// Count transactions that started <= pubAddAfter but committed >
	// pubAddAfter. This is a strict lower bound on M1: a txn whose
	// BEGIN landed before publication-add and whose COMMIT landed
	// after. Count rows by joining against the row records.
	straddling := 0
	straddlingCommitLSNs := map[pglogrepl.LSN]struct{}{}
	for _, tx := range txns {
		if tx.startLSN <= pubAddAfter && tx.commitLSN > pubAddAfter {
			straddling++
			straddlingCommitLSNs[tx.commitLSN] = struct{}{}
		}
	}
	affectedRows := 0
	for _, r := range recs {
		if r.phase != "row" {
			continue
		}
		commitStr, _ := r.attrs["txn_commit_lsn"].(string)
		c, err := pglogrepl.ParseLSN(commitStr)
		if err != nil {
			continue
		}
		if _, ok := straddlingCommitLSNs[c]; ok {
			affectedRows++
		}
	}

	verdict := "INCONCLUSIVE"
	switch {
	case straddling == 0:
		// No straddling transactions seen. Mechanism is a NON-FACTOR for
		// THIS run; can't conclude HOLDS without more samples but we
		// can rule it out for the observed window.
		verdict = "FAILS (no straddling txns observed in this run)"
	case straddling > 0 && affectedRows > 0:
		verdict = "HOLDS (straddling txns observed; their row events landed in the captured trace)"
	case straddling > 0 && affectedRows == 0:
		verdict = "INCONCLUSIVE (straddling txns observed but row events for them not captured in dispatch — possible filter)"
	}

	t.Logf("VERDICT_M1: long_txn_observed=%d affected_rows=%d txns_total=%d %s",
		straddling, affectedRows, len(txns), verdict)
}

// renderVerdictM2 surfaces the LSN_S vs LSN_pubadd ordering. If
// LSN_S < LSN_pubadd, the snapshot would miss any rows committed in
// the gap, AND the active stream would have already filtered events
// at LSNs in (LSN_S, LSN_pubadd) since pgoutput hadn't yet picked up
// the catalog change for the new table.
func renderVerdictM2(t *testing.T, recs []diagRecord) {
	t.Helper()
	var snapLSN, beforeLSN, afterLSN pglogrepl.LSN
	for _, r := range recs {
		if r.phase != "snapshot-open" {
			continue
		}
		s, _ := r.attrs["lsn_snapshot"].(string)
		b, _ := r.attrs["lsn_before_pub_add"].(string)
		a, _ := r.attrs["lsn_after_pub_add"].(string)
		if v, err := pglogrepl.ParseLSN(s); err == nil {
			snapLSN = v
		}
		if v, err := pglogrepl.ParseLSN(b); err == nil {
			beforeLSN = v
		}
		if v, err := pglogrepl.ParseLSN(a); err == nil {
			afterLSN = v
		}
	}

	if snapLSN == 0 || afterLSN == 0 {
		t.Logf("VERDICT_M2: INCONCLUSIVE — snapshot-open / pub-add-window records missing (snap=%s pub_after=%s)",
			snapLSN.String(), afterLSN.String())
		return
	}
	ordering := "after"
	switch {
	case snapLSN < afterLSN:
		ordering = "before"
	case snapLSN == afterLSN:
		ordering = "equal"
	}
	verdict := "FAILS (snapshot LSN >= LSN_pubadd; ordering invariant holds)"
	if ordering == "before" {
		verdict = "HOLDS (snapshot LSN < LSN_pubadd; rows in the gap would be neither in the snapshot nor delivered by pgoutput post-publication-add)"
	}
	t.Logf("VERDICT_M2: lsn_snapshot=%s lsn_pubadd_before=%s lsn_pubadd_after=%s ordering=%s %s",
		snapLSN.String(), beforeLSN.String(), afterLSN.String(), ordering, verdict)
}

// renderVerdictM3 looks at the first observed CDC event on the new
// table and compares its WAL LSN against lsn_after_pub_add. A delta
// substantially larger than the ALTER PUBLICATION WAL record itself
// would indicate pgoutput's catalog-snapshot lagged the catalog
// change visibly.
//
// PHASE A.2 extension: cross-reference each missing row's source-side
// commit LSN (captured by the loader after each INSERT) against the
// [lsn_pubadd_before, lsn_pubadd_after] window and classify into
// before / inside / after / unknown buckets. The classification drives
// the reframed M3 verdict:
//
//   - HOLDS_reframed — every missing row's commit LSN falls INSIDE
//     [pubadd_before, pubadd_after]. Confirms M3 (reframed): rows
//     committed during the ALTER PUBLICATION txn's WAL window are
//     filtered.
//   - FAILS — missing rows scatter outside the window; a fifth
//     mechanism is in play.
//   - INCONCLUSIVE — mixed classification or insufficient data
//     (no missing rows captured, LSN map empty, pub-add LSN
//     unparseable, etc.).
func renderVerdictM3(t *testing.T, recs []diagRecord, missing []string, lsnByBody map[string]string) {
	t.Helper()
	pubAddBefore := lookupSinglePubAddLSN(recs, "lsn_before_pub_add")
	pubAddAfter := lookupSinglePubAddLSN(recs, "lsn_after_pub_add")
	if pubAddAfter == 0 {
		t.Logf("VERDICT_M3: INCONCLUSIVE — no addtable.diag pub-add-window record captured")
		return
	}

	// Find the FIRST row event on relation "events" (the new table).
	// first_seen_for_rel is the marker the pump emits. Phase A.1
	// instrumentation; still load-bearing for the rel_first_event LSN
	// summary line below.
	var firstEventsLSN pglogrepl.LSN
	for _, r := range recs {
		if r.phase != "row" {
			continue
		}
		rel, _ := r.attrs["relation"].(string)
		if rel != "events" {
			continue
		}
		first, _ := r.attrs["first_seen_for_rel"].(bool)
		if !first {
			continue
		}
		walStartStr, _ := r.attrs["wal_start"].(string)
		if v, err := pglogrepl.ParseLSN(walStartStr); err == nil {
			firstEventsLSN = v
			break
		}
	}

	// Phase A.2 classification. For each missing row body, look up its
	// captured commit LSN and bucket it against [pubAddBefore, pubAddAfter].
	var (
		mBefore, mInside, mAfter, mUnknown int
		insidePreview                      []string
		outsidePreview                     []string
	)
	const previewCap = 6
	for _, body := range missing {
		lsnStr, ok := lsnByBody[body]
		if !ok || lsnStr == "" {
			mUnknown++
			continue
		}
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			mUnknown++
			continue
		}
		switch {
		case pubAddBefore != 0 && lsn < pubAddBefore:
			mBefore++
			if len(outsidePreview) < previewCap {
				outsidePreview = append(outsidePreview, fmt.Sprintf("%s@%s(before)", body, lsn.String()))
			}
		case lsn >= pubAddBefore && lsn <= pubAddAfter:
			mInside++
			if len(insidePreview) < previewCap {
				insidePreview = append(insidePreview, fmt.Sprintf("%s@%s", body, lsn.String()))
			}
		default: // lsn > pubAddAfter
			mAfter++
			if len(outsidePreview) < previewCap {
				outsidePreview = append(outsidePreview, fmt.Sprintf("%s@%s(after)", body, lsn.String()))
			}
		}
	}

	verdict := "INCONCLUSIVE"
	totalClassified := mBefore + mInside + mAfter
	switch {
	case len(missing) == 0:
		verdict = "INCONCLUSIVE (no missing rows captured in this run; cannot test reframed M3)"
	case totalClassified == 0:
		verdict = "INCONCLUSIVE (all missing rows have unknown LSN — loader LSN capture failed or skipped)"
	case mInside == totalClassified && mInside > 0:
		verdict = "HOLDS_reframed (every missing row's commit LSN falls INSIDE [pubadd_before, pubadd_after]; M3-reframed confirmed)"
	case mInside == 0:
		verdict = "FAILS (no missing rows fall in the window; reframed M3 doesn't hold — a fifth mechanism is in play)"
	default:
		verdict = "INCONCLUSIVE (mixed classification — some missing rows inside the window, some outside; M3-reframed contributes but isn't the only surface)"
	}

	// Phase A.1 line shape preserved at the front for continuity with
	// the existing ADR rows; Phase A.2 fields appended.
	delta := int64(firstEventsLSN) - int64(pubAddAfter)
	relFirstStr := firstEventsLSN.String()
	if firstEventsLSN == 0 {
		relFirstStr = "<none>"
	}
	t.Logf("VERDICT_M3: rel_first_event_lsn=%s lsn_pubadd_before=%s lsn_pubadd_after=%s delta_bytes=%d missing_total=%d missing_before=%d missing_inside=%d missing_after=%d missing_unknown=%d inside_preview=%v outside_preview=%v %s",
		relFirstStr, pubAddBefore.String(), pubAddAfter.String(), delta,
		len(missing), mBefore, mInside, mAfter, mUnknown,
		insidePreview, outsidePreview, verdict)
}

// renderVerdictM4 reports on whether finalInserted disagrees with the
// source-side committed row set. M4 holds if the counter is wrong;
// M4 fails if the counter is exactly right and any "missing" rows
// represent real loss.
func renderVerdictM4(t *testing.T, finalInserted int64, sourceCommitted, targetDelivered, missing []string) {
	t.Helper()
	src := len(sourceCommitted)
	tgt := len(targetDelivered)
	if src == 0 {
		t.Logf("VERDICT_M4: INCONCLUSIVE — no load-* rows committed on source")
		return
	}
	verdict := ""
	switch {
	case int64(src) != finalInserted:
		verdict = fmt.Sprintf("HOLDS (counter=%d != source_committed=%d; off-by %d)",
			finalInserted, src, finalInserted-int64(src))
	case len(missing) == 0:
		verdict = "FAILS (counter agrees with source AND target delivered everything; no loss observed in this run)"
	default:
		verdict = fmt.Sprintf("FAILS (counter agrees with source but target is missing %d rows — real loss, not a counter artifact)", len(missing))
	}
	missingPreview := missing
	const previewCap = 12
	if len(missingPreview) > previewCap {
		missingPreview = missingPreview[:previewCap]
	}
	t.Logf("VERDICT_M4: source_committed=%d target_delivered=%d counter=%d missing_count=%d missing_ids_preview=%v %s",
		src, tgt, finalInserted, len(missing), missingPreview, verdict)
}

// renderVerdictM5Attribution implements the ADR-0036 Phase A.3
// distinction between M5c (applier-side drop) and M5a/M5b
// (pgoutput-filter or streamer-snapshot-handoff race). It builds a
// `applierByBody` map from the `addtable.diag: applier insert
// received` records emitted by the PG applier's dispatch site, then
// cross-references each missing row's body against the map.
//
//   - applier_received counts the missing rows that the applier
//     dispatch site DID see (M5c surface).
//   - pgoutput_filtered counts the missing rows the applier never
//     saw (M5a or M5b — pgoutput-side filter or upstream race).
//
// Verdict:
//
//   - M5c_HOLDS — every missing row was received by the applier;
//     the loss is downstream of pgoutput (applier dropped them
//     after receiving). Fix lives in the applier.
//   - M5a_OR_M5b_HOLDS — no missing rows reach the applier;
//     pgoutput-side filter or streamer-handoff race. Fix is
//     upstream (Path B dual-slot may help; Path C quiesce always
//     closes the gap). Phase A.4 (server-side DEBUG2 logging)
//     would further distinguish M5a from M5b.
//   - MIXED — partial; some missing rows reach the applier, some
//     don't. Both mechanisms may be contributing.
//   - INCONCLUSIVE — no missing rows, or no applier_insert_received
//     records captured (instrumentation didn't fire).
//
// Phase A.3 is purely observational; no production fix is gated on
// the verdict shape. The verdict drives the next iteration's path
// (M5c → applier-side fix; M5a/M5b → upstream fix or Path C).
func renderVerdictM5Attribution(t *testing.T, recs []diagRecord, missing []string) {
	t.Helper()

	// Build the applier-received body→lsn map. We log each
	// occurrence; for the body-keyed lookup the latest entry wins
	// (per-body inserts in this test are unique, so collisions
	// don't occur for load-* rows). Rows with empty body are
	// skipped — they're either non-events-table inserts (customers,
	// post-add-sentinel for non-events) or the body extraction
	// failed at the probe; either way they're irrelevant for the
	// load-* set-diff cross-reference.
	applierByBody := make(map[string]string)
	for _, r := range recs {
		if r.phase != "applier_insert_received" {
			continue
		}
		rel, _ := r.attrs["relation"].(string)
		if rel != "events" {
			continue
		}
		body, _ := r.attrs["body"].(string)
		if body == "" {
			continue
		}
		lsn, _ := r.attrs["lsn"].(string)
		applierByBody[body] = lsn
	}

	if len(missing) == 0 {
		t.Logf("VERDICT_M5_ATTRIBUTION: missing_total=0 applier_received=0 pgoutput_filtered=0 INCONCLUSIVE (no missing rows in this run; cannot attribute)")
		return
	}
	if len(applierByBody) == 0 {
		t.Logf("VERDICT_M5_ATTRIBUTION: missing_total=%d applier_received=0 pgoutput_filtered=%d INCONCLUSIVE (no applier_insert_received records captured for events; instrumentation did not fire — check that the DEBUG slog handler is active and the applier probe is wired)",
			len(missing), len(missing))
		return
	}

	var applierPreview, filteredPreview []string
	const previewCap = 6
	applierReceived := 0
	pgoutputFiltered := 0
	for _, body := range missing {
		lsn, ok := applierByBody[body]
		if ok {
			applierReceived++
			if len(applierPreview) < previewCap {
				applierPreview = append(applierPreview, fmt.Sprintf("%s@%s", body, lsn))
			}
		} else {
			pgoutputFiltered++
			if len(filteredPreview) < previewCap {
				filteredPreview = append(filteredPreview, body)
			}
		}
	}

	verdict := "INCONCLUSIVE"
	switch {
	case applierReceived == len(missing) && applierReceived > 0:
		verdict = "M5c_HOLDS (all missing rows reached the applier dispatch site; the applier dropped them after receiving — fix lives in the applier)"
	case pgoutputFiltered == len(missing) && pgoutputFiltered > 0:
		verdict = "M5a_OR_M5b_HOLDS (no missing rows reach the applier; pgoutput filtered them OR they slipped through the streamer-snapshot-handoff — Phase A.4 server-side DEBUG2 needed to distinguish M5a from M5b)"
	case applierReceived > 0 && pgoutputFiltered > 0:
		verdict = "MIXED (partial; both applier-side and upstream losses contribute — both mechanisms in play)"
	}

	t.Logf("VERDICT_M5_ATTRIBUTION: missing_total=%d applier_received=%d pgoutput_filtered=%d preview_applier=%v preview_filtered=%v %s",
		len(missing), applierReceived, pgoutputFiltered, applierPreview, filteredPreview, verdict)
}

// lookupSinglePubAddLSN scans for the single addtable.diag pub-add-window
// record and returns the requested LSN field. Returns 0 when missing or
// unparseable; verdict callers handle 0 as INCONCLUSIVE.
func lookupSinglePubAddLSN(recs []diagRecord, field string) pglogrepl.LSN {
	for _, r := range recs {
		if r.phase != "pub-add-window" {
			continue
		}
		v, _ := r.attrs[field].(string)
		if v == "" {
			continue
		}
		if parsed, err := pglogrepl.ParseLSN(v); err == nil {
			return parsed
		}
	}
	return 0
}

// loadRowsByPattern queries the body column of the events table on the
// supplied DSN, filtered to rows whose body starts with prefix, and
// returns the body values sorted lexicographically. Used by both the
// source-committed check (passed sourceDSN) and the target-delivered
// check (passed targetDSN).
func loadRowsByPattern(dsn, table, prefix string) ([]string, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	q := fmt.Sprintf("SELECT body FROM %s WHERE body LIKE $1 ORDER BY body", table)
	rows, err := db.QueryContext(ctx, q, prefix+"%")
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}
	sort.Strings(out)
	return out, nil
}

// stringSetDiff returns elements present in a but not in b. Both
// inputs must be sorted (loadRowsByPattern sorts on the way out).
func stringSetDiff(a, b []string) []string {
	bset := make(map[string]struct{}, len(b))
	for _, s := range b {
		bset[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := bset[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
