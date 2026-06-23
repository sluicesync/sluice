//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 159 — postgres-trigger CDC `sluice_cdc_state.source_position`
// watermark must ADVANCE past the cold-start anchor `{"last_id":0}` as
// changes are applied.
//
// The pgtrigger congruence + streamer tests
// (migrate_pgtrigger_streamer_integration_test.go) assert CONVERGENCE
// (every committed row lands on the target). None assert the durable
// SOURCE watermark advances — which is exactly how Bug 159 slipped
// through: under the ADR-0106 fast-by-default concurrent key-hash apply
// path (--apply-concurrency auto:N>1, the production default for a PG
// target), the postgres-trigger source is a MARKER-LESS stream (it emits
// no ir.TxBegin/ir.TxCommit), so the laneapply orchestrator checkpointed
// the resume position only every checkpointEveryChanges (2000) routed
// changes or at a barrier / end-of-stream. A low-volume trigger sync
// therefore applied every change correctly yet left source_position
// frozen at `{"last_id":0}` — the capture log was never reclaimable by
// the consumed cursor (unbounded growth) and every warm-resume re-read
// the whole log from id 0.
//
// This test drives the REAL Streamer (default config = concurrent apply)
// with a postgres-trigger source, applies a handful of CDC changes
// (FAR fewer than the 2000-change checkpoint cadence), and asserts the
// persisted source_position advances to a non-zero last_id while the
// data converges. It is the watermark-advance pin the prior tests
// lacked.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pgTrigWMTable is the user table this test syncs.
const pgTrigWMTable = "wm_events"

// pgTrigWMSeedDDL creates the table and seeds a few rows so the
// cold-start has data to copy (and the change-log starts non-empty after
// the seed inserts are captured post-setup).
const pgTrigWMSeedDDL = `
	CREATE TABLE ` + pgTrigWMTable + ` (
		id   BIGINT PRIMARY KEY,
		n    INTEGER NOT NULL,
		note TEXT
	);
	INSERT INTO ` + pgTrigWMTable + ` (id, n, note)
	SELECT g, g * 2, 'seed-' || g FROM generate_series(1, 5) g;
`

// TestPGTriggerStreamer_SourcePositionAdvances is the Bug 159 pin. It
// drives the Streamer with the production-default apply config (auto:N
// concurrent key-hash apply) and asserts that, after applying a small
// number of CDC changes, sluice_cdc_state.source_position advances past
// the cold-start anchor `{"last_id":0}` — and tracks the change-log
// high-water — even though the change count is far below the orchestrator's
// 2000-change checkpoint cadence.
func TestPGTriggerStreamer_SourcePositionAdvances(t *testing.T) {
	src, tgt, cleanup := startPGTrigStreamerPGPair(t)
	defer cleanup()

	pgTrigStreamerExec(t, src, pgTrigWMSeedDDL)

	// Source-side trigger setup — the operator's `sluice trigger setup`.
	if _, err := pgtrigger.Setup(context.Background(), src, pgtrigger.SetupOptions{
		Tables: []string{pgTrigWMTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "pgtrig-watermark"
	streamer := &Streamer{
		Source:    trigEng,
		Target:    tgtEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  streamID,
		// Defaults left at the production shape: ApplyConcurrency=0 → auto:N
		// (>1 on a PG target), ApplyBatchSize=0. The Streamer's own
		// resolveApplyConcurrency turns the unset value into the concurrent
		// key-hash path — exactly the path the rig ran when it found Bug 159.
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}
	// ApplyBatchSize>1 is what makes dispatchApply route to ApplyBatch (then
	// to the concurrent path). The CLI default is "auto" → the engine
	// ceiling; mirror that here so the test exercises the same dispatch.
	streamer.ApplyBatchSize = 1000

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("Streamer.Run did not return after ctx cancel")
		}
	}()

	// Wait until the cold-start anchor row exists (CDC mode entered).
	waitForCDCStateRow(t, tgt, streamID, 60*time.Second)

	// Drive a SMALL number of CDC changes — far below the 2000-change
	// checkpoint cadence. Each is its own committed source transaction, so
	// each gets its own change-log id.
	pgTrigStreamerExec(t, src, "INSERT INTO "+pgTrigWMTable+" (id, n, note) VALUES (101, 202, 'cdc-a');")
	pgTrigStreamerExec(t, src, "UPDATE "+pgTrigWMTable+" SET n = 99999 WHERE id = 1;")
	pgTrigStreamerExec(t, src, "DELETE FROM "+pgTrigWMTable+" WHERE id = 2;")
	pgTrigStreamerExec(t, src, "INSERT INTO "+pgTrigWMTable+" (id, n, note) VALUES (102, 204, 'cdc-b');")

	// Poll for BOTH conditions together (they settle at different times: the
	// data converges first, then the async coordinator checkpoint persists the
	// watermark within ~checkpointIdlePeriod):
	//   1. data convergence — id 101/102 present, id 2 gone, id 1 updated;
	//   2. the persisted source_position advances past {"last_id":0} and
	//      reaches the change-log high-water (the Bug 159 ground-truth pin).
	// Use ONE persistent handle each for src/tgt during the poll so the test's
	// own connection churn doesn't collide with the applier's W-lane pool on
	// the prebaked PG's max_connections (open-per-poll exhausted it).
	tgtDB, err := sql.Open("pgx", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	tgtDB.SetMaxOpenConns(2)
	srcDB, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	srcDB.SetMaxOpenConns(2)

	deadline := time.Now().Add(90 * time.Second)
	var (
		converged bool
		lastSeen  int64 = -1
		maxID     int64
		ids       []int64
	)
	for time.Now().Before(deadline) {
		ids = pgTrigWMQueryIDs(t, tgtDB)
		converged = pgTrigWMConverged(ids) && pgTrigWMQueryUpdate(t, tgtDB)
		maxID = pgTrigWMQueryChangeLogMax(t, srcDB)
		lastSeen = pgTrigWMQueryLastID(t, tgtDB, streamID)
		if converged && maxID > 0 && lastSeen >= maxID {
			return // PASS: data converged AND the watermark tracked the log high-water.
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("Bug 159 pin failed: converged=%v (tgt_ids=%v); persisted source_position last_id=%d vs "+
		"change-log max(id)=%d — a frozen watermark means the consumed cursor never persists "+
		"(capture log unprunable; warm-resume re-reads from 0)",
		converged, ids, lastSeen, maxID)
}

// pgTrigWMConverged checks the expected post-CDC id set: 1,3,4,5,101,102
// present and 2 absent (deleted).
func pgTrigWMConverged(ids []int64) bool {
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	for _, id := range []int64{1, 3, 4, 5, 101, 102} {
		if !set[id] {
			return false
		}
	}
	return !set[2]
}

// pgTrigWMQueryIDs returns the target id set via a shared handle.
func pgTrigWMQueryIDs(t *testing.T, db *sql.DB) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id FROM "`+pgTrigWMTable+`" ORDER BY id`)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return ids
		}
		ids = append(ids, id)
	}
	return ids
}

// pgTrigWMQueryUpdate reports whether id=1's UPDATE (n=99999) landed, via a
// shared handle.
func pgTrigWMQueryUpdate(t *testing.T, db *sql.DB) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ok bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM "`+pgTrigWMTable+`" WHERE id = 1 AND n = 99999)`).Scan(&ok); err != nil {
		return false
	}
	return ok
}

// pgTrigWMQueryChangeLogMax returns max(id) from the source change-log via a
// shared handle.
func pgTrigWMQueryChangeLogMax(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT max(id) FROM public.`+pgtrigger.ChangeLogTable).Scan(&id); err != nil {
		return -1
	}
	if !id.Valid {
		return 0
	}
	return id.Int64
}

// pgTrigWMQueryLastID returns the decoded last_id from the persisted
// source_position token via a shared handle, or -1 when absent/unparseable.
func pgTrigWMQueryLastID(t *testing.T, db *sql.DB, streamID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var token string
	if err := db.QueryRowContext(ctx,
		"SELECT source_position FROM sluice_cdc_state WHERE stream_id = $1", streamID).Scan(&token); err != nil {
		return -1
	}
	var pt struct {
		LastID int64 `json:"last_id"`
	}
	if err := json.Unmarshal([]byte(token), &pt); err != nil {
		return -1
	}
	return pt.LastID
}

// waitForCDCStateRow blocks until the per-target sluice_cdc_state row for
// streamID exists (i.e. cold-start finished and CDC mode is entering),
// failing loudly on timeout.
func waitForCDCStateRow(t *testing.T, dsn, streamID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		err = db.QueryRowContext(ctx,
			"SELECT count(*) FROM sluice_cdc_state WHERE stream_id = $1", streamID).Scan(&n)
		cancel()
		_ = db.Close()
		if err == nil && n == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("sluice_cdc_state row for stream %q never appeared within %s", streamID, timeout)
}
