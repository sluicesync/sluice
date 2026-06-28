//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration safety pin for `sluice trigger prune` (ADR-0137, Bug 165). The
// correctness crux is silent-loss avoidance: a change-log row may be pruned ONLY
// when the TARGET has durably applied it, and a prune must NOT break warm-resume
// exactly-once. These tests prove, against real databases, that:
//
//   - rows at/below (durable frontier - keep) are gone, rows above remain
//     (the id <= cut boundary, ground-truthed on the live change-log);
//   - the target's durable position is UNCHANGED by the prune;
//   - warm-resume AFTER the prune still converges exactly-once (no drop/dup) —
//     the load-bearing proof that pruning didn't delete a row resume needs;
//   - a prune with NO durable position (fresh setup, nothing applied yet)
//     REFUSES loudly and deletes nothing.
//
// Two source engines exercise the two distinct DELETE code paths: sqlite-trigger
// (a local file *sql.DB) and pgtrigger (SQL over pgx). Both stream into a PG
// target. Shared helpers (seedSQLiteTriggerSource, sqliteExec, pgEventIDs,
// waitFor*) live in streamer_sqlite_trigger_cross_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// readDurableLastID reads the durably-persisted CDC position for streamID from
// the PG target (the SAME read path `sluice trigger prune` uses) and decodes the
// trigger-CDC {"last_id":N} token. ok=false when no position row exists yet.
func readDurableLastID(t *testing.T, targetDSN, streamID string) (int64, bool) {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	applier, err := pgEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target applier: %v", err)
	}
	if c, isCloser := applier.(interface{ Close() error }); isCloser {
		defer func() { _ = c.Close() }()
	}
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		t.Fatalf("read durable position: %v", err)
	}
	if !found {
		return 0, false
	}
	id, err := sqlitetrigger.AppliedLastID(pos.Token)
	if err != nil {
		t.Fatalf("decode durable token %q: %v", pos.Token, err)
	}
	return id, true
}

// sqliteChangeLogIDs returns the sorted id set still in the source change-log.
func sqliteChangeLogIDs(t *testing.T, path string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open source change-log: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id FROM "`+sqlitetrigger.ChangeLogTable+`" ORDER BY id`)
	if err != nil {
		t.Fatalf("query change-log ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan change-log id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("change-log rows err: %v", err)
	}
	return ids
}

// TestTriggerPrune_SQLiteToPostgres_WarmResumeStillExactlyOnce is the headline
// safety pin: prune after a durable apply, then prove warm-resume still
// converges exactly-once.
func TestTriggerPrune_SQLiteToPostgres_WarmResumeStillExactlyOnce(t *testing.T) {
	src := seedSQLiteTriggerSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "sqlite-trigger-prune-pg"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    srcEng,
			Target:    pgEng,
			SourceDSN: src,
			TargetDSN: pgTarget,
			StreamID:  streamID,
		}
	}

	// ---- Run 1: cold-start + a batch of CDC so the durable position advances ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()

	if !waitForRowCount(t, pgTarget, "events", 2, 90*time.Second) {
		cancel1()
		t.Fatal("cold-start never delivered the 2 seed rows")
	}
	// Several CDC ops to push the change-log id well above a small keep margin.
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (3, 300, NULL, 'cdc-3')`)
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (4, 400, NULL, 'cdc-4')`)
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (5, 500, NULL, 'cdc-5')`)
	sqliteExec(t, src, `UPDATE events SET big = 999 WHERE id = 1`)
	sqliteExec(t, src, `DELETE FROM events WHERE id = 2`)

	if !waitForEventBig(t, pgTarget, 5, 500, 60*time.Second) {
		cancel1()
		t.Fatalf("CDC batch never converged: %v", pgEventIDs(t, pgTarget))
	}
	if !waitForEventBig(t, pgTarget, 1, 999, 30*time.Second) {
		cancel1()
		t.Fatal("CDC UPDATE of id=1 never propagated")
	}

	// Wait until the durable position has advanced to cover the whole batch. The
	// applier persists the watermark on durable apply; poll until it stabilizes
	// at a frontier >= the number of change-log rows we produced.
	var frontier int64
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		id, found := readDurableLastID(t, pgTarget, streamID)
		if found && id >= 5 {
			frontier = id
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if frontier < 5 {
		cancel1()
		t.Fatalf("durable frontier never advanced to cover the batch (got %d)", frontier)
	}

	// ---- Hard-stop, then PRUNE while stopped ----
	cancel1()
	select {
	case <-run1:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (1) did not return after ctx cancel")
	}

	// Re-read the durable frontier after the stop (it is the authority for the cut).
	frontier, found := readDurableLastID(t, pgTarget, streamID)
	if !found {
		t.Fatal("durable position vanished after stop")
	}
	const keep = int64(2)
	cut := frontier - keep
	if cut <= 0 {
		t.Fatalf("test setup: frontier %d - keep %d = %d (need > 0)", frontier, keep, cut)
	}

	before := sqliteChangeLogIDs(t, src)
	res, err := sqlitetrigger.Prune(context.Background(), src, sqlitetrigger.PruneOptions{Cut: cut})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	after := sqliteChangeLogIDs(t, src)

	// (a) rows id <= cut are gone; rows id > cut remain.
	for _, id := range after {
		if id <= cut {
			t.Errorf("change-log id %d <= cut %d survived the prune", id, cut)
		}
	}
	for _, id := range before {
		if id > cut {
			if !containsID(after, id) {
				t.Errorf("change-log id %d > cut %d was wrongly pruned", id, cut)
			}
		}
	}
	if res.Deleted == 0 {
		t.Errorf("Prune reported 0 deleted; expected to reap rows id <= %d (before=%v)", cut, before)
	}
	t.Logf("pruned %d rows at cut=%d (frontier=%d); before=%v after=%v", res.Deleted, cut, frontier, before, after)

	// (b) the durable position is UNCHANGED by the prune.
	postFrontier, found := readDurableLastID(t, pgTarget, streamID)
	if !found || postFrontier != frontier {
		t.Errorf("durable frontier changed across prune: was %d, now %d (found=%v)", frontier, postFrontier, found)
	}

	// ---- Run 2: warm-resume AFTER the prune; assert exactly-once ----
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()

	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (6, 600, NULL, 'cdc-6')`)
	sqliteExec(t, src, `UPDATE events SET big = 1001 WHERE id = 5`)

	if !waitForEventBig(t, pgTarget, 6, 600, 60*time.Second) {
		cancel2()
		t.Fatalf("warm-resume after prune: post-resume INSERT id=6 never landed: %v", pgEventIDs(t, pgTarget))
	}
	if !waitForEventBig(t, pgTarget, 5, 1001, 30*time.Second) {
		t.Error("warm-resume after prune: post-resume UPDATE of id=5 never propagated")
	}
	// Final exactly-once state: exactly {1,3,4,5,6}; id=2 still gone (no
	// resurrection), no duplicates — the prune did not break resume.
	if got := pgEventIDs(t, pgTarget); !containsExactly(got, []int64{1, 3, 4, 5, 6}) {
		t.Errorf("warm-resume-after-prune final id set = %v; want [1 3 4 5 6] (exactly-once)", got)
	}

	cancel2()
	select {
	case <-run2:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (2) did not return after ctx cancel")
	}
}

// TestTriggerPrune_RefusesWithoutDurablePosition pins the no-prune-blind guard:
// a prune for a stream with no persisted position must report ok=false so the
// CLI refuses loudly — never derive a cut from a missing frontier.
func TestTriggerPrune_RefusesWithoutDurablePosition(t *testing.T) {
	src := seedSQLiteTriggerSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// No streamer has run, so the target has no cdc-state row for this stream.
	// ReadPosition returns ok=false — the exact signal the CLI keys off to refuse
	// loudly (no durable frontier ⇒ no safe lower bound ⇒ never prune blind).
	_, found := readDurableLastID(t, pgTarget, "never-streamed")
	if found {
		t.Fatal("expected NO durable position for a never-streamed stream (the CLI must refuse and never derive a cut)")
	}
	// The source change-log exists (setup ran); the prune must never touch it
	// without a durable frontier. (Seeds pre-date the triggers, so it's empty.)
	_ = sqliteChangeLogIDs(t, src)
}

// TestTriggerPrune_PgtriggerToPostgres exercises the pgtrigger DELETE code path
// (SQL over pgx) end-to-end: durable apply, prune, warm-resume exactly-once. It
// reuses the shared `events` (id, big, blb, note) shape so the waitForEventBig /
// pgEventIDs helpers apply.
func TestTriggerPrune_PgtriggerToPostgres(t *testing.T) {
	_, srcDSN, srcCleanup := startPostgres(t)
	defer srcCleanup()
	_, dstDSN, dstCleanup := startPostgres(t)
	defer dstCleanup()

	// Seed the source table and install the pgtrigger capture state.
	pgExec(t, srcDSN, `CREATE TABLE events (id BIGINT PRIMARY KEY, big BIGINT NOT NULL, blb BYTEA, note TEXT)`)
	pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES (1, 100, NULL, 'seed-1'), (2, 200, NULL, 'seed-2')`)
	if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{
		Tables: []string{"events"},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	srcEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "pgtrigger-prune"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    srcEng,
			Target:    pgEng,
			SourceDSN: srcDSN,
			TargetDSN: dstDSN,
			StreamID:  streamID,
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()

	if !waitForRowCount(t, dstDSN, "events", 2, 90*time.Second) {
		cancel1()
		t.Fatal("cold-start never delivered the 2 seed rows")
	}
	pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES (3, 30, NULL, 'cdc-3'), (4, 40, NULL, 'cdc-4'), (5, 50, NULL, 'cdc-5')`)
	pgExec(t, srcDSN, `UPDATE events SET big = 99 WHERE id = 1`)
	pgExec(t, srcDSN, `DELETE FROM events WHERE id = 2`)

	if !waitForEventBig(t, dstDSN, 5, 50, 60*time.Second) {
		cancel1()
		t.Fatalf("pgtrigger CDC batch never converged: %v", pgEventIDs(t, dstDSN))
	}

	var frontier int64
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		id, found := readDurablePgtriggerLastID(t, dstDSN, streamID)
		if found && id >= 5 {
			frontier = id
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if frontier < 5 {
		cancel1()
		t.Fatalf("durable frontier never advanced (got %d)", frontier)
	}

	cancel1()
	select {
	case <-run1:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (1) did not return")
	}

	frontier, found := readDurablePgtriggerLastID(t, dstDSN, streamID)
	if !found {
		t.Fatal("durable position vanished after stop")
	}
	const keep = int64(2)
	cut := frontier - keep
	if cut <= 0 {
		t.Fatalf("test setup: frontier %d - keep %d = %d (need > 0)", frontier, keep, cut)
	}

	before := pgChangeLogIDs(t, srcDSN)
	res, err := pgtrigger.Prune(context.Background(), srcDSN, pgtrigger.PruneOptions{Cut: cut})
	if err != nil {
		t.Fatalf("pgtrigger.Prune: %v", err)
	}
	after := pgChangeLogIDs(t, srcDSN)
	for _, id := range after {
		if id <= cut {
			t.Errorf("change-log id %d <= cut %d survived the prune", id, cut)
		}
	}
	for _, id := range before {
		if id > cut && !containsID(after, id) {
			t.Errorf("change-log id %d > cut %d was wrongly pruned", id, cut)
		}
	}
	if res.Deleted == 0 {
		t.Errorf("pgtrigger.Prune reported 0 deleted; before=%v", before)
	}

	postFrontier, found := readDurablePgtriggerLastID(t, dstDSN, streamID)
	if !found || postFrontier != frontier {
		t.Errorf("durable frontier changed across prune: was %d, now %d", frontier, postFrontier)
	}

	// Warm-resume after prune: exactly-once.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()

	pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES (6, 60, NULL, 'cdc-6')`)
	pgExec(t, srcDSN, `UPDATE events SET big = 101 WHERE id = 5`)

	if !waitForEventBig(t, dstDSN, 6, 60, 60*time.Second) {
		cancel2()
		t.Fatalf("warm-resume after prune: id=6 never landed: %v", pgEventIDs(t, dstDSN))
	}
	if !waitForEventBig(t, dstDSN, 5, 101, 30*time.Second) {
		t.Error("warm-resume after prune: UPDATE of id=5 never propagated")
	}
	if got := pgEventIDs(t, dstDSN); !containsExactly(got, []int64{1, 3, 4, 5, 6}) {
		t.Errorf("warm-resume-after-prune final id set = %v; want [1 3 4 5 6]", got)
	}

	cancel2()
	select {
	case <-run2:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (2) did not return")
	}
}

// --- pgtrigger-prune helpers ---

// pgExec runs one statement against a PG DSN.
func pgExec(t *testing.T, dsn, stmt string, args ...any) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg for exec: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
		t.Fatalf("pg exec %q: %v", stmt, err)
	}
}

// pgChangeLogIDs returns the sorted change-log id set on a pgtrigger source.
func pgChangeLogIDs(t *testing.T, dsn string) []int64 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id FROM `+pgtrigger.ChangeLogTable+` ORDER BY id`)
	if err != nil {
		t.Fatalf("query change-log ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan change-log id: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("change-log rows err: %v", err)
	}
	return out
}

// readDurablePgtriggerLastID mirrors readDurableLastID but decodes via the
// pgtrigger codec (same {"last_id":N} shape; kept separate so each engine's codec
// owns its decode).
func readDurablePgtriggerLastID(t *testing.T, targetDSN, streamID string) (int64, bool) {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	applier, err := pgEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target applier: %v", err)
	}
	if c, isCloser := applier.(interface{ Close() error }); isCloser {
		defer func() { _ = c.Close() }()
	}
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		t.Fatalf("read durable position: %v", err)
	}
	if !found {
		return 0, false
	}
	id, err := pgtrigger.AppliedLastID(pos.Token)
	if err != nil {
		t.Fatalf("decode durable token %q: %v", pos.Token, err)
	}
	return id, true
}

// containsID reports whether ids contains id.
func containsID(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// containsExactly reports whether got equals want as ordered sets.
func containsExactly(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
