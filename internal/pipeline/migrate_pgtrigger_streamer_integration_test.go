//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 94 / Bug 93 — postgres-trigger through the REAL Streamer.
//
// The existing pgtrigger congruence tests
// (migrate_pgtrigger_congruence_integration_test.go,
// migrate_pgtrigger_cross_congruence_integration_test.go) drive the
// trigger reader via the MANUAL path (Setup → Migrator bulk-copy →
// OpenCDCReader → OpenChangeApplier). That path bypasses the Streamer's
// engine-neutral coldStart entirely — which is precisely why it MASKED
// Bug 94: coldStart calls OpenSnapshotStream, and the trigger engine
// USED to delegate OpenSnapshotStream to the composed slot-based
// pgoutput path. Under the Streamer, a `postgres-trigger` source would
// therefore silently create a replication slot (forbidden on the
// managed tier the engine exists for) and never engage the capture-log
// poller. The manual tests couldn't see it.
//
// This test drives Streamer.Run (the actual `sync start` path) with
// Source = the postgres-trigger engine, for BOTH postgres-trigger →
// postgres-trigger and postgres-trigger → mysql. It asserts:
//
//  1. NO pgoutput slot is created for the trigger source
//     (`count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'`
//     stays 0) — this is what proves the POLLER path, not the slot path,
//     is engaged: the exact thing the old tests missed. The PG container
//     boots wal_level=logical so a regression to the delegated slot path
//     WOULD succeed in creating a slot (and this assertion would catch
//     it), rather than failing for an unrelated wal_level reason.
//  2. The capture-log poller IS consumed: trigger CDC INSERT/UPDATE/
//     DELETE land on the target byte-correct.
//  3. NO-LOSS under concurrent writes during the bulk-copy window (the
//     handoff-correctness check): a writer goroutine inserts rows that
//     interleave with the snapshot/bulk-copy window, and EVERY committed
//     row must land on the target — no gap. This is the assertion that
//     would catch a wrong CDC anchor (a too-high anchor silently skips
//     an in-flight low-id row; the contiguous-committed-prefix anchor in
//     pgtrigger.readChangeLogAnchor prevents it).
//  4. The headline `trigger setup → migrate` flow to MySQL no longer
//     fails on the capture tables (Bug 93): the postgres SchemaReader
//     now excludes sluice_change_log / sluice_change_log_meta, so the
//     cross-engine create-tables phase never sees committed_at's
//     untranslatable statement_timestamp() default.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pgTrigStreamerTable is the single user table the Streamer migrates.
const pgTrigStreamerTable = "events"

// pgTrigStreamerSeedDDL creates the table and seeds 50 rows. A simple
// shape (PK + an integer + text) is enough — the value-family fidelity
// is already pinned by the congruence tests; this test pins the SLOT-
// LESS handoff path the congruence tests can't reach.
const pgTrigStreamerSeedDDL = `
	CREATE TABLE ` + pgTrigStreamerTable + ` (
		id    BIGINT PRIMARY KEY,
		n     INTEGER NOT NULL,
		note  TEXT
	);
	INSERT INTO ` + pgTrigStreamerTable + ` (id, n, note)
	SELECT g, g * 2, 'seed-' || g FROM generate_series(1, 50) g;
`

// pgTrigStreamerSeedCount is the number of seed rows.
const pgTrigStreamerSeedCount = 50

// TestPGTriggerStreamer_SameEngine_NoSlot_NoLoss drives the Streamer
// with a postgres-trigger source into a postgres-trigger target and
// asserts (1) no slot, (2) poller consumed (CDC lands), (3) no-loss
// under concurrent writes, all through the REAL coldStart path.
func TestPGTriggerStreamer_SameEngine_NoSlot_NoLoss(t *testing.T) {
	src, tgt, cleanup := startPGTrigStreamerPGPair(t)
	defer cleanup()

	pgTrigStreamerExec(t, src, pgTrigStreamerSeedDDL)

	// Source-side trigger setup — the operator's `sluice trigger setup`.
	if _, err := pgtrigger.Setup(context.Background(), src, pgtrigger.SetupOptions{
		Tables: []string{pgTrigStreamerTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	stop, concurrentDone := runPGTrigStreamer(t, src, tgt, pgtrigger.EngineName, "pgtrig-streamer-same")
	defer stop()

	// (3) Concurrent writes finished; gather the full committed id set.
	committedIDs := <-concurrentDone

	// (1) NO slot was created for the trigger source.
	assertNoSluiceSlot(t, src)

	// (2)+(3) Every committed row — seed + CDC + concurrent — lands on
	// the target. The drain predicate waits for the full set.
	waitForPGTrigStreamerDrained(t, tgt, committedIDs, 90*time.Second)

	// (2) CDC fidelity: the UPDATE and DELETE from the deterministic
	// sequence are reflected.
	assertPGTrigStreamerCDCApplied(t, tgt)

	// (1) re-check after drain — a slot must NOT have appeared mid-CDC.
	assertNoSluiceSlot(t, src)
}

// TestPGTriggerStreamer_CrossEngine_MySQL_NoSlot_NoLoss drives the
// Streamer with a postgres-trigger source into a MySQL target. Same
// assertions, plus it is the Bug 93 headline: the cross-engine create-
// tables phase must not choke on the capture tables.
func TestPGTriggerStreamer_CrossEngine_MySQL_NoSlot_NoLoss(t *testing.T) {
	src, srcCleanup := startPGTrigStreamerPG(t)
	defer srcCleanup()
	tgt, tgtCleanup := startPGTrigStreamerMySQL(t)
	defer tgtCleanup()

	pgTrigStreamerExec(t, src, pgTrigStreamerSeedDDL)

	if _, err := pgtrigger.Setup(context.Background(), src, pgtrigger.SetupOptions{
		Tables: []string{pgTrigStreamerTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	stop, concurrentDone := runPGTrigStreamer(t, src, tgt, "mysql", "pgtrig-streamer-cross")
	defer stop()

	committedIDs := <-concurrentDone

	// (1) NO slot on the PG source.
	assertNoSluiceSlot(t, src)

	// (2)+(3)+(4) Every committed row lands on the MySQL target. Reaching
	// this point at all proves Bug 93 (create-tables didn't choke on the
	// capture tables); the count proves no-loss.
	waitForPGTrigStreamerDrainedMySQL(t, tgt, committedIDs, 120*time.Second)

	assertPGTrigStreamerCDCAppliedMySQL(t, tgt)
	assertNoSluiceSlot(t, src)
}

// runPGTrigStreamer starts Streamer.Run in a goroutine and drives the
// no-loss-under-concurrent-write workload. It returns a stop closure
// and a channel that delivers the full set of committed ids once the
// concurrent writer finishes.
//
// The workload, all AFTER the trigger is installed (so every change is
// captured by the §2 capture log):
//
//   - A background writer inserts ids 1000..1000+N continuously,
//     committing one per ~5ms, starting the instant Run begins. These
//     interleave with the snapshot/bulk-copy window — some land in the
//     snapshot (≤ anchor, bulk-copied), some after (> anchor, CDC-
//     replayed). A wrong anchor would drop a row in the boundary.
//   - A deterministic CDC sequence (UPDATE id=5, DELETE id=50) applied
//     once, to pin INSERT/UPDATE/DELETE fidelity through the poller.
//
// committedIDs is every id that MUST exist on the target after drain:
// the 49 surviving seed rows (1..50 minus deleted id=50) + the
// concurrent ids.
func runPGTrigStreamer(t *testing.T, src, tgt, targetEngine, streamID string) (stop func(), committedDone <-chan []int64) {
	t.Helper()

	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	tgtEng, ok := engines.Get(targetEngine)
	if !ok {
		t.Fatalf("target engine %q not registered", targetEngine)
	}

	streamer := &Streamer{
		Source:    trigEng,
		Target:    tgtEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  streamID,
		// Belt-and-suspenders: the postgres SchemaReader already excludes
		// the capture tables (Bug 93), but an explicit exclude keeps the
		// intent legible and survives a reader-filter regression.
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	const concurrentN = 40
	const concurrentBase = 1000
	done := make(chan []int64, 1)

	// Background concurrent writer: starts immediately so its inserts
	// race the snapshot/bulk-copy window. Each is its own committed txn.
	go func() {
		ids := make([]int64, 0, concurrentN)
		db, err := sql.Open("pgx", src)
		if err != nil {
			done <- ids
			return
		}
		defer func() { _ = db.Close() }()
		for i := 0; i < concurrentN; i++ {
			id := int64(concurrentBase + i)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := db.ExecContext(
				ctx,
				"INSERT INTO "+pgTrigStreamerTable+" (id, n, note) VALUES ($1,$2,$3)",
				id, id*2, fmt.Sprintf("concurrent-%d", id),
			)
			cancel()
			if err == nil {
				ids = append(ids, id)
			}
			time.Sleep(5 * time.Millisecond)
		}
		done <- ids
	}()

	// Deterministic CDC fidelity sequence applied once, after a short
	// delay so it lands during CDC (post-anchor) on at least one leg.
	go func() {
		time.Sleep(300 * time.Millisecond)
		pgTrigStreamerExec(t, src, "UPDATE "+pgTrigStreamerTable+" SET n = 99999 WHERE id = 5;")
		pgTrigStreamerExec(t, src, "DELETE FROM "+pgTrigStreamerTable+" WHERE id = 50;")
	}()

	stop = func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("Streamer.Run did not return after ctx cancel")
		}
	}
	return stop, done
}

// expectedPGTrigStreamerIDs builds the full set of ids that MUST exist
// on the target after drain: surviving seed rows (1..50 minus id=50
// deleted) plus the concurrent ids.
func expectedPGTrigStreamerIDs(concurrent []int64) []int64 {
	out := make([]int64, 0, pgTrigStreamerSeedCount+len(concurrent))
	for id := int64(1); id <= pgTrigStreamerSeedCount; id++ {
		if id == 50 { // deleted by the CDC sequence
			continue
		}
		out = append(out, id)
	}
	out = append(out, concurrent...)
	return out
}

// waitForPGTrigStreamerDrained polls a PG target until every expected
// id is present AND id=50 is absent AND id=5 reflects the UPDATE. Fails
// loudly (naming the first missing id) on timeout.
func waitForPGTrigStreamerDrained(t *testing.T, dsn string, concurrent []int64, timeout time.Duration) {
	t.Helper()
	want := expectedPGTrigStreamerIDs(concurrent)
	deadline := time.Now().Add(timeout)
	var lastMissing int64 = -1
	for time.Now().Before(deadline) {
		missing, ok := pgTrigStreamerMissingID(dsn, want, false)
		if ok && pgTrigStreamerUpdateApplied(dsn, false) {
			return
		}
		lastMissing = missing
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("PG target never drained: first missing id=%d (want %d rows incl %d concurrent); update_applied=%v",
		lastMissing, len(want), len(concurrent), pgTrigStreamerUpdateApplied(dsn, false))
}

// waitForPGTrigStreamerDrainedMySQL is the MySQL-target analogue.
func waitForPGTrigStreamerDrainedMySQL(t *testing.T, dsn string, concurrent []int64, timeout time.Duration) {
	t.Helper()
	want := expectedPGTrigStreamerIDs(concurrent)
	deadline := time.Now().Add(timeout)
	var lastMissing int64 = -1
	for time.Now().Before(deadline) {
		missing, ok := pgTrigStreamerMissingID(dsn, want, true)
		if ok && pgTrigStreamerUpdateApplied(dsn, true) {
			return
		}
		lastMissing = missing
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("MySQL target never drained: first missing id=%d (want %d rows incl %d concurrent); update_applied=%v",
		lastMissing, len(want), len(concurrent), pgTrigStreamerUpdateApplied(dsn, true))
}

// pgTrigStreamerMissingID returns the first id in want NOT present on
// the target, and ok=true when ALL are present. mysql selects the
// driver. Read failures return ok=false so the poll keeps trying.
func pgTrigStreamerMissingID(dsn string, want []int64, mysql bool) (int64, bool) {
	driver, ident := "pgx", `"`+pgTrigStreamerTable+`"`
	if mysql {
		driver, ident = "mysql", "`"+pgTrigStreamerTable+"`"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return -1, false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Single round-trip: pull the present id set, diff in Go. Avoids a
	// per-id query storm on the 90-row set.
	rows, err := db.QueryContext(ctx, "SELECT id FROM "+ident)
	if err != nil {
		return -1, false
	}
	defer func() { _ = rows.Close() }()
	present := make(map[int64]bool, len(want))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return -1, false
		}
		present[id] = true
	}
	if err := rows.Err(); err != nil {
		return -1, false
	}
	for _, id := range want {
		if !present[id] {
			return id, false
		}
	}
	// Also require id=50 absent (the DELETE) — guards against the delete
	// being silently dropped while every insert landed.
	if present[50] {
		return 50, false
	}
	return -1, true
}

// pgTrigStreamerUpdateApplied reports whether id=5's UPDATE (n=99999)
// landed on the target.
func pgTrigStreamerUpdateApplied(dsn string, mysql bool) bool {
	driver, ident := "pgx", `"`+pgTrigStreamerTable+`"`
	if mysql {
		driver, ident = "mysql", "`"+pgTrigStreamerTable+"`"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var ok bool
	if err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM "+ident+" WHERE id = 5 AND n = 99999)").Scan(&ok); err != nil {
		return false
	}
	return ok
}

// assertPGTrigStreamerCDCApplied verifies the CDC UPDATE + DELETE landed
// on a PG target.
func assertPGTrigStreamerCDCApplied(t *testing.T, dsn string) {
	t.Helper()
	if !pgTrigStreamerUpdateApplied(dsn, false) {
		t.Fatal("PG target: CDC UPDATE (id=5 n=99999) not applied — poller path not consumed")
	}
	if _, ok := pgTrigStreamerMissingID(dsn, []int64{5}, false); !ok {
		// id=5 must still exist (it was updated, not deleted).
		t.Fatal("PG target: id=5 unexpectedly absent after UPDATE")
	}
}

// assertPGTrigStreamerCDCAppliedMySQL is the MySQL-target analogue.
func assertPGTrigStreamerCDCAppliedMySQL(t *testing.T, dsn string) {
	t.Helper()
	if !pgTrigStreamerUpdateApplied(dsn, true) {
		t.Fatal("MySQL target: CDC UPDATE (id=5 n=99999) not applied — poller path not consumed")
	}
}

// assertNoSluiceSlot fails if ANY sluice% replication slot exists on the
// PG source. This is the load-bearing assertion: it proves the trigger-
// native (slot-less) snapshot path is engaged, not the delegated slot-
// based pgoutput path. The container boots wal_level=logical so a
// regression to the delegated path WOULD create a slot here.
func assertNoSluiceSlot(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("assertNoSluiceSlot open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'").Scan(&n); err != nil {
		t.Fatalf("assertNoSluiceSlot query: %v", err)
	}
	if n != 0 {
		t.Fatalf("EXPECTED NO replication slot for the trigger source, found %d sluice%% slot(s) — "+
			"OpenSnapshotStream is engaging the delegated slot-based pgoutput path (Bug 94 regression), "+
			"not the trigger-native slot-less poller", n)
	}
}

// ---- container boot helpers (file-unique names for build-tag isolation) ----

// startPGTrigStreamerPGPair boots ONE wal_level=logical PG container and
// creates a source + target DB pair. wal_level=logical is DELIBERATE:
// it lets a Bug-94 regression actually create a slot, so assertNoSluiceSlot
// is a true signal rather than masked by a wal_level failure.
func startPGTrigStreamerPGPair(t *testing.T) (src, tgt string, cleanup func()) {
	t.Helper()
	names, terminate := bootPGTrigStreamerPG(t, []string{"src_db", "tgt_db"})
	return names["src_db"], names["tgt_db"], terminate
}

// startPGTrigStreamerPG boots a PG container with a single source DB.
func startPGTrigStreamerPG(t *testing.T) (src string, cleanup func()) {
	t.Helper()
	names, terminate := bootPGTrigStreamerPG(t, []string{"src_db"})
	return names["src_db"], terminate
}

// bootPGTrigStreamerPG is the shared PG boot. Returns a map of
// dbName→DSN and a terminate closure.
func bootPGTrigStreamerPG(t *testing.T, dbNames []string) (dsns map[string]string, terminate func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start PG container: %v", err)
	}
	terminate = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	baseConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("PG connection string: %v", err)
	}

	db, err := sql.Open("pgx", baseConn)
	if err != nil {
		terminate()
		t.Fatalf("open PG: %v", err)
	}
	defer func() { _ = db.Close() }()

	dsns = make(map[string]string, len(dbNames))
	for _, name := range dbNames {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create database %q: %v", name, err)
		}
		dsn, derr := pgTrigStreamerSwapPGDB(baseConn, name)
		if derr != nil {
			terminate()
			t.Fatalf("build PG DSN for %q: %v", name, derr)
		}
		dsns[name] = dsn
	}
	return dsns, terminate
}

// startPGTrigStreamerMySQL boots a MySQL container with a single target
// DB.
func startPGTrigStreamerMySQL(t *testing.T) (tgt string, cleanup func()) {
	t.Helper()

	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("MySQL connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open MySQL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE tgt_db"); err != nil {
		terminate()
		t.Fatalf("create MySQL database: %v", err)
	}
	dsn, derr := buildMySQLDSN(srcConn, "tgt_db")
	if derr != nil {
		terminate()
		t.Fatalf("build MySQL DSN: %v", derr)
	}
	return dsn, terminate
}

// pgTrigStreamerSwapPGDB replaces the database-name component of a PG
// URI DSN.
func pgTrigStreamerSwapPGDB(orig, newDB string) (string, error) {
	u, err := url.Parse(orig)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("DSN has no db-name path: %q", orig)
	}
	u.Path = "/" + strings.TrimPrefix(newDB, "/")
	return u.String(), nil
}

// pgTrigStreamerExec runs a (possibly multi-statement) DDL/DML block
// against a PG DSN.
func pgTrigStreamerExec(t *testing.T, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PG: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("PG exec: %v", err)
	}
}
