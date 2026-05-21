//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Track 1c — Phase B item 1: PS-realistic position-loss chaos
// (streamer level).
//
// docs/dev/notes/prep-planetscale-vitess-readiness.md (Phase 1c)
// records the operator-reported PlanetScale pain: multi-day sync
// outages requiring full table re-syncs when (1) the sync is down
// past binlog retention (PS default 3 days; gtid_purged advances past
// the consumer position), or (2) a node is replaced / restored from
// backup / failed over (binlogs do NOT carry to the new instance).
//
// Established code-truth (verified, and EXTENDED here): position-loss
// is handled by design — cdc_reader.go::verifyPositionResumable
// detects it on every resume, wraps ir.ErrPositionInvalid, and the
// streamer's ADR-0022 fall-through re-enters coldStart. The file/pos
// binlog-purge case is already pinned by
// streamer_mysql_purged_integration_test.go.
//
// PHASE-A GROUND-TRUTH (code-traced + empirically confirmed below):
// the MySQL snapshot→CDC handoff ALWAYS anchors the persisted
// position in file/pos mode (cdc_snapshot.go captures SHOW MASTER
// STATUS → positionModeFilePos), EVEN when the source runs
// gtid_mode=ON. So the streamer's resume-validation path is
// verifyBinlogFilePresent in BOTH gtid_mode=OFF and gtid_mode=ON
// deployments. The GTID-set branch (verifyGTIDSetReachable) is
// exercised only by a caller-supplied GTID position — covered at the
// reader level in cdc_reader_gtid_position_loss_integration_test.go.
//
// This file covers the two PS-realistic streamer-level cases the
// existing purge test does NOT:
//
//   - gtid_mode=ON + binlog purged past resume: confirms the
//     file/pos fall-through still fires when the source runs GTID
//     mode (the PS topology — PS/Vitess sources are GTID-mode).
//   - Fresh-instance / node-replace: resume against a DIFFERENT
//     MySQL instance carrying the same data but NO binlog history
//     covering the persisted position (the PS "node replaced /
//     restored from backup" mechanism — binlogs genuinely don't
//     carry over).
//
// Oracle (loud-failure tenet floor): loud ir.ErrPositionInvalid
// detection → ADR-0022 cold-start re-snapshot executes end-to-end →
// data correct after (no gap, no dup, src == dst).
//
// Reuses startMySQLBinlog / applyDDLMySQL / waitForRowCountMySQL /
// readPersistedPositionMySQL / pollRowCountMySQL /
// equalStringSlicesMySQL verbatim from
// streamer_resume_mysql_integration_test.go and purgeAllButLatestBinlog
// from streamer_mysql_purged_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLGTID boots a single MySQL container with binlog AND GTID
// mode enabled. Sibling of startMySQLBinlog (file/pos-mode); the only
// difference is --gtid-mode=ON --enforce-gtid-consistency=ON so the
// source matches the PS/Vitess topology (GTID-mode). Note: the
// streamer's snapshot handoff still persists a file/pos position even
// here (Phase-A finding) — the value of the GTID-mode boot is
// proving the file/pos fall-through is unaffected by gtid_mode=ON.
func startMySQLGTID(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(
		ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
					"--gtid-mode=ON",
					"--enforce-gtid-consistency=ON",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildMySQLDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

// TestStreamer_MySQLGTIDMode_BinlogPurgedFallsThroughToColdStart is
// the gtid_mode=ON counterpart of
// TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart. It
// confirms the PS-realistic property: even when the source runs in
// GTID mode (the PS/Vitess topology), the streamer's persisted
// position is file/pos, and a binlog-retention-exceeded resume is
// detected loudly (verifyBinlogFilePresent) → ADR-0022 cold-start →
// data correct after (no gap, no dup).
//
//  1. Cold-start MySQL→MySQL (source in gtid_mode=ON); drive a CDC
//     change so the persisted position is concrete.
//  2. Cancel the streamer.
//  3. More txns + FLUSH + PURGE BINARY LOGS so the file the position
//     references is gone (retention-exceeded).
//  4. Drop dest tables (Bug 9 pre-flight gate).
//  5. Re-run sync start; assert cold-start fall-through re-seeds the
//     dest with ALL source rows and a fresh position is written.
func TestStreamer_MySQLGTIDMode_BinlogPurgedFallsThroughToColdStart(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startMySQLGTID(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE retained (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO retained (payload) VALUES
			('seed-1'), ('seed-2'), ('seed-3'), ('seed-4'), ('seed-5');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "mysql-gtidmode-purge"

	// ---- Phase 1: cold-start; drive a CDC change. ----
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "retained", 5, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}
	applyDDLMySQL(t, sourceDSN, "INSERT INTO retained (payload) VALUES ('cdc-1')")
	if !waitForRowCountMySQL(t, targetDSN, "retained", 6, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC did not advance after first writer event")
	}

	persistedBefore := readPersistedPositionMySQL(t, targetDSN, streamID)
	if persistedBefore == "" {
		streamCancel()
		<-runErr
		t.Fatal("persisted position is empty after CDC change")
	}
	t.Logf("persisted position before loss (gtid_mode=ON source) = %q", persistedBefore)

	// ---- Phase 2: cancel. ----
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Streamer.Run returned err on cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}

	// ---- Phase 3: purge the binlog file the position references. ----
	applyDDLMySQL(t, sourceDSN, "INSERT INTO retained (payload) VALUES ('post-cancel-1')")
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO retained (payload) VALUES ('post-cancel-2')")
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	purgeAllButLatestBinlog(t, sourceDSN)

	// ---- Phase 4: drop dest tables (Bug 9 pre-flight gate). ----
	applyDDLMySQL(t, targetDSN, "DROP TABLE retained")

	// ---- Phase 5: re-run; cold-start fall-through. ----
	resumeStreamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resumeStreamer.Run(resumeCtx) }()

	// 8 rows: 5 seed + cdc-1 + post-cancel-1 + post-cancel-2.
	if !waitForRowCountMySQL(t, targetDSN, "retained", 8, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("after gtid_mode purge fall-through, dst was not re-seeded by cold-start (got %d rows)",
			pollRowCountMySQL(targetDSN, "retained"))
	}
	applyDDLMySQL(t, sourceDSN, "INSERT INTO retained (payload) VALUES ('cdc-after-fallthrough')")
	if !waitForRowCountMySQL(t, targetDSN, "retained", 9, 30*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("CDC did not advance after fall-through cold-start")
	}

	persistedAfter := readPersistedPositionMySQL(t, targetDSN, streamID)
	if persistedAfter == "" || persistedAfter == persistedBefore {
		resumeCancel()
		<-resumeErr
		t.Fatalf("persisted position not refreshed: before=%q after=%q", persistedBefore, persistedAfter)
	}

	srcPayloads := selectAllPayloadsMySQL(t, sourceDSN, "retained")
	dstPayloads := selectAllPayloadsMySQL(t, targetDSN, "retained")
	if !equalStringSlicesMySQL(srcPayloads, dstPayloads) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("post-recovery src != dst (gap or dup): src=%v dst=%v", srcPayloads, dstPayloads)
	}
	t.Logf("PHASE-B (gtid_mode=ON purge): recovery verified — src == dst (%d rows), exactly-once held; "+
		"file/pos fall-through unaffected by gtid_mode=ON", len(srcPayloads))

	resumeCancel()
	select {
	case err := <-resumeErr:
		if err != nil {
			t.Errorf("resume Streamer.Run returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("resume Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_MySQL_FreshInstanceNodeReplaceFallsThroughToColdStart
// reproduces the PlanetScale "node replaced / restored from backup"
// mechanism. Binlogs are instance-local; when PS replaces the
// underlying node the new instance carries the same logical data but
// NO binlog history covering the consumer's persisted position. A
// resume against it must detect the position is unreachable and fall
// through to ADR-0022 cold-start rather than silently skipping the
// delta.
//
// Local analog: two independent MySQL containers. Source A is where
// the stream cold-starts and persists a position. Source B is a
// FRESH instance (separate binlog lineage) seeded with the same
// logical rows PLUS an extra row that exists ONLY on B. The resume
// is pointed at B with the persisted-from-A position. A's binlog
// file names do not exist on B, so the position is unreachable →
// loud ir.ErrPositionInvalid → cold-start re-snapshots B IN FULL
// (picking up B's extra row — proving the WHOLE table was
// re-snapshotted, not a silent partial / delta-replay).
func TestStreamer_MySQL_FreshInstanceNodeReplaceFallsThroughToColdStart(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	// Capture DEBUG slog so the resume-validation decision (node-
	// replace identity check) is observable as ground truth, not
	// inferred from row counts alone. Same lockedBuffer + JSON-handler
	// pattern the ADR-0036 diagnose test uses.
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(prevDefault)

	srcA, tgtA, cleanupA := startMySQLBinlog(t)
	defer cleanupA()

	const seedDDL = `
		CREATE TABLE noderepl (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO noderepl (payload) VALUES
			('shared-1'), ('shared-2'), ('shared-3');
	`
	applyDDLMySQL(t, srcA, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "mysql-node-replace"

	// ---- Phase 1: cold-start against A, persist a real position. ----
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcA,
		TargetDSN: tgtA,
		StreamID:  streamID,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, tgtA, "noderepl", 3, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows on instance A")
	}
	applyDDLMySQL(t, srcA, "INSERT INTO noderepl (payload) VALUES ('a-cdc-1')")
	if !waitForRowCountMySQL(t, tgtA, "noderepl", 4, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC did not advance on instance A")
	}
	persistedFromA := readPersistedPositionMySQL(t, tgtA, streamID)
	if persistedFromA == "" {
		streamCancel()
		<-runErr
		t.Fatal("persisted position empty after CDC on A")
	}
	t.Logf("persisted position from instance A = %q", persistedFromA)

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Streamer.Run (A) returned err on cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run (A) did not return after cancel")
	}

	// ---- Phase 2: stand up a FRESH instance B (independent binlog
	// lineage). Same shared rows + an extra B-only row. ----
	srcB, tgtB, cleanupB := startMySQLBinlog(t)
	defer cleanupB()

	const seedB = `
		CREATE TABLE noderepl (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO noderepl (payload) VALUES
			('shared-1'), ('shared-2'), ('shared-3'), ('a-cdc-1'), ('b-only-row');
	`
	applyDDLMySQL(t, srcB, seedB)

	// Carry A's persisted position into B's control table so the
	// resume against B starts from a position B's binlog lineage does
	// not contain (the same sluice_cdc_state row surviving the node
	// swap, simulated).
	seedCDCStateMySQL(t, tgtB, streamID, persistedFromA)

	// ---- Phase 3: resume against B with A's position.
	// verifyPositionResumable finds A's binlog filename absent on B →
	// loud ir.ErrPositionInvalid → ADR-0022 cold-start re-snapshots
	// B IN FULL. ----
	resumeStreamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcB,
		TargetDSN: tgtB,
		StreamID:  streamID,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resumeStreamer.Run(resumeCtx) }()

	// All FIVE of B's rows must land — including b-only-row, which
	// proves the whole table was re-snapshotted from B.
	if !waitForRowCountMySQL(t, tgtB, "noderepl", 5, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("after node-replace fall-through, B's dst was not fully re-seeded (got %d rows; want 5)",
			pollRowCountMySQL(tgtB, "noderepl"))
	}

	srcBPayloads := selectAllPayloadsMySQL(t, srcB, "noderepl")
	dstBPayloads := selectAllPayloadsMySQL(t, tgtB, "noderepl")
	if !equalStringSlicesMySQL(srcBPayloads, dstBPayloads) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("post-recovery src(B) != dst(B): src=%v dst=%v", srcBPayloads, dstBPayloads)
	}
	// Loud-failure oracle: the recovery must have gone through the
	// ADR-0022 cold-start fall-through, which means the resume-
	// validation step must have LOUDLY refused the unreachable
	// position. The streamer logs this as the WARN "warm resume:
	// persisted position is no longer valid; falling through to cold
	// start". Asserting on it (not just the row count) is what
	// distinguishes "loud detection → re-snapshot" from a lucky
	// re-snapshot that happened for an unrelated reason. The full
	// re-seed of b-only-row already proves data correctness; this
	// proves the *mechanism* was the loud one.
	logs := string(logBuf.Bytes())
	if !strings.Contains(logs, "falling through to cold start") {
		t.Fatalf("PHASE-B (node-replace): data ended correct BUT the loud fall-through WARN "+
			"never fired — the persisted position was treated as resumable against a fresh "+
			"instance with an unrelated binlog lineage (silent-gap class). This is the "+
			"loud-failure-tenet violation the hardening must close. Captured logs:\n%s", logs)
	}
	t.Logf("PHASE-B (node-replace): recovery verified — loud fall-through fired AND fresh "+
		"instance B fully re-snapshotted (src == dst, %d rows incl. b-only-row); no silent "+
		"partial / delta-replay", len(srcBPayloads))

	resumeCancel()
	select {
	case err := <-resumeErr:
		// Tolerate context.Canceled: the streamer can be cancelled mid-
		// startup (e.g., in `SHOW BINARY LOGS` while reaching steady-
		// state CDC) and surface that as a wrapped context.Canceled
		// rather than a clean nil. The assertion's intent is "exits
		// cleanly on cancel"; both nil and context.Canceled satisfy it.
		// Surfaced after CI 26135099781: the captureSlog race-fix
		// removed a slowdown that was reliably putting the streamer
		// past start-CDC by the time resumeCancel fired.
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("resume Streamer.Run (B) returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("resume Streamer.Run (B) did not return after ctx cancel")
	}
}

// seedCDCStateMySQL writes a position token into the target's
// sluice_cdc_state control table for the given stream id, creating
// the row if absent. Simulates the persisted CDC state surviving a
// source-side node swap (the position came from the old instance;
// the new instance can't satisfy it). The DDL mirrors the engine's
// ensureControlTable shape so the streamer's own idempotent ensure
// call is a clean no-op.
func seedCDCStateMySQL(t *testing.T, dsn, streamID, token string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const ddl = `
		CREATE TABLE IF NOT EXISTS sluice_cdc_state (
			stream_id              VARCHAR(255) NOT NULL,
			source_position        TEXT         NOT NULL,
			updated_at             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
				ON UPDATE CURRENT_TIMESTAMP,
			stop_requested_at      TIMESTAMP    NULL,
			slot_name              VARCHAR(255) NULL,
			source_dsn_fingerprint VARCHAR(255) NULL,
			target_schema          VARCHAR(255) NULL,
			PRIMARY KEY (stream_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create sluice_cdc_state: %v", err)
	}
	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO sluice_cdc_state (stream_id, source_position) VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE source_position = VALUES(source_position)`,
		streamID, token,
	); err != nil {
		t.Fatalf("seed sluice_cdc_state row: %v", err)
	}
}

// selectAllPayloadsMySQL is the payload-column analog of
// selectAllEmailsMySQL (the helper in
// streamer_resume_mysql_integration_test.go reads `email`; these
// chaos tables use `payload`). Returns the sorted payload list for
// the src == dst exactly-once oracle.
func selectAllPayloadsMySQL(t *testing.T, dsn, table string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT payload FROM %s ORDER BY payload", table))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}
