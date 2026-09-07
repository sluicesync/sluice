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
// PHASE-A GROUND-TRUTH, as it stood when this file was written: the
// MySQL snapshot→CDC handoff anchored the persisted position in
// file/pos mode EVEN when the source ran gtid_mode=ON, so the
// streamer's resume validation was verifyBinlogFilePresent in both
// deployments. That was audit 2026-09-01's SLM-4 finding, fixed in
// cdc_snapshot_position.go: the handoff now anchors a GTID set on a
// gtid_mode=ON source, so the gtid_mode=ON cell below exercises the
// GTID-set branch (verifyGTIDSetReachable) through the streamer, and
// streamer_mysql_gtid_handoff_integration_test.go pins the anchor's
// mode itself.
//
// This file covers the two PS-realistic streamer-level cases the
// existing purge test does NOT:
//
//   - gtid_mode=ON + binlog purged past resume: confirms the
//     position-invalid fall-through still fires when the source runs
//     GTID mode (the PS topology — PS/Vitess sources are GTID-mode).
//   - Fresh-instance / node-replace: resume against a DIFFERENT
//     MySQL instance carrying the same data but NO binlog history
//     covering the persisted position (the PS "node replaced /
//     restored from backup" mechanism — binlogs genuinely don't
//     carry over).
//
// THE TWO CASES NO LONGER SHARE AN ORACLE (audit SLM-6, v0.146.0), and
// the split is the point:
//
//   - PURGE: the same server advanced past the position. Loud
//     ir.ErrPositionInvalid → ADR-0022 cold-start re-snapshot executes
//     end-to-end → data correct after (no gap, no dup, src == dst).
//     Unchanged.
//   - NODE REPLACE: a DIFFERENT server is answering. This now refuses
//     TERMINALLY and does NOT re-snapshot, because re-copying means
//     dropping the target's tables and repopulating them from an
//     instance that never produced the position — right for a purge,
//     destructive when sluice cannot tell an intended replacement from
//     a stale connection string. Its oracle is the refusal plus the
//     target being UNTOUCHED; the row-count-equality oracle that used
//     to apply here was asserting the destructive behaviour.
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
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLGTID boots a single MySQL container with binlog AND GTID
// mode enabled. Sibling of startMySQLBinlog (file/pos-mode); the only
// difference is --gtid-mode=ON --enforce-gtid-consistency=ON so the
// source matches the PS/Vitess topology (GTID-mode). The streamer's
// snapshot handoff persists a GTID-set position here (SLM-4; it used to
// persist file/pos regardless), so a resume against this source runs
// the GTID arm of the position check.
func startMySQLGTID(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()

	container := runMySQLWithRetry(
		t,
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
// confirms the PS-realistic property: when the source runs in GTID
// mode (the PS/Vitess topology), the streamer's persisted position is
// a GTID set (SLM-4), and a binlog-retention-exceeded resume —
// gtid_purged advanced past the set — is detected loudly
// (verifyGTIDSetReachable) → ADR-0022 cold-start → data correct after
// (no gap, no dup).
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
		"position-invalid fall-through fires on the GTID arm", len(srcPayloads))

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

// TestStreamer_MySQL_FreshInstanceNodeReplaceRefusesTerminally
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
func TestStreamer_MySQL_FreshInstanceNodeReplaceRefusesTerminally(t *testing.T) {
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

	// ---- Phase 4: the resume must REFUSE, terminally, and must NOT
	// have touched B's target. ----
	//
	// THIS ASSERTION IS INVERTED FROM WHAT IT WAS (audit SLM-6, v0.146.0),
	// and the inversion is the point of the change. It used to require the
	// full re-snapshot of B — all five rows including b-only-row — as proof
	// that the ADR-0022 fall-through had run. That fall-through is exactly
	// the destructive behaviour: it DROPS the target's tables and re-copies
	// them from whichever instance is now answering the DSN. Right for a
	// routine purge, where the same server has advanced past the position;
	// wrong here, where B is a DIFFERENT SERVER that sluice cannot
	// distinguish from a stale connection string or a load-balanced endpoint
	// that landed elsewhere.
	//
	// The original concern this test was written for — silently skipping the
	// delta — is still covered, and more conservatively: nothing is skipped
	// because nothing proceeds.
	var resumeFinalErr error
	select {
	case resumeFinalErr = <-resumeErr:
	case <-time.After(90 * time.Second):
		resumeCancel()
		<-resumeErr
		t.Fatal("PHASE-B (node-replace): the resume against a FRESH instance neither refused nor " +
			"returned; it must not sit there having silently accepted a foreign lineage")
	}
	if resumeFinalErr == nil {
		t.Fatalf("PHASE-B (node-replace): the resume against instance B SUCCEEDED. Either it streamed "+
			"from a byte offset in an unrelated binlog lineage (silent-gap class), or it re-copied "+
			"this target from a server that never produced its position. dst now holds %d rows.",
			pollRowCountMySQL(tgtB, "noderepl"))
	}
	for _, want := range []string{"server_uuid", "REFUSING", "--restart-from-scratch"} {
		if !strings.Contains(resumeFinalErr.Error(), want) {
			t.Errorf("PHASE-B: the refusal does not name %q — an operator mid-incident has to be able "+
				"to tell which instance answered and what to do:\n%v", want, resumeFinalErr)
		}
	}

	// THE LOAD-BEARING HALF: B's target must be UNTOUCHED. This is what
	// distinguishes the fix from the old behaviour, and no error-message
	// assertion can stand in for it — the old path also logged loudly, then
	// dropped the tables anyway.
	//
	// b-only-row is the witness: it exists ONLY on the instance sluice was
	// never authorized to copy from. If the destructive fall-through had
	// run, B's target would hold all five of B's rows including it.
	//
	// In this rig B's target starts EMPTY — only the sluice_cdc_state row
	// was carried across to simulate the swap — so "untouched" shows up as
	// the table not existing at all, which is the strongest form of the
	// evidence and is exactly what the first run of this rewrite reported
	// ("Table 'target_db.noderepl' doesn't exist"). Both shapes are accepted
	// as proof; what is refused is the row being there.
	if tableExistsMySQL(t, tgtB, "noderepl") {
		dstBPayloads := selectAllPayloadsMySQL(t, tgtB, "noderepl")
		for _, p := range dstBPayloads {
			if p == "b-only-row" {
				t.Fatalf("PHASE-B (node-replace): the target was RE-COPIED from instance B — b-only-row "+
					"exists only there, and it is now in a target sluice refused to resume against. "+
					"That is the destructive auto-resnapshot this refusal exists to prevent; on a "+
					"misconfigured DSN it is an operator's target repopulated from the wrong "+
					"database. dst=%v", dstBPayloads)
			}
		}
		t.Logf("PHASE-B: target table present but carries no b-only-row (%d rows) — not re-copied",
			len(dstBPayloads))
	} else {
		t.Log("PHASE-B: target table does not exist on B — the refusal fired before any table was " +
			"created, so nothing was copied from an unverified instance")
	}

	logs := string(logBuf.Bytes())
	if !strings.Contains(logs, "SOURCE-INSTANCE-IDENTITY-CHANGED") {
		t.Errorf("PHASE-B (node-replace): the grep-stable marker never fired, so an operator scrolling "+
			"their logs has nothing to search for. Captured logs:\n%s", logs)
	}
	if strings.Contains(logs, "falling through to cold start") {
		t.Errorf("PHASE-B (node-replace): the ADR-0022 fall-through ran. The identity refusal must be " +
			"terminal — routing it into the fall-through is what dropped the target and re-copied " +
			"from an unverified instance.")
	}
	t.Log("PHASE-B (node-replace): verified — terminal refusal, target untouched, marker fired")

	// The resume already returned its terminal error above — that is the
	// whole point of the change, so there is nothing left to wait for.
	// Cancelling is cleanup only; the channel is already drained.
	//
	// The block that used to stand here waited on resumeErr and tolerated
	// context.Canceled, because under the old contract the streamer kept
	// RUNNING after the fall-through re-snapshot and had to be cancelled
	// out of steady-state CDC. A terminal refusal never reaches steady
	// state.
	resumeCancel()
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
// tableExistsMySQL reports whether table exists in the DSN's database.
//
// It exists so the node-replace pin can tell "the target was never touched"
// (the table was never created, because the refusal fired first) apart from
// "the target was re-copied". Querying rows to prove absence fails with
// "Table doesn't exist", which is the right ANSWER arriving as the wrong
// SHAPE — the assertion has to be able to read it as evidence.
func tableExistsMySQL(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table,
	).Scan(&n); err != nil {
		t.Fatalf("table-exists probe: %v", err)
	}
	return n > 0
}

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
