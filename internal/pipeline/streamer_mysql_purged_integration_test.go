//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for MySQL binlog-purged fall-through to cold-start
// (ADR-0022 extended to MySQL).
//
// Pre-fix shape: when the source server's binlog file referenced by
// the persisted position had been rotated and purged (typical when
// expire_logs_seconds rolled it off, or an operator ran PURGE BINARY
// LOGS), sluice's MySQL CDC reader would surface the syncer's raw
// "Could not find first log file name in binary log index file"
// error from go-mysql with no recovery path.
//
// Post-fix: the reader's resolveStartPosition pre-flights the binlog
// file via SHOW BINARY LOGS. If absent, it returns
// fmt.Errorf("...: %w", ir.ErrPositionInvalid). The streamer's
// existing fall-through (added in v0.5.2 for PG slot-missing) detects
// this engine-neutrally and re-enters coldStart.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart
// exercises the recovery path for MySQL file/pos mode:
//
//  1. Cold-start sluice on MySQL → MySQL; let bulk-copy + a CDC change
//     land so sluice_cdc_state has a real persisted position.
//  2. Cancel the streamer.
//  3. FLUSH BINARY LOGS to rotate to a fresh binlog file.
//  4. PURGE BINARY LOGS to delete everything before the active file —
//     this purges the file the persisted position references.
//  5. Drop dest tables (Bug 9 pre-flight gate).
//  6. Re-run sync start.
//  7. Assert: cold-start runs, dest is reseeded, fresh position is
//     written.
//
// Pre-fix, step 6 errors out with "Could not find first log file
// name." Post-fix, the WARN fires and cold-start completes.
func TestStreamer_MySQLToMySQL_BinlogPurgedFallsThroughToColdStart(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE purged (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO purged (payload) VALUES
			('seed-1'), ('seed-2'), ('seed-3'), ('seed-4'), ('seed-5');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "mysql-binlog-purged"

	// ---- Phase 1: cold-start, drive a CDC change so the persisted
	// position is non-empty. ----
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

	if !waitForRowCountMySQL(t, targetDSN, "purged", 5, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}

	applyDDLMySQL(t, sourceDSN, "INSERT INTO purged (payload) VALUES ('cdc-1')")
	if !waitForRowCountMySQL(t, targetDSN, "purged", 6, 30*time.Second) {
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

	// ---- Phase 2: cancel the streamer. ----
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Streamer.Run returned err on cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}

	// ---- Phase 3: rotate + purge so the file referenced by the
	// persisted position is gone. ----
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	// Drive another rotation so PURGE BEFORE has more than one
	// candidate to remove. Without this, the "active" binlog right
	// after FLUSH is the only one MySQL refuses to purge, and the
	// purge would no-op on the file we want gone.
	applyDDLMySQL(t, sourceDSN, "INSERT INTO purged (payload) VALUES ('post-flush')")
	applyDDLMySQL(t, sourceDSN, "FLUSH BINARY LOGS")
	// Purge everything except the most recent file — that includes
	// the file referenced by the persisted position.
	purgeAllButLatestBinlog(t, sourceDSN)

	// ---- Phase 4: drop dest tables (Bug 9 pre-flight gate). ----
	applyDDLMySQL(t, targetDSN, "DROP TABLE purged")

	// ---- Phase 5: re-run sync start; pre-flight should detect the
	// missing binlog file, the streamer should fall through to
	// coldStart. ----
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

	// ---- Phase 6: assert cold-start ran. The dropped table is
	// recreated and seeded with all 7 rows from the source (5 seeds
	// + cdc-1 + post-flush). ----
	if !waitForRowCountMySQL(t, targetDSN, "purged", 7, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("after binlog-purged fall-through, dst was not re-seeded by cold-start (got %d rows)",
			pollRowCountMySQL(targetDSN, "purged"))
	}

	// Drive one more CDC change so the resumed streamer writes a
	// fresh position.
	applyDDLMySQL(t, sourceDSN, "INSERT INTO purged (payload) VALUES ('cdc-after-fallthrough')")
	if !waitForRowCountMySQL(t, targetDSN, "purged", 8, 30*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("CDC did not advance after fall-through cold-start")
	}

	persistedAfter := readPersistedPositionMySQL(t, targetDSN, streamID)
	if persistedAfter == "" {
		resumeCancel()
		<-resumeErr
		t.Fatal("persisted position is empty after fall-through cold-start")
	}
	if persistedAfter == persistedBefore {
		resumeCancel()
		<-resumeErr
		t.Fatalf("persisted position was not refreshed by fall-through cold-start: before=%q after=%q",
			persistedBefore, persistedAfter)
	}

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

// purgeAllButLatestBinlog runs PURGE BINARY LOGS TO <last>, where
// <last> is the most recent file in SHOW BINARY LOGS. MySQL's PURGE
// keeps the named file and any after it, removing all earlier ones.
// We need this two-step (find latest, then PURGE TO it) instead of
// PURGE BEFORE NOW() because BEFORE depends on file timestamps the
// filesystem may not have advanced inside a fresh container.
func purgeAllButLatestBinlog(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		t.Fatalf("SHOW BINARY LOGS columns: %v", err)
	}
	holders := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}

	var latest string
	for rows.Next() {
		if err := rows.Scan(holders...); err != nil {
			_ = rows.Close()
			t.Fatalf("scan: %v", err)
		}
		switch v := dest[0].(type) {
		case string:
			latest = v
		case []byte:
			latest = string(v)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()

	if latest == "" {
		t.Fatal("SHOW BINARY LOGS returned no rows")
	}

	if _, err := db.ExecContext(ctx, "PURGE BINARY LOGS TO '"+latest+"'"); err != nil {
		t.Fatalf("PURGE BINARY LOGS TO %q: %v", latest, err)
	}
}
